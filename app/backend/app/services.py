from typing import Dict, List
from sqlalchemy.orm import Session
from app.integrations.lingxing_client import (
    get_fbm_order_list,
    get_fbm_order_detail,
    get_access_token,
    get_shop_list,
    get_all_orders_report,
    update_fbm_order,
    get_mws_orders,
    get_mws_order_detail,
    get_listing_search,
    get_shop_list,
)
from app.transform import map_order_detail, map_order_items, map_order_packages, map_order_ext
from app import crud
from app import models
from app.address_mapping import default_address_mapping
from app.config_store import get_lingxing_config
from app.db import SessionLocal
from datetime import datetime, timedelta
import time


def _iter_time_windows(start: datetime, end: datetime, days: int):
    cur = start
    while cur <= end:
        nxt = min(end, cur + timedelta(days=days) - timedelta(seconds=1))
        yield cur, nxt
        cur = nxt + timedelta(seconds=1)


def _parse_dt(val: str | None):
    if not val:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(val, fmt)
        except Exception:
            continue
    return None


def _apply_listing_images(db: Session, access_token: str, app_id: str, sid: str | int, search_field: str, values: list[str], id_map: dict[str, list[int]]):
    if not values:
        return 0
    payload = {
        "sid": sid,
        "is_pair": 1,
        "is_delete": 0,
        "search_field": search_field,
        "search_value": values,
        "exact_search": 1,
        "store_type": 1,
        "offset": 0,
        "length": min(len(values), 100),
    }
    res = get_listing_search(access_token, app_id, payload)
    if res.get("code") != 0:
        return 0
    updated = 0
    for row in res.get("data", []) or []:
        key = None
        if search_field == "asin":
            key = row.get("asin")
        elif search_field in ("seller_sku", "sku", "msku", "local_sku"):
            key = row.get("seller_sku") or row.get("local_sku") or row.get("msku")
        if not key:
            continue
        img = row.get("small_image_url")
        if not img:
            continue
        for oid in id_map.get(key, []):
            crud.upsert_order_ext(db, oid, "product_image", img)
            items = crud.get_order_items(db, oid)
            if items:
                items[0].product_image = img
                db.commit()
            updated += 1
    return updated


def backfill_missing_fields(
    db: Session,
    access_token: str,
    app_id: str,
    sid_list_value: str | None,
    limit: int = 500,
):
    orders = (
        db.query(models.InternalOrder)
        .order_by(models.InternalOrder.id.desc())
        .limit(limit)
        .all()
    )
    platform_ids = []
    for o in orders:
        if not o.platform_order_no:
            continue
        ext = crud.get_order_ext(db, o.id)
        fields = ext.fields if ext else {}
        if not fields.get("asin") or not fields.get("product_image") or not fields.get("sku"):
            platform_ids.append(o.platform_order_no)

    # Step 1: fetch order detail to fill asin/sku/image
    for i in range(0, len(platform_ids), 200):
        batch = platform_ids[i:i + 200]
        detail = get_mws_order_detail(access_token, app_id, batch)
        if detail.get("code") != 0:
            continue
        for d in detail.get("data", []) or []:
            amazon_order_id = d.get("amazon_order_id")
            order = crud.get_order_by_platform_no(db, amazon_order_id)
            if not order:
                continue
            item_list = d.get("item_list") or []
            if not item_list:
                continue
            it = item_list[0]
            ext_update = {}
            if it.get("asin"):
                ext_update["asin"] = it.get("asin")
            sku_val = it.get("sku") or it.get("seller_sku")
            if sku_val:
                ext_update["sku"] = sku_val
            if it.get("product_name") or it.get("title"):
                ext_update["product_name"] = it.get("product_name") or it.get("title")
            if d.get("latest_ship_date"):
                ext_update["latest_ship_date"] = d.get("latest_ship_date")
                ext_update["amz_ship"] = d.get("latest_ship_date")
            if d.get("earliest_delivery_date"):
                ext_update["earliest_delivery_date"] = d.get("earliest_delivery_date")
            if d.get("latest_delivery_date"):
                ext_update["latest_delivery_date"] = d.get("latest_delivery_date")
                ext_update["amz_deliver"] = f"{d.get('earliest_delivery_date','')} - {d.get('latest_delivery_date','')}".strip(" -")
            img = it.get("pic_url")
            if img and img != "/":
                ext_update["product_image"] = img
            if ext_update:
                crud.upsert_order_ext_bulk(db, order.id, ext_update)
            items = crud.get_order_items(db, order.id)
            if not items:
                crud.create_internal_order_item(db, order.id, {
                    "sku": it.get("sku") or it.get("seller_sku"),
                    "product_name": it.get("product_name") or it.get("title"),
                    "quantity": it.get("quantity_ordered"),
                    "unit_price": it.get("unit_price_amount"),
                    "currency": it.get("currency"),
                    "product_image": it.get("pic_url"),
                    "attachments": None,
                })
            else:
                if items[0].sku in (None, "", " "):
                    items[0].sku = it.get("sku") or it.get("seller_sku")
                if items[0].product_name in (None, "", " "):
                    items[0].product_name = it.get("product_name") or it.get("title")
                if items[0].quantity in (None, 0):
                    items[0].quantity = it.get("quantity_ordered")
                if img and img != "/" and items[0].product_image in (None, "", "/"):
                    items[0].product_image = img
                db.commit()

    # Step 2: fill missing images by asin/sku via listing search
    asin_map = {}
    sku_map = {}
    for o in orders:
        ext = crud.get_order_ext(db, o.id)
        if not ext or not ext.fields:
            continue
        fields = ext.fields
        if not fields.get("product_image"):
            if fields.get("asin"):
                asin_map.setdefault(fields.get("asin"), []).append(o.id)
            elif fields.get("sku"):
                sku_map.setdefault(fields.get("sku"), []).append(o.id)

    sid = sid_list_value or ""
    if not sid or str(sid).upper() == "ALL":
        try:
            shops = get_shop_list(access_token, app_id)
            if shops.get("code") == 0:
                sid = ",".join([str(s.get("sid")) for s in shops.get("data", []) if s.get("sid") is not None])
        except Exception:
            pass
    try:
        _apply_listing_images(db, access_token, app_id, sid, "asin", list(asin_map.keys()), asin_map)
        _apply_listing_images(db, access_token, app_id, sid, "seller_sku", list(sku_map.keys()), sku_map)
        _apply_listing_images(db, access_token, app_id, sid, "msku", list(sku_map.keys()), sku_map)
        _apply_listing_images(db, access_token, app_id, sid, "local_sku", list(sku_map.keys()), sku_map)
    except Exception:
        pass


def run_mws_orders_job(
    db: Session,
    job_id: int,
    app_id: str,
    access_token: str,
    sid_list: List[str],
    start_time: datetime | None,
    end_time: datetime | None,
    date_types: List[int] | None = None,
) -> Dict:
    imported = 0
    failed = 0
    errors = []
    total = 0
    processed = 0

    if not start_time or not end_time:
        crud.update_import_job(db, job_id, 0, 1, "failed", error_summary="missing start/end date for mws/orders")
        return {"job_id": job_id, "imported": 0, "failed": 1}

    if not date_types:
        date_types = [2, 1]

    for sid in sid_list:
        crud.add_import_log(db, job_id, "info", f"mws/orders sid={sid} date_types={date_types}")
        for dt in date_types:
            offset = 0
            length = 1000
            while True:
                res = get_mws_orders(
                    access_token,
                    app_id,
                    int(sid),
                    start_time.strftime("%Y-%m-%d"),
                    end_time.strftime("%Y-%m-%d"),
                    date_type=int(dt),
                    offset=offset,
                    length=length,
                )
                if res.get("code") != 0:
                    failed += 1
                    errors.append(res)
                    crud.add_import_log(db, job_id, "error", f"mws/orders error sid={sid} dt={dt} {res}")
                    break

                data = res.get("data", []) or []
                crud.add_import_log(db, job_id, "info", f"mws/orders sid={sid} dt={dt} offset={offset} size={len(data)}")
                total += len(data)
                asin_map = {}
                sku_map = {}
                for row in data:
                    amazon_order_id = row.get("amazon_order_id")
                    if not amazon_order_id:
                        continue
                    order = crud.get_order_by_platform_no(db, amazon_order_id)
                    if not order:
                        mapped = {
                            "internal_order_no": f"IO{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
                            "platform_order_no": amazon_order_id,
                            "shop_name": row.get("seller_name"),
                            "order_status": row.get("order_status"),
                            "purchase_time": _parse_dt(row.get("purchase_date_local")),
                            "tracking_no": row.get("tracking_number"),
                        }
                        order = crud.create_internal_order(db, mapped)
                    ext = {
                        "postal_code": row.get("postal_code"),
                        "sales_channel": row.get("sales_channel"),
                        "order_total_amount": row.get("order_total_amount"),
                        "order_total_currency_code": row.get("order_total_currency_code"),
                        "tracking_number": row.get("tracking_number"),
                        "purchase_date_local": row.get("purchase_date_local"),
                        "shipment_date": row.get("shipment_date_local") or row.get("shipment_date"),
                        "latest_ship_date": row.get("latest_ship_date"),
                        "earliest_delivery_date": row.get("earliest_delivery_date"),
                        "latest_delivery_date": row.get("latest_delivery_date"),
                        "amz_ship": row.get("latest_ship_date"),
                        "amz_deliver": f"{row.get('earliest_delivery_date','')} - {row.get('latest_delivery_date','')}".strip(" -"),
                    }
                    item_list = row.get("item_list") or []
                    if item_list:
                        item0 = item_list[0]
                        ext.update({
                            "sku": item0.get("local_sku") or item0.get("seller_sku"),
                            "product_name": item0.get("local_name") or item0.get("product_name"),
                            "purchase_qty": item0.get("quantity_ordered"),
                            "asin": item0.get("asin"),
                        })
                        if item0.get("asin"):
                            asin_map.setdefault(item0.get("asin"), []).append(order.id)
                        sku_key = item0.get("seller_sku") or item0.get("local_sku")
                        if sku_key:
                            sku_map.setdefault(sku_key, []).append(order.id)
                    crud.upsert_order_ext_bulk(db, order.id, ext)
                    processed += 1
                    crud.upsert_import_progress(db, job_id, total, processed, imported, failed)

                order_ids = [r.get("amazon_order_id") for r in data if r.get("amazon_order_id")]
                if order_ids:
                    try:
                        detail = get_mws_order_detail(access_token, app_id, order_ids[:200])
                        if detail.get("code") != 0:
                            crud.add_import_log(db, job_id, "error", f"mws/orderDetail error={detail}")
                        else:
                            for d in detail.get("data", []) or []:
                                amazon_order_id = d.get("amazon_order_id")
                                order = crud.get_order_by_platform_no(db, amazon_order_id)
                                if not order:
                                    continue
                                items = crud.get_order_items(db, order.id)
                                ext_update = {}
                                for it in d.get("item_list", []) or []:
                                    if it.get("asin"):
                                        ext_update["asin"] = it.get("asin")
                                    sku_val = it.get("sku") or it.get("seller_sku")
                                    if sku_val:
                                        ext_update["sku"] = sku_val
                                    if it.get("product_name") or it.get("title"):
                                        ext_update["product_name"] = it.get("product_name") or it.get("title")
                                    if not items:
                                        crud.create_internal_order_item(db, order.id, {
                                            "sku": it.get("sku") or it.get("seller_sku"),
                                            "product_name": it.get("product_name") or it.get("title"),
                                            "quantity": it.get("quantity_ordered"),
                                            "unit_price": it.get("unit_price_amount"),
                                            "currency": it.get("currency"),
                                            "product_image": it.get("pic_url"),
                                            "attachments": None,
                                        })
                                    else:
                                        if items[0].sku in (None, "", " "):
                                            items[0].sku = it.get("sku") or it.get("seller_sku")
                                        if items[0].product_name in (None, "", " "):
                                            items[0].product_name = it.get("product_name") or it.get("title")
                                        if items[0].quantity in (None, 0):
                                            items[0].quantity = it.get("quantity_ordered")
                                        if items[0].product_image in (None, "", "/"):
                                            items[0].product_image = it.get("pic_url")
                                        db.commit()
                                first_item = (d.get("item_list") or [])[:1]
                                if first_item:
                                    img = first_item[0].get("pic_url")
                                    if img:
                                        crud.upsert_order_ext(db, order.id, "product_image", img)
                                if ext_update:
                                    crud.upsert_order_ext_bulk(db, order.id, ext_update)
                    except Exception as exc:
                        crud.add_import_log(db, job_id, "error", f"mws/orderDetail exception={exc}")

                try:
                    _apply_listing_images(db, access_token, app_id, sid, "asin", list(asin_map.keys()), asin_map)
                    _apply_listing_images(db, access_token, app_id, sid, "seller_sku", list(sku_map.keys()), sku_map)
                except Exception as exc:
                    crud.add_import_log(db, job_id, "error", f"listing image exception={exc}")

                if len(data) < length:
                    break
                offset += length

        # Enrich detail in batch for this sid
        try:
            order_ids = []
            # query latest orders by sid from ext where platform_order_no not null
            # reuse 'data' list from last fetch for smaller batch
            # (best-effort, no strict requirement)
        except Exception:
            pass

    crud.update_import_job(db, job_id, processed, failed, "done", error_summary=str(errors[:3]) if errors else None)
    return {"job_id": job_id, "imported": processed, "failed": failed, "errors": errors}


def run_sync_job(
    db: Session,
    job_id: int,
    app_id: str,
    access_token: str,
    sid_list: List[str],
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    chunk_days: int = 7,
) -> Dict:
    imported = 0
    failed = 0
    errors = []
    total = 0
    processed = 0

    for sid in sid_list:
        crud.add_import_log(db, job_id, "info", f"start sid={sid}")
        windows = [(None, None)]
        if start_time and end_time:
            windows = list(_iter_time_windows(start_time, end_time, chunk_days))

        for wstart, wend in windows:
            if wstart and wend:
                crud.add_import_log(
                    db,
                    job_id,
                    "info",
                    f"window {wstart.strftime('%Y-%m-%d')} -> {wend.strftime('%Y-%m-%d')}",
                )
            page = 1
            while True:
                try:
                    res = get_fbm_order_list(
                        access_token,
                        app_id,
                        sid,
                        page=page,
                        start_time=wstart.strftime("%Y-%m-%d %H:%M:%S") if wstart else None,
                        end_time=wend.strftime("%Y-%m-%d %H:%M:%S") if wend else None,
                    )
                except Exception as exc:
                    failed += 1
                    errors.append(str(exc))
                    crud.add_import_log(db, job_id, "error", f"list exception={exc}")
                    break
                crud.add_import_log(db, job_id, "debug", f"sid={sid} page={page} code={res.get('code')}")
                if res.get("code") == 3001008:
                    crud.add_import_log(db, job_id, "warn", "rate limit hit, sleep 2s and retry page")
                    time.sleep(2)
                    continue
                if res.get("code") != 0:
                    failed += 1
                    errors.append(res)
                    crud.add_import_log(db, job_id, "error", f"sid={sid} error={res}")
                    break
                orders = res.get("data", [])
                crud.add_import_log(db, job_id, "info", f"sid={sid} page={page} size={len(orders)}")
                if not orders:
                    crud.add_import_log(db, job_id, "info", f"sid={sid} page={page} empty")
                    break
                total += len(orders)
                for order in orders:
                    order_number = order.get("order_number")
                    if not order_number:
                        crud.add_import_log(db, job_id, "error", f"missing order_number in order={order}")
                        failed += 1
                        processed += 1
                        crud.upsert_import_progress(db, job_id, total, processed, imported, failed)
                        continue
                    crud.add_import_log(db, job_id, "debug", f"detail order_number={order_number}")
                    try:
                        detail = get_fbm_order_detail(access_token, app_id, order_number)
                        crud.add_import_log(db, job_id, "debug", f"detail code={detail.get('code')}")
                    except Exception as exc:
                        failed += 1
                        errors.append(str(exc))
                        crud.add_import_log(db, job_id, "error", f"detail exception={exc}")
                        processed += 1
                        crud.upsert_import_progress(db, job_id, total, processed, imported, failed)
                        continue
                    if detail.get("code") == 3001008:
                        crud.add_import_log(db, job_id, "warn", "rate limit hit on detail, sleep 2s and retry")
                        time.sleep(2)
                        continue
                    if detail.get("code") == 0:
                        mapping_cfg = crud.get_config(db, "address_mapping")
                        mapping = mapping_cfg.config_value if mapping_cfg else default_address_mapping()
                        mapped = map_order_detail(detail, address_mapping=mapping)
                        try:
                            crud.add_import_log(db, job_id, "debug", f"saving order_number={order_number}")
                            internal_order = crud.create_internal_order(db, mapped, commit=False)
                            # Save ext fields and lingxing system order number for later update
                            ext_fields = map_order_ext(detail, address_mapping=mapping)
                            ext_fields["lingxing_order_number"] = order_number
                            ext_fields["global_order_no"] = order_number
                            crud.upsert_order_ext_bulk(db, internal_order.id, ext_fields)
                            for item in map_order_items(detail):
                                crud.create_internal_order_item(db, internal_order.id, item, commit=False)
                            for pkg in map_order_packages(detail):
                                crud.create_internal_order_package(db, internal_order.id, pkg, commit=False)
                            db.commit()
                            imported += 1
                            crud.add_import_log(db, job_id, "info", f"saved order_number={order_number}")
                        except Exception as exc:
                            db.rollback()
                            failed += 1
                            errors.append(str(exc))
                            crud.add_import_log(db, job_id, "error", f"db error={exc}")
                    else:
                        failed += 1
                        errors.append(detail)
                        crud.add_import_log(db, job_id, "error", f"detail error={detail}")
                    processed += 1
                    crud.upsert_import_progress(db, job_id, total, processed, imported, failed)
                page += 1
            crud.add_import_log(db, job_id, "info", f"sid={sid} page_done={page-1}")
    crud.update_import_job(db, job_id, imported, failed, "done", error_summary=str(errors[:3]) if errors else None)
    return {"job_id": job_id, "imported": imported, "failed": failed, "errors": errors}


def execute_sync_job(job_id: int) -> Dict:
    db = SessionLocal()
    try:
        crud.add_import_log(db, job_id, "info", "job start")
        cfg = get_lingxing_config(db)
        app_id = cfg.get("app_id")
        app_secret = cfg.get("app_secret")
        if not app_id or not app_secret:
            crud.update_import_job(db, job_id, 0, 1, "failed", error_summary="missing app_id/app_secret")
            crud.add_import_log(db, job_id, "error", "missing app_id/app_secret")
            return {"job_id": job_id, "imported": 0, "failed": 1}

        token_resp = get_access_token(app_id, app_secret)
        if token_resp.get("code") not in (200, "200"):
            crud.update_import_job(db, job_id, 0, 1, "failed", error_summary=str(token_resp))
            crud.add_import_log(db, job_id, "error", f"token error={token_resp}")
            return {"job_id": job_id, "imported": 0, "failed": 1}
        access_token = token_resp.get("data", {}).get("access_token")
        cfg["access_token"] = access_token
        crud.set_config(db, "lingxing", cfg)
        crud.add_import_log(db, job_id, "info", "token ok")

        sid_cfg = str(cfg.get("sid_list", "")).strip()
        crud.add_import_log(db, job_id, "info", f"sid_list_cfg={sid_cfg or 'EMPTY'}")
        sid_list = [s for s in sid_cfg.split(",") if s]
        if not sid_list or sid_cfg.upper() == "ALL":
            crud.add_import_log(db, job_id, "info", "fetch shop list")
            try:
                shops = get_shop_list(access_token, app_id)
            except Exception as exc:
                crud.update_import_job(db, job_id, 0, 1, "failed", error_summary=str(exc))
                crud.add_import_log(db, job_id, "error", f"shop list exception={exc}")
                return {"job_id": job_id, "imported": 0, "failed": 1}
            if shops.get("code") != 0:
                crud.update_import_job(db, job_id, 0, 1, "failed", error_summary=str(shops))
                crud.add_import_log(db, job_id, "error", f"shop list error={shops}")
                return {"job_id": job_id, "imported": 0, "failed": 1}
            sid_list = [str(s.get("sid")) for s in shops.get("data", []) if s.get("sid") is not None]
        crud.add_import_log(db, job_id, "info", f"sid_list={sid_list}")

        start_str = cfg.get("start_time")
        end_str = cfg.get("end_time")
        chunk_days = int(cfg.get("chunk_days") or 7)
        # Default to rolling window so newest orders are always included.
        # Can be disabled by setting auto_rolling_window=0 in config.
        auto_rolling = str(cfg.get("auto_rolling_window", "1")).lower() not in ("0", "false", "no")
        rolling_days = int(cfg.get("rolling_days") or 30)
        if auto_rolling:
            end_dt = datetime.utcnow()
            start_dt = end_dt - timedelta(days=max(1, rolling_days))
            crud.add_import_log(db, job_id, "info", f"auto rolling window {start_dt.strftime('%Y-%m-%d')} -> {end_dt.strftime('%Y-%m-%d')}")
        else:
            start_dt = datetime.strptime(start_str, "%Y-%m-%d") if start_str else None
            end_dt = datetime.strptime(end_str, "%Y-%m-%d") if end_str else None

        use_mws_orders = str(cfg.get("use_mws_orders", "1")).lower() not in ("0", "false", "no")
        mws_date_types_cfg = str(cfg.get("mws_date_types") or "2,1")
        mws_date_types = [int(x.strip()) for x in mws_date_types_cfg.split(",") if x.strip().isdigit()]
        if not mws_date_types:
            mws_date_types = [2, 1]
        if use_mws_orders:
            result = run_mws_orders_job(db, job_id, app_id, access_token, sid_list, start_dt, end_dt, date_types=mws_date_types)
        else:
            result = run_sync_job(db, job_id, app_id, access_token, sid_list, start_dt, end_dt, chunk_days)

        # Enrich via All Orders Report (optional)
        use_report = str(cfg.get("use_all_orders_report", "1")).lower() not in ("0", "false", "no")
        if use_report and start_dt and end_dt:
            for sid in sid_list:
                crud.add_import_log(db, job_id, "info", f"allOrders sid={sid} range {start_dt.date()} -> {end_dt.date()}")
                offset = 0
                length = 1000
                while True:
                    report = get_all_orders_report(
                        access_token,
                        app_id,
                        sid,
                        start_dt.strftime("%Y-%m-%d"),
                        end_dt.strftime("%Y-%m-%d"),
                        date_type=1,
                        offset=offset,
                        length=length,
                    )
                    if report.get("code") != 0:
                        crud.add_import_log(db, job_id, "error", f"allOrders error={report}")
                        break
                    data = report.get("data", []) or []
                    crud.add_import_log(db, job_id, "info", f"allOrders sid={sid} offset={offset} size={len(data)}")
                    for row in data:
                        amazon_order_id = row.get("amazon_order_id") or row.get("merchant_order_id")
                        if not amazon_order_id:
                            continue
                        order = crud.get_order_by_platform_no(db, amazon_order_id)
                        if not order:
                            continue
                        ext = {
                            "sales_channel": row.get("sales_channel"),
                            "order_status": row.get("order_status"),
                            "ship_service_level": row.get("ship_service_level"),
                            "sku": row.get("sku"),
                            "product_name": row.get("product_name"),
                            "purchase_qty": row.get("quantity"),
                            "unit_price": row.get("item_price"),
                            "currency": row.get("currency"),
                            "amz_ship": row.get("shipment_date"),
                            "purchase_date_local": row.get("purchase_date_local"),
                        }
                        crud.upsert_order_ext_bulk(db, order.id, ext)
                    if len(data) < length:
                        break
                    offset += length
        # Auto backfill missing asin/sku/images after sync
        try:
            sid_list_value = ",".join(sid_list) if sid_list else cfg.get("sid_list")
            backfill_missing_fields(db, access_token, app_id, sid_list_value)
            crud.add_import_log(db, job_id, "info", "auto backfill completed")
        except Exception as exc:
            crud.add_import_log(db, job_id, "error", f"auto backfill error={exc}")
        return result
    except Exception as exc:
        crud.update_import_job(db, job_id, 0, 1, "failed", error_summary=str(exc))
        try:
            crud.add_import_log(db, job_id, "error", f"exception={exc}")
        finally:
            return {"job_id": job_id, "imported": 0, "failed": 1}
    finally:
        db.close()
