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
    get_mp_order_list,
    get_shop_list,
)
from app.transform import map_order_detail, map_order_items, map_order_packages, map_order_ext
from app import crud
from app import models
from app.address_mapping import default_address_mapping
from app.config_store import get_lingxing_config
from app.db import SessionLocal
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time


def _clean(v):
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in ("none", "null", "nan"):
        return None
    return s


def _to_ymd(v):
    if v in (None, "", 0, "0"):
        return None
    try:
        # 秒级时间戳
        if isinstance(v, (int, float)) or str(v).isdigit():
            ts = int(v)
            if ts > 10_000_000_000:  # 毫秒级
                ts = ts // 1000
            return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
        s = str(v).strip()
        if "T" in s:
            s = s.replace("Z", "")
            return datetime.fromisoformat(s[:19]).strftime("%Y-%m-%d")
        return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
    except Exception:
        return None


def _country_name(code: str | None) -> str:
    c = (code or "").strip().upper()
    return {"US": "美国", "CA": "加拿大", "CN": "中国"}.get(c, c or "")


def _enrich_orders_from_mp_list(
    db: Session,
    access_token: str,
    app_id: str,
    sid: str,
    platform_order_nos: list[str],
    job_id: int | None = None,
):
    order_nos = [str(x).strip() for x in (platform_order_nos or []) if str(x).strip()]
    if not order_nos:
        return 0
    payload = {
        "offset": 0,
        "length": min(200, len(order_nos)),
        "platform_code": [10001],
        "store_id": [str(sid)],
        "platform_order_nos": order_nos[:200],
    }
    res = get_mp_order_list(access_token, app_id, payload)
    if res.get("code") != 0:
        if job_id is not None:
            crud.add_import_log(db, job_id, "warn", f"mp/list enrich sid={sid} error={res}")
        return 0
    rows = ((res.get("data") or {}).get("list") or [])
    updated = 0
    for row in rows:
        candidates = set()
        for p in (row.get("platform_info") or []):
            for k in ("platform_order_no", "platform_order_name"):
                v = _clean(p.get(k))
                if v:
                    candidates.add(v)
        for it in (row.get("item_info") or []):
            v = _clean(it.get("platform_order_no"))
            if v:
                candidates.add(v)
        if not candidates:
            continue
        target = None
        for no in candidates:
            if no in order_nos:
                target = no
                break
        if not target:
            continue
        order = crud.get_order_by_platform_no(db, target)
        if not order:
            continue

        raw_addr = row.get("address_info")
        addr = {}
        if isinstance(raw_addr, list):
            addr = raw_addr[0] if raw_addr else {}
        elif isinstance(raw_addr, dict):
            addr = raw_addr

        name = _clean(addr.get("receiver_name"))
        line1 = _clean(addr.get("address_line1"))
        line2 = _clean(addr.get("address_line2"))
        line3 = _clean(addr.get("address_line3"))
        city = _clean(addr.get("city"))
        state = _clean(addr.get("state_or_region"))
        zip5 = _clean(addr.get("postal_code"))
        country_code = _clean(addr.get("receiver_country_code"))
        phone = _clean(addr.get("receiver_mobile") or addr.get("receiver_tel"))
        buyer_name = _clean(row.get("buyer_name")) or name
        address_type = "住宅" if str(row.get("address_type") or "1") == "1" else "商业地址"

        city_line = ", ".join([x for x in [city, state] if x])
        if zip5:
            city_line = f"{city_line} {zip5}".strip()
        customer_lines = [x for x in [name, line1, " ".join([x for x in [line2, line3] if x]).strip(), city_line] if x]
        if country_code:
            customer_lines.append(_country_name(country_code))
        customer_lines.append(f"地址类型:  {address_type}")
        if buyer_name:
            customer_lines.append(f"联系买家:\t{buyer_name}")
        if phone:
            customer_lines.append(f"电话:\t{phone}")

        ext = {
            "receiver_name": name,
            "address_line1": line1,
            "address_line2": line2,
            "address_line3": line3,
            "city": city,
            "state_or_region": state,
            "postal_code": zip5,
            "receiver_country_code": country_code,
            "receiver_mobile": phone,
            "电话": phone,
            "buyer_name": buyer_name,
            "客户地址": "\n".join([x for x in customer_lines if x]),
            "出单日期": _to_ymd(row.get("global_purchase_time")),
            "发货日": _to_ymd(row.get("global_delivery_time") or row.get("global_latest_ship_time")),
        }

        # 补物流方式/单号（若有）
        logistics = row.get("logistics_info") or {}
        tracking_no = _clean(logistics.get("tracking_no") or logistics.get("waybill_no"))
        carrier = _clean(logistics.get("actual_carrier") or logistics.get("logistics_provider_name") or logistics.get("logistics_type_name"))
        if tracking_no:
            ext["联邦单号"] = tracking_no
        if carrier:
            ext["联邦方式"] = carrier
        # 仅保留有值字段，避免覆盖已有正确值
        ext = {k: v for k, v in ext.items() if v not in (None, "", "None", "null")}
        if ext:
            crud.upsert_order_ext_bulk(db, order.id, ext)
            updated += 1
    return updated


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
    cleared_item_orders: set[str] = set()
    seen_item_keys: set[str] = set()

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
                retry = 0
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
                    if res.get("code") != 3001008:
                        break
                    retry += 1
                    if retry >= 5:
                        break
                    wait_s = min(8, 2 ** retry)
                    crud.add_import_log(
                        db,
                        job_id,
                        "warn",
                        f"mws/orders rate limited sid={sid} dt={dt} offset={offset} retry={retry} wait={wait_s}s",
                    )
                    time.sleep(wait_s)
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
                    # 每次任务内，按订单先清空一次商品行再重建，避免历史脏数据/重复累加
                    if amazon_order_id not in cleared_item_orders:
                        db.query(models.InternalOrderItem).filter(
                            models.InternalOrderItem.internal_order_id == order.id
                        ).delete(synchronize_session=False)
                        db.commit()
                        cleared_item_orders.add(amazon_order_id)
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
                        # 重建多SKU行：同SKU合并数量，不同SKU拆行
                        for it in item_list:
                            sku_val = (it.get("sku") or it.get("seller_sku") or "").strip()
                            order_item_id = str(it.get("order_item_id") or "").strip()
                            sig = f"{amazon_order_id}|{order_item_id}|{sku_val}|{it.get('quantity_ordered')}|{it.get('unit_price_amount') or it.get('item_price_amount')}"
                            if sig in seen_item_keys:
                                continue
                            seen_item_keys.add(sig)
                            qty = int(it.get("quantity_ordered") or it.get("quantity") or 0)
                            pname = it.get("product_name") or it.get("title") or ""
                            uprice = float(it.get("unit_price_amount") or it.get("item_price_amount") or 0)
                            curr = it.get("currency") or row.get("currency") or "USD"
                            pimg = it.get("pic_url") or ""
                            # 合并同SKU
                            target_item = None
                            if sku_val:
                                for ex in crud.get_order_items(db, order.id):
                                    if (ex.sku or "").strip() == sku_val:
                                        target_item = ex
                                        break
                            if target_item:
                                target_item.quantity = int(target_item.quantity or 0) + qty
                                if not (target_item.product_name or "").strip():
                                    target_item.product_name = pname
                                if not (target_item.product_image or "").strip():
                                    target_item.product_image = pimg
                                if (target_item.unit_price in (None, 0, 0.0, "0", "0.0", "0.00")) and uprice:
                                    target_item.unit_price = uprice
                                db.commit()
                            else:
                                crud.create_internal_order_item(
                                    db,
                                    order.id,
                                    {
                                        "sku": sku_val,
                                        "product_name": pname,
                                        "quantity": qty,
                                        "unit_price": uprice,
                                        "currency": curr,
                                        "product_image": pimg,
                                        "attachments": None,
                                    },
                                )
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
                                ext_update = {}
                                detail_items = d.get("item_list", []) or []
                                # 每次按详情重建商品行：
                                # - 同一订单同 SKU 合并数量
                                # - 同一订单不同 SKU 保留多行
                                if detail_items:
                                    try:
                                        db.query(models.InternalOrderItem).filter(
                                            models.InternalOrderItem.internal_order_id == order.id
                                        ).delete(synchronize_session=False)
                                        grouped = {}
                                        for it in detail_items:
                                            sku_val = (it.get("sku") or it.get("seller_sku") or "").strip()
                                            key = sku_val or f"__NO_SKU__{it.get('order_item_id') or id(it)}"
                                            g = grouped.get(key)
                                            qty = int(it.get("quantity_ordered") or 0)
                                            if g is None:
                                                grouped[key] = {
                                                    "sku": sku_val,
                                                    "product_name": it.get("product_name") or it.get("title") or "",
                                                    "quantity": qty,
                                                    "unit_price": float(it.get("unit_price_amount") or it.get("item_price_amount") or 0),
                                                    "currency": it.get("currency") or d.get("currency") or "USD",
                                                    "product_image": it.get("pic_url") or "",
                                                }
                                            else:
                                                g["quantity"] = int(g.get("quantity") or 0) + qty
                                                if not g.get("product_name"):
                                                    g["product_name"] = it.get("product_name") or it.get("title") or ""
                                                if not g.get("product_image"):
                                                    g["product_image"] = it.get("pic_url") or ""
                                        for g in grouped.values():
                                            crud.create_internal_order_item(
                                                db,
                                                order.id,
                                                {
                                                    "sku": g.get("sku"),
                                                    "product_name": g.get("product_name"),
                                                    "quantity": g.get("quantity"),
                                                    "unit_price": g.get("unit_price"),
                                                    "currency": g.get("currency"),
                                                    "product_image": g.get("product_image"),
                                                    "attachments": None,
                                                },
                                            )
                                    except Exception as exc:
                                        crud.add_import_log(db, job_id, "error", f"rebuild order_items exception={exc}")

                                for it in detail_items:
                                    if it.get("asin"):
                                        ext_update["asin"] = it.get("asin")
                                    sku_val = it.get("sku") or it.get("seller_sku")
                                    if sku_val:
                                        ext_update["sku"] = sku_val
                                    if it.get("product_name") or it.get("title"):
                                        ext_update["product_name"] = it.get("product_name") or it.get("title")
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

                # 自动补全买家信息（地址/电话/联系人），确保新增订单立刻可见
                try:
                    page_order_nos = [str(r.get("amazon_order_id")).strip() for r in data if r.get("amazon_order_id")]
                    if page_order_nos:
                        enriched = _enrich_orders_from_mp_list(db, access_token, app_id, str(sid), page_order_nos, job_id=job_id)
                        if enriched:
                            crud.add_import_log(db, job_id, "info", f"mp/list enriched sid={sid} count={enriched}")
                except Exception as exc:
                    crud.add_import_log(db, job_id, "error", f"mp/list enrich exception sid={sid} {exc}")

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
        shops_data = []
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
            shops_data = shops.get("data", []) or []
            sid_list = [str(s.get("sid")) for s in shops_data if s.get("sid") is not None]
        else:
            try:
                shops = get_shop_list(access_token, app_id)
                if shops.get("code") == 0:
                    shops_data = shops.get("data", []) or []
            except Exception:
                shops_data = []
        sid_name_map = {
            str(s.get("sid")): (s.get("name") or s.get("shop_name") or s.get("account_name") or "")
            for s in shops_data
            if s.get("sid") is not None
        }
        crud.add_import_log(db, job_id, "info", f"sid_list={sid_list}")

        start_str = cfg.get("start_time")
        end_str = cfg.get("end_time")
        chunk_days = int(cfg.get("chunk_days") or 7)
        # Default to rolling window so newest orders are always included.
        # Can be disabled by setting auto_rolling_window=0 in config.
        auto_rolling = str(cfg.get("auto_rolling_window", "1")).lower() not in ("0", "false", "no")
        rolling_days = int(cfg.get("rolling_days") or 30)
        if auto_rolling:
            # 用北京时间做滚动窗口，并把结束日期扩到明天，避免时区边界导致“今天订单”漏抓
            now_cn = datetime.now(ZoneInfo("Asia/Shanghai"))
            end_dt = now_cn + timedelta(days=1)
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
            all_orders_date_types_cfg = str(cfg.get("all_orders_date_types") or "1,2")
            all_orders_date_types = [int(x.strip()) for x in all_orders_date_types_cfg.split(",") if x.strip().isdigit()]
            if not all_orders_date_types:
                all_orders_date_types = [1, 2]
            for sid in sid_list:
                for report_dt in all_orders_date_types:
                    crud.add_import_log(
                        db,
                        job_id,
                        "info",
                        f"allOrders sid={sid} dt={report_dt} range {start_dt.date()} -> {end_dt.date()}",
                    )
                    offset = 0
                    length = 1000
                    while True:
                        retry = 0
                        while True:
                            report = get_all_orders_report(
                                access_token,
                                app_id,
                                sid,
                                start_dt.strftime("%Y-%m-%d"),
                                end_dt.strftime("%Y-%m-%d"),
                                date_type=report_dt,
                                offset=offset,
                                length=length,
                            )
                            if report.get("code") != 3001008:
                                break
                            retry += 1
                            if retry >= 5:
                                break
                            wait_s = min(8, 2 ** retry)
                            crud.add_import_log(
                                db,
                                job_id,
                                "warn",
                                f"allOrders rate limited sid={sid} dt={report_dt} offset={offset} retry={retry} wait={wait_s}s",
                            )
                            time.sleep(wait_s)
                        if report.get("code") != 0:
                            crud.add_import_log(db, job_id, "error", f"allOrders sid={sid} dt={report_dt} error={report}")
                            break
                        data = report.get("data", []) or []
                        crud.add_import_log(db, job_id, "info", f"allOrders sid={sid} dt={report_dt} offset={offset} size={len(data)}")
                        for row in data:
                            amazon_order_id = row.get("amazon_order_id") or row.get("merchant_order_id")
                            if not amazon_order_id:
                                continue
                            order = crud.get_order_by_platform_no(db, amazon_order_id)
                            if not order:
                                mapped = {
                                    "internal_order_no": f"IO{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
                                    "platform_order_no": amazon_order_id,
                                    "shop_name": sid_name_map.get(str(sid)) or row.get("seller_name"),
                                    "order_status": row.get("order_status"),
                                    "purchase_time": _parse_dt(row.get("purchase_date_local")),
                                }
                                order = crud.create_internal_order(db, mapped)
                                first_sku = row.get("sku")
                                if first_sku or row.get("product_name"):
                                    crud.create_internal_order_item(
                                        db,
                                        order.id,
                                        {
                                            "sku": first_sku or "",
                                            "product_name": row.get("product_name") or "",
                                            "quantity": int(row.get("quantity") or 1),
                                            "unit_price": float(row.get("item_price") or 0),
                                            "currency": row.get("currency") or "USD",
                                            "product_image": "",
                                            "attachments": None,
                                        },
                                    )
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
                        # 对 allOrders 新增/更新订单，再补一次买家地址信息
                        try:
                            report_order_nos = [
                                str((r.get("amazon_order_id") or r.get("merchant_order_id"))).strip()
                                for r in data
                                if (r.get("amazon_order_id") or r.get("merchant_order_id"))
                            ]
                            if report_order_nos:
                                enriched = _enrich_orders_from_mp_list(
                                    db, access_token, app_id, str(sid), report_order_nos, job_id=job_id
                                )
                                if enriched:
                                    crud.add_import_log(
                                        db, job_id, "info", f"mp/list enriched(allOrders) sid={sid} count={enriched}"
                                    )
                        except Exception as exc:
                            crud.add_import_log(db, job_id, "error", f"mp/list enrich(allOrders) sid={sid} {exc}")
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
