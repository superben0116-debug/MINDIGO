from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.services import execute_sync_job
from app import crud, models
from app.config_store import get_lingxing_config
from app.integrations.lingxing_client import get_access_token, create_manual_order, get_fbm_order_detail, get_fbm_order_list, get_shop_list, get_mp_order_list, get_mws_order_detail, get_mws_orders
from app.transform import map_order_ext
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import threading

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/sync-status")
def sync_status(db: Session = Depends(get_db)):
    cfg = get_lingxing_config(db)
    app_id = str(cfg.get("app_id") or "").strip()
    app_secret = str(cfg.get("app_secret") or "").strip()
    sid_list = str(cfg.get("sid_list") or "").strip()
    placeholder = {"", "APP_ID", "ACCESS_TOKEN", "SID1", "app_id", "access_token", "sid1"}
    ready = bool(app_id and app_secret and sid_list and app_id not in placeholder and app_secret not in placeholder)
    latest = (
        db.query(models.ImportJob)
        .filter(models.ImportJob.job_type == "lingxing_fbm")
        .order_by(models.ImportJob.id.desc())
        .first()
    )
    return {
        "ready": ready,
        "config": {
            "app_id_set": bool(app_id and app_id not in placeholder),
            "app_secret_set": bool(app_secret and app_secret not in placeholder),
            "sid_list": sid_list,
        },
        "latest_job": {
            "id": latest.id if latest else None,
            "status": latest.status if latest else None,
            "success": latest.success_count if latest else 0,
            "failed": latest.failed_count if latest else 0,
            "error_summary": latest.error_summary if latest else "",
            "start_time": latest.start_time if latest else None,
            "end_time": latest.end_time if latest else None,
        },
    }


@router.post("/sync-fbm-orders")
def sync_orders(db: Session = Depends(get_db)):
    job = crud.create_import_job(db, "lingxing_fbm")
    thread = threading.Thread(target=execute_sync_job, args=(job.id,), daemon=True)
    thread.start()
    return {"job_id": job.id, "status": "queued"}


@router.post("/create-manual-order")
def create_manual_order_api(payload: dict, db: Session = Depends(get_db)):
    platform_code = payload.get("platform_code")
    store_id = payload.get("store_id")
    orders = payload.get("orders") or []
    if not platform_code or not store_id or not orders:
        raise HTTPException(status_code=400, detail="missing platform_code/store_id/orders")
    cfg = get_lingxing_config(db)
    app_id = cfg.get("app_id")
    app_secret = cfg.get("app_secret")
    if not app_id or not app_secret:
        raise HTTPException(status_code=400, detail="lingxing config not set")
    token = get_access_token(app_id, app_secret)
    if token.get("code") not in (200, "200"):
        raise HTTPException(status_code=400, detail=str(token))
    access_token = token.get("data", {}).get("access_token")
    body = {
        "platform_code": platform_code,
        "store_id": str(store_id),
        "orders": orders,
    }
    resp = create_manual_order(access_token, app_id, body)
    created = []
    if resp.get("code") == 0:
        success = resp.get("data", {}).get("success_details") or []
        success_map = {str(s.get("platform_order_no")): s for s in success}
        for o in orders:
            pno = str(o.get("platform_order_no") or "")
            if not pno:
                continue
            order = crud.get_order_by_platform_no(db, pno)
            if not order:
                mapped = {
                    "internal_order_no": f"IO{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
                    "platform_order_no": pno,
                    "order_status": "待审核",
                    "purchase_time": datetime.utcnow(),
                }
                order = crud.create_internal_order(db, mapped)
            ext = crud.get_order_ext(db, order.id)
            fields = ext.fields if ext else {}
            ext_update = {}
            def set_if_missing(key: str, val: str | None):
                if val is None or val == "":
                    return
                if fields.get(key) in (None, "", " "):
                    ext_update[key] = val

            set_if_missing("平台", str(platform_code))
            set_if_missing("店铺ID", str(store_id))
            set_if_missing("订单编号", pno)
            set_if_missing("内部订单号", order.internal_order_no)
            set_if_missing("订单类型", "FBM自发货")
            set_if_missing("收件人", o.get("receiver_name"))
            if o.get("address_line1") or o.get("city") or o.get("postal_code"):
                addr_line = o.get("address_line1") or ""
                city_line = f"{o.get('city','')}, {o.get('state_or_region','')}".strip(", ")
                if o.get("postal_code"):
                    city_line = f"{city_line} {o.get('postal_code')}".strip()
                set_if_missing("客户地址", "\n".join([x for x in [o.get("receiver_name"), addr_line, city_line] if x]))
            set_if_missing("币种", o.get("amount_currency"))
            set_if_missing("售价", str(o.get("order_total_amount")) if o.get("order_total_amount") is not None else None)
            set_if_missing("买家留言", o.get("buyer_note"))
            if o.get("global_purchase_time"):
                try:
                    set_if_missing("出单日期", datetime.fromtimestamp(int(o.get("global_purchase_time"))).strftime("%Y-%m-%d %H:%M:%S"))
                except Exception:
                    pass
            items = o.get("items") or []
            if items:
                it = items[0]
                set_if_missing("SKU", it.get("sku"))
                set_if_missing("MSKU", it.get("msku"))
                set_if_missing("采购数量", str(it.get("quantity")) if it.get("quantity") is not None else None)
                set_if_missing("单价", str(it.get("unit_price")) if it.get("unit_price") is not None else None)
            if pno in success_map and success_map[pno].get("global_order_no"):
                set_if_missing("lingxing_global_order_no", str(success_map[pno].get("global_order_no")))
            if ext_update:
                crud.upsert_order_ext_bulk(db, order.id, ext_update)
            created.append(pno)
    return {"ok": resp.get("code") == 0, "resp": resp, "created": created}


@router.post("/enrich-fbm-addresses")
def enrich_fbm_addresses(payload: dict, db: Session = Depends(get_db)):
    ids = payload.get("order_ids") or []
    max_pages = int(payload.get("max_pages") or 5)
    platform_code = payload.get("platform_code")
    store_id = payload.get("store_id")
    cfg = get_lingxing_config(db)
    app_id = cfg.get("app_id")
    app_secret = cfg.get("app_secret")
    if not app_id or not app_secret:
        raise HTTPException(status_code=400, detail="lingxing config not set")
    token = get_access_token(app_id, app_secret)
    if token.get("code") not in (200, "200"):
        raise HTTPException(status_code=400, detail=str(token))
    access_token = token.get("data", {}).get("access_token")

    if not ids:
        ids = [o.id for o in crud.list_internal_orders(db, limit=2000, offset=0)]
    # Expand selected ids to all duplicated internal rows sharing same platform_order_no
    # so UI-visible duplicate rows are updated together.
    try:
        id_set = set(int(x) for x in ids)
    except Exception:
        id_set = set(ids)
    pnos = []
    for oid in list(id_set):
        o = crud.get_internal_order(db, oid)
        if o and o.platform_order_no:
            pnos.append(str(o.platform_order_no))
    if pnos:
        for row in db.query(models.InternalOrder).filter(models.InternalOrder.platform_order_no.in_(pnos)).all():
            id_set.add(row.id)
    ids = list(id_set)

    updated = 0
    resolved = 0
    missing = 0
    used_list = 0
    scan_cache = {}
    missing_order_no = 0
    debug_misses = []
    updated_ids = []
    updated_address = 0
    updated_ship_date = 0
    updated_deliver_date = 0
    diagnostics = []
    force_refresh = bool(payload.get("force_refresh", True))
    mws_logistics_cache = {}
    fbm_list_logistics_cache = {}

    def norm_order_no(v: str) -> str:
        if not v:
            return ""
        s = str(v).strip().lower()
        # normalize '#1064', '113-xxxx', spaces and symbols
        return "".join(ch for ch in s if ch.isalnum())

    def is_amazon_order_no(v: str) -> bool:
        import re
        return bool(re.fullmatch(r"\d{3}-\d{7}-\d{7}", str(v or "").strip()))

    def sanitize_platform_codes(v):
        if v is None:
            return []
        raw = v if isinstance(v, list) else [v]
        out = []
        mapping = {
            "AMAZON": 10001,
            "AMAZON VC": 10035,
            "SHOPIFY": 10002,
            "EBAY": 10003,
        }
        for x in raw:
            if x is None:
                continue
            s = str(x).strip()
            if not s:
                continue
            if s.isdigit():
                out.append(int(s))
                continue
            if s.upper() in mapping:
                out.append(mapping[s.upper()])
        return list(dict.fromkeys(out))

    def sanitize_store_ids(v):
        if v is None:
            return []
        raw = v if isinstance(v, list) else [v]
        out = []
        for x in raw:
            if x is None:
                continue
            s = str(x).strip()
            if s and s.isdigit():
                out.append(s)
        return list(dict.fromkeys(out))

    def extract_target_order_no(order, fields: dict):
        import re
        candidates = [
            order.platform_order_no,
            fields.get("订单编号"),
            fields.get("platform_order_no"),
            fields.get("amazon_order_id"),
            fields.get("平台单号"),
            fields.get("订单号"),
        ]
        # Prefer strict amazon order format
        for c in candidates:
            s = str(c or "").strip()
            if re.fullmatch(r"\d{3}-\d{7}-\d{7}", s):
                return s
        # Try extracting from raw text blob
        raw_text = str(fields.get("raw_text") or "")
        m = re.search(r"(\d{3}-\d{7}-\d{7})", raw_text)
        if m:
            return m.group(1)
        # Fallback to first non-empty candidate
        for c in candidates:
            s = str(c or "").strip()
            if s:
                return s
        return ""

    def query_mp_rows(target_no: str, row_platform_code, row_store_id):
        raw = str(target_no or "").strip()
        compact = norm_order_no(raw)
        variants = [v for v in [raw, compact, raw[1:] if raw.startswith("#") else ""] if v]
        variants = list(dict.fromkeys(variants))
        if not variants:
            return []
        pcfg = sanitize_platform_codes(platform_code) if platform_code else sanitize_platform_codes(row_platform_code)
        scfg = sanitize_store_ids(store_id) if store_id else sanitize_store_ids(row_store_id)
        if not pcfg and is_amazon_order_no(raw):
            pcfg = [10001]

        now_ts = int(datetime.utcnow().timestamp())
        query_variants = []
        for key in ["platform_order_nos", "platform_order_names"]:
            # V1: minimal payload (preferred by doc when querying by order no/name)
            query_variants.append({"offset": 0, "length": 200, key: variants})
            # V2: with platform/store constraints
            body_ps = {"offset": 0, "length": 200, key: variants}
            if pcfg:
                body_ps["platform_code"] = pcfg
            if scfg:
                body_ps["store_id"] = scfg
            query_variants.append(body_ps)
            # V3: with date window fallback (some tenants enforce time range)
            body_time = dict(body_ps)
            body_time["date_type"] = "update_time"
            body_time["start_time"] = now_ts - 365 * 24 * 3600
            body_time["end_time"] = now_ts
            query_variants.append(body_time)

        last_err = None
        for body in query_variants:
            res = get_mp_order_list(access_token, app_id, body)
            if res.get("code") == 0:
                rows = res.get("data", {}).get("list", []) or []
                if rows:
                    return rows
            else:
                last_err = {"code": res.get("code"), "message": res.get("message"), "error_details": res.get("error_details")}
        if last_err:
            return {"__error__": last_err}
        return []

    def scan_mp_match(target_no_norm: str, row_platform_code, row_store_id):
        cache_key = f"{target_no_norm}|{row_platform_code}|{row_store_id}"
        if cache_key in scan_cache:
            return scan_cache[cache_key]
        now_ts = int(datetime.utcnow().timestamp())
        max_scan_pages = int(payload.get("max_scan_pages") or 20)
        page_size = 500
        body_base = {
            "date_type": "update_time",
            "start_time": now_ts - 365 * 24 * 3600,
            "end_time": now_ts,
            "length": page_size,
        }
        pcfg = sanitize_platform_codes(platform_code) if platform_code else sanitize_platform_codes(row_platform_code)
        scfg = sanitize_store_ids(store_id) if store_id else sanitize_store_ids(row_store_id)
        if not pcfg:
            pcfg = [10001]
        if pcfg:
            body_base["platform_code"] = pcfg
        if scfg:
            body_base["store_id"] = scfg

        for page in range(max_scan_pages):
            body = dict(body_base)
            body["offset"] = page * page_size
            res = get_mp_order_list(access_token, app_id, body)
            if res.get("code") != 0:
                break
            rows = res.get("data", {}).get("list", []) or []
            if not rows:
                break
            for row in rows:
                candidates = [row.get("reference_no")]
                for it in row.get("item_info", []) or []:
                    candidates.append(it.get("platform_order_no"))
                for it in row.get("platform_info", []) or []:
                    candidates.append(it.get("platform_order_no"))
                    candidates.append(it.get("platform_order_name"))
                for cand in candidates:
                    if norm_order_no(cand) == target_no_norm:
                        scan_cache[cache_key] = row
                        return row
        scan_cache[cache_key] = None
        return None

    def _clean(v):
        if v is None:
            return ""
        s = str(v).strip()
        return "" if s.lower() in ("none", "null", "nan") else s

    def _to_ymd(v):
        s = _clean(v)
        if not s:
            return ""
        # timestamp
        if s.isdigit() and len(s) >= 10:
            try:
                dt = datetime.fromtimestamp(int(s[:10]), tz=ZoneInfo("UTC")).astimezone(ZoneInfo("America/Los_Angeles"))
                return dt.strftime("%Y-%m-%d")
            except Exception:
                pass
        # ISO datetime with timezone -> America/Los_Angeles date
        if "T" in s and (s.endswith("Z") or "+" in s):
            try:
                iso = s.replace("Z", "+00:00")
                dt = datetime.fromisoformat(iso).astimezone(ZoneInfo("America/Los_Angeles"))
                return dt.strftime("%Y-%m-%d")
            except Exception:
                pass
        # plain datetime
        for sep in ["T", " "]:
            if sep in s and len(s) >= 10:
                return s[:10]
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return s[:10]
        return s

    def _norm_carrier(v: str) -> str:
        s = _clean(v)
        if not s:
            return ""
        up = s.upper()
        if "FEDEX" in up:
            return "FedEx"
        if "UPS" in up:
            return "UPS"
        return s

    def _strict_carrier(v: str) -> str:
        s = _clean(v)
        if not s:
            return ""
        up = s.upper()
        if "FEDEX" in up:
            return "FedEx"
        if "UPS" in up:
            return "UPS"
        return ""

    def _query_logistics_from_mws_orders(target_no: str):
        if target_no in mws_logistics_cache:
            return mws_logistics_cache[target_no]
        sid_cfg = str(cfg.get("sid_list", "")).strip()
        sid_list = [s for s in sid_cfg.split(",") if s] if sid_cfg and sid_cfg.upper() != "ALL" else []
        if not sid_list:
            shops = get_shop_list(access_token, app_id)
            if shops.get("code") == 0:
                sid_list = [str(s.get("sid")) for s in shops.get("data", []) if s.get("sid") is not None]
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=120)
        start_date = start_dt.strftime("%Y-%m-%d")
        end_date = end_dt.strftime("%Y-%m-%d")
        for sid in sid_list[:30]:
            offset = 0
            while offset <= 5000:
                res = get_mws_orders(
                    access_token=access_token,
                    app_id=app_id,
                    sid=int(sid),
                    start_date=start_date,
                    end_date=end_date,
                    date_type=1,
                    offset=offset,
                    length=1000,
                )
                if res.get("code") != 0:
                    break
                rows = res.get("data", []) or []
                if not rows:
                    break
                hit = None
                for r in rows:
                    if str(r.get("amazon_order_id") or "").strip() == str(target_no).strip():
                        hit = r
                        break
                if hit:
                    out = {
                        "tracking_no": _clean(hit.get("tracking_number")),
                        "carrier_hint": _strict_carrier(hit.get("ship_service_level")),
                        "shipment_date": _to_ymd(hit.get("shipment_date_local") or hit.get("shipment_date") or hit.get("shipment_date_utc")),
                    }
                    mws_logistics_cache[target_no] = out
                    return out
                if len(rows) < 1000:
                    break
                offset += 1000
        mws_logistics_cache[target_no] = {}
        return {}

    def _query_logistics_from_fbm_list(target_no: str):
        if target_no in fbm_list_logistics_cache:
            return fbm_list_logistics_cache[target_no]
        sid_cfg = str(cfg.get("sid_list", "")).strip()
        sid_list = [s for s in sid_cfg.split(",") if s] if sid_cfg and sid_cfg.upper() != "ALL" else []
        if not sid_list:
            shops = get_shop_list(access_token, app_id)
            if shops.get("code") == 0:
                sid_list = [str(s.get("sid")) for s in shops.get("data", []) if s.get("sid") is not None]
        end_dt = datetime.utcnow()
        start_dt = end_dt - timedelta(days=120)
        for sid in sid_list[:30]:
            page = 1
            while page <= max_pages:
                res = get_fbm_order_list(
                    access_token=access_token,
                    app_id=app_id,
                    sid=sid,
                    page=page,
                    start_time=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                )
                if res.get("code") != 0:
                    break
                rows = res.get("data", []) or []
                if not rows:
                    break
                hit = None
                for r in rows:
                    if norm_order_no(r.get("platform_order_no") or r.get("amazon_order_id") or "") == norm_order_no(target_no):
                        hit = r
                        break
                if hit:
                    out = {
                        "tracking_no": _clean(hit.get("tracking_number")),
                        "carrier_hint": _strict_carrier(hit.get("logistics_provider_name") or hit.get("logistics_type_name")),
                        "shipment_date": _to_ymd(hit.get("shipment_date") or hit.get("shipment_date_local")),
                    }
                    fbm_list_logistics_cache[target_no] = out
                    return out
                page += 1
        fbm_list_logistics_cache[target_no] = {}
        return {}

    def build_ext_from_mp_row(row: dict) -> dict:
        ext = {}
        raw_addr = row.get("address_info")
        if isinstance(raw_addr, list):
            addr = raw_addr[0] if raw_addr else {}
        elif isinstance(raw_addr, dict):
            addr = raw_addr
        else:
            addr = {}
        name = _clean(addr.get("receiver_name"))
        line1 = _clean(addr.get("address_line1"))
        line2 = _clean(addr.get("address_line2"))
        line3 = _clean(addr.get("address_line3"))
        district = _clean(addr.get("district"))
        doorplate = _clean(addr.get("doorplate_no"))
        city = _clean(addr.get("city"))
        state = _clean(addr.get("state_or_region"))
        zip5 = _clean(addr.get("postal_code"))
        phone = _clean(addr.get("receiver_mobile") or addr.get("receiver_tel"))
        ext["receiver_name"] = name
        ext["receiver_mobile"] = phone
        if phone:
            ext["电话"] = phone
        ext["address_line1"] = line1
        ext["address_line2"] = line2
        ext["address_line3"] = line3
        ext["district"] = district
        ext["doorplate_no"] = doorplate
        ext["city"] = city
        ext["state_or_region"] = state
        ext["postal_code"] = zip5
        ext["receiver_country_code"] = addr.get("receiver_country_code")
        if addr.get("company_name"):
            ext["company_name"] = _clean(addr.get("company_name"))
        # build full customer address block
        city_line = f"{city}, {state}".strip(", ")
        if zip5:
            city_line = f"{city_line} {zip5}".strip()
        mid_line = " ".join([x for x in [line2, line3, district, doorplate] if x])
        full_addr = "\n".join([x for x in [name, line1, mid_line, city_line] if x])
        if full_addr:
            if phone:
                full_addr = f"{full_addr}\n电话: {phone}"
            ext["客户地址"] = full_addr
        # item_info -> sku/asin/name
        items = row.get("item_info") or []
        if items:
            it0 = items[0]
            if it0.get("local_sku"):
                ext["SKU"] = it0.get("local_sku")
            if it0.get("msku"):
                ext["MSKU"] = it0.get("msku")
            if it0.get("product_no"):
                ext["ASIN"] = it0.get("product_no")
            if it0.get("local_product_name"):
                ext["产品名"] = it0.get("local_product_name")
            if it0.get("quantity"):
                ext["采购数量"] = str(it0.get("quantity"))
            if it0.get("unit_price_amount"):
                ext["单价"] = str(it0.get("unit_price_amount"))
        # logistics_info -> tracking
        logistics = row.get("logistics_info") or {}
        # keep raw logistics fields for audit/debug
        for k in [
            "actual_carrier", "cost_amount", "cost_currency_code",
            "logistics_provider_id", "logistics_provider_name",
            "logistics_time", "logistics_type_id", "logistics_type_name",
            "pkg_fee_weight", "pkg_fee_weight_unit",
            "pkg_height", "pkg_length", "pkg_size_unit", "pkg_width",
            "pre_cost_amount", "pre_fee_weight", "pre_fee_weight_unit",
            "pre_pkg_height", "pre_pkg_length", "pre_pkg_width",
            "pre_weight", "status", "tracking_no", "waybill_no",
            "weight", "weight_unit",
        ]:
            if logistics.get(k) not in (None, "", "None", "null"):
                ext[k] = logistics.get(k)
        if logistics.get("tracking_no"):
            ext["联邦单号"] = _clean(logistics.get("tracking_no"))
        elif logistics.get("waybill_no"):
            ext["联邦单号"] = _clean(logistics.get("waybill_no"))
        carrier_name = _clean(logistics.get("actual_carrier")) or _clean(logistics.get("logistics_provider_name")) or _clean(logistics.get("logistics_type_name"))
        if carrier_name:
            ext["联邦方式"] = _norm_carrier(carrier_name)
        if ext.get("联邦单号") and not ext.get("联邦方式"):
            t = str(ext.get("联邦单号")).upper()
            if t.startswith("1Z"):
                ext["联邦方式"] = "UPS"
            elif t.isdigit():
                ext["联邦方式"] = "FedEx"
        # timestamps
        if row.get("global_purchase_time"):
            ext["出单日期"] = _to_ymd(row.get("global_purchase_time"))
        # shipping/delivery date fallback chain
        ship_ts = row.get("global_delivery_time") or row.get("global_latest_ship_time")
        if not ship_ts:
            pinfo = row.get("platform_info") or []
            if pinfo and isinstance(pinfo, list):
                ship_ts = pinfo[0].get("latest_ship_time") or pinfo[0].get("delivery_time")
        if ship_ts:
            try:
                ext["发货日"] = _to_ymd(ship_ts)
            except Exception:
                ext["发货日"] = _to_ymd(ship_ts)
        deliver_ts = None
        pinfo = row.get("platform_info") or []
        if pinfo and isinstance(pinfo, list):
            # keep first platform_info raw fields
            p0 = pinfo[0] or {}
            for k in [
                "cancel_time", "delivery_time", "latest_ship_time", "order_from",
                "payment_status", "payment_time", "platform_code",
                "platform_order_name", "platform_order_no", "purchase_time",
                "shipping_status", "status"
            ]:
                if p0.get(k) not in (None, "", "None", "null"):
                    ext[f"platform_{k}"] = p0.get(k)
            if p0.get("platform_order_name"):
                ext["平台订单编号"] = p0.get("platform_order_name")
            if p0.get("platform_order_no"):
                ext["平台订单号"] = p0.get("platform_order_no")
            if p0.get("purchase_time") and not ext.get("出单日期"):
                ext["出单日期"] = _to_ymd(p0.get("purchase_time"))
            if p0.get("latest_ship_time") and not ext.get("发货日"):
                ext["发货日"] = _to_ymd(p0.get("latest_ship_time"))
            deliver_ts = pinfo[0].get("delivery_time")
        if deliver_ts:
            try:
                ext["送达日"] = _to_ymd(deliver_ts)
            except Exception:
                ext["送达日"] = _to_ymd(deliver_ts)
        # order tags
        tags = row.get("order_tag") or []
        if tags:
            names = [str(t.get("tag_name")).strip() for t in tags if t.get("tag_name")]
            if names:
                ext["订单标签"] = ", ".join(names)
        ex_tags = row.get("exception_order_tag") or []
        if ex_tags:
            ext["异常标签"] = ", ".join([str(x) for x in ex_tags if x not in (None, "")])
        pd_tags = row.get("pending_order_tag") or row.get("pending_order_tag_order_tag") or []
        if pd_tags:
            ext["待办标签"] = ", ".join([str(x) for x in pd_tags if x not in (None, "")])
        return ext
    # Batch prefetch from /pb/mp/order/v2/list to reduce misses/rate limit on per-order lookup
    batch_map = {}
    pre_targets = []
    for oid in ids:
        o = crud.get_internal_order(db, oid)
        if not o:
            continue
        ext_obj = crud.get_order_ext(db, oid)
        fs = ext_obj.fields if ext_obj else {}
        t = extract_target_order_no(o, fs)
        if t:
            pre_targets.append(t)
    pre_targets = list(dict.fromkeys(pre_targets))[:200]
    if pre_targets:
        now_ts = int(datetime.utcnow().timestamp())
        batch_bodies = [
            {"offset": 0, "length": 500, "platform_code": [10001], "platform_order_nos": pre_targets},
            {"offset": 0, "length": 500, "platform_code": [10001], "platform_order_names": pre_targets},
            {"offset": 0, "length": 500, "platform_code": [10001], "platform_order_nos": pre_targets, "date_type": "update_time", "start_time": now_ts - 365 * 24 * 3600, "end_time": now_ts},
            {"offset": 0, "length": 500, "platform_code": [10001], "platform_order_names": pre_targets, "date_type": "update_time", "start_time": now_ts - 365 * 24 * 3600, "end_time": now_ts},
        ]
        for body in batch_bodies:
            res = get_mp_order_list(access_token, app_id, body)
            if res.get("code") != 0:
                continue
            rows = res.get("data", {}).get("list", []) or []
            for row in rows:
                candidates = [row.get("reference_no")]
                for it in row.get("item_info", []) or []:
                    candidates.append(it.get("platform_order_no"))
                for it in row.get("platform_info", []) or []:
                    candidates.append(it.get("platform_order_no"))
                    candidates.append(it.get("platform_order_name"))
                for cand in candidates:
                    n = norm_order_no(cand)
                    if n:
                        batch_map[n] = row

    # MWS detail supplemental map for delivery range and richer item fields
    mws_map = {}
    if pre_targets:
        for i in range(0, len(pre_targets), 200):
            batch = pre_targets[i:i + 200]
            dres = get_mws_order_detail(access_token, app_id, batch)
            if dres.get("code") == 0:
                for d in dres.get("data", []) or []:
                    ano = d.get("amazon_order_id")
                    if ano:
                        mws_map[norm_order_no(ano)] = d

    for oid in ids:
        order = crud.get_internal_order(db, oid)
        if not order:
            continue
        ext_obj = crud.get_order_ext(db, oid)
        fields = ext_obj.fields if ext_obj else {}
        target_no = extract_target_order_no(order, fields)
        target_no_norm = norm_order_no(target_no)
        row_platform_code = fields.get("平台")
        row_store_id = fields.get("店铺ID")
        if not target_no:
            missing_order_no += 1
            missing += 1
            diagnostics.append({"id": oid, "order_no": "", "reason": "missing_target_order_no"})
            continue
        mp_row_hit = batch_map.get(target_no_norm)
        order_number = fields.get("lingxing_order_number") or fields.get("global_order_no")
        detail_code = None
        detail_data_obj = {}
        if not order_number:
            # try resolve by platform_order_no via list scan
            if target_no:
                sid_cfg = str(cfg.get("sid_list", "")).strip()
                sid_list = [s for s in sid_cfg.split(",") if s] if sid_cfg and sid_cfg.upper() != "ALL" else []
                if not sid_list:
                    shops = get_shop_list(access_token, app_id)
                    if shops.get("code") == 0:
                        sid_list = [str(s.get("sid")) for s in shops.get("data", []) if s.get("sid") is not None]
                # default to last 60 days window if not configured
                end_dt = datetime.utcnow()
                start_dt = end_dt - timedelta(days=60)
                start_cfg = cfg.get("start_time")
                end_cfg = cfg.get("end_time")
                if start_cfg and end_cfg:
                    try:
                        start_dt = datetime.strptime(start_cfg, "%Y-%m-%d")
                        end_dt = datetime.strptime(end_cfg, "%Y-%m-%d")
                    except Exception:
                        pass
                found = None
                for sid in sid_list:
                    page = 1
                    while page <= max_pages:
                        res = get_fbm_order_list(
                            access_token,
                            app_id,
                            sid,
                            page=page,
                            start_time=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                            end_time=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        if res.get("code") != 0:
                            break
                        data = res.get("data", []) or []
                        if not data:
                            break
                        for row in data:
                            if norm_order_no(row.get("platform_order_no") or row.get("amazon_order_id") or "") == target_no_norm:
                                found = row.get("order_number")
                                break
                        if found:
                            break
                        page += 1
                    if found:
                        break
                if found:
                    order_number = found
                    fields["lingxing_order_number"] = found
                    resolved += 1
        if not order_number:
            # fallback: multi-platform order list by platform_order_no/platform_order_name
            if target_no:
                lst = query_mp_rows(target_no, row_platform_code, row_store_id)
                if isinstance(lst, dict) and lst.get("__error__"):
                    if len(debug_misses) < 10:
                        debug_misses.append({"order_no": target_no, "stage": "query_mp_rows", "error": lst.get("__error__")})
                    lst = []
                if lst:
                    for row in lst:
                        # try match platform_order_no from item_info or platform_info
                        hit = False
                        for it in row.get("item_info", []) or []:
                            cand = it.get("platform_order_no")
                            if norm_order_no(cand) == target_no_norm:
                                hit = True
                                break
                        if not hit:
                            for it in row.get("platform_info", []) or []:
                                cand_no = it.get("platform_order_no")
                                cand_name = it.get("platform_order_name")
                                if norm_order_no(cand_no) == target_no_norm or norm_order_no(cand_name) == target_no_norm:
                                    hit = True
                                    break
                        if not hit:
                            ref_no = row.get("reference_no")
                            if norm_order_no(ref_no) == target_no_norm:
                                hit = True
                        if hit:
                            mp_row_hit = row
                            order_number = row.get("global_order_no") or row.get("original_global_order_no")
                            if order_number:
                                fields["global_order_no"] = order_number
                                resolved += 1
                            break
                    if not mp_row_hit:
                        mp_row_hit = lst[0]
                        order_number = mp_row_hit.get("global_order_no") or mp_row_hit.get("original_global_order_no")
        if not mp_row_hit and target_no:
            row = scan_mp_match(target_no_norm, row_platform_code, row_store_id)
            if row:
                mp_row_hit = row
                if not order_number:
                    order_number = row.get("global_order_no") or row.get("original_global_order_no")
                    if order_number:
                        resolved += 1
        ext_update = {}
        if order_number:
            detail = get_fbm_order_detail(access_token, app_id, str(order_number))
            detail_code = detail.get("code")
            if detail.get("code") == 0:
                ext_update = map_order_ext(detail)
                data_obj = detail.get("data") or {}
                detail_data_obj = data_obj
                # keep contact + logistics normalized
                phone = _clean(data_obj.get("receiver_mobile") or data_obj.get("receiver_tel"))
                if phone:
                    ext_update["电话"] = phone
                    ext_update["receiver_mobile"] = phone
                # logistics from FBM detail should be highest priority
                tn = _clean(data_obj.get("tracking_number"))
                if tn:
                    ext_update["联邦单号"] = tn
                lpn = _strict_carrier(data_obj.get("actual_carrier") or data_obj.get("logistics_provider_name") or data_obj.get("logistics_type_name"))
                if lpn:
                    ext_update["联邦方式"] = lpn
                # date fields (prefer date-only)
                if data_obj.get("shipment_date"):
                    ext_update["发货日"] = _to_ymd(data_obj.get("shipment_date"))
                if data_obj.get("purchase_time"):
                    ext_update["出单日期"] = _to_ymd(data_obj.get("purchase_time"))

        # supplement from mws detail by amazon order no
        mws_row = mws_map.get(target_no_norm)
        if mws_row:
            if not ext_update.get("发货日"):
                ext_update["发货日"] = _to_ymd(mws_row.get("latest_ship_date") or mws_row.get("shipment_date") or mws_row.get("shipment_date_local"))
            if not ext_update.get("送达日"):
                e = _to_ymd(mws_row.get("earliest_delivery_date"))
                l = _to_ymd(mws_row.get("latest_delivery_date"))
                if e and l:
                    ext_update["送达日"] = f"{e} - {l}"
                elif e or l:
                    ext_update["送达日"] = e or l
            if not ext_update.get("联邦单号"):
                tn = _clean(mws_row.get("tracking_number"))
                if tn:
                    ext_update["联邦单号"] = tn
            if ext_update.get("联邦单号"):
                t = str(ext_update.get("联邦单号")).upper()
                if t.startswith("1Z"):
                    ext_update["联邦方式"] = "UPS"
            if not ext_update.get("联邦方式"):
                svc = _strict_carrier(mws_row.get("ship_service_level") or mws_row.get("shipment_service_level_category"))
                if svc:
                    ext_update["联邦方式"] = svc
            items = mws_row.get("item_list") or []
            if items:
                it = items[0]
                if not ext_update.get("ASIN") and it.get("asin"):
                    ext_update["ASIN"] = it.get("asin")
                if not ext_update.get("MSKU") and it.get("seller_sku"):
                    ext_update["MSKU"] = it.get("seller_sku")
        # fallback logistics from mws/orders list when pb/mp and detail both empty
        if not ext_update.get("联邦单号") or not _strict_carrier(ext_update.get("联邦方式")):
            mws_logi = _query_logistics_from_mws_orders(target_no)
            if mws_logi.get("tracking_no") and not ext_update.get("联邦单号"):
                ext_update["联邦单号"] = mws_logi.get("tracking_no")
            if mws_logi.get("carrier_hint") and not _strict_carrier(ext_update.get("联邦方式")):
                ext_update["联邦方式"] = mws_logi.get("carrier_hint")
            if mws_logi.get("shipment_date") and not ext_update.get("发货日"):
                ext_update["发货日"] = mws_logi.get("shipment_date")
        # final fallback from FBM order list (sometimes has tracking while detail is empty)
        if not ext_update.get("联邦单号") or not _strict_carrier(ext_update.get("联邦方式")):
            fbm_logi = _query_logistics_from_fbm_list(target_no)
            if fbm_logi.get("tracking_no") and not ext_update.get("联邦单号"):
                ext_update["联邦单号"] = fbm_logi.get("tracking_no")
            if fbm_logi.get("carrier_hint") and not _strict_carrier(ext_update.get("联邦方式")):
                ext_update["联邦方式"] = fbm_logi.get("carrier_hint")
            if fbm_logi.get("shipment_date") and not ext_update.get("发货日"):
                ext_update["发货日"] = fbm_logi.get("shipment_date")
        # if detail missing address, fallback to mp order list by platform order no
        if not ext_update.get("address_line1") and (target_no):
            if mp_row_hit:
                row = mp_row_hit
                ext_update.update(build_ext_from_mp_row(row))
                used_list += 1
            else:
                lst = query_mp_rows(target_no, row_platform_code, row_store_id)
                if isinstance(lst, dict) and lst.get("__error__"):
                    if len(debug_misses) < 10:
                        debug_misses.append({"order_no": target_no, "stage": "query_mp_rows_fallback", "error": lst.get("__error__")})
                    lst = []
                if lst:
                    row = lst[0]
                    ext_update.update(build_ext_from_mp_row(row))
                    used_list += 1
        if not order_number and not ext_update:
            missing += 1
            if len(debug_misses) < 10:
                debug_misses.append({"order_no": target_no, "stage": "final_miss"})
            diagnostics.append({
                "id": oid,
                "order_no": target_no,
                "reason": "no_detail_and_no_list_data",
                "detail_code": detail_code,
                "has_mp_row": bool(mp_row_hit),
            })
            continue
        # build customer_address display if possible
        name = _clean(ext_update.get("receiver_name") or ext_update.get("buyer_name") or ext_update.get("customer_name"))
        line1 = _clean(ext_update.get("address_line1"))
        line2 = _clean(ext_update.get("address_line2"))
        line3 = _clean(ext_update.get("address_line3"))
        district = _clean(ext_update.get("district"))
        doorplate = _clean(ext_update.get("doorplate_no"))
        city = _clean(ext_update.get("city"))
        state = _clean(ext_update.get("state_or_region"))
        zip5 = _clean(ext_update.get("postal_code"))
        city_line = f"{city}, {state}".strip(", ")
        if zip5:
            city_line = f"{city_line} {zip5}".strip()
        addr_mid = " ".join([x for x in [line2, line3, district, doorplate] if x])
        customer_address = "\n".join([x for x in [name, line1, addr_mid, city_line] if x])
        phone_for_addr = _clean(ext_update.get("电话") or ext_update.get("receiver_mobile") or ext_update.get("receiver_tel"))
        if customer_address and phone_for_addr and f"电话: {phone_for_addr}" not in customer_address:
            customer_address = f"{customer_address}\n电话: {phone_for_addr}"
        if customer_address:
            ext_update["客户地址"] = customer_address
        # only fill missing
        final_update = {}
        force_keys = {
            "客户地址", "address_line1", "address_line2", "address_line3", "district", "doorplate_no",
            "city", "state_or_region", "postal_code", "receiver_name", "receiver_mobile", "电话",
            "发货日", "送达日", "出单日期", "联邦方式", "联邦单号",
        }
        for k, v in ext_update.items():
            v = _clean(v)
            if v in ("", " "):
                continue
            oldv = _clean(fields.get(k))
            if force_refresh and k in force_keys and oldv != v:
                final_update[k] = v
                continue
            if oldv in ("", " "):
                final_update[k] = v
            # allow replacing bad placeholder addresses
            if k == "客户地址" and ("none" in str(fields.get(k, "")).lower() or "null" in str(fields.get(k, "")).lower()):
                final_update[k] = v
            # allow replacing invalid long garbage logistics method with normalized carrier
            if k == "联邦方式":
                ov = _clean(fields.get(k))
                if ov and (len(ov) > 30 or "订单内容" in ov):
                    final_update[k] = v
            if k == "联邦方式":
                ov = _clean(fields.get(k))
                nv = _strict_carrier(v) or _norm_carrier(v)
                # upgrade from non-carrier labels like "AD US Dom 2"
                if ov and ov.upper() not in ("FEDEX", "UPS") and nv.upper() in ("FEDEX", "UPS"):
                    final_update[k] = nv
                # clear non-carrier garbage values
                if ov and ov.upper() not in ("FEDEX", "UPS") and (not _strict_carrier(v)):
                    final_update[k] = ""
            if k == "联邦单号":
                ov = _clean(fields.get(k))
                # replace short/invalid tracking with UPS/FedEx-like tracking id
                if ov and len(ov) < 10 and len(_clean(v)) >= 10:
                    final_update[k] = v
        # cleanup legacy non-carrier values when new payload has no valid carrier
        if force_refresh:
            old_carrier = _clean(fields.get("联邦方式"))
            new_carrier = _strict_carrier(ext_update.get("联邦方式"))
            if old_carrier and old_carrier.upper() not in ("FEDEX", "UPS") and not new_carrier:
                final_update["联邦方式"] = ""
        if final_update:
            crud.upsert_order_ext_bulk(db, oid, final_update)
            updated += 1
            updated_ids.append(oid)
            if final_update.get("客户地址"):
                updated_address += 1
            if final_update.get("发货日"):
                updated_ship_date += 1
            if final_update.get("送达日"):
                updated_deliver_date += 1
            diagnostics.append({
                "id": oid,
                "order_no": target_no,
                "updated_keys": list(final_update.keys())[:20],
                "detail_code": detail_code,
                "has_mp_row": bool(mp_row_hit),
                "logistics_from_mp": {
                    "actual_carrier": ((mp_row_hit or {}).get("logistics_info") or {}).get("actual_carrier"),
                    "logistics_provider_name": ((mp_row_hit or {}).get("logistics_info") or {}).get("logistics_provider_name"),
                    "logistics_type_name": ((mp_row_hit or {}).get("logistics_info") or {}).get("logistics_type_name"),
                    "tracking_no": ((mp_row_hit or {}).get("logistics_info") or {}).get("tracking_no"),
                    "waybill_no": ((mp_row_hit or {}).get("logistics_info") or {}).get("waybill_no"),
                    "status": ((mp_row_hit or {}).get("logistics_info") or {}).get("status"),
                },
                "logistics_from_detail": {
                    "tracking_number": detail_data_obj.get("tracking_number"),
                    "actual_carrier": detail_data_obj.get("actual_carrier"),
                    "logistics_provider_name": detail_data_obj.get("logistics_provider_name"),
                    "logistics_type_name": detail_data_obj.get("logistics_type_name"),
                },
                "logistics_from_mws_orders": mws_logistics_cache.get(target_no, {}),
                "logistics_from_fbm_list": fbm_list_logistics_cache.get(target_no, {}),
            })
        else:
            current_focus = {k: fields.get(k) for k in ["客户地址", "电话", "发货日", "送达日", "联邦方式", "联邦单号"]}
            next_focus = {k: ext_update.get(k) for k in ["客户地址", "电话", "发货日", "送达日", "联邦方式", "联邦单号"]}
            diagnostics.append({
                "id": oid,
                "order_no": target_no,
                "reason": "no_new_fields_to_update",
                "detail_code": detail_code,
                "has_mp_row": bool(mp_row_hit),
                "has_address_like": bool(ext_update.get("address_line1") or ext_update.get("city") or ext_update.get("客户地址")),
                "has_ship_like": bool(ext_update.get("发货日")),
                "has_deliver_like": bool(ext_update.get("送达日")),
                "current_focus": current_focus,
                "next_focus": next_focus,
                "logistics_from_mp": {
                    "actual_carrier": ((mp_row_hit or {}).get("logistics_info") or {}).get("actual_carrier"),
                    "logistics_provider_name": ((mp_row_hit or {}).get("logistics_info") or {}).get("logistics_provider_name"),
                    "logistics_type_name": ((mp_row_hit or {}).get("logistics_info") or {}).get("logistics_type_name"),
                    "tracking_no": ((mp_row_hit or {}).get("logistics_info") or {}).get("tracking_no"),
                    "waybill_no": ((mp_row_hit or {}).get("logistics_info") or {}).get("waybill_no"),
                    "status": ((mp_row_hit or {}).get("logistics_info") or {}).get("status"),
                },
                "logistics_from_detail": {
                    "tracking_number": detail_data_obj.get("tracking_number"),
                    "actual_carrier": detail_data_obj.get("actual_carrier"),
                    "logistics_provider_name": detail_data_obj.get("logistics_provider_name"),
                    "logistics_type_name": detail_data_obj.get("logistics_type_name"),
                },
                "logistics_from_mws_orders": mws_logistics_cache.get(target_no, {}),
                "logistics_from_fbm_list": fbm_list_logistics_cache.get(target_no, {}),
            })
    return {
        "ok": True,
        "updated": updated,
        "resolved": resolved,
        "missing": missing,
        "used_list": used_list,
        "missing_order_no": missing_order_no,
        "debug_misses": debug_misses,
        "updated_ids": updated_ids[:50],
        "db_file": str(db.bind.url.database) if getattr(db, "bind", None) and getattr(db.bind, "url", None) else "",
        "updated_address": updated_address,
        "updated_ship_date": updated_ship_date,
        "updated_deliver_date": updated_deliver_date,
        "diagnostics": diagnostics[:50],
        "effective_order_ids": ids[:200],
    }
