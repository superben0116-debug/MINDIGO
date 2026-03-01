from datetime import datetime, timedelta
import re
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app import models
from app.config_store import get_lingxing_config
from app.integrations.lingxing_client import (
    get_access_token,
    get_rma_manage_list,
    get_shop_list,
    get_mail_list,
    get_mail_detail,
)

router = APIRouter()
SHOP_NAME_ALIAS = {
    "NAIROLET-US": "亚丰源",
    "煌明科技-US": "煌明",
    "简丽欧-US": "简丽欧",
    "晨阳铺货A-US": "晨阳",
    "TIZAZO-US": "口福轩",
    "Kadaligh-US": "维利安",
    "口服轩-CA": "口福轩",
    "爱瑞柔-US": "爱瑞柔",
    "路蔻尔-US": "路蔻尔",
    "聚乐-US": "聚乐",
    "译文-US": "译文",
}


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _to_date(s: str | None) -> str:
    txt = str(s or "").strip()
    if not txt:
        return ""
    if len(txt) >= 10 and txt[4] == "-" and txt[7] == "-":
        return txt[:10]
    return txt


def _status_text(rec: dict) -> str:
    # 规则：有处理方式/操作时间明显晚于创建时间，视为已回复
    process_way = str(rec.get("processWayName") or rec.get("processWay") or "").strip()
    if process_way:
        return "已回复"
    ct = str(rec.get("createTime") or "").strip()
    ot = str(rec.get("operationTime") or "").strip()
    if ct and ot and ot > ct:
        return "已回复"
    return "待回复"


def _pick_first_str(src: dict, keys: list[str]) -> str:
    for k in keys:
        v = src.get(k)
        if v is None:
            continue
        txt = str(v).strip()
        if txt:
            return txt
    return ""


def _map_shop_name(v: str | None) -> str:
    txt = str(v or "").strip()
    if not txt:
        return ""
    return SHOP_NAME_ALIAS.get(txt, txt)


def _extract_email_like(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        txt = value.strip()
        m = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", txt)
        return m.group(0) if m else ""
    if isinstance(value, dict):
        # explicit keys first
        for k in ("email", "bind_email", "mail", "mailbox", "account_email", "shop_email", "store_email"):
            got = _extract_email_like(value.get(k))
            if got:
                return got
        # then scan all keys containing mail/email
        for k, v in value.items():
            lk = str(k).lower()
            if "mail" in lk or "email" in lk:
                got = _extract_email_like(v)
                if got:
                    return got
        # recursive fallback
        for _, v in value.items():
            got = _extract_email_like(v)
            if got:
                return got
    if isinstance(value, list):
        for x in value:
            got = _extract_email_like(x)
            if got:
                return got
    return ""


def _extract_shop_email(s: dict) -> str:
    direct = _pick_first_str(
        s,
        [
            "email",
            "bind_email",
            "mail",
            "mailbox",
            "account_email",
            "seller_email",
            "store_email",
            "shop_email",
        ],
    )
    if direct:
        return direct
    for parent in ("auth", "contact", "extra", "meta", "setting", "settings"):
        nested = s.get(parent)
        if not isinstance(nested, dict):
            continue
        hit = _pick_first_str(
            nested,
            [
                "email",
                "bind_email",
                "mail",
                "mailbox",
                "account_email",
            ],
        )
        if hit:
            return hit
    return _extract_email_like(s)


def _resolve_sid_list(access_token: str, app_id: str, cfg: dict, sid_payload) -> list[int]:
    sid = sid_payload or []
    if isinstance(sid, (str, int)):
        sid = [sid]
    out = [int(x) for x in sid if str(x).strip().isdigit()]
    if out:
        return out
    sid_list_cfg = str(cfg.get("sid_list") or "").strip()
    if sid_list_cfg and sid_list_cfg.upper() != "ALL":
        out = [int(x.strip()) for x in sid_list_cfg.split(",") if x.strip().isdigit()]
        if out:
            return out
    shops = get_shop_list(access_token, app_id)
    rows = shops.get("data") or []
    if isinstance(rows, dict):
        rows = rows.get("list") or rows.get("records") or rows.get("items") or []
    return [int(s.get("sid")) for s in (rows or []) if isinstance(s, dict) and str(s.get("sid") or "").isdigit()]


def _mail_map(cfg: dict) -> dict:
    mm = cfg.get("customer_mail_map")
    return mm if isinstance(mm, dict) else {}


def _emails_from_map(cfg: dict, sid_list: list[int] | None = None, shop_names: list[str] | None = None) -> list[str]:
    mm = _mail_map(cfg)
    out = []
    sid_list = sid_list or []
    shop_names = [str(x or "").strip() for x in (shop_names or []) if str(x or "").strip()]
    for sid in sid_list:
        hit = _extract_email_like(mm.get(str(sid)))
        if hit:
            out.append(hit)
    for nm in shop_names:
        hit = _extract_email_like(mm.get(nm))
        if hit:
            out.append(hit)
    return list(dict.fromkeys(out))


def _fetch_mail_items(access_token: str, app_id: str, payload: dict):
    emails = payload.get("emails") or []
    emails = [str(x).strip() for x in emails if str(x).strip()]
    if not emails:
        manual = str(payload.get("email_text") or "").strip()
        if manual:
            emails = [x.strip() for x in manual.replace(";", ",").split(",") if x.strip()]
    if not emails:
        return {"total": 0, "items": [], "debug": [{"stage": "mail", "error": "missing emails"}]}

    start_date = _to_date(payload.get("start_date")) or _to_date(payload.get("startTime"))
    end_date = _to_date(payload.get("end_date")) or _to_date(payload.get("endTime"))
    flag = str(payload.get("flag") or "receive").strip()
    offset = int(payload.get("offset") or 0)
    length = int(payload.get("length") or 50)

    items = []
    total = 0
    sent_subjects = set()
    debug = []

    for em in emails:
        body_sent = {
            "flag": "sent",
            "email": em,
            "start_date": start_date,
            "end_date": end_date,
            "offset": 0,
            "length": max(length, 100),
        }
        rs = get_mail_list(access_token, app_id, body_sent)
        if rs.get("code") == 0:
            debug.append({"email": em, "stage": "sent", "count": len(rs.get("data") or []), "total": rs.get("total", 0)})
            for r in (rs.get("data") or []):
                sub = str(r.get("subject") or "").strip().lower()
                if sub:
                    sent_subjects.add(sub)
        else:
            debug.append({"email": em, "stage": "sent", "error": rs})

    for em in emails:
        body = {
            "flag": flag,
            "email": em,
            "start_date": start_date,
            "end_date": end_date,
            "offset": offset,
            "length": length,
        }
        res = get_mail_list(access_token, app_id, body)
        if res.get("code") != 0:
            debug.append({"email": em, "stage": "list", "error": res})
            continue
        debug.append({"email": em, "stage": "list", "count": len(res.get("data") or []), "total": res.get("total", 0)})
        total += int(res.get("total") or 0)
        for r in (res.get("data") or []):
            subject = str(r.get("subject") or "")
            reply_status = "已回复" if subject.strip().lower() in sent_subjects else ("已回复" if flag == "sent" else "待回复")
            items.append(
                {
                    "webmail_uuid": r.get("webmail_uuid"),
                    "date": r.get("date"),
                    "subject": subject,
                    "from_name": r.get("from_name"),
                    "from_address": r.get("from_address"),
                    "to_name": r.get("to_name"),
                    "to_address": r.get("to_address"),
                    "has_attachment": r.get("has_attachment"),
                    "email": em,
                    "replyStatus": reply_status,
                    "source": "mail",
                }
            )
    return {"total": total, "items": items, "debug": debug}


@router.post("/rma/list")
def rma_list(payload: dict, db: Session = Depends(get_db)):
    cfg = get_lingxing_config(db)
    app_id = str(cfg.get("app_id") or "").strip()
    app_secret = str(cfg.get("app_secret") or "").strip()
    if not app_id or not app_secret:
        raise HTTPException(status_code=400, detail="missing app_id/app_secret")

    token = get_access_token(app_id, app_secret)
    if token.get("code") not in (200, "200"):
        raise HTTPException(status_code=400, detail=str(token))
    access_token = token.get("data", {}).get("access_token")

    sid_list_cfg = str(cfg.get("sid_list") or "").strip()
    sid = payload.get("sid")
    if not sid:
        if sid_list_cfg and sid_list_cfg.upper() != "ALL":
            sid = [int(x.strip()) for x in sid_list_cfg.split(",") if x.strip().isdigit()]
        else:
            sid = []
    if not sid:
        try:
            shops = get_shop_list(access_token, app_id)
            if shops.get("code") == 0:
                sid = [int(s.get("sid")) for s in (shops.get("data") or []) if str(s.get("sid") or "").isdigit()]
        except Exception:
            sid = []
    if not sid:
        raise HTTPException(status_code=400, detail="missing sid; please set sid_list in config or pass sid")

    today = datetime.utcnow().date()
    start = _to_date(payload.get("startTime")) or str(today - timedelta(days=30))
    end = _to_date(payload.get("endTime")) or str(today)

    req = {
        "sid": sid,
        "searchTimeFiled": str(payload.get("searchTimeFiled") or "operationTime"),
        "startTime": start,
        "endTime": end,
        "searchValue": payload.get("searchValue") or [""],
        "searchField": str(payload.get("searchField") or "msku"),
        "sortColumn": str(payload.get("sortColumn") or "operationTime"),
        "sortType": str(payload.get("sortType") or "desc"),
        "pageNum": int(payload.get("pageNum") or 1),
        "pageSize": int(payload.get("pageSize") or 20),
    }
    res = get_rma_manage_list(access_token, app_id, req)
    if res.get("code") != 0:
        raise HTTPException(status_code=400, detail=res)

    data = res.get("data") or {}
    records = data.get("records") or []
    items = []
    for r in records:
        items.append(
            {
                "id": r.get("id"),
                "rmaNo": r.get("rmaNo"),
                "createTime": r.get("createTime"),
                "operationTime": r.get("operationTime"),
                "amazonOrderId": r.get("amazonOrderId"),
                "asin": r.get("asin"),
                "sellerSku": r.get("sellerSku"),
                "sku": r.get("sku"),
                "itemName": r.get("itemName"),
                "sellerName": r.get("sellerName"),
                "country": r.get("country"),
                "buyerName": r.get("buyerName"),
                "buyerEmail": r.get("buyerEmail"),
                "remark": r.get("remark"),
                "channelSourceName": r.get("channelSourceName"),
                "afterSaleTypeName": r.get("afterSaleTypeName"),
                "processWayName": r.get("processWayName"),
                "replyStatus": _status_text(r),
            }
        )

    return {
        "total": data.get("total", 0),
        "pageNum": data.get("current", req["pageNum"]),
        "pageSize": data.get("size", req["pageSize"]),
        "pageCount": data.get("pageCount", 1),
        "items": items,
        "request_payload": req,
    }


@router.get("/shops")
def customer_service_shops(db: Session = Depends(get_db)):
    cfg = get_lingxing_config(db)
    app_id = str(cfg.get("app_id") or "").strip()
    app_secret = str(cfg.get("app_secret") or "").strip()
    if not app_id or not app_secret:
        raise HTTPException(status_code=400, detail="missing app_id/app_secret")
    token = get_access_token(app_id, app_secret)
    if token.get("code") not in (200, "200"):
        raise HTTPException(status_code=400, detail=str(token))
    access_token = token.get("data", {}).get("access_token")
    mm = _mail_map(cfg)
    shops = get_shop_list(access_token, app_id)
    if shops.get("code") != 0:
        raise HTTPException(status_code=400, detail=shops)
    rows = []
    raw_data = shops.get("data") or []
    if isinstance(raw_data, dict):
        raw_data = raw_data.get("list") or raw_data.get("records") or raw_data.get("items") or []
    seen = set()
    for s in raw_data:
        if not isinstance(s, dict):
            continue
        sid = s.get("sid")
        if sid is None:
            continue
        name = _pick_first_str(
            s,
            [
                "seller_name",
                "shop_name",
                "sellerName",
                "name",
                "store_name",
                "storeName",
                "shopName",
                "seller",
            ],
        )
        if not name:
            name = f"SID-{sid}"
        name = _map_shop_name(name)
        email = _extract_shop_email(s)
        if not email:
            email = _extract_email_like(mm.get(str(sid))) or _extract_email_like(mm.get(name))
        key = (str(sid), name, email)
        if key in seen:
            continue
        seen.add(key)
        rows.append(
            {
                "sid": int(sid),
                "shop_name": name,
                "email": email,
                "country": s.get("country") or "",
                "marketplace": s.get("marketplace") or "",
            }
        )
    # fallback: append shop names from internal orders so UI always has named stores
    try:
        order_shop_rows = db.query(models.InternalOrder.shop_name).distinct().all()
        for (sn,) in order_shop_rows:
            raw_name = str(sn or "").strip()
            if not raw_name:
                continue
            name = _map_shop_name(raw_name)
            key = ("", name, "")
            if any(str(r.get("shop_name") or "").strip() == name for r in rows):
                continue
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "sid": 0,
                    "shop_name": name,
                    "email": "",
                    "country": "",
                    "marketplace": "",
                    "source": "internal_orders",
                }
            )
    except Exception:
        pass
    rows.sort(key=lambda x: (0 if x.get("email") else 1, str(x.get("shop_name") or "")))
    return {
        "items": rows,
        "debug": {
            "raw_count": len(raw_data),
            "sample_keys": list(raw_data[0].keys()) if raw_data else [],
            "sample": raw_data[0] if raw_data else {},
            "shops_without_email": sum(1 for x in rows if not x.get("email")),
            "from_internal_orders": sum(1 for x in rows if x.get("source") == "internal_orders"),
            "mail_map_count": len(mm.keys()) if isinstance(mm, dict) else 0,
        },
    }


@router.get("/mail-map")
def get_customer_mail_map(db: Session = Depends(get_db)):
    cfg = get_lingxing_config(db)
    mm = _mail_map(cfg)
    return {"items": mm}


@router.post("/mail-map")
def set_customer_mail_map(payload: dict, db: Session = Depends(get_db)):
    mapping = payload.get("mapping") or {}
    if not isinstance(mapping, dict):
        raise HTTPException(status_code=400, detail="mapping must be object")
    # 保留可识别邮箱的项
    cleaned = {}
    for k, v in mapping.items():
        kk = str(k or "").strip()
        vv = _extract_email_like(v)
        if kk and vv:
            cleaned[kk] = vv
    cfg = get_lingxing_config(db)
    cfg["customer_mail_map"] = cleaned
    crud.set_config(db, "lingxing", cfg)
    return {"ok": True, "count": len(cleaned)}


@router.post("/reply/ai")
def ai_reply(payload: dict):
    text = str(payload.get("text") or "").strip()
    lang = str(payload.get("lang") or "en").strip().lower()
    if not text:
        return {"reply": ""}
    if lang == "zh":
        out = "您好，已收到您的反馈，我们会尽快为您处理并在24小时内回复处理方案。"
    else:
        out = "Thanks for your message. We have received your request and will provide a solution within 24 hours."
    return {"reply": out}


@router.post("/mail/list")
def mail_list(payload: dict, db: Session = Depends(get_db)):
    cfg = get_lingxing_config(db)
    app_id = str(cfg.get("app_id") or "").strip()
    app_secret = str(cfg.get("app_secret") or "").strip()
    if not app_id or not app_secret:
        raise HTTPException(status_code=400, detail="missing app_id/app_secret")
    token = get_access_token(app_id, app_secret)
    if token.get("code") not in (200, "200"):
        raise HTTPException(status_code=400, detail=str(token))
    access_token = token.get("data", {}).get("access_token")

    sid_list = [int(x) for x in (payload.get("sid") or []) if str(x).strip().isdigit()]
    map_emails = _emails_from_map(cfg, sid_list=sid_list, shop_names=payload.get("shop_names") or [])
    merged_payload = dict(payload or {})
    emails = [str(x).strip() for x in (merged_payload.get("emails") or []) if str(x).strip()]
    merged_payload["emails"] = list(dict.fromkeys(emails + map_emails))
    out = _fetch_mail_items(access_token, app_id, merged_payload)
    if map_emails:
        out.setdefault("debug", []).append({"stage": "mail_map", "emails": map_emails})
    out["items"].sort(key=lambda x: str(x.get("date") or ""), reverse=True)
    return out


@router.post("/inbox/list")
def inbox_list(payload: dict, db: Session = Depends(get_db)):
    cfg = get_lingxing_config(db)
    app_id = str(cfg.get("app_id") or "").strip()
    app_secret = str(cfg.get("app_secret") or "").strip()
    if not app_id or not app_secret:
        raise HTTPException(status_code=400, detail="missing app_id/app_secret")
    token = get_access_token(app_id, app_secret)
    if token.get("code") not in (200, "200"):
        raise HTTPException(status_code=400, detail=str(token))
    access_token = token.get("data", {}).get("access_token")

    sid_input = [int(x) for x in (payload.get("sid") or []) if str(x).strip().isdigit()]
    map_emails = _emails_from_map(cfg, sid_list=sid_input, shop_names=payload.get("shop_names") or [])
    merged_payload = dict(payload or {})
    emails = [str(x).strip() for x in (merged_payload.get("emails") or []) if str(x).strip()]
    merged_payload["emails"] = list(dict.fromkeys(emails + map_emails))
    mail_out = _fetch_mail_items(access_token, app_id, merged_payload)
    items = list(mail_out.get("items") or [])
    debug = [{"stage": "mail", "summary": {"count": len(items), "total": mail_out.get("total", 0), "debug": mail_out.get("debug", []), "map_emails": map_emails}}]
    total = int(mail_out.get("total") or 0)

    sid = _resolve_sid_list(access_token, app_id, cfg, payload.get("sid"))
    start = _to_date(payload.get("startTime")) or _to_date(payload.get("start_date")) or str((datetime.utcnow().date() - timedelta(days=30)))
    end = _to_date(payload.get("endTime")) or _to_date(payload.get("end_date")) or str(datetime.utcnow().date())
    req = {
        "sid": sid,
        "searchTimeFiled": str(payload.get("searchTimeFiled") or "operationTime"),
        "startTime": start,
        "endTime": end,
        "sortColumn": str(payload.get("sortColumn") or "operationTime"),
        "sortType": str(payload.get("sortType") or "desc"),
        "pageNum": int(payload.get("pageNum") or 1),
        "pageSize": int(payload.get("pageSize") or 100),
    }
    search_values = payload.get("searchValue")
    search_field = str(payload.get("searchField") or "").strip()
    if isinstance(search_values, list):
        clean_vals = [str(x).strip() for x in search_values if str(x).strip()]
        if clean_vals and search_field:
            req["searchValue"] = clean_vals
            req["searchField"] = search_field
    rma = get_rma_manage_list(access_token, app_id, req)
    debug.append({"stage": "rma", "attempt": 1, "request": req, "code": rma.get("code"), "message": rma.get("message")})
    # 回退1：若无数据，改用创建时间维度重试
    if rma.get("code") == 0 and int((rma.get("data") or {}).get("total") or 0) == 0:
        req2 = dict(req)
        req2["searchTimeFiled"] = "createTime"
        req2["sortColumn"] = "createTime"
        rma2 = get_rma_manage_list(access_token, app_id, req2)
        debug.append({"stage": "rma", "attempt": 2, "request": req2, "code": rma2.get("code"), "message": rma2.get("message")})
        if rma2.get("code") == 0 and int((rma2.get("data") or {}).get("total") or 0) > 0:
            rma = rma2
    # 回退2：若仍无数据，扩大时间窗到最近60天重试
    if rma.get("code") == 0 and int((rma.get("data") or {}).get("total") or 0) == 0:
        req3 = dict(req)
        req3["startTime"] = str((datetime.utcnow().date() - timedelta(days=60)))
        req3["endTime"] = str(datetime.utcnow().date())
        rma3 = get_rma_manage_list(access_token, app_id, req3)
        debug.append({"stage": "rma", "attempt": 3, "request": req3, "code": rma3.get("code"), "message": rma3.get("message")})
        if rma3.get("code") == 0 and int((rma3.get("data") or {}).get("total") or 0) > 0:
            rma = rma3
    if rma.get("code") == 0:
        data = rma.get("data") or {}
        recs = data.get("records") or []
        total += int(data.get("total") or 0)
        for r in recs:
            items.append(
                {
                    "webmail_uuid": f"RMA-{r.get('id')}",
                    "date": r.get("operationTime") or r.get("createTime"),
                    "subject": r.get("itemName") or f"RMA {r.get('rmaNo') or ''}".strip(),
                    "from_name": r.get("sellerName") or "",
                    "from_address": "",
                    "to_name": r.get("buyerName") or "",
                    "to_address": r.get("buyerEmail") or "",
                    "has_attachment": 0,
                    "email": "",
                    "replyStatus": _status_text(r),
                    "source": "rma",
                    "body": (r.get("remark") or "").strip() or (r.get("itemName") or ""),
                    "rmaNo": r.get("rmaNo"),
                    "amazonOrderId": r.get("amazonOrderId"),
                    "shopName": _map_shop_name(r.get("sellerName")),
                    "channelSourceName": r.get("channelSourceName"),
                }
            )
        debug.append({"stage": "rma", "records": len(recs), "total": data.get("total", 0)})
    else:
        debug.append({"stage": "rma", "error": rma})

    items.sort(key=lambda x: str(x.get("date") or ""), reverse=True)
    return {"total": total, "items": items, "debug": debug}


@router.post("/mail/detail")
def mail_detail(payload: dict, db: Session = Depends(get_db)):
    webmail_uuid = str(payload.get("webmail_uuid") or "").strip()
    if not webmail_uuid:
        raise HTTPException(status_code=400, detail="missing webmail_uuid")
    cfg = get_lingxing_config(db)
    app_id = str(cfg.get("app_id") or "").strip()
    app_secret = str(cfg.get("app_secret") or "").strip()
    if not app_id or not app_secret:
        raise HTTPException(status_code=400, detail="missing app_id/app_secret")
    token = get_access_token(app_id, app_secret)
    if token.get("code") not in (200, "200"):
        raise HTTPException(status_code=400, detail=str(token))
    access_token = token.get("data", {}).get("access_token")
    res = get_mail_detail(access_token, app_id, webmail_uuid)
    if res.get("code") != 0:
        raise HTTPException(status_code=400, detail=res)
    return {"item": res.get("data") or {}}


@router.post("/diagnose")
def customer_service_diagnose(payload: dict, db: Session = Depends(get_db)):
    """
    快速诊断客服邮件链路：
    1) 配置是否存在
    2) token是否可获取
    3) 店铺列表是否可读
    4) 邮件接口(sent/receive)是否返回数据
    """
    out = {
        "ok": False,
        "steps": [],
        "emails": [],
    }
    try:
        cfg = get_lingxing_config(db)
        app_id = str(cfg.get("app_id") or "").strip()
        app_secret = str(cfg.get("app_secret") or "").strip()
        out["steps"].append(
            {
                "stage": "config",
                "app_id_set": bool(app_id),
                "app_secret_set": bool(app_secret),
                "sid_list": str(cfg.get("sid_list") or "ALL"),
            }
        )
        if not app_id or not app_secret:
            return out

        token = get_access_token(app_id, app_secret)
        out["steps"].append({"stage": "token", "code": token.get("code"), "message": token.get("message")})
        if token.get("code") not in (200, "200"):
            return out
        access_token = token.get("data", {}).get("access_token")
        if not access_token:
            out["steps"].append({"stage": "token", "error": "missing access_token"})
            return out

        shops = get_shop_list(access_token, app_id)
        shop_data = shops.get("data") or []
        if isinstance(shop_data, dict):
            shop_data = shop_data.get("list") or shop_data.get("records") or shop_data.get("items") or []
        out["steps"].append(
            {
                "stage": "shops",
                "code": shops.get("code"),
                "count": len(shop_data) if isinstance(shop_data, list) else 0,
                "sample_keys": list(shop_data[0].keys()) if isinstance(shop_data, list) and shop_data else [],
            }
        )
        if shops.get("code") != 0:
            return out

        emails = payload.get("emails") or []
        emails = [str(x).strip() for x in emails if str(x).strip()]
        if not emails:
            # 自动从店铺提取前5个邮箱做诊断
            seen = set()
            for s in (shop_data if isinstance(shop_data, list) else []):
                if not isinstance(s, dict):
                    continue
                em = _extract_shop_email(s)
                if em and em not in seen:
                    emails.append(em)
                    seen.add(em)
                if len(emails) >= 5:
                    break
        out["emails"] = emails
        if not emails:
            out["steps"].append({"stage": "email_pick", "error": "no email found in shops"})
            return out

        start_date = _to_date(payload.get("start_date")) or str((datetime.utcnow() - timedelta(days=7)).date())
        end_date = _to_date(payload.get("end_date")) or str(datetime.utcnow().date())
        for em in emails:
            for flag in ("receive", "sent"):
                body = {
                    "flag": flag,
                    "email": em,
                    "start_date": start_date,
                    "end_date": end_date,
                    "offset": 0,
                    "length": 20,
                }
                rs = get_mail_list(access_token, app_id, body)
                out["steps"].append(
                    {
                        "stage": "mail_list",
                        "flag": flag,
                        "email": em,
                        "code": rs.get("code"),
                        "message": rs.get("message"),
                        "count": len(rs.get("data") or []) if isinstance(rs.get("data"), list) else 0,
                        "total": rs.get("total", 0),
                        "request": body,
                    }
                )
        out["ok"] = True
        return out
    except Exception as e:
        out["steps"].append({"stage": "exception", "error": str(e)})
        return out
