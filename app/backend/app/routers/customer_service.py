from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.config_store import get_lingxing_config
from app.integrations.lingxing_client import (
    get_access_token,
    get_rma_manage_list,
    get_shop_list,
    get_mail_list,
    get_mail_detail,
)

router = APIRouter()


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
    shops = get_shop_list(access_token, app_id)
    if shops.get("code") != 0:
        raise HTTPException(status_code=400, detail=shops)
    rows = []
    for s in (shops.get("data") or []):
        sid = s.get("sid")
        if sid is None:
            continue
        rows.append(
            {
                "sid": int(sid),
                "shop_name": s.get("seller_name") or s.get("shop_name") or "",
                "email": s.get("email")
                or s.get("bind_email")
                or s.get("mail")
                or s.get("mailbox")
                or s.get("account_email")
                or "",
                "country": s.get("country") or "",
                "marketplace": s.get("marketplace") or "",
            }
        )
    return {"items": rows}


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

    emails = payload.get("emails") or []
    emails = [str(x).strip() for x in emails if str(x).strip()]
    if not emails:
        raise HTTPException(status_code=400, detail="missing emails")

    start_date = _to_date(payload.get("start_date")) or _to_date(payload.get("startTime"))
    end_date = _to_date(payload.get("end_date")) or _to_date(payload.get("endTime"))
    flag = str(payload.get("flag") or "receive").strip()
    offset = int(payload.get("offset") or 0)
    length = int(payload.get("length") or 50)

    items = []
    total = 0
    sent_subjects = set()
    debug = []

    # 先取 sent，建立“已回复”对照
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
                }
            )
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
