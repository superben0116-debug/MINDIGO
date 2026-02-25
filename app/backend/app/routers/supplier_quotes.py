from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session
from app.db import SessionLocal
from app import crud, models
from app.quote_templates import build_supplier_visible_payload
from app.auth import get_current_user
from datetime import datetime
import re

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _get_factory_rule(db: Session, supplier_name: str) -> tuple[str, int]:
    # default rule
    if supplier_name == "吉嘉":
        default = ("C", 300)
    else:
        default = ("F", 1)
    cfg = crud.get_config(db, "supplier_factory_rules")
    rules = cfg.config_value if cfg and isinstance(cfg.config_value, dict) else {}
    rule = rules.get(supplier_name) if isinstance(rules, dict) else None
    if isinstance(rule, dict):
        p = str(rule.get("prefix") or default[0]).strip() or default[0]
        try:
            start = int(rule.get("start") or default[1])
        except Exception:
            start = default[1]
        return p, start
    return default


def _next_factory_no(db: Session, supplier_name: str) -> str:
    prefix, start = _get_factory_rule(db, supplier_name)
    quotes = db.query(models.SupplierQuoteRequest).order_by(models.SupplierQuoteRequest.id.desc()).all()
    max_no = start - 1
    for q in quotes:
        vp = q.visible_payload or {}
        if str(vp.get("supplier_name") or "") != supplier_name:
            continue
        fn = str(vp.get("factory_no") or "")
        m = re.match(rf"^{re.escape(prefix)}(\d+)$", fn)
        if m:
            try:
                max_no = max(max_no, int(m.group(1)))
            except Exception:
                pass
    return f"{prefix}{max_no + 1}"


@router.get("/{quote_no}")
def get_supplier_quote(quote_no: str, request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    req = crud.get_quote_request_by_no(db, quote_no)
    if not req:
        raise HTTPException(status_code=404, detail="quote not found")
    if user.role == "supplier":
        sup = str((req.visible_payload or {}).get("supplier_name") or "").strip()
        if not user.supplier_name or sup != str(user.supplier_name).strip():
            raise HTTPException(status_code=403, detail="forbidden")
    ext_obj = crud.get_order_ext(db, req.internal_order_id)
    fields = (ext_obj.fields if ext_obj else {})
    rsp = db.query(models.SupplierQuoteResponse).filter(models.SupplierQuoteResponse.quote_request_id == req.id).order_by(models.SupplierQuoteResponse.id.desc()).first()
    vp = req.visible_payload or {}
    items = list(vp.get("items", []) or [])
    if items:
        order_items = crud.get_order_items(db, req.internal_order_id)
        ext_obj2 = crud.get_order_ext(db, req.internal_order_id)
        extf = ext_obj2.fields if ext_obj2 and ext_obj2.fields else {}
        fallback_img = (order_items[0].product_image if order_items and order_items[0].product_image not in (None, "", "/") else None) or extf.get("产品图") or extf.get("product_image")
        if fallback_img:
            for it in items:
                if not it.get("image_url") or it.get("image_url") == "/":
                    it["image_url"] = fallback_img
    return {
        "quote_no": req.quote_no,
        "items": items,
        "supplier_name": vp.get("supplier_name"),
        "order_id": req.internal_order_id,
        "factory_no": fields.get("工厂内部单号") or fields.get("factory_model") or vp.get("factory_no") or req.quote_no,
        "marks": fields.get("箱唛") or fields.get("marks") or vp.get("base_marks"),
        "seq": vp.get("seq"),
        "order_date": vp.get("order_date"),
        "ship_date": vp.get("ship_date") or fields.get("供应商出货日期") or fields.get("发货日期") or "",
        "unit_price": str(rsp.quoted_unit_price) if rsp and rsp.quoted_unit_price is not None else (vp.get("quoted_unit_price") or ""),
        "supplier_input": {
            "quoted_unit_price": str(rsp.quoted_unit_price) if rsp and rsp.quoted_unit_price is not None else (vp.get("quoted_unit_price") or None),
            "quoted_total_price": None,
            "lead_time_days": None,
            "supplier_remark": vp.get("supplier_remark") or (rsp.supplier_remark if rsp else "") or ""
        }
    }


@router.get("/")
def list_supplier_quotes(
    supplier_name: str | None = None,
    status: str | None = None,
    keyword: str | None = None,
    request: Request = None,
    db: Session = Depends(get_db),
):
    user = get_current_user(request, db)
    if user.role == "supplier":
        supplier_name = user.supplier_name or user.username
    all_quotes = db.query(models.SupplierQuoteRequest).order_by(models.SupplierQuoteRequest.created_at.desc()).all()
    items = []
    normalized_supplier = (supplier_name or "").strip().lower()
    normalized_status = (status or "").strip().lower()
    kw = (keyword or "").strip().lower()
    for q in all_quotes:
        sup = (q.visible_payload or {}).get("supplier_name")
        sup_norm = str(sup or "").strip().lower()
        if normalized_supplier and sup_norm != normalized_supplier:
            continue
        if normalized_status and str(q.quote_status or "").strip().lower() != normalized_status:
            continue
        payload_items = (q.visible_payload or {}).get("items", [])
        first_item = payload_items[0] if payload_items else {}
        if not first_item.get("image_url") or first_item.get("image_url") == "/":
            oitems = crud.get_order_items(db, q.internal_order_id)
            oext = crud.get_order_ext(db, q.internal_order_id)
            extf = oext.fields if oext and oext.fields else {}
            fallback_img = (oitems[0].product_image if oitems and oitems[0].product_image not in (None, "", "/") else None) or extf.get("产品图") or extf.get("product_image")
            if fallback_img:
                first_item = dict(first_item)
                first_item["image_url"] = fallback_img
        first_name = str(first_item.get("product_name") or "")
        if kw and kw not in str(q.quote_no or "").lower() and kw not in first_name.lower():
            continue
        rsp = db.query(models.SupplierQuoteResponse).filter(models.SupplierQuoteResponse.quote_request_id == q.id).order_by(models.SupplierQuoteResponse.id.desc()).first()
        items.append({
            "quote_no": q.quote_no,
            "supplier_name": sup,
            "assigned_supplier": (q.visible_payload or {}).get("assigned_supplier"),
            "status": q.quote_status,
            "order_id": q.internal_order_id,
            "image_url": first_item.get("image_url"),
            "product_name": first_item.get("product_name"),
            "quantity": first_item.get("quantity"),
            "dimension": first_item.get("dimension"),
            "dimension_unit": first_item.get("dimension_unit"),
            "marks": first_item.get("marks"),
            "seq": (q.visible_payload or {}).get("seq"),
            "order_date": (q.visible_payload or {}).get("order_date"),
            "factory_no": (q.visible_payload or {}).get("factory_no") or q.quote_no,
            "unit_price": str(rsp.quoted_unit_price) if rsp and rsp.quoted_unit_price is not None else "",
            "ship_date": (q.visible_payload or {}).get("ship_date", ""),
            "remark": first_item.get("remark") or "",
            "created_at": q.created_at,
        })
    return {
        "items": items,
        "debug": {
            "supplier_filter": supplier_name or "",
            "matched": len(items),
            "all_count": len(all_quotes),
        },
    }


def _upsert_quote_response_price(db: Session, req: models.SupplierQuoteRequest, price_val):
    if price_val is None or str(price_val).strip() == "":
        return
    rsp = (
        db.query(models.SupplierQuoteResponse)
        .filter(models.SupplierQuoteResponse.quote_request_id == req.id)
        .order_by(models.SupplierQuoteResponse.id.desc())
        .first()
    )
    if rsp:
        rsp.quoted_unit_price = price_val
        rsp.submitted_at = datetime.utcnow()
        return
    crud.submit_quote_response(
        db,
        req.id,
        {
            "quoted_unit_price": price_val,
            "quoted_total_price": None,
            "lead_time_days": None,
            "supplier_remark": "",
        },
    )


@router.post("/actions/batch-update")
def batch_update_supplier_quotes(payload: dict, request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    updates = payload.get("updates") or []
    if not isinstance(updates, list) or not updates:
        raise HTTPException(status_code=400, detail="missing updates")
    changed = []
    skipped = []
    for u in updates:
        quote_no = str((u or {}).get("quote_no") or "").strip()
        if not quote_no:
            skipped.append({"quote_no": "", "reason": "missing_quote_no"})
            continue
        req = crud.get_quote_request_by_no(db, quote_no)
        if not req:
            skipped.append({"quote_no": quote_no, "reason": "not_found"})
            continue
        if user.role == "supplier":
            sup = str((req.visible_payload or {}).get("supplier_name") or "").strip()
            if not user.supplier_name or sup != str(user.supplier_name).strip():
                skipped.append({"quote_no": quote_no, "reason": "forbidden"})
                continue
        vp = dict(req.visible_payload or {})
        touched = []
        if "quoted_unit_price" in u:
            vp["quoted_unit_price"] = u.get("quoted_unit_price")
            _upsert_quote_response_price(db, req, u.get("quoted_unit_price"))
            touched.append("quoted_unit_price")
        if "factory_no" in u:
            vp["factory_no"] = u.get("factory_no")
            touched.append("factory_no")
        if "ship_date" in u:
            vp["ship_date"] = u.get("ship_date")
            touched.append("ship_date")
        if "supplier_remark" in u:
            vp["supplier_remark"] = u.get("supplier_remark")
            touched.append("supplier_remark")
        if isinstance(u.get("items"), list):
            vp["items"] = u.get("items")
            touched.append("items")
        req.visible_payload = vp
        req.updated_at = datetime.utcnow()
        changed.append({"quote_no": quote_no, "updated_keys": touched})
    db.commit()
    return {"ok": True, "updated": len(changed), "changed": changed, "skipped": skipped}


@router.post("/{quote_no}/submit")
def submit_supplier_quote(quote_no: str, payload: dict, request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    req = crud.get_quote_request_by_no(db, quote_no)
    if not req:
        raise HTTPException(status_code=404, detail="quote not found")
    if user.role == "supplier":
        sup = str((req.visible_payload or {}).get("supplier_name") or "").strip()
        if not user.supplier_name or sup != str(user.supplier_name).strip():
            raise HTTPException(status_code=403, detail="forbidden")
    crud.submit_quote_response(db, req.id, payload)
    crud.update_quote_status(db, req.id, "submitted")
    # write back to internal order ext
    if req.internal_order_id:
        to_upsert = {}
        if payload.get("factory_no"):
            to_upsert["factory_model"] = payload.get("factory_no")
            to_upsert["工厂内部单号"] = payload.get("factory_no")
        if payload.get("marks"):
            to_upsert["marks"] = payload.get("marks")
            to_upsert["箱唛"] = payload.get("marks")
        if payload.get("quoted_unit_price"):
            to_upsert["quoted_unit_price"] = payload.get("quoted_unit_price")
            to_upsert["单价"] = payload.get("quoted_unit_price")
            to_upsert["单价（元）"] = payload.get("quoted_unit_price")
            to_upsert["unit_price"] = payload.get("quoted_unit_price")
        if payload.get("ship_date"):
            to_upsert["供应商出货日期"] = payload.get("ship_date")
        if payload.get("supplier_remark"):
            to_upsert["备注"] = payload.get("supplier_remark")
        if to_upsert:
            crud.upsert_order_ext_bulk(db, req.internal_order_id, to_upsert)
    return {"quote_no": quote_no, "status": "submitted"}


@router.patch("/{quote_no}/draft")
def save_supplier_quote_draft(quote_no: str, payload: dict, request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    req = crud.get_quote_request_by_no(db, quote_no)
    if not req:
        raise HTTPException(status_code=404, detail="quote not found")
    if user.role == "supplier":
        sup = str((req.visible_payload or {}).get("supplier_name") or "").strip()
        if not user.supplier_name or sup != str(user.supplier_name).strip():
            raise HTTPException(status_code=403, detail="forbidden")

    vp = dict(req.visible_payload or {})
    if payload.get("factory_no") is not None:
        vp["factory_no"] = payload.get("factory_no")
    if payload.get("order_date") is not None:
        vp["order_date"] = payload.get("order_date")
    if payload.get("ship_date") is not None:
        vp["ship_date"] = payload.get("ship_date")
    if payload.get("quoted_unit_price") is not None:
        vp["quoted_unit_price"] = payload.get("quoted_unit_price")
    if payload.get("supplier_remark") is not None:
        vp["supplier_remark"] = payload.get("supplier_remark")
    if isinstance(payload.get("items"), list):
        vp["items"] = payload.get("items")

    req.visible_payload = vp
    req.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(req)
    return {"quote_no": quote_no, "status": req.quote_status, "saved": True}


@router.post("/batch-create")
def batch_create_quotes(payload: dict, request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="admin_required")
    order_ids = payload.get("order_ids") or []
    suppliers = payload.get("suppliers") or []
    supplier_rules = payload.get("supplier_rules") or {}
    if not suppliers:
        suppliers = ["默认供应商"]
    if isinstance(supplier_rules, dict) and supplier_rules:
        cfg = crud.get_config(db, "supplier_factory_rules")
        old = cfg.config_value if cfg and isinstance(cfg.config_value, dict) else {}
        merged = dict(old)
        for k, v in supplier_rules.items():
            if isinstance(v, dict):
                try:
                    s = int(v.get("start") or 1)
                except Exception:
                    s = 1
                merged[str(k)] = {"prefix": str(v.get("prefix") or ""), "start": s}
        crud.set_config(db, "supplier_factory_rules", merged)
    quote_nos = []
    debug_created = []
    missing_marks = []
    # generate sequential F+number
    existing = db.query(models.SupplierQuoteRequest.quote_no).all()
    max_no = 0
    for (qn,) in existing:
        if not qn:
            continue
        m = re.match(r"F(\d+)$", str(qn))
        if m:
            try:
                max_no = max(max_no, int(m.group(1)))
            except Exception:
                pass
    for order_id in order_ids:
        order = crud.get_internal_order(db, order_id)
        order_date = order.purchase_time.strftime("%Y-%m-%d") if order and order.purchase_time else ""
        seq = order_id
        for sup in suppliers:
            max_no += 1
            quote_no = f"F{max_no}"
            visible_payload = build_supplier_visible_payload(db, order_id)
            visible_payload["supplier_name"] = sup
            visible_payload["order_date"] = order_date
            visible_payload["seq"] = seq
            visible_payload["factory_no"] = _next_factory_no(db, sup)
            req = crud.create_quote_request(db, order_id, quote_no, visible_payload)
            quote_nos.append(req.quote_no)
            mdbg = (visible_payload or {}).get("mark_debug") or {}
            if mdbg.get("missing"):
                missing_marks.append(
                    {
                        "quote_no": req.quote_no,
                        "order_id": order_id,
                        "supplier_name": sup,
                        "source_product_name": mdbg.get("source_product_name") or "",
                    }
                )
            debug_created.append(
                {
                    "quote_no": req.quote_no,
                    "order_id": order_id,
                    "supplier_name": sup,
                    "items_len": len((visible_payload or {}).get("items") or []),
                    "base_marks": (visible_payload or {}).get("base_marks") or "",
                }
            )
    return {
        "quote_nos": quote_nos,
        "debug": {
            "requested_order_ids": order_ids,
            "requested_suppliers": suppliers,
            "created_count": len(quote_nos),
            "created": debug_created,
            "missing_marks": missing_marks,
        },
    }


@router.post("/check-existing")
def check_existing_quotes(payload: dict, db: Session = Depends(get_db)):
    order_ids = payload.get("order_ids") or []
    if not order_ids:
        return {"items": [], "count": 0}
    rows = (
        db.query(models.SupplierQuoteRequest)
        .filter(models.SupplierQuoteRequest.internal_order_id.in_(order_ids))
        .order_by(models.SupplierQuoteRequest.id.desc())
        .all()
    )
    out = []
    for r in rows:
        vp = r.visible_payload or {}
        out.append(
            {
                "quote_no": r.quote_no,
                "order_id": r.internal_order_id,
                "supplier_name": vp.get("supplier_name"),
                "status": r.quote_status,
            }
        )
    return {"items": out, "count": len(out)}


@router.delete("/{quote_no}")
def delete_supplier_quote(quote_no: str, request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="admin_required")
    req = crud.get_quote_request_by_no(db, quote_no)
    if not req:
        raise HTTPException(status_code=404, detail="quote not found")
    db.query(models.SupplierQuoteResponse).filter(models.SupplierQuoteResponse.quote_request_id == req.id).delete()
    db.delete(req)
    db.commit()
    return {"ok": True, "quote_no": quote_no}


@router.post("/repair-marks")
def repair_quote_marks(payload: dict, request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="admin_required")
    quote_nos = payload.get("quote_nos") or []
    if not quote_nos:
        raise HTTPException(status_code=400, detail="missing quote_nos")
    fixed = 0
    debug = []
    for qn in quote_nos:
        req = crud.get_quote_request_by_no(db, str(qn))
        if not req:
            debug.append({"quote_no": qn, "status": "not_found"})
            continue
        new_payload = build_supplier_visible_payload(db, req.internal_order_id)
        old = dict(req.visible_payload or {})
        # preserve business fields
        for k in ("supplier_name", "assigned_supplier", "factory_no", "order_date", "seq", "ship_date", "quoted_unit_price", "supplier_remark"):
            if old.get(k) is not None:
                new_payload[k] = old.get(k)
        req.visible_payload = new_payload
        req.updated_at = datetime.utcnow()
        base = (new_payload or {}).get("base_marks") or ""
        debug.append({"quote_no": qn, "status": "ok", "base_marks": base, "missing": (base == "")})
        if base:
            fixed += 1
    db.commit()
    return {"ok": True, "processed": len(quote_nos), "fixed": fixed, "debug": debug}


@router.post("/confirm")
def confirm_supplier_quotes(payload: dict, request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="admin_required")
    quote_nos = payload.get("quote_nos") or []
    supplier_name = (payload.get("supplier_name") or "").strip()
    if not quote_nos:
        raise HTTPException(status_code=400, detail="missing quote_nos")
    updated = 0
    for qn in quote_nos:
        req = crud.get_quote_request_by_no(db, str(qn))
        if not req:
            continue
        vp = dict(req.visible_payload or {})
        req.quote_status = "confirmed"
        if supplier_name:
            # 仅记录订单分配供应商，不改报价原始归属供应商
            vp["assigned_supplier"] = supplier_name
        else:
            vp["assigned_supplier"] = vp.get("supplier_name")
        req.visible_payload = vp
        req.updated_at = datetime.utcnow()
        updated += 1
    db.commit()
    return {"ok": True, "confirmed": updated, "quote_nos": quote_nos, "supplier_name": supplier_name}
