from fastapi import APIRouter, Depends, Body
from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.kapi_mapper import map_order_to_kapi_rows
from app.kapi_mapper import get_kapi_default_values
from app.xlsx_utils import write_xlsx
from app import models
from app.config_store import get_shipper_config
import os
from typing import Any
import re

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _normalize_tokens(payload) -> list[str]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        vals = payload.get("order_ids") or payload.get("ids") or []
    else:
        vals = payload
    if not isinstance(vals, list):
        vals = [vals]
    out = []
    for v in vals:
        s = str(v).strip()
        if not s:
            continue
        parts = [p.strip() for p in re.split(r"[\s,\uFF0C;]+", s) if p.strip()]
        out.extend(parts or [s])
    return out


def _resolve_order_token(db: Session, token: str) -> int | None:
    t = str(token or "").strip()
    if not t:
        return None
    if t.isdigit():
        row = db.query(models.InternalOrder.id).filter(models.InternalOrder.id == int(t)).first()
        if row:
            return int(t)
    row = db.query(models.InternalOrder.id).filter(models.InternalOrder.platform_order_no == t).first()
    if row:
        return int(row[0])
    row = db.query(models.InternalOrder.id).filter(models.InternalOrder.internal_order_no == t).first()
    if row:
        return int(row[0])
    # fallback: match ext sequence fields such as 序列/seq
    ext_rows = db.query(models.InternalOrderExt.internal_order_id, models.InternalOrderExt.fields).all()
    for oid, fields in ext_rows:
        f = fields or {}
        seq = f.get("序列")
        if seq is None:
            seq = f.get("seq")
        if str(seq or "").strip() == t:
            return int(oid)
    return None


def _resolve_template_path() -> str:
    # routers/kapi_exports.py -> app/backend/app/routers
    # project root should be 4 levels up
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
    p1 = os.path.join(project_root, "docs", "inputs", "kapi_template.xlsx")
    if os.path.exists(p1):
        return p1
    # backward compatibility fallback
    p2 = os.path.join(project_root, "app", "docs", "inputs", "kapi_template.xlsx")
    if os.path.exists(p2):
        return p2
    return p1


@router.get("/template")
def get_kapi_template():
    return {"template": "kapi_template.xlsx"}


@router.get("/template-header")
def get_kapi_template_header(db: Session = Depends(get_db)):
    template_path = _resolve_template_path()
    shipper = get_shipper_config(db)
    header, defaults = get_kapi_default_values(template_path, shipper)
    return {"header": header, "defaults": defaults, "template_path": template_path}


@router.post("/preview")
def preview_kapi_export(order_ids: Any = Body(...), db: Session = Depends(get_db)):
    raw_input = order_ids
    order_ids = _normalize_tokens(order_ids)
    template_path = _resolve_template_path()
    header = []
    rows = []
    row_order_ids = []
    debug = []
    resolved_ids = []
    for raw in order_ids:
        token = str(raw).strip()
        if not token:
            continue
        oid = _resolve_order_token(db, token)
        if oid is None:
            debug.append({"input": token, "resolved": None, "row_count": 0})
            continue
        resolved_ids.append(oid)
    for oid in resolved_ids:
        h, r = map_order_to_kapi_rows(db, oid, template_path)
        debug.append({"order_id": oid, "row_count": len(r or [])})
        if not header:
            header = h
        rows.extend(r)
        row_order_ids.extend([oid] * len(r or []))
    if not rows and header:
        rows = [["" for _ in header]]
    if not order_ids:
        debug.append({"reason": "empty_input_after_normalize", "raw_input": raw_input})
    if not resolved_ids and order_ids:
        sample = db.query(models.InternalOrder.id, models.InternalOrder.platform_order_no, models.InternalOrder.internal_order_no).order_by(models.InternalOrder.id.desc()).limit(8).all()
        debug.append(
            {
                "reason": "no_resolved_ids",
                "hints": [
                    "可输入内部ID",
                    "可输入平台订单号",
                    "可输入内部订单号",
                ],
                "sample_recent_orders": [
                    {"id": int(x[0]), "platform_order_no": x[1], "internal_order_no": x[2]} for x in sample
                ],
            }
        )
    return {
        "header": header,
        "rows": rows,
        "debug": debug,
        "total_rows": len(rows),
        "resolved_ids": resolved_ids,
        "row_order_ids": row_order_ids,
        "input_tokens": order_ids,
        "raw_input": raw_input,
    }


@router.post("/")
def create_kapi_export(order_ids: Any = Body(...), db: Session = Depends(get_db)):
    raw_input = order_ids
    order_ids = _normalize_tokens(order_ids)
    template_path = _resolve_template_path()
    header = []
    rows = []
    row_order_ids = []
    debug = []
    resolved_ids = []
    for raw in order_ids:
        token = str(raw).strip()
        if not token:
            continue
        oid = _resolve_order_token(db, token)
        if oid is None:
            debug.append({"input": token, "resolved": None, "row_count": 0})
            continue
        resolved_ids.append(oid)
    for oid in resolved_ids:
        h, r = map_order_to_kapi_rows(db, oid, template_path)
        debug.append({"order_id": oid, "row_count": len(r or [])})
        if not header:
            header = h
        rows.extend(r)
        row_order_ids.extend([oid] * len(r or []))
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
    export_dir = os.path.join(project_root, "app", "backend", "exports")
    os.makedirs(export_dir, exist_ok=True)
    export_path = os.path.join(export_dir, "kapi_export.xlsx")
    write_xlsx(export_path, header, rows)
    if not order_ids:
        debug.append({"reason": "empty_input_after_normalize", "raw_input": raw_input})
    if not resolved_ids and order_ids:
        sample = db.query(models.InternalOrder.id, models.InternalOrder.platform_order_no, models.InternalOrder.internal_order_no).order_by(models.InternalOrder.id.desc()).limit(8).all()
        debug.append(
            {
                "reason": "no_resolved_ids",
                "hints": [
                    "可输入内部ID",
                    "可输入平台订单号",
                    "可输入内部订单号",
                ],
                "sample_recent_orders": [
                    {"id": int(x[0]), "platform_order_no": x[1], "internal_order_no": x[2]} for x in sample
                ],
            }
        )
    return {
        "export_batch_no": "KAPI-0001",
        "file": export_path,
        "debug": debug,
        "total_rows": len(rows),
        "resolved_ids": resolved_ids,
        "row_order_ids": row_order_ids,
        "input_tokens": order_ids,
        "raw_input": raw_input,
    }


@router.post("/from-rows")
def create_kapi_export_from_rows(payload: dict = Body(...)):
    header = payload.get("header") or []
    rows = payload.get("rows") or []
    if not isinstance(header, list) or not isinstance(rows, list):
        return {"error": "invalid_payload"}
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))
    export_dir = os.path.join(project_root, "app", "backend", "exports")
    os.makedirs(export_dir, exist_ok=True)
    export_path = os.path.join(export_dir, "kapi_export.xlsx")
    write_xlsx(export_path, header, rows)
    return {"export_batch_no": "KAPI-0001", "file": export_path, "total_rows": len(rows)}
