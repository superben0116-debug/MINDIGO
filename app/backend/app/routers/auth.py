import os
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session
from app.db import SessionLocal
from app import models, crud
from app.auth import (
    SESSION_COOKIE,
    create_session,
    delete_session,
    get_current_user,
    require_admin,
    verify_password,
    make_password_hash,
)

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/login")
def login(payload: dict, response: Response, db: Session = Depends(get_db)):
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "")
    expect_role = str(payload.get("role") or "").strip().lower()
    if not username or not password:
        raise HTTPException(status_code=400, detail="missing username/password")
    user = db.query(models.AuthUser).filter(models.AuthUser.username == username).first()
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="invalid_credentials")
    if not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="invalid_credentials")
    if expect_role and user.role != expect_role:
        raise HTTPException(status_code=403, detail="role_mismatch")
    sess = create_session(db, user.id)
    cookie_secure = str(os.getenv("ERP_COOKIE_SECURE", "0")).strip().lower() in ("1", "true", "yes")
    response.set_cookie(
        key=SESSION_COOKIE,
        value=sess.token,
        httponly=True,
        samesite="lax",
        secure=cookie_secure,
        path="/",
        max_age=7 * 24 * 3600,
    )
    return {
        "ok": True,
        "user": {
            "id": user.id,
            "username": user.username,
            "role": user.role,
            "supplier_name": user.supplier_name,
        },
    }


@router.post("/logout")
def logout(request: Request, response: Response, db: Session = Depends(get_db)):
    token = request.cookies.get(SESSION_COOKIE) or ""
    delete_session(db, token)
    response.delete_cookie(SESSION_COOKIE, path="/")
    return {"ok": True}


@router.get("/me")
def me(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    return {
        "ok": True,
        "user": {
            "id": user.id,
            "username": user.username,
            "role": user.role,
            "supplier_name": user.supplier_name,
        },
    }


@router.get("/users")
def list_users(request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    rows = db.query(models.AuthUser).order_by(models.AuthUser.id.asc()).all()
    return {
        "items": [
            {
                "id": x.id,
                "username": x.username,
                "role": x.role,
                "supplier_name": x.supplier_name,
                "is_active": bool(x.is_active),
            }
            for x in rows
        ]
    }


@router.post("/users")
def create_user(payload: dict, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "").strip()
    role = str(payload.get("role") or "supplier").strip().lower()
    supplier_name = str(payload.get("supplier_name") or "").strip() or None
    if role not in ("admin", "supplier"):
        raise HTTPException(status_code=400, detail="invalid role")
    if not username or not password:
        raise HTTPException(status_code=400, detail="missing username/password")
    exists = db.query(models.AuthUser.id).filter(models.AuthUser.username == username).first()
    if exists:
        raise HTTPException(status_code=400, detail="username_exists")
    now = datetime.utcnow()
    user = models.AuthUser(
        username=username,
        password_hash=make_password_hash(password),
        role=role,
        supplier_name=supplier_name,
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"ok": True, "id": user.id}


@router.patch("/users/{user_id}")
def update_user(user_id: int, payload: dict, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    user = db.query(models.AuthUser).filter(models.AuthUser.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="user_not_found")
    if payload.get("password"):
        user.password_hash = make_password_hash(str(payload.get("password")))
    if payload.get("supplier_name") is not None:
        user.supplier_name = str(payload.get("supplier_name") or "").strip() or None
    if payload.get("is_active") is not None:
        user.is_active = bool(payload.get("is_active"))
    user.updated_at = datetime.utcnow()
    db.commit()
    return {"ok": True}


def _get_supplier_meta(db: Session) -> dict:
    cfg = crud.get_config(db, "supplier_meta")
    val = cfg.config_value if cfg else {}
    return val if isinstance(val, dict) else {}


def _set_supplier_meta(db: Session, val: dict):
    crud.set_config(db, "supplier_meta", val if isinstance(val, dict) else {})


def _get_supplier_rules(db: Session) -> dict:
    cfg = crud.get_config(db, "supplier_factory_rules")
    val = cfg.config_value if cfg else {}
    return val if isinstance(val, dict) else {}


def _set_supplier_rules(db: Session, val: dict):
    crud.set_config(db, "supplier_factory_rules", val if isinstance(val, dict) else {})


@router.get("/suppliers")
def list_supplier_accounts(request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    rows = (
        db.query(models.AuthUser)
        .filter(models.AuthUser.role == "supplier")
        .order_by(models.AuthUser.id.asc())
        .all()
    )
    meta = _get_supplier_meta(db)
    rules = _get_supplier_rules(db)
    items = []
    for x in rows:
        supplier_name = str(x.supplier_name or x.username)
        m = meta.get(supplier_name, {}) if isinstance(meta, dict) else {}
        r = rules.get(supplier_name, {}) if isinstance(rules, dict) else {}
        items.append(
            {
                "id": x.id,
                "username": x.username,
                "supplier_name": supplier_name,
                "contact": str(m.get("contact") or ""),
                "phone": str(m.get("phone") or ""),
                "prefix": str(r.get("prefix") or "F"),
                "start": int(r.get("start") or 1),
                "is_active": bool(x.is_active),
            }
        )
    return {"items": items}


@router.post("/suppliers")
def create_supplier_account(payload: dict, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "").strip()
    supplier_name = str(payload.get("supplier_name") or "").strip()
    contact = str(payload.get("contact") or "").strip()
    phone = str(payload.get("phone") or "").strip()
    prefix = str(payload.get("prefix") or "F").strip().upper() or "F"
    try:
        start = int(payload.get("start") or 1)
    except Exception:
        start = 1
    if not username or not password or not supplier_name:
        raise HTTPException(status_code=400, detail="missing username/password/supplier_name")
    exists = db.query(models.AuthUser.id).filter(models.AuthUser.username == username).first()
    if exists:
        raise HTTPException(status_code=400, detail="username_exists")

    now = datetime.utcnow()
    user = models.AuthUser(
        username=username,
        password_hash=make_password_hash(password),
        role="supplier",
        supplier_name=supplier_name,
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    meta = _get_supplier_meta(db)
    meta[supplier_name] = {"contact": contact, "phone": phone}
    _set_supplier_meta(db, meta)
    rules = _get_supplier_rules(db)
    rules[supplier_name] = {"prefix": prefix, "start": start}
    _set_supplier_rules(db, rules)

    return {"ok": True, "id": user.id}


@router.patch("/suppliers/{user_id}")
def update_supplier_account(user_id: int, payload: dict, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    user = (
        db.query(models.AuthUser)
        .filter(models.AuthUser.id == user_id, models.AuthUser.role == "supplier")
        .first()
    )
    if not user:
        raise HTTPException(status_code=404, detail="supplier_not_found")

    old_name = str(user.supplier_name or user.username)
    if payload.get("username"):
        username = str(payload.get("username")).strip()
        exists = (
            db.query(models.AuthUser.id)
            .filter(models.AuthUser.username == username, models.AuthUser.id != user_id)
            .first()
        )
        if exists:
            raise HTTPException(status_code=400, detail="username_exists")
        user.username = username
    if payload.get("password"):
        user.password_hash = make_password_hash(str(payload.get("password")))
    if payload.get("supplier_name"):
        user.supplier_name = str(payload.get("supplier_name")).strip()
    if payload.get("is_active") is not None:
        user.is_active = bool(payload.get("is_active"))
    user.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(user)

    new_name = str(user.supplier_name or user.username)
    meta = _get_supplier_meta(db)
    rules = _get_supplier_rules(db)
    if old_name != new_name:
        if old_name in meta and new_name not in meta:
            meta[new_name] = meta.pop(old_name)
        if old_name in rules and new_name not in rules:
            rules[new_name] = rules.pop(old_name)
    m = meta.get(new_name, {})
    if payload.get("contact") is not None:
        m["contact"] = str(payload.get("contact") or "")
    if payload.get("phone") is not None:
        m["phone"] = str(payload.get("phone") or "")
    meta[new_name] = m
    r = rules.get(new_name, {})
    if payload.get("prefix") is not None:
        r["prefix"] = str(payload.get("prefix") or "F").strip().upper() or "F"
    if payload.get("start") is not None:
        try:
            r["start"] = int(payload.get("start") or 1)
        except Exception:
            r["start"] = 1
    rules[new_name] = r
    _set_supplier_meta(db, meta)
    _set_supplier_rules(db, rules)
    return {"ok": True}


@router.delete("/suppliers/{user_id}")
def delete_supplier_account(user_id: int, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    user = (
        db.query(models.AuthUser)
        .filter(models.AuthUser.id == user_id, models.AuthUser.role == "supplier")
        .first()
    )
    if not user:
        raise HTTPException(status_code=404, detail="supplier_not_found")
    supplier_name = str(user.supplier_name or user.username)
    db.query(models.AuthSession).filter(models.AuthSession.user_id == user_id).delete()
    db.delete(user)
    db.commit()

    meta = _get_supplier_meta(db)
    if supplier_name in meta:
        meta.pop(supplier_name, None)
        _set_supplier_meta(db, meta)
    rules = _get_supplier_rules(db)
    if supplier_name in rules:
        rules.pop(supplier_name, None)
        _set_supplier_rules(db, rules)
    return {"ok": True}
