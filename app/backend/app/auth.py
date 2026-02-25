import os
import hashlib
import secrets
from datetime import datetime, timedelta
from fastapi import HTTPException, Request
from sqlalchemy.orm import Session
from app import models

SESSION_COOKIE = "erp_session"
SESSION_HOURS = 24 * 7


def _hash_password(password: str, salt: str) -> str:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120000)
    return dk.hex()


def make_password_hash(password: str) -> str:
    salt = secrets.token_hex(16)
    return f"{salt}${_hash_password(password, salt)}"


def verify_password(password: str, stored: str) -> bool:
    if not stored or "$" not in stored:
        return False
    salt, h = stored.split("$", 1)
    return secrets.compare_digest(_hash_password(password, salt), h)


def create_session(db: Session, user_id: int) -> models.AuthSession:
    token = secrets.token_urlsafe(48)
    now = datetime.utcnow()
    sess = models.AuthSession(
        token=token,
        user_id=user_id,
        created_at=now,
        expires_at=now + timedelta(hours=SESSION_HOURS),
    )
    db.add(sess)
    db.commit()
    db.refresh(sess)
    return sess


def delete_session(db: Session, token: str):
    if not token:
        return
    db.query(models.AuthSession).filter(models.AuthSession.token == token).delete()
    db.commit()


def _get_user_by_token(db: Session, token: str):
    if not token:
        return None
    now = datetime.utcnow()
    row = (
        db.query(models.AuthSession, models.AuthUser)
        .join(models.AuthUser, models.AuthUser.id == models.AuthSession.user_id)
        .filter(models.AuthSession.token == token)
        .first()
    )
    if not row:
        return None
    sess, user = row
    if not user.is_active or sess.expires_at < now:
        db.query(models.AuthSession).filter(models.AuthSession.id == sess.id).delete()
        db.commit()
        return None
    return user


def get_current_user_optional(request: Request, db: Session):
    token = request.cookies.get(SESSION_COOKIE) or ""
    return _get_user_by_token(db, token)


def get_current_user(request: Request, db: Session):
    user = get_current_user_optional(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="not_authenticated")
    return user


def require_admin(request: Request, db: Session):
    user = get_current_user(request, db)
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="admin_required")
    return user


def ensure_default_admin(db: Session):
    has_admin = db.query(models.AuthUser.id).filter(models.AuthUser.role == "admin").first()
    if has_admin:
        return
    username = os.getenv("ERP_ADMIN_USERNAME", "admin")
    password = os.getenv("ERP_ADMIN_PASSWORD", "admin123456")
    now = datetime.utcnow()
    user = models.AuthUser(
        username=username,
        password_hash=make_password_hash(password),
        role="admin",
        supplier_name=None,
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    db.add(user)
    db.commit()
