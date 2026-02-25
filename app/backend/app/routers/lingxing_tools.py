from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.integrations.lingxing_client import get_access_token
from app import crud

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/refresh-token")
def refresh_lingxing_token(db: Session = Depends(get_db)):
    cfg = crud.get_config(db, "lingxing")
    if not cfg:
        raise HTTPException(status_code=400, detail="lingxing config not set")
    app_id = cfg.config_value.get("app_id")
    app_secret = cfg.config_value.get("app_secret")
    if not app_id or not app_secret:
        raise HTTPException(status_code=400, detail="missing app_id/app_secret")
    resp = get_access_token(app_id, app_secret)
    if resp.get("code") not in (200, "200"):
        return resp
    data = resp.get("data", {})
    cfg.config_value["access_token"] = data.get("access_token")
    crud.set_config(db, "lingxing", cfg.config_value)
    return {"access_token": data.get("access_token"), "expires_in": data.get("expires_in")}
