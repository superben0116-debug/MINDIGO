from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.db import SessionLocal
from app import crud
from app.address_mapping import default_address_mapping
from app.interface_registry import default_interface_registry
from app.config_store import default_kapi_config, default_lingxing_config, default_shipper_config

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/")
def get_all_config(db: Session = Depends(get_db)):
    addr = crud.get_config(db, "address_mapping")
    shipper = crud.get_config(db, "shipper")
    lingxing = crud.get_config(db, "lingxing")
    kapi = crud.get_config(db, "kapi")
    interfaces = crud.get_config(db, "interface_registry")
    return {
        "address_mapping": addr.config_value if addr else default_address_mapping(),
        "shipper": shipper.config_value if shipper else default_shipper_config(),
        "lingxing": lingxing.config_value if lingxing else default_lingxing_config(),
        "kapi": kapi.config_value if kapi else default_kapi_config(),
        "interface_registry": interfaces.config_value if interfaces else default_interface_registry(),
    }


@router.get("/lingxing")
def get_lingxing_config(db: Session = Depends(get_db)):
    cfg = crud.get_config(db, "lingxing")
    return cfg.config_value if cfg else default_lingxing_config()


@router.get("/shipper")
def get_shipper_config(db: Session = Depends(get_db)):
    cfg = crud.get_config(db, "shipper")
    return cfg.config_value if cfg else default_shipper_config()


@router.put("/address-mapping")
def set_address_mapping(payload: dict, db: Session = Depends(get_db)):
    obj = crud.set_config(db, "address_mapping", payload)
    return {"key": obj.config_key, "value": obj.config_value}


@router.put("/shipper")
def set_shipper(payload: dict, db: Session = Depends(get_db)):
    obj = crud.set_config(db, "shipper", payload)
    return {"key": obj.config_key, "value": obj.config_value}


@router.put("/lingxing")
def set_lingxing(payload: dict, db: Session = Depends(get_db)):
    obj = crud.set_config(db, "lingxing", payload)
    return {"key": obj.config_key, "value": obj.config_value}


@router.get("/kapi")
def get_kapi_config(db: Session = Depends(get_db)):
    cfg = crud.get_config(db, "kapi")
    return cfg.config_value if cfg else default_kapi_config()


@router.put("/kapi")
def set_kapi(payload: dict, db: Session = Depends(get_db)):
    obj = crud.set_config(db, "kapi", payload)
    return {"key": obj.config_key, "value": obj.config_value}


@router.get("/interface-registry")
def get_interface_registry(db: Session = Depends(get_db)):
    cfg = crud.get_config(db, "interface_registry")
    return cfg.config_value if cfg else default_interface_registry()


@router.put("/interface-registry")
def set_interface_registry(payload: dict, db: Session = Depends(get_db)):
    obj = crud.set_config(db, "interface_registry", payload)
    return {"key": obj.config_key, "value": obj.config_value}


@router.get("/debug")
def config_debug(db: Session = Depends(get_db)):
    rows = db.execute(text("SELECT config_key FROM app_config")).fetchall()
    keys = [r[0] for r in rows]
    return {"keys": keys}


@router.post("/address-detect")
def detect_address_fields(payload: dict):
    data = payload.get("data") if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        data = {}
    keys = sorted(list(data.keys()))
    return {"keys": keys}


@router.get("/self-check")
def config_self_check(db: Session = Depends(get_db)):
    lingxing = crud.get_config(db, "lingxing")
    shipper = crud.get_config(db, "shipper")
    missing = []

    lingxing_val = lingxing.config_value if lingxing else {}
    shipper_val = shipper.config_value if shipper else {}

    if not lingxing_val.get("app_id"):
        missing.append("lingxing.app_id")
    if not lingxing_val.get("app_secret"):
        missing.append("lingxing.app_secret")
    if not lingxing_val.get("sid_list"):
        missing.append("lingxing.sid_list")

    # Shipper fields optional for order fetching phase

    return {
        "ok": len(missing) == 0,
        "missing": missing,
    }
