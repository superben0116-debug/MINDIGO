from typing import Dict
from sqlalchemy.orm import Session
from app import crud
from app.config import SHIPPER as DEFAULT_SHIPPER, LINGXING_APP_ID, LINGXING_ACCESS_TOKEN, LINGXING_SID_LIST


def get_shipper_config(db: Session) -> Dict:
    cfg = crud.get_config(db, "shipper")
    if cfg:
        return cfg.config_value
    return default_shipper_config()


def get_lingxing_config(db: Session) -> Dict:
    cfg = crud.get_config(db, "lingxing")
    if cfg:
        return cfg.config_value
    return default_lingxing_config()


def default_lingxing_config() -> Dict:
    return {
        "app_id": LINGXING_APP_ID,
        "app_secret": "",
        "access_token": LINGXING_ACCESS_TOKEN,
        "sid_list": ",".join(LINGXING_SID_LIST),
        "start_time": "2026-01-01",
        "end_time": "2026-12-31",
        "chunk_days": 7,
        "use_all_orders_report": 1,
        "use_mws_orders": 1,
        "listing_api_path": "/erp/sc/data/mws/listing",
        "listing_sid": "101",
    }


def default_kapi_config() -> Dict:
    return {
        "base_url": "https://tms-api.wdshiplabel.com",
        "api_key": "",
    }


def default_shipper_config() -> Dict:
    defaults = {
        "zip": "91733",
        "city": "South El Monte",
        "state": "CA",
        "country": "US",
        "address_type": "Business with dock",
        "service": "",
        "contact_name": "mike",
        "contact_phone": "567-227-7777",
        "contact_email": "chenjinrong@wedoexpress.com",
        "address_name": "CHAINYO SUPPLYCHAIN MANAGEMENT INC",
        "address_line1": "1230 Santa Anita Ave",
        "address_line2": "Unit H",
        "pickup_time_from": "09:30",
        "pickup_time_to": "17:30",
    }
    out = dict(defaults)
    out.update({k: v for k, v in (DEFAULT_SHIPPER or {}).items() if v not in (None, "")})
    return out
