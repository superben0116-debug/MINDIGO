import os
from typing import Dict
from sqlalchemy.orm import Session
from app import crud
from app.config import SHIPPER as DEFAULT_SHIPPER, LINGXING_APP_ID, LINGXING_APP_SECRET, LINGXING_ACCESS_TOKEN, LINGXING_SID_LIST


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
    env_app_id = os.getenv("ERP_LINGXING_APP_ID", os.getenv("LINGXING_APP_ID", LINGXING_APP_ID)).strip()
    env_app_secret = os.getenv("ERP_LINGXING_APP_SECRET", os.getenv("LINGXING_APP_SECRET", LINGXING_APP_SECRET)).strip()
    env_access_token = os.getenv("ERP_LINGXING_ACCESS_TOKEN", os.getenv("LINGXING_ACCESS_TOKEN", LINGXING_ACCESS_TOKEN)).strip()
    env_sid_list = os.getenv("ERP_LINGXING_SID_LIST", os.getenv("LINGXING_SID_LIST", ",".join(LINGXING_SID_LIST))).strip()
    if not env_sid_list:
        env_sid_list = "ALL"
    return {
        "app_id": env_app_id,
        "app_secret": env_app_secret,
        "access_token": env_access_token,
        "sid_list": env_sid_list,
        "start_time": "2026-01-01",
        "end_time": "2026-12-31",
        "chunk_days": 7,
        "use_all_orders_report": 1,
        "use_mws_orders": 1,
        "mws_date_types": "2,1",
        "all_orders_date_types": "1,2",
        "listing_api_path": "/erp/sc/data/mws/listing",
        "listing_sid": "101",
        "customer_mail_map": {},  # { "5448": "xxx@163.com", "口福轩": "yyy@163.com" }
    }


def default_kapi_config() -> Dict:
    return {
        "base_url": os.getenv("ERP_KAPI_BASE_URL", os.getenv("KAPI_BASE_URL", "https://tms-api.wdshiplabel.com")).strip() or "https://tms-api.wdshiplabel.com",
        "api_key": os.getenv("ERP_KAPI_API_KEY", os.getenv("KAPI_API_KEY", "")).strip(),
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
