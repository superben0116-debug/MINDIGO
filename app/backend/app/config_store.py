from typing import Dict
from sqlalchemy.orm import Session
from app import crud
from app.config import SHIPPER as DEFAULT_SHIPPER, LINGXING_APP_ID, LINGXING_ACCESS_TOKEN, LINGXING_SID_LIST


def get_shipper_config(db: Session) -> Dict:
    cfg = crud.get_config(db, "shipper")
    if cfg:
        return cfg.config_value
    return DEFAULT_SHIPPER


def get_lingxing_config(db: Session) -> Dict:
    cfg = crud.get_config(db, "lingxing")
    if cfg:
        return cfg.config_value
    return {
        "app_id": LINGXING_APP_ID,
        "app_secret": "",
        "access_token": LINGXING_ACCESS_TOKEN,
        "sid_list": ",".join(LINGXING_SID_LIST),
    }
