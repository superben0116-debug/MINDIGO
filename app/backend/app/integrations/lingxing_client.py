import hashlib
import time
import base64
import json
from urllib.parse import quote
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Dict

from Crypto.Cipher import AES  # Requires pycryptodome
import requests

API_BASE = "https://openapi.lingxing.com"


def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


_HTTP = _session()


def _pad(data: bytes) -> bytes:
    pad_len = 16 - (len(data) % 16)
    return data + bytes([pad_len]) * pad_len


def generate_sign(params: Dict[str, str], app_id: str) -> str:
    items = [(k, v) for k, v in params.items() if v is not None and v != ""]
    items.sort(key=lambda x: x[0])
    raw = "&".join([f"{k}={v}" for k, v in items])
    md5 = hashlib.md5(raw.encode("utf-8")).hexdigest().upper()
    cipher = AES.new(app_id.encode("utf-8"), AES.MODE_ECB)
    encrypted = cipher.encrypt(_pad(md5.encode("utf-8")))
    sign = base64.b64encode(encrypted).decode("utf-8")
    return quote(sign, safe="")


def _sign_safe_payload(payload: dict) -> dict:
    """
    Lingxing sign rule compatibility:
    - list/dict/bool should be stringified for sign calculation
    - original json body must remain original types when sending request
    """
    out = {}
    for k, v in (payload or {}).items():
        if isinstance(v, bool):
            out[k] = "true" if v else "false"
        elif isinstance(v, (list, dict, tuple)):
            out[k] = json.dumps(v, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        else:
            out[k] = v
    return out


def get_access_token(app_id: str, app_secret: str) -> dict:
    url = f"{API_BASE}/api/auth-server/oauth/access-token"
    resp = _HTTP.post(url, files={"appId": (None, app_id), "appSecret": (None, app_secret)}, timeout=30)
    return resp.json()


def refresh_token(app_id: str, refresh_token_value: str) -> dict:
    url = f"{API_BASE}/api/auth-server/oauth/refresh"
    resp = _HTTP.post(url, files={"appId": (None, app_id), "refreshToken": (None, refresh_token_value)}, timeout=30)
    return resp.json()


def get_fbm_order_list(
    access_token: str,
    app_id: str,
    sid: str,
    page: int = 1,
    length: int = 100,
    start_time: str | None = None,
    end_time: str | None = None,
):
    url = f"{API_BASE}/erp/sc/routing/order/Order/getOrderList"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    body = {
        "sid": sid,
        "page": int(page),
        "length": int(length),
    }
    if start_time:
        body["start_time"] = start_time
    if end_time:
        body["end_time"] = end_time

    sign_params = {**common, **body}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=body, timeout=30)
    return resp.json()


def get_fbm_order_detail(access_token: str, app_id: str, order_number: str):
    url = f"{API_BASE}/erp/sc/routing/order/Order/getOrderDetail"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    body = {"order_number": order_number}
    sign_params = {**common, **body}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=body, timeout=30)
    return resp.json()


def get_shop_list(access_token: str, app_id: str):
    url = f"{API_BASE}/erp/sc/data/seller/lists"
    timestamp = int(time.time())
    params = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    params["sign"] = generate_sign(params, app_id)
    resp = _HTTP.get(url, params=params, timeout=30)
    return resp.json()


def get_all_orders_report(
    access_token: str,
    app_id: str,
    sid: str,
    start_date: str,
    end_date: str,
    date_type: int = 1,
    offset: int = 0,
    length: int = 1000,
):
    url = f"{API_BASE}/erp/sc/data/mws_report/allOrders"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    body = {
        "sid": int(sid),
        "date_type": int(date_type),
        "start_date": start_date,
        "end_date": end_date,
        "offset": int(offset),
        "length": int(length),
    }
    sign_params = {**common, **body}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=body, timeout=30)
    return resp.json()


def get_fba_address_detail(access_token: str, app_id: str, address_id: int):
    url = f"{API_BASE}/basicOpen/openapi/fbaShipment/shoppingAddress"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    body = {"id": int(address_id)}
    sign_params = {**common, **body}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=body, timeout=30)
    return resp.json()


def update_fbm_order(access_token: str, app_id: str, order_list: list):
    url = f"{API_BASE}/pb/mp/order/v2/updateOrder"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    body = {"order_list": order_list}
    sign_params = {**common, **_sign_safe_payload(body)}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=body, timeout=30)
    return resp.json()


def create_manual_order(access_token: str, app_id: str, payload: dict):
    url = f"{API_BASE}/pb/mp/order/v2/create"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    sign_params = {**common, **_sign_safe_payload(payload)}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=payload, timeout=30)
    return resp.json()


def get_mp_order_list(access_token: str, app_id: str, payload: dict):
    url = f"{API_BASE}/pb/mp/order/v2/list"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    sign_params = {**common, **_sign_safe_payload(payload)}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=payload, timeout=30)
    return resp.json()


def get_rma_manage_list(access_token: str, app_id: str, payload: dict):
    url = f"{API_BASE}/basicOpen/customerService/rmaManage/list"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    sign_params = {**common, **_sign_safe_payload(payload)}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=payload, timeout=30)
    return resp.json()


def get_mail_list(access_token: str, app_id: str, payload: dict):
    url = f"{API_BASE}/erp/sc/data/mail/lists"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    sign_params = {**common, **_sign_safe_payload(payload)}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=payload, timeout=30)
    return resp.json()


def get_mail_detail(access_token: str, app_id: str, webmail_uuid: str):
    url = f"{API_BASE}/erp/sc/data/mail/detail"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    body = {"webmail_uuid": webmail_uuid}
    sign_params = {**common, **body}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=body, timeout=30)
    return resp.json()

def get_mws_orders(
    access_token: str,
    app_id: str,
    sid: int,
    start_date: str,
    end_date: str,
    date_type: int = 1,
    offset: int = 0,
    length: int = 1000,
):
    url = f"{API_BASE}/erp/sc/data/mws/orders"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    body = {
        "sid": int(sid),
        "start_date": start_date,
        "end_date": end_date,
        "date_type": int(date_type),
        "offset": int(offset),
        "length": int(length),
    }
    sign_params = {**common, **body}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=body, timeout=30)
    return resp.json()


def get_mws_order_detail(access_token: str, app_id: str, order_ids: list[str] | str):
    url = f"{API_BASE}/erp/sc/data/mws/orderDetail"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    if isinstance(order_ids, str):
        order_ids = [order_ids]
    body = {
        "order_id": ",".join(order_ids),
    }
    sign_params = {**common, **body}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=body, timeout=30)
    return resp.json()


def get_listing_search(access_token: str, app_id: str, payload: dict, api_path: str = "/erp/sc/data/mws/listing"):
    url = f"{API_BASE}{api_path}"
    timestamp = int(time.time())
    common = {
        "access_token": access_token,
        "app_key": app_id,
        "timestamp": str(timestamp),
    }
    sign_params = {**common, **_sign_safe_payload(payload)}
    common["sign"] = generate_sign(sign_params, app_id)
    resp = _HTTP.post(url, params=common, json=payload, timeout=30)
    return resp.json()
