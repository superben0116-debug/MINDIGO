from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.db import SessionLocal
from app import crud
from datetime import datetime
import requests

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/seed")
def seed_data(db: Session = Depends(get_db)):
    order = crud.create_internal_order(db, {
        "internal_order_no": f"IO{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "platform_order_no": "113-TEST",
        "shop_name": "测试店铺",
        "order_status": "待发货",
        "purchase_time": datetime.utcnow(),
        "region": "US",
        "tracking_no": "TRACK-TEST",
        "customer_name": "John Doe",
        "customer_phone": "1234567890",
        "customer_zip": "10001",
        "customer_city": "New York",
        "customer_state": "NY",
        "customer_country": "US",
        "customer_address_line1": "123 Test St",
    })
    crud.create_internal_order_item(db, order.id, {
        "sku": "SKU-TEST",
        "product_name": "测试产品",
        "quantity": 1,
        "unit_price": 100,
        "currency": "USD",
        "product_image": "",
    })
    crud.create_internal_order_package(db, order.id, {
        "length_cm": 120,
        "width_cm": 80,
        "height_cm": 60,
        "weight_kg": 45.5,
    })
    return {"order_id": order.id}


@router.get("/egress-ip")
def egress_ip():
    providers = [
        "https://api.ipify.org?format=json",
        "https://ifconfig.me/all.json",
        "https://ipinfo.io/json",
    ]
    out = {"ok": False, "providers": []}
    for u in providers:
        try:
            r = requests.get(u, timeout=10)
            txt = r.text
            ip = ""
            if r.headers.get("content-type", "").startswith("application/json"):
                js = r.json()
                ip = str(js.get("ip") or js.get("ip_addr") or js.get("address") or "")
            if not ip and txt:
                ip = txt.strip().split("\n")[0].strip()
            row = {"url": u, "status": r.status_code, "ip": ip}
            out["providers"].append(row)
            if ip:
                out["ok"] = True
                out["ip"] = ip
                return out
        except Exception as e:
            out["providers"].append({"url": u, "error": str(e)})
    return out
