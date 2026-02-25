from datetime import datetime
from typing import Dict, Optional
from app.address_mapping import default_address_mapping, pick_first


def cm_to_in(value):
    if value is None:
        return None
    return float(value) / 2.54


def kg_to_lb(value):
    if value is None:
        return None
    return float(value) * 2.20462


def oversize_flag(length_in, weight_lb):
    if length_in is None or weight_lb is None:
        return False
    return length_in >= 80 or weight_lb >= 150


def map_order_detail(detail: Dict, address_mapping: Optional[Dict] = None) -> Dict:
    data = detail.get("data", {})
    mapping = address_mapping or default_address_mapping()
    purchase_time = data.get("purchase_time")
    if isinstance(purchase_time, str):
        try:
            purchase_time = datetime.strptime(purchase_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            purchase_time = None
    return {
        "internal_order_no": f"IO{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}",
        "platform_order_no": (data.get("order_item") or [{}])[0].get("platform_order_id"),
        "shop_name": data.get("shop_name"),
        "order_status": data.get("order_status"),
        "purchase_time": purchase_time,
        "region": data.get("country_code"),
        "customer_address_summary": data.get("customer_comment"),
        "customer_name": pick_first(data, mapping.get("name", [])),
        "customer_phone": pick_first(data, mapping.get("phone", [])),
        "customer_zip": pick_first(data, mapping.get("zip", [])),
        "customer_city": pick_first(data, mapping.get("city", [])),
        "customer_state": pick_first(data, mapping.get("state", [])),
        "customer_country": pick_first(data, mapping.get("country", [])),
        "customer_address_line1": pick_first(data, mapping.get("address_line1", [])),
        "customer_address_line2": pick_first(data, mapping.get("address_line2", [])),
        "logistics_provider": data.get("logistics_provider_name"),
        "logistics_type": data.get("logistics_type_name"),
        "tracking_no": data.get("tracking_number"),
        "total_cost": data.get("logistics_freight"),
        "total_profit": data.get("gross_profit_amount"),
    }


def map_order_ext(detail: Dict, address_mapping: Optional[Dict] = None) -> Dict:
    data = detail.get("data", {})
    mapping = address_mapping or default_address_mapping()
    ext: Dict = {}

    # Basic product hints
    if data.get("order_item"):
        item0 = data.get("order_item")[0]
        ext["product_name"] = item0.get("product_name")
        ext["sku"] = item0.get("sku")
        ext["purchase_qty"] = item0.get("quality")
        ext["unit_price"] = item0.get("item_unit_price")

    # Buyer message / comment as fallback clues
    ext["buyer_message"] = data.get("buyer_message")
    ext["customer_comment"] = data.get("customer_comment")
    ext["buyer_choose_express"] = data.get("buyer_choose_express")

    # Address info if present in detail
    addr = {}
    if isinstance(data.get("address_info"), dict):
        addr = data.get("address_info") or {}
    elif isinstance(data.get("addressInfo"), dict):
        addr = data.get("addressInfo") or {}
    elif isinstance(data.get("address"), dict):
        addr = data.get("address") or {}

    def pick_any(*vals):
        for v in vals:
            if v:
                return v
        return None

    ext["address_line1"] = pick_any(addr.get("address_line1"), addr.get("addressLine1"), data.get("ship_to_address"), pick_first(data, mapping.get("address_line1", [])))
    ext["address_line2"] = pick_any(addr.get("address_line2"), addr.get("addressLine2"), data.get("address_line2"), data.get("addressLine2"), pick_first(data, mapping.get("address_line2", [])))
    ext["address_line3"] = pick_any(addr.get("address_line3"), addr.get("addressLine3"), data.get("address_line3"), data.get("addressLine3"))
    ext["district"] = pick_any(addr.get("district"), data.get("district"))
    ext["doorplate_no"] = pick_any(addr.get("doorplate_no"), data.get("doorplate_no"))
    ext["city"] = pick_any(addr.get("city"), addr.get("cityName"), data.get("ship_to_city"), pick_first(data, mapping.get("city", [])))
    ext["postal_code"] = pick_any(addr.get("postal_code"), addr.get("postalCode"), data.get("ship_to_postal_code"), pick_first(data, mapping.get("zip", [])))
    ext["state_or_region"] = pick_any(addr.get("state_or_region"), addr.get("stateOrRegion"), data.get("ship_to_province_code"), pick_first(data, mapping.get("state", [])))
    ext["receiver_country_code"] = pick_any(addr.get("receiver_country_code"), addr.get("receiverCountryCode"), data.get("ship_to_country"), pick_first(data, mapping.get("country", [])))
    ext["receiver_name"] = pick_any(addr.get("receiver_name"), addr.get("receiverName"), data.get("ship_to_name"), data.get("buyer_name"), pick_first(data, mapping.get("name", [])))
    ext["receiver_mobile"] = pick_any(addr.get("receiver_mobile"), addr.get("receiverMobile"), data.get("receiver_mobile"), data.get("buyer_phone"), pick_first(data, mapping.get("phone", [])))
    ext["receiver_tel"] = pick_any(addr.get("receiver_tel"), addr.get("receiverTel"), pick_first(data, mapping.get("phone", [])))
    ext["buyer_email"] = data.get("buyer_email")
    ext["buyer_name"] = data.get("buyer_name")

    # Convenience customer display fields
    ext["customer_name"] = ext.get("receiver_name")
    ext["customer_phone"] = ext.get("receiver_mobile") or ext.get("receiver_tel")
    ext["customer_city"] = ext.get("city")
    ext["customer_state"] = ext.get("state_or_region")
    ext["customer_zip"] = ext.get("postal_code")
    ext["customer_country"] = ext.get("receiver_country_code")
    ext["customer_address"] = " ".join([x for x in [ext.get("address_line1"), ext.get("address_line2")] if x])

    return ext


def map_order_items(detail: Dict) -> list:
    items = []
    for item in detail.get("data", {}).get("order_item", []) or []:
        items.append({
            "sku": item.get("sku"),
            "product_name": item.get("product_name"),
            "quantity": item.get("quality"),
            "unit_price": item.get("item_unit_price"),
            "currency": item.get("currency_code"),
            "product_image": item.get("pic_url"),
            "attachments": item.get("attachments") or item.get("newAttachments"),
        })
    return items


def map_order_packages(detail: Dict) -> list:
    data = detail.get("data", {})
    length_cm = data.get("pkg_length")
    width_cm = data.get("pkg_width")
    height_cm = data.get("pkg_height")
    weight_kg = data.get("pkg_real_weight")
    length_in = cm_to_in(length_cm)
    weight_lb = kg_to_lb(weight_kg)
    return [{
        "length_cm": length_cm,
        "width_cm": width_cm,
        "height_cm": height_cm,
        "length_in": length_in,
        "width_in": cm_to_in(width_cm),
        "height_in": cm_to_in(height_cm),
        "weight_kg": weight_kg,
        "weight_lb": weight_lb,
        "billed_weight": data.get("logistics_pre_weight"),
        "oversize_flag": oversize_flag(length_in, weight_lb),
    }]
