from typing import Dict, List, Any


def default_address_mapping() -> Dict[str, List[str]]:
    return {
        "name": ["buyer_name", "receiver_name"],
        "phone": ["buyer_phone", "receiver_phone"],
        "zip": ["buyer_zip", "receiver_zip"],
        "city": ["buyer_city", "receiver_city"],
        "state": ["buyer_state", "receiver_state"],
        "country": ["buyer_country", "receiver_country"],
        "address_line1": ["buyer_address_line1", "receiver_address_line1"],
        "address_line2": ["buyer_address_line2", "receiver_address_line2"],
    }


def pick_first(data: Dict[str, Any], keys: List[str]):
    for k in keys:
        if data.get(k):
            return data.get(k)
    return None
