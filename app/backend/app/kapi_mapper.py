import re
from typing import Dict, List, Tuple
from app import crud
from app.config_store import get_shipper_config
from sqlalchemy.orm import Session
from app.xlsx_utils import read_xlsx
from datetime import date


def load_kapi_template(template_path: str) -> Tuple[List[str], Dict[str, str]]:
    data = read_xlsx(template_path)
    sheet = data[list(data.keys())[0]]
    header = sheet[0] if sheet else []
    row1 = sheet[1] if len(sheet) > 1 else []
    row2 = sheet[2] if len(sheet) > 2 else []
    maxlen = max(len(header), len(row1), len(row2)) if header else 0
    header += [""] * (maxlen - len(header))
    row1 += [""] * (maxlen - len(row1))
    row2 += [""] * (maxlen - len(row2))
    defaults: Dict[str, str] = {}
    for h, a, b in zip(header, row1, row2):
        if a == b and a != "":
            defaults[h] = a
    return header, defaults


KAPI_FIXED_DEFAULTS: Dict[str, str] = {
    "Shipper Zip Code*": "91733",
    "Shipper City*": "South El Monte",
    "Shipper State*": "CA",
    "Shipper Country*": "US",
    "Shipper Address Type*": "Business with dock",
    "Shipper Contact Name": "mike",
    "Shipper Contact Phone": "567-227-7777",
    "Shipper Contact Email": "chenjinrong@wedoexpress.com",
    "Shipper Address Name": "CHAINYO SUPPLYCHAIN MANAGEMENT INC",
    "Shipper Address Line1": "1230 Santa Anita Ave",
    "Shipper Address Line2": "Unit H",
    "Pickup Date*": "2026-02-03",
    "Pickup Time From": "09:30",
    "Pickup Time To": "17:30",
    "Receiver Country*": "US",
    "Receiver Address Type*": "Residential",
    "Receiver Service": "Lift-Gate;APPT",
    "Receiver Contact Email": "chenjinrong@wedoexpress.com",
    "Delivery Time From": "09:00",
    "Delivery Time To": "16:30",
    "Size Unit*": "in/lb",
    "Name*": "Bathroom Vanity",
    "Package Type*": "CRATE",
    "Package Qty*": "1",
    "Pallet Type*": "PALLETS",
    "Pallet Qty*": "1",
}


def _merge_kapi_defaults(template_defaults: Dict[str, str], shipper: Dict) -> Dict[str, str]:
    out = dict(template_defaults or {})
    out.update(KAPI_FIXED_DEFAULTS)
    # Allow configured shipper values to override fixed defaults only when non-empty.
    mapping = {
        "Shipper Zip Code*": shipper.get("zip", ""),
        "Shipper City*": shipper.get("city", ""),
        "Shipper State*": shipper.get("state", ""),
        "Shipper Country*": shipper.get("country", ""),
        "Shipper Address Type*": shipper.get("address_type", ""),
        "Shipper Service": shipper.get("service", ""),
        "Shipper Contact Name": shipper.get("contact_name", ""),
        "Shipper Contact Phone": shipper.get("contact_phone", ""),
        "Shipper Contact Email": shipper.get("contact_email", ""),
        "Shipper Address Name": shipper.get("address_name", ""),
        "Shipper Address Line1": shipper.get("address_line1", ""),
        "Shipper Address Line2": shipper.get("address_line2", ""),
        "Pickup Time From": shipper.get("pickup_time_from", ""),
        "Pickup Time To": shipper.get("pickup_time_to", ""),
    }
    for k, v in mapping.items():
        if v not in (None, ""):
            out[k] = str(v)
    return out


def _product_tail(name: str) -> str:
    if not name:
        return ""
    parts = [p for p in name.split("\n") if p.strip()]
    return parts[-1].strip() if parts else name.strip()


def _extract_product_code(name: str) -> str:
    txt = str(name or "")
    if not txt:
        return ""
    # Prefer the standardized converted code segment, e.g. MSMSFGKFBV00-1.52m or ... SLT
    m = re.search(r"([A-Z0-9]{6,}-\d+(?:\.\d+)?m(?:\s*SLT)?)", txt, flags=re.I)
    if m:
        return m.group(1).strip()
    # Compatible pattern for some legacy values: code-size without m suffix, e.g. XXX-1.32 / XXX-0 / XXX-1.32-SLT
    m_legacy = re.search(r"([A-Z0-9]{6,}-[A-Z0-9.]+(?:[-_\s]*SLT)?)", txt, flags=re.I)
    if m_legacy:
        return m_legacy.group(1).strip()
    # Strict fallback: only return compact token-like code, never full product title
    # Examples: MSMSFGKFBV58-1.42m / MSMSFGKFBV58-1.42m-SLT / MSMSFGKFBV58-1.42m_SLT
    candidates = re.findall(r"[A-Z0-9][A-Z0-9_-]{5,}", txt, flags=re.I)
    for token in candidates:
        t = token.strip(" ,.;:()[]{}")
        if not re.search(r"[A-Z]", t, flags=re.I):
            continue
        if not re.search(r"\d", t):
            continue
        if "-" not in t:
            continue
        if " " in t:
            continue
        return t
    # Try code hints from mixed chinese+code format.
    m2 = re.search(r"([A-Z]{2,}[A-Z0-9]*-[0-9]+(?:\.[0-9]+)?m(?:\s*SLT)?)", txt, flags=re.I)
    if m2:
        return m2.group(1).strip()
    return ""


def _parse_addr(addr: str):
    lines = [l.strip() for l in (addr or "").split("\n") if l.strip()]
    name = lines[0] if lines else ""
    line1 = lines[1] if len(lines) > 1 else ""
    city = state = zipc = ""
    if len(lines) > 2:
        m = re.search(r"(.+),\\s*([A-Z]{2})\\s*(\\d{5})(?:-\\d{4})?", lines[2])
        if m:
            city, state, zipc = m.group(1).strip(), m.group(2), m.group(3)
    country = "US" if any("美国" in l for l in lines) else ""
    return {
        "name": name,
        "line1": line1,
        "city": city,
        "state": state,
        "zip": zipc,
        "country": country,
    }


def _parse_addr_robust(addr: str):
    d = _parse_addr(addr)
    if d.get("city") and d.get("state") and d.get("zip"):
        return d
    txt = " ".join([x.strip() for x in (addr or "").split("\n") if x.strip()])
    m = re.search(r"(.+),\s*([A-Z]{2})\s*(\d{5})(?:-\d{4})?$", txt)
    if not m:
        return d
    left = m.group(1).strip()
    state = m.group(2).strip()
    zipc = m.group(3).strip()
    first_digit = re.search(r"\d", left)
    if first_digit:
        i = first_digit.start()
        name = left[:i].strip() or d.get("name", "")
        addr_city = left[i:].strip()
    else:
        name = d.get("name", "")
        addr_city = left
    city = d.get("city", "")
    line1 = d.get("line1", "")
    if not city:
        toks = addr_city.split()
        if len(toks) >= 3:
            city = " ".join(toks[-2:])
            line1 = " ".join(toks[:-2]).strip() or line1
        else:
            city = addr_city
    return {
        "name": name or d.get("name", ""),
        "line1": line1 or d.get("line1", ""),
        "city": city,
        "state": state,
        "zip": zipc,
        "country": d.get("country", "") or "US",
    }


def _zip5(v: str) -> str:
    s = str(v or "").strip()
    m = re.search(r"(\d{5})", s)
    return m.group(1) if m else s


def get_kapi_default_values(template_path: str, shipper: Dict) -> Tuple[List[str], Dict[str, str]]:
    headers, template_defaults = load_kapi_template(template_path)
    return headers, _merge_kapi_defaults(template_defaults, shipper)


def map_order_to_kapi_rows(db: Session, internal_order_id: int, template_path: str) -> Tuple[List[str], List[List[str]]]:
    headers, template_defaults = load_kapi_template(template_path)
    order = crud.get_internal_order(db, internal_order_id)
    packages = crud.get_order_packages(db, internal_order_id)
    shipper = get_shipper_config(db)
    defaults = _merge_kapi_defaults(template_defaults, shipper)
    ext = crud.get_order_ext(db, internal_order_id)
    ext_fields = ext.fields if ext else {}
    items = crud.get_order_items(db, internal_order_id)

    rows: List[List[str]] = []
    if not packages:
        packages = [None]
    for pkg in packages:
        row = ["" for _ in headers]
        idx = {h: i for i, h in enumerate(headers)}
        # defaults
        for h, v in defaults.items():
            if h in idx:
                row[idx[h]] = v

        # Receiver
        if order:
            addr_text = ext_fields.get("customer_address") or ext_fields.get("客户地址") or ""
            addr = _parse_addr_robust(addr_text)
            line1_struct = (
                ext_fields.get("address_line1")
                or ext_fields.get("客户地址1")
                or ext_fields.get("customer_address_line1")
                or ""
            )
            line2_struct = (
                ext_fields.get("address_line2")
                or ext_fields.get("address_line3")
                or ext_fields.get("doorplate_no")
                or ext_fields.get("district")
                or ""
            )
            city_struct = ext_fields.get("city") or ext_fields.get("customer_city") or ""
            state_struct = ext_fields.get("state_or_region") or ext_fields.get("customer_state") or ""
            zip_struct = _zip5(ext_fields.get("postal_code") or ext_fields.get("customer_zip") or "")
            name_struct = (
                ext_fields.get("receiver_name")
                or ext_fields.get("buyer_name")
                or ext_fields.get("customer_name")
                or ""
            )
            phone_struct = (
                ext_fields.get("receiver_mobile")
                or ext_fields.get("receiver_tel")
                or ext_fields.get("电话")
                or ext_fields.get("customer_phone")
                or ""
            )
            for key, val in {
                "Receiver Zip Code*": zip_struct or order.customer_zip or addr.get("zip", ""),
                "Receiver City*": city_struct or order.customer_city or addr.get("city", ""),
                "Receiver State*": state_struct or order.customer_state or addr.get("state", ""),
                "Receiver Country*": (ext_fields.get("receiver_country_code") or order.customer_country or addr.get("country", "") or "US"),
                "Receiver Address Type*": defaults.get("Receiver Address Type*", "Residential"),
                "Receiver Contact Name": name_struct or order.customer_name or addr.get("name", ""),
                "Receiver Contact Phone": phone_struct or order.customer_phone or "",
                "Receiver Address Name": name_struct or addr.get("name", ""),
                "Receiver Address Line1": line1_struct or order.customer_address_line1 or addr.get("line1", ""),
                "Receiver Address Line2": line2_struct or order.customer_address_line2 or "",
            }.items():
                if key in idx:
                    row[idx[key]] = val

        # Order & package
        if "Customer orderNo" in idx:
            val = (
                ext_fields.get("产品编码")
                or ext_fields.get("product_code")
                or ext_fields.get("箱唛")
                or ext_fields.get("marks")
                or ext_fields.get("base_marks")
                or _extract_product_code(ext_fields.get("产品名") or "")
                or _extract_product_code(ext_fields.get("product_name") or "")
            )
            if not val and items:
                for it in items:
                    val = (
                        _extract_product_code(getattr(it, "product_name", "") or "")
                        or _extract_product_code(getattr(it, "sku", "") or "")
                    )
                    if val:
                        break
            # Do not fallback to internal order no; keep empty when code is unavailable.
            row[idx["Customer orderNo"]] = val
        if "Ref" in idx:
            row[idx["Ref"]] = (
                ext_fields.get("单号")
                or ext_fields.get("tracking_no")
                or ext_fields.get("联邦单号")
                or (order.tracking_no if order else "")
                or ""
            )
        if "Declared($)*" in idx:
            row[idx["Declared($)*"]] = str(
                ext_fields.get("售价")
                or ext_fields.get("sale_price")
                or ext_fields.get("单价")
                or ""
            )
        if "Length*" in idx:
            try:
                lcm = float(ext_fields.get("长cm") or 0)
                row[idx["Length*"]] = f"{(lcm / 2.54):.2f}" if lcm > 0 else ""
            except Exception:
                row[idx["Length*"]] = ""
        if "Weight*" in idx:
            row[idx["Weight*"]] = str(
                ext_fields.get("镑重量＜150lb")
                or ext_fields.get("镑重量\n＜150lb")
                or ""
            )
        if "Pickup Date*" in idx and not row[idx["Pickup Date*"]]:
            row[idx["Pickup Date*"]] = str(date.today())
        if pkg:
            if "Box Weight" in idx:
                row[idx["Box Weight"]] = str(getattr(pkg, "weight_lb", "") or "")
            if "Box Length" in idx:
                row[idx["Box Length"]] = str(getattr(pkg, "length_in", "") or "")
            if " Box Width" in idx:
                row[idx[" Box Width"]] = str(getattr(pkg, "width_in", "") or "")
            if " Box Height" in idx:
                row[idx[" Box Height"]] = str(getattr(pkg, "height_in", "") or "")
        rows.append(row)
    return headers, rows
