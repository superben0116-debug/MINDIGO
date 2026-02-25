from datetime import datetime
import re
from typing import Dict
from app import crud
from sqlalchemy.orm import Session


def _to_float(v):
    if v is None:
        return None
    try:
        s = str(v).strip().replace("cm", "").replace("CM", "")
        return float(s)
    except Exception:
        return None


def _fmt_size_m(size_cm):
    f = _to_float(size_cm)
    if not f:
        return ""
    m = f / 100.0
    txt = f"{m:.2f}".rstrip("0").rstrip(".")
    return f"{txt}米"


def _extract_size_cm_from_text(text: str):
    if not text:
        return None
    t = str(text).lower()
    m_m = re.search(r"(\d+(?:\.\d+)?)\s*m\b", t)
    if m_m:
        try:
            return float(m_m.group(1)) * 100.0
        except Exception:
            pass
    m_zh = re.search(r"(\d+(?:\.\d+)?)\s*米", str(text))
    if m_zh:
        try:
            return float(m_zh.group(1)) * 100.0
        except Exception:
            pass
    m_in = re.search(r"(\d+(?:\.\d+)?)\s*(?:in|inch|inches|英寸|\"|''?)", t)
    if m_in:
        try:
            return float(m_in.group(1)) * 2.54
        except Exception:
            pass
    return None


def _extract_inches_from_name(name: str):
    text = (name or "").lower()
    vals = []
    for m in re.finditer(r"(\d+(?:\.\d+)?)\s*(?:in|inch|inches|英寸|''|\")", text):
        try:
            v = float(m.group(1))
            if v >= 20:
                vals.append(v)
        except Exception:
            pass
    if not vals:
        for m in re.finditer(r"(\d+(?:\.\d+)?)", text):
            try:
                v = float(m.group(1))
                if v >= 20:
                    vals.append(v)
            except Exception:
                pass
    return max(vals) if vals else None


def _infer_color_zh(raw_name: str):
    n = (raw_name or "").lower()
    mapping = [
        ("wood", "木色"), ("oak", "木色"), ("walnut", "胡桃木色"),
        ("black", "黑色"), ("grey", "灰色"), ("gray", "灰色"),
        ("white", "白色"), ("gold", "金色"), ("blue", "蓝色"),
        ("green", "绿色"), ("beige", "米色"), ("cream", "白色"),
    ]
    for kw, zh in mapping:
        if kw in n:
            return zh
    return "木色"


def _derive_code_from_raw_name(raw_name: str, platform_order_no: str):
    inch = _extract_inches_from_name(raw_name or "")
    if not inch:
        return ""
    size_m = inch * 2.54 / 100.0
    size_code = f"{size_m:.2f}m"
    color_zh = _infer_color_zh(raw_name or "")
    color_code_map = {"木色": "MS", "黑色": "HS", "灰色": "HUI", "白色": "BS", "金色": "JS", "蓝色": "LS", "绿色": "LVS", "米色": "MIS", "胡桃木色": "HTMS"}
    color_code = color_code_map.get(color_zh, "MS")
    style_code = "MSFG"
    seed = re.sub(r"\D", "", str(platform_order_no or "0"))[-2:] or "28"
    return f"{color_code}{style_code}KFBV{seed}-{size_code}"


def _extract_mark_from_name(name: str) -> str:
    if not name:
        return ""
    text = str(name).strip()
    lines = [x.strip() for x in text.splitlines() if x.strip()]
    # 优先：中文后面的英文编码行（通常是第二行）
    if len(lines) >= 2:
        cand = lines[-1]
        m = re.search(r"([A-Z][A-Z0-9-]*-\d+(?:\.\d+)?m)", cand)
        if m:
            return m.group(1).replace(" SLT", "").strip()
    # 次优：整段文本末尾的英文编码
    m = re.search(r"([A-Z][A-Z0-9-]*-\d+(?:\.\d+)?m)(?:\s+SLT)?\s*$", text)
    if m:
        return m.group(1).strip()
    # 严格模式：未识别到“英文段编码”则返回空，不再回退到ASIN/品牌串
    return ""


def _extract_mark_strict(*texts) -> str:
    pat = re.compile(r"([A-Z]{2,}[A-Z0-9-]*-\d+(?:\.\d+)?m)\b")
    for t in texts:
        if not t:
            continue
        s = str(t).strip()
        if not s:
            continue
        # 优先从最后一行抓（通常是中文名下一行英文编码）
        lines = [x.strip() for x in s.splitlines() if x.strip()]
        scan = list(reversed(lines)) + [s]
        for seg in scan:
            m = pat.search(seg)
            if not m:
                continue
            v = m.group(1).strip()
            # 排除 ASIN / 品牌型串
            if v.upper().startswith("B0") and len(v) <= 16:
                continue
            if "AMAZON" in v.upper():
                continue
            return v
    return ""


def build_supplier_visible_payload(db: Session, internal_order_id: int) -> Dict:
    items = crud.get_order_items(db, internal_order_id)
    ext = crud.get_order_ext(db, internal_order_id)
    order = crud.get_internal_order(db, internal_order_id)
    packages = crud.get_order_packages(db, internal_order_id)

    # Supplier sees only a single product image and dimension unit.
    image_url = items[0].product_image if items else None
    product_name = items[0].product_name if items else None
    quantity = items[0].quantity if items else None
    dimension = None
    if ext and ext.fields:
        image_url = image_url or ext.fields.get("product_image")
        product_name = product_name or ext.fields.get("product_name") or ext.fields.get("产品名")
        quantity = quantity or ext.fields.get("purchase_qty") or ext.fields.get("采购数量")
        l = ext.fields.get("len_cm")
        w = ext.fields.get("wid_cm")
        h = ext.fields.get("hei_cm")
        if l or w or h:
            dimension = f"{l or ''}×{w or ''}×{h or ''} cm"
        if not dimension:
            l = ext.fields.get("长cm")
            w = ext.fields.get("宽cm")
            h = ext.fields.get("高cm")
            if l or w or h:
                dimension = f"{l or ''}×{w or ''}×{h or ''} cm"
    if not dimension and packages:
        p = packages[0]
        if p.length_cm or p.width_cm or p.height_cm:
            dimension = f"{p.length_cm or ''}×{p.width_cm or ''}×{p.height_cm or ''} cm"

    if not product_name and ext and ext.fields:
        product_name = ext.fields.get("asin") or ext.fields.get("ASIN") or ext.fields.get("sku") or ext.fields.get("SKU") or "未获取产品名"
    if not quantity:
        quantity = 1

    extf = ext.fields if ext and ext.fields else {}
    size_cm = extf.get("厘米") or extf.get("size_cm") or extf.get("长cm") or extf.get("len_cm")
    if not size_cm:
        size_cm = _extract_size_cm_from_text(extf.get("产品名") or product_name)
    if not size_cm and dimension:
        m = re.search(r"(\d+(?:\.\d+)?)", str(dimension))
        if m:
            size_cm = m.group(1)
    size_m_cn = _fmt_size_m(size_cm)
    part_1 = f"{size_m_cn}柜体" if size_m_cn else "柜体"
    part_2 = "LED智能镜柜"
    part_3 = "水槽"

    base_mark = _extract_mark_strict(
        extf.get("产品名"),
        extf.get("product_name"),
        product_name,
        (items[0].product_name if items else ""),
        extf.get("箱唛"),
        extf.get("marks"),
    )
    if not base_mark:
        base_mark = _derive_code_from_raw_name(
            extf.get("product_name") or product_name or (items[0].product_name if items else ""),
            order.platform_order_no if order else "",
        )
    # 严格要求：箱唛必须来自产品名英文段；识别不到则留空，禁止回退ASIN/品牌
    if not base_mark:
        base_mark = ""

    quote_items = [
        {
            "image_url": image_url,
            "dimension_unit": "cm",
            "product_name": part_1,
            "quantity": quantity,
            "dimension": str(size_cm or "").strip(),
            "marks": base_mark,
            "remark": "",
        },
        {
            "image_url": image_url,
            "dimension_unit": "cm",
            "product_name": part_2,
            "quantity": quantity,
            "dimension": str(size_cm or "").strip(),
            "marks": base_mark,
            "remark": "",
        },
        {
            "image_url": image_url,
            "dimension_unit": "cm",
            "product_name": part_3,
            "quantity": quantity,
            "dimension": str(size_cm or "").strip(),
            "marks": f"{base_mark} SLT".strip(),
            "remark": "水龙头单独打包",
        },
    ]

    return {
        "generated_at": datetime.utcnow().isoformat(),
        "items": quote_items,
        "raw_product_name": product_name,
        "base_marks": base_mark,
        "mark_debug": {
            "source_product_name": extf.get("产品名") or extf.get("product_name") or product_name or "",
            "base_marks": base_mark,
            "missing": (base_mark == ""),
            "platform_order_no": order.platform_order_no if order else "",
        },
    }
