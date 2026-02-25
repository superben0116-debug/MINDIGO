#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
etl_reference_v3.py

Anchored (between-text) parsing version to reduce misalignment.

Key idea:
- NEVER global-search fields that may appear multiple times.
- First cut stable blocks using fixed anchors:
  - header_block = between(order_block, "订单一览", "订单内容")
  - addr_block   = between(order_block, "配送地址", "美国") fallback to "地址类型:" / "联系买家:"
  - buyer_block  = between(order_block, "联系买家:", "更多详情") fallback to "收税模型:"
- Then extract fields by "A -> B" between anchors inside header_block.

Also supports:
- Multi-package parsing (split by "包裹 N")
- Choose small-parcel FedEx tracking (often in later package)
- Region from ZIP first digit: 0-3 美东, 4-7 美中, 8-9 美西 (append digit)

Dependencies:
  pip install pandas openpyxl
"""

from __future__ import annotations
import re
import argparse
from typing import Dict, List, Optional, Tuple

import pandas as pd


# ---------------------------
# Basic normalization
# ---------------------------
def normalize_text(text: str) -> str:
    if text is None:
        return ""
    t = text.replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ")
    t = re.sub(r"[ \u00A0]+", " ", t)  # normalize spaces
    return t


def between(text: str, start: str, end: str, *, from_last_start: bool = False) -> str:
    """Return substring between start and end. If not found, return ''."""
    if not text:
        return ""
    sidx = text.rfind(start) if from_last_start else text.find(start)
    if sidx < 0:
        return ""
    sidx += len(start)
    eidx = text.find(end, sidx)
    if eidx < 0:
        return ""
    return text[sidx:eidx]


def between_any_end(text: str, start: str, ends: List[str]) -> str:
    """Return substring after start until the earliest end found in ends (after start)."""
    if not text:
        return ""
    sidx = text.find(start)
    if sidx < 0:
        return ""
    sidx += len(start)
    eidxs = []
    for e in ends:
        ei = text.find(e, sidx)
        if ei >= 0:
            eidxs.append(ei)
    if not eidxs:
        return ""
    return text[sidx:min(eidxs)]


def after_line(text: str, start: str) -> str:
    """Return content after start until end-of-line."""
    if not text:
        return ""
    sidx = text.find(start)
    if sidx < 0:
        return ""
    sidx += len(start)
    nl = text.find("\n", sidx)
    if nl < 0:
        return text[sidx:].strip()
    return text[sidx:nl].strip()


def clean_value(v: str) -> str:
    return (v or "").strip().strip("\u200b").strip()


# ---------------------------
# Order block split
# ---------------------------
ORDER_ID_RE = re.compile(r"订单编号：#\s*(\d{3}-\d{7}-\d{7})")


def split_order_blocks(text: str) -> List[Tuple[str, str]]:
    ms = list(ORDER_ID_RE.finditer(text))
    blocks: List[Tuple[str, str]] = []
    for i, m in enumerate(ms):
        oid = m.group(1)
        start = m.start()
        end = ms[i + 1].start() if i + 1 < len(ms) else len(text)
        blocks.append((oid, text[start:end]))
    return blocks


# ---------------------------
# Address parsing
# ---------------------------
CITY_STATE_ZIP_RE = re.compile(
    r"^\s*(?P<city>[^,]+)\s*,\s*(?P<state>[A-Z]{2})\s*(?P<zip>\d{5})(?:-\d{4})?\s*$"
)


def parse_city_state_zip(line: str) -> Tuple[str, str, str]:
    if not line:
        return "", "", ""
    m = CITY_STATE_ZIP_RE.match(line.strip())
    if not m:
        return "", "", ""
    return m.group("city").strip(), m.group("state").strip(), m.group("zip").strip()


def zip_to_region(zip5: str) -> str:
    if not zip5 or not zip5[0].isdigit():
        return ""
    d = int(zip5[0])
    if 0 <= d <= 3:
        return f"美东{d}"
    if 4 <= d <= 7:
        return f"美中{d}"
    return f"美西{d}"


def extract_address_block(text_block: str) -> str:
    # Prefer end="美国" else fallback to "地址类型:" or "联系买家:"
    return clean_value(between_any_end(text_block, "配送地址", ["美国", "地址类型:", "联系买家:"]))


def parse_address(addr_block: str) -> Tuple[str, str, str, str]:
    """
    Returns: name, line1, line2(joined), cityline
    Strategy:
      - remove empty lines
      - name = first line
      - cityline = last line
      - middle lines => address lines (0..n)
    """
    if not addr_block:
        return "", "", "", ""
    lines = [clean_value(x) for x in addr_block.splitlines() if clean_value(x)]
    if len(lines) < 2:
        return "", "", "", ""
    name = lines[0]
    cityline = lines[-1]
    mid = lines[1:-1]
    line1 = mid[0] if len(mid) >= 1 else ""
    line2 = " ".join(mid[1:]) if len(mid) >= 2 else ""
    return name, line1, line2, cityline


# ---------------------------
# Phone parsing (anchored)
# ---------------------------
def normalize_phone(s: str) -> str:
    digits = re.sub(r"\D+", "", s or "")
    if digits.startswith("1") and len(digits) == 11:
        digits = digits[1:]
    return digits


def extract_phone(order_block: str) -> str:
    buyer_block = between_any_end(order_block, "联系买家:", ["更多详情", "收税模型:"])
    m = re.search(r"电话:\s*([+\d\-\s]+)", buyer_block or "")
    if m:
        return normalize_phone(m.group(1))
    # fallback (first occurrence)
    m2 = re.search(r"电话:\s*([+\d\-\s]+)", order_block)
    return normalize_phone(m2.group(1)) if m2 else ""


# ---------------------------
# Header parsing (anchored)
# ---------------------------
def extract_header_block(order_block: str) -> str:
    # Only parse top fields from stable header between '订单一览' and '订单内容'
    return clean_value(between(order_block, "订单一览", "订单内容"))


def parse_top_dates(header_block: str) -> Dict[str, str]:
    """
    Extract by A->B anchoring to avoid package-level duplicates.
    """
    rec: Dict[str, str] = {}
    ship = clean_value(between(header_block, "发货日期:", "送达日期:"))
    deliver = clean_value(between(header_block, "送达日期:", "购买日期:"))

    buy = ""
    if "购买日期:" in header_block:
        buy = clean_value(between_any_end(header_block, "购买日期:", ["配送服务:", "配送:", "销售渠道:"]))
        if not buy:
            buy = after_line(header_block, "购买日期:")

    if ship:
        rec["发货日"] = ship
    if deliver:
        rec["送达日"] = deliver
    if buy:
        rec["出单日期"] = buy
    return rec


# ---------------------------
# Package parsing (anchored)
# ---------------------------
PKG_ANCHOR_RE = re.compile(r"包裹\s*(\d+)")


def split_packages(order_block: str) -> List[Tuple[str, str]]:
    ms = list(PKG_ANCHOR_RE.finditer(order_block))
    out: List[Tuple[str, str]] = []
    for i, m in enumerate(ms):
        idx = m.group(1)
        start = m.start()
        end = ms[i + 1].start() if i + 1 < len(ms) else len(order_block)
        out.append((idx, order_block[start:end]))
    return out


def parse_package(pkg_block: str, pkg_idx: str) -> Dict[str, str]:
    carrier = clean_value(between_any_end(pkg_block, "承运人", ["追踪编码", "配送服务", "\n"]))
    tracking = clean_value(between_any_end(pkg_block, "追踪编码", ["配送服务", "\n"]))

    service = ""
    if "配送服务" in pkg_block:
        rhs = after_line(pkg_block, "配送服务")
        service = ("配送服务" + (rhs if rhs else "")).strip()

    return {
        "package_index": pkg_idx,
        "carrier": carrier,
        "tracking": tracking,
        "service_line": service,
    }


def choose_small_parcel(packages: List[Dict[str, str]]) -> Dict[str, str]:
    """
    Prefer FedEx with tracking; if multiple, choose one with longer tracking,
    and if tie choose later one (often second row).
    """
    if not packages:
        return {"carrier": "", "tracking": "", "service_line": "", "package_index": ""}

    fedex = [
        p for p in packages
        if p.get("tracking") and (
            "fedex" in (p.get("carrier", "").lower() + " " + p.get("service_line", "").lower())
        )
    ]
    if fedex:
        best = None
        best_len = -1
        for p in fedex:
            tl = len(p.get("tracking", ""))
            if tl > best_len:
                best = p
                best_len = tl
            elif tl == best_len:
                best = p  # later wins
        return best or fedex[-1]

    anytrk = [p for p in packages if p.get("tracking")]
    return anytrk[0] if anytrk else packages[-1]


# ---------------------------
# Optional freight heuristics
# ---------------------------
FREIGHT_CARRIER_RE = re.compile(r"\b(CEVA\s*Logistics|CEVA)\b", re.IGNORECASE)
FREIGHT_NO_RE = re.compile(r"(?:真实号码|真实号|PRO|运单号|单号)\s*[:：]?\s*([A-Za-z0-9]+)")


def parse_freight(order_block: str) -> Tuple[str, str]:
    carrier = ""
    freight_no = ""
    mc = FREIGHT_CARRIER_RE.search(order_block)
    if mc:
        carrier = mc.group(1)
    mn = FREIGHT_NO_RE.search(order_block)
    if mn:
        freight_no = mn.group(1)
    return carrier, freight_no


# ---------------------------
# Main parse: amazon text -> internal_orders upsert records
# ---------------------------
def parse_amazon_orders(text: str) -> List[Dict[str, str]]:
    text = normalize_text(text)
    out: List[Dict[str, str]] = []
    for oid, order_block in split_order_blocks(text):
        rec: Dict[str, str] = {"订单编号": oid}

        header = extract_header_block(order_block)
        rec.update(parse_top_dates(header))

        # Address (prefer header, fallback to order)
        addr_block = extract_address_block(header) or extract_address_block(order_block)
        name, line1, line2, cityline = parse_address(addr_block)
        if name and cityline:
            addr_lines = [name]
            if line1:
                addr_lines.append(line1)
            if line2:
                addr_lines.append(line2)
            addr_lines.append(cityline)
            rec["客户地址"] = "\n".join(addr_lines)

            city, state, zip5 = parse_city_state_zip(cityline)
            rec["城市"] = city
            rec["州"] = state
            rec["邮编"] = zip5
            rec["区域"] = zip_to_region(zip5)

        phone = extract_phone(order_block)
        if phone:
            rec["电话"] = phone

        # Packages
        pkgs: List[Dict[str, str]] = []
        for idx, blk in split_packages(order_block):
            pkgs.append(parse_package(blk, idx))
        chosen = choose_small_parcel(pkgs)
        if chosen.get("tracking"):
            rec["联邦单号"] = chosen["tracking"]
        if chosen.get("service_line"):
            rec["联邦方式"] = chosen["service_line"]

        # Optional freight
        fc, fn = parse_freight(order_block)
        if fc:
            rec["货运承运人"] = fc
        if fn:
            rec["货运单号"] = fn

        out.append(rec)

    return out


# ---------------------------
# internal_orders upsert & supplier quote & export
# ---------------------------
def product_code_from_name(product_name: str) -> Optional[str]:
    if not isinstance(product_name, str) or not product_name.strip():
        return None
    return product_name.splitlines()[-1].strip()


def upsert_internal_orders(internal_df: pd.DataFrame, parsed_orders: List[Dict[str, str]]) -> pd.DataFrame:
    if "订单编号" not in internal_df.columns:
        raise ValueError("internal_orders must contain column: 订单编号")

    df = internal_df.copy()
    ensure_cols = [
        "发货日", "送达日", "出单日期",
        "客户地址", "电话",
        "联邦方式", "联邦单号",
        "区域", "城市", "州", "邮编",
        "货运承运人", "货运单号",
    ]
    for c in ensure_cols:
        if c not in df.columns:
            df[c] = ""

    for rec in parsed_orders:
        oid = rec.get("订单编号")
        if not oid:
            continue
        mask = df["订单编号"].astype(str) == str(oid)
        if mask.any():
            idx = df.index[mask][0]
            for k, v in rec.items():
                if k in df.columns:
                    df.at[idx, k] = v
        else:
            new_row = {col: "" for col in df.columns}
            for k, v in rec.items():
                if k in df.columns:
                    new_row[k] = v
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    return df


def apply_supplier_quotes(internal_df: pd.DataFrame, supplier_df: pd.DataFrame) -> pd.DataFrame:
    df = internal_df.copy()
    if "产品名" not in df.columns or "箱唛" not in supplier_df.columns:
        return df

    if "产品编码" not in df.columns:
        df["产品编码"] = df["产品名"].apply(product_code_from_name)

    sq = supplier_df.copy()
    if "单价（元）" in sq.columns:
        sq = sq[sq["箱唛"].notna() & sq["单价（元）"].notna()].copy()
    else:
        sq = sq[sq["箱唛"].notna()].copy()

    for c in ["工厂内部单号", "供应商出货日期"]:
        if c not in df.columns:
            df[c] = ""

    for _, r in sq.iterrows():
        code = str(r.get("箱唛", "")).strip()
        if not code:
            continue
        mask = df["产品编码"].astype(str) == code
        if not mask.any():
            continue
        idx = df.index[mask][0]

        if "工厂内部\n单号" in sq.columns and pd.notna(r.get("工厂内部\n单号")):
            df.at[idx, "工厂内部单号"] = r.get("工厂内部\n单号")
        if "下单日期" in sq.columns and pd.notna(r.get("下单日期")) and "下单日期" in df.columns:
            df.at[idx, "下单日期"] = r.get("下单日期")
        if "发货日期" in sq.columns and pd.notna(r.get("发货日期")):
            df.at[idx, "供应商出货日期"] = r.get("发货日期")
        if "数量（套）" in sq.columns and pd.notna(r.get("数量（套）")) and "采购数量" in df.columns:
            df.at[idx, "采购数量"] = r.get("数量（套）")
        if "单价（元）" in sq.columns and pd.notna(r.get("单价（元）")) and "单价" in df.columns:
            df.at[idx, "单价"] = r.get("单价（元）")
        if "总价" in df.columns and "数量（套）" in sq.columns and "单价（元）" in sq.columns:
            try:
                df.at[idx, "总价"] = float(r.get("数量（套）")) * float(r.get("单价（元）"))
            except Exception:
                pass

    return df


def build_kapi_defaults(kapi_template_df: pd.DataFrame) -> Dict[str, object]:
    defaults: Dict[str, object] = {}
    for col in kapi_template_df.columns:
        if kapi_template_df[col].nunique(dropna=False) == 1:
            defaults[col] = kapi_template_df[col].iloc[0]
    return defaults


def export_kapi(internal_df: pd.DataFrame, kapi_template_df: pd.DataFrame, order_ids: Optional[List[str]] = None) -> pd.DataFrame:
    df = internal_df.copy()
    if order_ids:
        df = df[df["订单编号"].astype(str).isin([str(x) for x in order_ids])].copy()

    defaults = build_kapi_defaults(kapi_template_df)
    out_rows: List[Dict[str, object]] = []

    for _, r in df.iterrows():
        addr = str(r.get("客户地址", "") or "")
        lines = [clean_value(x) for x in addr.splitlines() if clean_value(x)]
        name = lines[0] if len(lines) > 0 else ""
        cityline = lines[-1] if len(lines) > 0 else ""
        # For kapi line1 = first address line (if exists)
        line1 = ""
        if len(lines) >= 3:
            line1 = lines[1]
        elif len(lines) == 2:
            line1 = lines[1]

        city, state, zip5 = parse_city_state_zip(cityline)

        code = product_code_from_name(r.get("产品名", "")) or r.get("产品编码", "") or ""
        phone = normalize_phone(str(r.get("电话", "") or ""))

        row: Dict[str, object] = {}
        row.update(defaults)

        row["Customer orderNo"] = code
        row["Ref"] = r.get("单号", "")
        row["Receiver Contact Name"] = name
        row["Receiver Address Name"] = name
        row["Receiver Address Line1"] = line1
        row["Receiver City*"] = city or ""
        row["Receiver State*"] = state or ""
        row["Receiver Zip Code*"] = zip5 or ""
        row["Receiver Contact Phone"] = phone

        row["Declared($)*"] = r.get("售价", "")
        try:
            row["Length*"] = float(r.get("长cm")) / 2.54
        except Exception:
            row["Length*"] = ""
        row["Weight*"] = r.get("镑重量\n＜150lb", "")

        ordered = {col: row.get(col, "") for col in kapi_template_df.columns}
        out_rows.append(ordered)

    return pd.DataFrame(out_rows, columns=list(kapi_template_df.columns))


def run_pipeline(
    amazon_txt_path: str,
    internal_xlsx_path: str,
    supplier_xlsx_path: str,
    kapi_template_path: str,
    out_internal_path: str,
    out_kapi_path: str,
    order_ids: Optional[List[str]] = None,
) -> Tuple[str, str]:
    with open(amazon_txt_path, "r", encoding="utf-8") as f:
        text = f.read()

    internal_df = pd.read_excel(internal_xlsx_path)
    supplier_df = pd.read_excel(supplier_xlsx_path)
    kapi_template_df = pd.read_excel(kapi_template_path)

    parsed = parse_amazon_orders(text)
    internal_df2 = upsert_internal_orders(internal_df, parsed)
    internal_df3 = apply_supplier_quotes(internal_df2, supplier_df)

    internal_df3.to_excel(out_internal_path, index=False)

    kapi_out = export_kapi(internal_df3, kapi_template_df, order_ids=order_ids)
    kapi_out.to_excel(out_kapi_path, index=False)

    return out_internal_path, out_kapi_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--amazon", default="amazon_order_text.txt")
    ap.add_argument("--internal", default="internal_orders.xlsx")
    ap.add_argument("--supplier", default="supplier_quote.xlsx")
    ap.add_argument("--kapi", default="kapi_template.xlsx")
    ap.add_argument("--out_internal", default="internal_orders.updated.xlsx")
    ap.add_argument("--out_kapi", default="kapi_export.xlsx")
    ap.add_argument("--order_ids", nargs="*", default=None)
    args = ap.parse_args()

    out_internal, out_kapi = run_pipeline(
        amazon_txt_path=args.amazon,
        internal_xlsx_path=args.internal,
        supplier_xlsx_path=args.supplier,
        kapi_template_path=args.kapi,
        out_internal_path=args.out_internal,
        out_kapi_path=args.out_kapi,
        order_ids=args.order_ids,
    )
    print(f"Wrote: {out_internal}")
    print(f"Wrote: {out_kapi}")


if __name__ == "__main__":
    main()
