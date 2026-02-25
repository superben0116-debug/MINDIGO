import os
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

BASE = "/Users/baicai/Downloads/终极 ERP/docs/inputs"
OUT = "/Users/baicai/Downloads/终极 ERP/docs/outputs"
NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def read_shared_strings(z):
    try:
        data = z.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ET.fromstring(data)
    strings = []
    for si in root.findall("a:si", NS):
        texts = []
        for t in si.findall(".//a:t", NS):
            texts.append(t.text or "")
        strings.append("".join(texts))
    return strings


def get_sheet_map(z):
    wb = ET.fromstring(z.read("xl/workbook.xml"))
    rels = ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))
    rid_to_target = {rel.get("Id"): rel.get("Target") for rel in rels.findall("{http://schemas.openxmlformats.org/package/2006/relationships}Relationship")}
    sheets = []
    for s in wb.findall("a:sheets/a:sheet", NS):
        rid = s.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        name = s.get("name")
        target = rid_to_target.get(rid)
        if target:
            if not target.startswith("xl/"):
                target = "xl/" + target
            sheets.append((name, target))
    return sheets


def sheet_to_rows(z, sheet_path, shared):
    root = ET.fromstring(z.read(sheet_path))
    rows = []

    def col_index(cell_ref: str) -> int:
        letters = "".join([ch for ch in cell_ref if ch.isalpha()])
        n = 0
        for ch in letters:
            n = n * 26 + (ord(ch.upper()) - 64)
        return n - 1

    for row in root.findall("a:sheetData/a:row", NS):
        cells = {}
        max_col = -1
        for c in row.findall("a:c", NS):
            ref = c.get("r") or ""
            idx = col_index(ref) if ref else len(cells)
            v = c.find("a:v", NS)
            if v is None:
                val = ""
            else:
                val = v.text or ""
                if c.get("t") == "s":
                    try:
                        val = shared[int(val)]
                    except Exception:
                        pass
            cells[idx] = val
            max_col = max(max_col, idx)
        r = ["" for _ in range(max_col + 1)]
        for idx, val in cells.items():
            r[idx] = val
        rows.append(r)
    return rows


def read_xlsx(path):
    with zipfile.ZipFile(path) as z:
        shared = read_shared_strings(z)
        sheets = get_sheet_map(z)
        data = {}
        for name, spath in sheets:
            rows = sheet_to_rows(z, spath, shared)
            data[name] = rows
        return data


def excel_date_to_iso(val):
    try:
        n = float(val)
    except Exception:
        return val
    base = datetime(1899, 12, 30)
    return (base + timedelta(days=n)).date().isoformat()


def parse_amazon_text(text):
    blocks = [b.strip() for b in text.split("订单详情") if b.strip()]
    if not blocks:
        blocks = [text.strip()]
    orders = []
    for b in blocks:
        order_no = re.search(r"订单编号[:：]\s*#?\s*([0-9-]+)", b)
        if not order_no:
            continue
        order_no = order_no.group(1)
        ship_date = re.search(r"发货日期:\s*([^\n]+)", b)
        deliver = re.search(r"送达日期:\s*([^\n]+)", b)
        purchase = re.search(r"购买日期:\s*([^\n]+)", b)
        addr_block = re.search(r"配送地址\s*\n\n([\s\S]+?)\n更多详情", b)
        addr = addr_block.group(1).strip() if addr_block else ""
        asin = re.search(r"ASIN:\s*([A-Z0-9]+)", b)
        sku = re.search(r"SKU:\s*([^\n]+)", b)
        product = re.search(r"已发货\s*\n\s*([\s\S]+?)\nASIN:", b)
        tracking = re.search(r"追踪编码\s*\n\s*([0-9A-Z]+)", b)
        carrier = re.search(r"承运人([^\n]+)", b)
        orders.append({
            "order_no": order_no,
            "ship_date": ship_date.group(1).strip() if ship_date else "",
            "deliver_date": deliver.group(1).strip() if deliver else "",
            "purchase_date": purchase.group(1).strip() if purchase else "",
            "address": addr,
            "asin": asin.group(1).strip() if asin else "",
            "sku": sku.group(1).strip() if sku else "",
            "product_name": product.group(1).strip().replace("\n", " ") if product else "",
            "tracking": tracking.group(1).strip() if tracking else "",
            "carrier": carrier.group(1).strip() if carrier else "",
        })
    return orders


def parse_supplier_quote(rows):
    header = rows[0]
    data = []
    cur = None
    for r in rows[1:]:
        r += [""] * (len(header) - len(r))
        seq = r[0].strip() if r[0] else ""
        if seq:
            if cur:
                data.append(cur)
            cur = {
                "seq": seq,
                "factory_no": r[1],
                "order_date": excel_date_to_iso(r[2]),
                "qty": r[3],
                "cm": r[4],
                "product_lines": [r[6]] if r[6] else [],
                "marks": r[7],
                "unit_price": r[8],
                "ship_date": excel_date_to_iso(r[9]),
                "remark_lines": [r[10]] if len(r) > 10 and r[10] else [],
            }
        elif cur:
            if r[6]:
                cur["product_lines"].append(r[6])
            if len(r) > 10 and r[10]:
                cur["remark_lines"].append(r[10])
    if cur:
        data.append(cur)
    for d in data:
        d["product_name"] = "\n".join([x for x in d["product_lines"] if x])
        d["remark"] = "\n".join([x for x in d["remark_lines"] if x])
    return data


def parse_internal_orders(rows):
    header = rows[0]
    out = []
    for r in rows[1:]:
        if not any(x for x in r if str(x).strip()):
            continue
        r += [""] * (len(header) - len(r))
        obj = {header[i]: r[i] for i in range(len(header))}
        out.append(obj)
    return out


def get_product_model_tail(product_name):
    if not product_name:
        return ""
    parts = [p for p in product_name.split("\n") if p.strip()]
    return parts[-1].strip() if parts else product_name.strip()


def compute_kapi_defaults(rows):
    header = rows[0]
    r1 = rows[1]
    r2 = rows[2]
    maxlen = max(len(header), len(r1), len(r2))
    header += [""] * (maxlen - len(header))
    r1 += [""] * (maxlen - len(r1))
    r2 += [""] * (maxlen - len(r2))
    defaults = {}
    diffs = {}
    for h, a, b in zip(header, r1, r2):
        if a == b and a != "":
            defaults[h] = a
        elif a != b:
            diffs[h] = (a, b)
    return defaults, diffs


def write_xlsx(path, header, rows):
    def cell(col, row):
        return f"{col}{row}"

    def col_name(n):
        name = ""
        while n > 0:
            n, r = divmod(n - 1, 26)
            name = chr(65 + r) + name
        return name

    def build_sheet_xml():
        sheet = ET.Element("worksheet", {"xmlns": NS["a"]})
        sheetData = ET.SubElement(sheet, "sheetData")
        all_rows = [header] + rows
        for i, row in enumerate(all_rows, start=1):
            r_el = ET.SubElement(sheetData, "row", {"r": str(i)})
            for j, val in enumerate(row, start=1):
                c = ET.SubElement(r_el, "c", {"r": cell(col_name(j), i), "t": "inlineStr"})
                is_el = ET.SubElement(c, "is")
                t_el = ET.SubElement(is_el, "t")
                t_el.text = "" if val is None else str(val)
        return ET.tostring(sheet, encoding="utf-8", xml_declaration=True)

    ct = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">
  <Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>
  <Default Extension=\"xml\" ContentType=\"application/xml\"/>
  <Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>
  <Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>
</Types>"""

    rels = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">
  <Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>
</Relationships>"""

    wb = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">
  <sheets>
    <sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/>
  </sheets>
</workbook>"""

    wb_rels = """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">
  <Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet1.xml\"/>
</Relationships>"""

    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", ct)
        z.writestr("_rels/.rels", rels)
        z.writestr("xl/workbook.xml", wb)
        z.writestr("xl/_rels/workbook.xml.rels", wb_rels)
        z.writestr("xl/worksheets/sheet1.xml", build_sheet_xml())


def main():
    os.makedirs(OUT, exist_ok=True)

    with open(os.path.join(BASE, "amazon_order_text.txt"), "r", encoding="utf-8") as f:
        amazon_text = f.read()
    amazon_orders = parse_amazon_text(amazon_text)

    sq = read_xlsx(os.path.join(BASE, "supplier_quote.xlsx"))
    sq_rows = sq[list(sq.keys())[0]]
    supplier_quotes = parse_supplier_quote(sq_rows)

    io = read_xlsx(os.path.join(BASE, "internal_orders.xlsx"))
    io_rows = io[list(io.keys())[0]]
    internal_orders = parse_internal_orders(io_rows)

    kt = read_xlsx(os.path.join(BASE, "kapi_template.xlsx"))
    kt_rows = kt[list(kt.keys())[0]]
    defaults, diffs = compute_kapi_defaults(kt_rows)

    # mapping summary
    mapping_path = os.path.join(OUT, "conversion_flow.md")
    with open(mapping_path, "w", encoding="utf-8") as f:
        f.write("# 转换流程梳理\n\n")
        f.write("## 订单号识别（2组）\n")
        for o in amazon_orders:
            f.write(f"- {o['order_no']}\n")
        f.write("\n## 供应商报价（合并多行）\n")
        for q in supplier_quotes:
            f.write(f"- 序号 {q['seq']} 工厂单号 {q['factory_no']} 产品名: {q['product_name']}\n")
        f.write("\n## 卡派模板默认值（上下两行一致）\n")
        for k, v in defaults.items():
            f.write(f"- {k}: {v}\n")
        f.write("\n## 卡派模板可变字段（按订单）\n")
        for k, v in diffs.items():
            f.write(f"- {k}: {v[0]} | {v[1]}\n")
        f.write("\n## 映射规则（核心）\n")
        f.write("- Customer orderNo = 产品名里末尾型号（内订表产品名最后一行）\n")

    # internal order generation
    header = io_rows[0]
    order_by_no = {o["order_no"]: o for o in amazon_orders}
    out_rows = []
    for row in internal_orders:
        order_no = row.get("订单编号")
        if order_no in order_by_no:
            info = order_by_no[order_no]
            row["出单日期"] = info.get("purchase_date")
            row["SKU"] = info.get("sku")
            row["产品名"] = info.get("product_name")
            row["联邦单号"] = info.get("tracking")
            row["联邦方式"] = info.get("carrier")
            row["客户地址"] = info.get("address")
            row["送达日"] = info.get("deliver_date")
            row["发货日"] = info.get("ship_date")
            row["ASIN"] = info.get("asin")
        out_rows.append([row.get(h, "") for h in header])

    internal_out = os.path.join(OUT, "internal_orders_generated.xlsx")
    write_xlsx(internal_out, header, out_rows)

    # supplier quote merged output
    sq_header = ["序号","工厂内部单号","下单日期","数量（套）","尺寸cm","产品名称","箱唛","单价（元）","发货日期","备注"]
    sq_rows_out = []
    for q in supplier_quotes:
        sq_rows_out.append([
            q.get("seq"), q.get("factory_no"), q.get("order_date"), q.get("qty"), q.get("cm"),
            q.get("product_name"), q.get("marks"), q.get("unit_price"), q.get("ship_date"), q.get("remark")
        ])
    supplier_out = os.path.join(OUT, "supplier_quote_merged.xlsx")
    write_xlsx(supplier_out, sq_header, sq_rows_out)

    # kapi export sample
    kt_header = kt_rows[0]
    rows = []
    for o in internal_orders[:2]:
        row = ["" for _ in kt_header]
        for i, h in enumerate(kt_header):
            if h in defaults:
                row[i] = defaults[h]
        product_name = o.get("产品名", "")
        model = get_product_model_tail(product_name)
        if "Customer orderNo" in kt_header:
            row[kt_header.index("Customer orderNo")] = model
        if "Ref" in kt_header:
            row[kt_header.index("Ref")] = o.get("单号", "")
        if "Receiver Zip Code*" in kt_header:
            addr = o.get("客户地址", "")
            m = re.search(r"(\d{5})(?:-\d{4})?", addr)
            row[kt_header.index("Receiver Zip Code*")] = m.group(1) if m else ""
        if "Receiver City*" in kt_header:
            m = re.search(r"\n([^,\n]+),\s*([A-Z]{2})\s*\d{5}", o.get("客户地址", ""))
            row[kt_header.index("Receiver City*")] = m.group(1) if m else ""
        if "Receiver State*" in kt_header:
            m = re.search(r"\n[^,\n]+,\s*([A-Z]{2})\s*\d{5}", o.get("客户地址", ""))
            row[kt_header.index("Receiver State*")] = m.group(1) if m else ""
        if "Receiver Contact Name" in kt_header:
            addr = o.get("客户地址", "")
            first = addr.split("\n")[0] if addr else ""
            row[kt_header.index("Receiver Contact Name")] = first
        if "Receiver Contact Phone" in kt_header:
            m = re.search(r"\+?\d+", o.get("客户地址", ""))
            row[kt_header.index("Receiver Contact Phone")] = m.group(0).replace("+", "") if m else ""
        if "Receiver Address Name" in kt_header:
            addr = o.get("客户地址", "")
            first = addr.split("\n")[0] if addr else ""
            row[kt_header.index("Receiver Address Name")] = first
        if "Receiver Address Line1" in kt_header:
            addr = o.get("客户地址", "")
            lines = addr.split("\n")
            row[kt_header.index("Receiver Address Line1")] = lines[1] if len(lines) > 1 else ""
        if "Declared($)*" in kt_header:
            row[kt_header.index("Declared($)*")] = o.get("售价", "")
        if "Length*" in kt_header:
            row[kt_header.index("Length*")] = o.get("长in\n＜80", "")
        if "Weight*" in kt_header:
            row[kt_header.index("Weight*")] = o.get("镑重量\n＜150lb", "")
        rows.append(row)

    out_path = os.path.join(OUT, "kapi_export_sample.xlsx")
    write_xlsx(out_path, kt_header, rows)

    print("wrote:", mapping_path)
    print("wrote:", internal_out)
    print("wrote:", supplier_out)
    print("wrote:", out_path)


if __name__ == "__main__":
    main()
