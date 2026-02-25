import zipfile
import xml.etree.ElementTree as ET

NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _read_shared_strings(z):
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


def _sheet_map(z):
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


def _col_index(cell_ref: str) -> int:
    letters = "".join([ch for ch in cell_ref if ch.isalpha()])
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch.upper()) - 64)
    return n - 1


def read_xlsx(path: str):
    with zipfile.ZipFile(path) as z:
        shared = _read_shared_strings(z)
        sheets = _sheet_map(z)
        data = {}
        for name, spath in sheets:
            root = ET.fromstring(z.read(spath))
            rows = []
            for row in root.findall("a:sheetData/a:row", NS):
                cells = {}
                max_col = -1
                for c in row.findall("a:c", NS):
                    ref = c.get("r") or ""
                    idx = _col_index(ref) if ref else len(cells)
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
            data[name] = rows
        return data


def write_xlsx(path: str, header, rows):
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
