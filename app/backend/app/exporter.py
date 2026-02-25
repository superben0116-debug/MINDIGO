import csv
from typing import List, Dict


def export_kapi_csv(file_path: str, rows: List[Dict[str, str]]):
    if not rows:
        return file_path
    headers = list(rows[0].keys())
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    return file_path
