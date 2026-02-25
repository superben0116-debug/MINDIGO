from pydantic import BaseModel
from typing import List, Optional


class PackageVisible(BaseModel):
    image_url: Optional[str] = None
    dimension_unit: Optional[str] = None
    length: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    weight: Optional[float] = None


class SupplierQuoteTemplate(BaseModel):
    quote_no: str
    items: List[PackageVisible]
    quoted_unit_price: Optional[float] = None
    quoted_total_price: Optional[float] = None
    lead_time_days: Optional[int] = None
    supplier_remark: Optional[str] = None
