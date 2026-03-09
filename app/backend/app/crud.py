from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from app import models


_BUYER_STICKY_FIELDS = {
    "客户地址",
    "customer_address",
    "receiver_name",
    "buyer_name",
    "customer_name",
    "receiver_mobile",
    "receiver_tel",
    "电话",
    "buyer_email",
    "receiver_country_code",
    "address_type",
    "address_type_name",
    "address_line1",
    "address_line2",
    "address_line3",
    "doorplate_no",
    "district",
    "city",
    "state_or_region",
    "postal_code",
}


def _is_blank_like(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        txt = value.strip()
        if not txt:
            return True
        low = txt.lower()
        if low in {"none", "null", "nan"}:
            return True
        if txt in {"None, None", "none, none"}:
            return True
    return False


def create_internal_order(db: Session, data: Dict, commit: bool = True) -> models.InternalOrder:
    obj = models.InternalOrder(
        internal_order_no=data["internal_order_no"],
        platform_order_no=data.get("platform_order_no"),
        shop_name=data.get("shop_name"),
        order_status=data.get("order_status"),
        purchase_time=data.get("purchase_time"),
        region=data.get("region"),
        customer_address_summary=data.get("customer_address_summary"),
        customer_name=data.get("customer_name"),
        customer_phone=data.get("customer_phone"),
        customer_zip=data.get("customer_zip"),
        customer_city=data.get("customer_city"),
        customer_state=data.get("customer_state"),
        customer_country=data.get("customer_country"),
        customer_address_line1=data.get("customer_address_line1"),
        customer_address_line2=data.get("customer_address_line2"),
        logistics_provider=data.get("logistics_provider"),
        logistics_type=data.get("logistics_type"),
        tracking_no=data.get("tracking_no"),
        total_cost=data.get("total_cost"),
        total_profit=data.get("total_profit"),
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(obj)
    if commit:
        db.commit()
        db.refresh(obj)
    else:
        db.flush()
    return obj


def create_internal_order_item(db: Session, internal_order_id: int, data: Dict, commit: bool = True):
    obj = models.InternalOrderItem(
        internal_order_id=internal_order_id,
        sku=data.get("sku"),
        product_name=data.get("product_name"),
        quantity=data.get("quantity"),
        unit_price=data.get("unit_price"),
        currency=data.get("currency"),
        product_image=data.get("product_image"),
        attachments=data.get("attachments"),
    )
    db.add(obj)
    if commit:
        db.commit()
        db.refresh(obj)
    return obj


def create_internal_order_package(db: Session, internal_order_id: int, data: Dict, commit: bool = True):
    obj = models.InternalOrderPackage(
        internal_order_id=internal_order_id,
        length_cm=data.get("length_cm"),
        width_cm=data.get("width_cm"),
        height_cm=data.get("height_cm"),
        length_in=data.get("length_in"),
        width_in=data.get("width_in"),
        height_in=data.get("height_in"),
        weight_kg=data.get("weight_kg"),
        weight_lb=data.get("weight_lb"),
        billed_weight=data.get("billed_weight"),
        oversize_flag=data.get("oversize_flag", False),
    )
    db.add(obj)
    if commit:
        db.commit()
        db.refresh(obj)
    return obj


def list_internal_orders(db: Session, limit: int = 50, offset: int = 0) -> List[models.InternalOrder]:
    return (
        db.query(models.InternalOrder)
        .order_by(models.InternalOrder.id.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )


def get_internal_order(db: Session, internal_order_id: int) -> Optional[models.InternalOrder]:
    return db.query(models.InternalOrder).filter(models.InternalOrder.id == internal_order_id).first()


def get_order_items(db: Session, internal_order_id: int) -> List[models.InternalOrderItem]:
    return (
        db.query(models.InternalOrderItem)
        .filter(models.InternalOrderItem.internal_order_id == internal_order_id)
        .all()
    )


def get_order_packages(db: Session, internal_order_id: int) -> List[models.InternalOrderPackage]:
    return (
        db.query(models.InternalOrderPackage)
        .filter(models.InternalOrderPackage.internal_order_id == internal_order_id)
        .all()
    )


def get_order_ext(db: Session, internal_order_id: int) -> Optional[models.InternalOrderExt]:
    return (
        db.query(models.InternalOrderExt)
        .filter(models.InternalOrderExt.internal_order_id == internal_order_id)
        .first()
    )


def get_order_by_platform_no(db: Session, platform_order_no: str) -> Optional[models.InternalOrder]:
    return (
        db.query(models.InternalOrder)
        .filter(models.InternalOrder.platform_order_no == platform_order_no)
        .first()
    )


def upsert_order_ext(db: Session, internal_order_id: int, field: str, value):
    obj = get_order_ext(db, internal_order_id)
    if obj:
        # IMPORTANT: JSON columns need reassignment with a new dict object
        # so SQLAlchemy can detect updates reliably on SQLite.
        fields = dict(obj.fields or {})
        fields[field] = value
        obj.fields = fields
        obj.updated_at = datetime.utcnow() if hasattr(obj, "updated_at") else None
        db.commit()
        db.refresh(obj)
        return obj
    obj = models.InternalOrderExt(internal_order_id=internal_order_id, fields={field: value})
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def upsert_order_ext_bulk(db: Session, internal_order_id: int, data: dict):
    obj = get_order_ext(db, internal_order_id)
    if obj:
        # IMPORTANT: JSON columns need reassignment with a new dict object
        # so SQLAlchemy can detect updates reliably on SQLite.
        fields = dict(obj.fields or {})
        for k, v in (data or {}).items():
            # 买家信息字段采用“已抓到就保留”的策略：
            # 后续同步若返回空/None，不覆盖已有有效值，避免信息丢失。
            if k in _BUYER_STICKY_FIELDS and _is_blank_like(v) and not _is_blank_like(fields.get(k)):
                continue
            fields[k] = v
        obj.fields = fields
        db.commit()
        db.refresh(obj)
        return obj
    obj = models.InternalOrderExt(internal_order_id=internal_order_id, fields=data)
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def create_quote_request(db: Session, internal_order_id: int, quote_no: str, visible_payload: Dict):
    # SQLite + BigInteger PK does not autoincrement reliably.
    last_id = db.query(models.SupplierQuoteRequest.id).order_by(models.SupplierQuoteRequest.id.desc()).first()
    next_id = (int(last_id[0]) + 1) if last_id and last_id[0] is not None else 1
    obj = models.SupplierQuoteRequest(
        id=next_id,
        internal_order_id=internal_order_id,
        quote_no=quote_no,
        quote_status="pending",
        visible_payload=visible_payload,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def get_quote_request_by_no(db: Session, quote_no: str) -> Optional[models.SupplierQuoteRequest]:
    return db.query(models.SupplierQuoteRequest).filter(models.SupplierQuoteRequest.quote_no == quote_no).first()


def update_quote_status(db: Session, quote_request_id: int, status: str):
    obj = db.query(models.SupplierQuoteRequest).filter(models.SupplierQuoteRequest.id == quote_request_id).first()
    if obj:
        obj.quote_status = status
        obj.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(obj)
    return obj


def submit_quote_response(db: Session, quote_request_id: int, data: Dict):
    # SQLite + BigInteger PK does not autoincrement reliably.
    last_id = db.query(models.SupplierQuoteResponse.id).order_by(models.SupplierQuoteResponse.id.desc()).first()
    next_id = (int(last_id[0]) + 1) if last_id and last_id[0] is not None else 1
    obj = models.SupplierQuoteResponse(
        id=next_id,
        quote_request_id=quote_request_id,
        quoted_unit_price=data.get("quoted_unit_price"),
        quoted_total_price=data.get("quoted_total_price"),
        lead_time_days=data.get("lead_time_days"),
        supplier_remark=data.get("supplier_remark"),
        submitted_at=datetime.utcnow(),
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def get_config(db: Session, key: str) -> Optional[models.AppConfig]:
    return db.query(models.AppConfig).filter(models.AppConfig.config_key == key).first()


def set_config(db: Session, key: str, value: Dict) -> models.AppConfig:
    obj = get_config(db, key)
    if obj:
        obj.config_value = value
        obj.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(obj)
        return obj
    obj = models.AppConfig(
        config_key=key,
        config_value=value,
        updated_at=datetime.utcnow(),
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def create_import_job(db: Session, job_type: str) -> models.ImportJob:
    obj = models.ImportJob(
        job_type=job_type,
        start_time=datetime.utcnow(),
        status="running",
        success_count=0,
        failed_count=0,
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def update_import_job(db: Session, job_id: int, success: int, failed: int, status: str, error_summary: Optional[str] = None):
    obj = db.query(models.ImportJob).filter(models.ImportJob.id == job_id).first()
    if obj:
        obj.success_count = success
        obj.failed_count = failed
        obj.status = status
        obj.error_summary = error_summary
        obj.end_time = datetime.utcnow()
        db.commit()
        db.refresh(obj)
    return obj


def upsert_import_progress(db: Session, job_id: int, total: int, processed: int, success: int, failed: int):
    obj = db.query(models.ImportJobProgress).filter(models.ImportJobProgress.job_id == job_id).first()
    if obj:
        obj.total = total
        obj.processed = processed
        obj.success = success
        obj.failed = failed
        obj.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(obj)
        return obj
    obj = models.ImportJobProgress(
        job_id=job_id,
        total=total,
        processed=processed,
        success=success,
        failed=failed,
        updated_at=datetime.utcnow(),
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj


def add_import_log(db: Session, job_id: int, level: str, message: str):
    obj = models.ImportJobLog(
        job_id=job_id,
        level=level,
        message=message,
        created_at=datetime.utcnow(),
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj
