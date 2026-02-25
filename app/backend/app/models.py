from sqlalchemy import Column, Integer, BigInteger, String, Numeric, Text, TIMESTAMP, Boolean, ForeignKey, JSON
from app.db import Base


class InternalOrder(Base):
    __tablename__ = "internal_orders"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    internal_order_no = Column(String(64), unique=True, nullable=False)
    platform_order_no = Column(String(64))
    shop_name = Column(String(128))
    order_status = Column(String(32))
    purchase_time = Column(TIMESTAMP)
    region = Column(String(32))
    customer_address_summary = Column(String(256))
    customer_name = Column(String(128))
    customer_phone = Column(String(64))
    customer_zip = Column(String(32))
    customer_city = Column(String(64))
    customer_state = Column(String(64))
    customer_country = Column(String(64))
    customer_address_line1 = Column(String(256))
    customer_address_line2 = Column(String(256))
    logistics_provider = Column(String(128))
    logistics_type = Column(String(128))
    tracking_no = Column(String(128))
    total_cost = Column(Numeric(12, 2))
    total_profit = Column(Numeric(12, 2))
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class InternalOrderItem(Base):
    __tablename__ = "internal_order_items"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    internal_order_id = Column(BigInteger, ForeignKey("internal_orders.id"))
    sku = Column(String(128))
    product_name = Column(String(256))
    quantity = Column(Integer)
    unit_price = Column(Numeric(12, 2))
    currency = Column(String(8))
    product_image = Column(String(512))
    attachments = Column(JSON)


class InternalOrderPackage(Base):
    __tablename__ = "internal_order_packages"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    internal_order_id = Column(BigInteger, ForeignKey("internal_orders.id"))
    length_cm = Column(Numeric(10, 2))
    width_cm = Column(Numeric(10, 2))
    height_cm = Column(Numeric(10, 2))
    length_in = Column(Numeric(10, 2))
    width_in = Column(Numeric(10, 2))
    height_in = Column(Numeric(10, 2))
    weight_kg = Column(Numeric(10, 2))
    weight_lb = Column(Numeric(10, 2))
    billed_weight = Column(Numeric(10, 2))
    oversize_flag = Column(Boolean, default=False)


class InternalOrderExt(Base):
    __tablename__ = "internal_order_ext"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    internal_order_id = Column(Integer, ForeignKey("internal_orders.id"), unique=True)
    fields = Column(JSON, nullable=False)


class SupplierQuoteRequest(Base):
    __tablename__ = "supplier_quote_requests"

    id = Column(BigInteger, primary_key=True, index=True)
    internal_order_id = Column(BigInteger, ForeignKey("internal_orders.id"))
    quote_no = Column(String(64), unique=True, nullable=False)
    quote_status = Column(String(32), default="pending")
    visible_payload = Column(JSON, nullable=False)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class SupplierQuoteResponse(Base):
    __tablename__ = "supplier_quote_responses"

    id = Column(BigInteger, primary_key=True, index=True)
    quote_request_id = Column(BigInteger, ForeignKey("supplier_quote_requests.id"))
    quoted_unit_price = Column(Numeric(12, 2))
    quoted_total_price = Column(Numeric(12, 2))
    lead_time_days = Column(Integer)
    supplier_remark = Column(Text)
    submitted_at = Column(TIMESTAMP)


class KapiExport(Base):
    __tablename__ = "kapi_exports"

    id = Column(BigInteger, primary_key=True, index=True)
    export_batch_no = Column(String(64), unique=True, nullable=False)
    created_by = Column(String(64))
    created_at = Column(TIMESTAMP)


class KapiExportItem(Base):
    __tablename__ = "kapi_export_items"

    id = Column(BigInteger, primary_key=True, index=True)
    export_id = Column(BigInteger, ForeignKey("kapi_exports.id"))
    internal_order_id = Column(BigInteger, ForeignKey("internal_orders.id"))
    mapped_fields = Column(JSON)


class IntegrationLingxingToken(Base):
    __tablename__ = "integration_lingxing_tokens"

    id = Column(BigInteger, primary_key=True, index=True)
    app_id = Column(String(128), nullable=False)
    access_token = Column(String(256), nullable=False)
    refresh_token = Column(String(256), nullable=False)
    expires_at = Column(TIMESTAMP, nullable=False)
    created_at = Column(TIMESTAMP)


class ImportJob(Base):
    __tablename__ = "import_jobs"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    job_type = Column(String(64))
    start_time = Column(TIMESTAMP)
    end_time = Column(TIMESTAMP)
    status = Column(String(32))
    success_count = Column(Integer)
    failed_count = Column(Integer)
    error_summary = Column(Text)


class ImportJobProgress(Base):
    __tablename__ = "import_job_progress"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("import_jobs.id"))
    total = Column(Integer)
    processed = Column(Integer)
    success = Column(Integer)
    failed = Column(Integer)
    updated_at = Column(TIMESTAMP)


class ImportJobLog(Base):
    __tablename__ = "import_job_logs"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey("import_jobs.id"))
    level = Column(String(16))
    message = Column(Text)
    created_at = Column(TIMESTAMP)


class AppConfig(Base):
    __tablename__ = "app_config"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    config_key = Column(String(128), unique=True, nullable=False)
    config_value = Column(JSON, nullable=False)
    updated_at = Column(TIMESTAMP)


class AuthUser(Base):
    __tablename__ = "auth_users"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    username = Column(String(64), unique=True, nullable=False)
    password_hash = Column(String(256), nullable=False)
    role = Column(String(16), nullable=False)  # admin / supplier
    supplier_name = Column(String(128))
    is_active = Column(Boolean, default=True)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class AuthSession(Base):
    __tablename__ = "auth_sessions"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    token = Column(String(128), unique=True, nullable=False)
    user_id = Column(Integer, ForeignKey("auth_users.id"), nullable=False)
    expires_at = Column(TIMESTAMP, nullable=False)
    created_at = Column(TIMESTAMP)
