-- ERP core schema (draft)

CREATE TABLE IF NOT EXISTS internal_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  internal_order_no VARCHAR(64) UNIQUE NOT NULL,
  platform_order_no VARCHAR(64),
  shop_name VARCHAR(128),
  order_status VARCHAR(32),
  purchase_time TIMESTAMP,
  region VARCHAR(32),
  customer_address_summary VARCHAR(256),
  customer_name VARCHAR(128),
  customer_phone VARCHAR(64),
  customer_zip VARCHAR(32),
  customer_city VARCHAR(64),
  customer_state VARCHAR(64),
  customer_country VARCHAR(64),
  customer_address_line1 VARCHAR(256),
  customer_address_line2 VARCHAR(256),
  logistics_provider VARCHAR(128),
  logistics_type VARCHAR(128),
  tracking_no VARCHAR(128),
  total_cost NUMERIC(12,2),
  total_profit NUMERIC(12,2),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS internal_order_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  internal_order_id BIGINT REFERENCES internal_orders(id),
  sku VARCHAR(128),
  product_name VARCHAR(256),
  quantity INT,
  unit_price NUMERIC(12,2),
  currency VARCHAR(8),
  product_image VARCHAR(512),
  attachments JSONB
);

CREATE TABLE IF NOT EXISTS internal_order_packages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  internal_order_id BIGINT REFERENCES internal_orders(id),
  length_cm NUMERIC(10,2),
  width_cm NUMERIC(10,2),
  height_cm NUMERIC(10,2),
  length_in NUMERIC(10,2),
  width_in NUMERIC(10,2),
  height_in NUMERIC(10,2),
  weight_kg NUMERIC(10,2),
  weight_lb NUMERIC(10,2),
  billed_weight NUMERIC(10,2),
  oversize_flag BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS internal_order_ext (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  internal_order_id INTEGER UNIQUE REFERENCES internal_orders(id),
  fields JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS supplier_quote_requests (
  id BIGSERIAL PRIMARY KEY,
  internal_order_id BIGINT REFERENCES internal_orders(id),
  quote_no VARCHAR(64) UNIQUE NOT NULL,
  quote_status VARCHAR(32) DEFAULT 'pending',
  visible_payload JSONB NOT NULL,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS supplier_quote_responses (
  id BIGSERIAL PRIMARY KEY,
  quote_request_id BIGINT REFERENCES supplier_quote_requests(id),
  quoted_unit_price NUMERIC(12,2),
  quoted_total_price NUMERIC(12,2),
  lead_time_days INT,
  supplier_remark TEXT,
  submitted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS kapi_exports (
  id BIGSERIAL PRIMARY KEY,
  export_batch_no VARCHAR(64) UNIQUE NOT NULL,
  created_by VARCHAR(64),
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS kapi_export_items (
  id BIGSERIAL PRIMARY KEY,
  export_id BIGINT REFERENCES kapi_exports(id),
  internal_order_id BIGINT REFERENCES internal_orders(id),
  mapped_fields JSONB
);

CREATE TABLE IF NOT EXISTS integration_lingxing_tokens (
  id BIGSERIAL PRIMARY KEY,
  app_id VARCHAR(128) NOT NULL,
  access_token VARCHAR(256) NOT NULL,
  refresh_token VARCHAR(256) NOT NULL,
  expires_at TIMESTAMP NOT NULL,
  created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS import_jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_type VARCHAR(64),
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status VARCHAR(32),
  success_count INT,
  failed_count INT,
  error_summary TEXT
);

CREATE TABLE IF NOT EXISTS import_job_progress (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id INTEGER REFERENCES import_jobs(id),
  total INT,
  processed INT,
  success INT,
  failed INT,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS import_job_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id INTEGER REFERENCES import_jobs(id),
  level VARCHAR(16),
  message TEXT,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS app_config (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  config_key VARCHAR(128) UNIQUE NOT NULL,
  config_value JSONB NOT NULL,
  updated_at TIMESTAMP DEFAULT NOW()
);
