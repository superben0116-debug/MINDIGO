# 数据模型草案（第一版）

## 核心表
- internal_orders
- internal_order_items
- internal_order_packages
- supplier_quotes
- supplier_quote_items
- kapi_exports
- kapi_export_items
- integration_lingxing_tokens
- import_jobs

## internal_orders（关键字段）
- internal_order_id
- platform_order_no
- shop_name
- order_status
- purchase_time
- region
- customer_address_summary
- logistics_provider
- logistics_type
- tracking_no
- total_cost
- total_profit
- created_at
- updated_at

## internal_order_items（关键字段）
- internal_order_item_id
- internal_order_id
- sku
- product_name
- quantity
- unit_price
- currency
- product_image
- attachments

## internal_order_packages（关键字段）
- package_id
- internal_order_id
- length_cm
- width_cm
- height_cm
- length_in
- width_in
- height_in
- weight_kg
- weight_lb
- billed_weight
- oversize_flag

## supplier_quotes（关键字段）
- quote_id
- internal_order_id
- supplier_id
- quote_status
- quoted_total
- quoted_at

## supplier_quote_items（关键字段）
- quote_item_id
- quote_id
- sku
- quantity
- quoted_unit_price
- lead_time
- remark

## kapi_exports（关键字段）
- export_id
- export_batch_no
- created_by
- created_at

## kapi_export_items（关键字段）
- export_item_id
- export_id
- internal_order_id
- mapped_fields_json

## integration_lingxing_tokens（关键字段）
- app_id
- access_token
- refresh_token
- expires_at

## import_jobs（关键字段）
- job_id
- start_time
- end_time
- status
- success_count
- failed_count
- error_summary
