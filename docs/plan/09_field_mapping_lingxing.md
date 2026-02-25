# 字段映射（领星 -> 内部订单）第一版

## 订单主表 internal_orders
- internal_order_no: 系统生成（规则：IO + 日期 + 序号）
- platform_order_no: data.order_item[].platform_order_id 或 data.platform_list[0]
- shop_name: data.shop_name
- order_status: data.order_status
- purchase_time: data.purchase_time
- region: data.country_code
- customer_address_summary: data.customer_comment（暂用）
- logistics_provider: data.logistics_provider_name
- logistics_type: data.logistics_type_name
- tracking_no: data.tracking_number
- total_cost: data.logistics_freight（如有）
- total_profit: data.gross_profit_amount（如有）

## 订单明细 internal_order_items
- sku: data.order_item[].sku
- product_name: data.order_item[].product_name
- quantity: data.order_item[].quality
- unit_price: data.order_item[].item_unit_price
- currency: data.order_item[].currency_code
- product_image: data.order_item[].pic_url
- attachments: data.order_item[].attachments / newAttachments

## 包裹明细 internal_order_packages
- length_cm: data.pkg_length
- width_cm: data.pkg_width
- height_cm: data.pkg_height
- length_in: pkg_length / 2.54
- width_in: pkg_width / 2.54
- height_in: pkg_height / 2.54
- weight_kg: data.pkg_real_weight
- weight_lb: pkg_real_weight * 2.20462
- billed_weight: data.logistics_pre_weight
- oversize_flag: (length_in >= 80) or (weight_lb >= 150)

## 待确认
- 平台订单号优先级（platform_list vs order_item.platform_order_id）
- 客户地址字段来源（当前未提供）
- 物流费用字段来源
