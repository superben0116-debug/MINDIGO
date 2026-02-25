# 卡派导出字段映射（第一版）

## 字段来源规则
- Shipper* 字段：使用系统配置（发货方固定地址）
- Receiver* 字段：使用内部订单收货地址
- Size Unit*: 默认 IN（可配置）
- 包裹尺寸/重量：来自 internal_order_packages
- Package Qty*: 默认包裹数量

## 关键字段映射
- Customer orderNo -> internal_order_no
- Receiver Zip Code* -> customer_zip
- Receiver City* -> customer_city
- Receiver State* -> customer_state
- Receiver Address Line1 -> customer_address_line1
- Receiver Address Line2 -> customer_address_line2
- Length* / Width* / Height* -> package length/width/height
- Weight* -> package weight

## 展开规则
- 每包裹一行
- 一个内部订单多个包裹会生成多行

## 待确认
- 是否允许导出时手动修改字段
- Pallet 相关字段是否固定
