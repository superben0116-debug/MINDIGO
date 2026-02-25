# 地址字段确认与映射（第一版）

## 目的
领星订单详情中可能存在不同的收货人字段命名，本系统支持可配置映射。

## 默认映射
- name: buyer_name, receiver_name
- phone: buyer_phone, receiver_phone
- zip: buyer_zip, receiver_zip
- city: buyer_city, receiver_city
- state: buyer_state, receiver_state
- country: buyer_country, receiver_country
- address_line1: buyer_address_line1, receiver_address_line1
- address_line2: buyer_address_line2, receiver_address_line2

## 配置 API
- GET /config
  - 返回当前映射
- PUT /config/address-mapping
  - 保存映射

## 使用建议
若领星返回字段不同，请提供字段名，我将更新映射即可。
