# 配置保存 API 与字段检测

## 配置保存
- PUT /config/lingxing
  - 保存领星配置（app_id, access_token, sid_list）
- PUT /config/shipper
  - 保存卡派发货方配置

## 地址字段检测
- POST /config/address-detect
  - body: {"data": { ... 领星订单详情 data ... }}
  - response: {"keys": ["field1", "field2", ...]}

## 使用建议
- 先调用 /config/address-detect 解析字段
- 再用 /config/address-mapping 调整映射
