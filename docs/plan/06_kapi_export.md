# 卡派导出方案（第一版）

## 导出逻辑
- 内部订单勾选
- 每个订单可展开为多个包裹行
- 导出字段按 kapi_template.csv

## 字段来源原则
- Shipper 与 Receiver 使用内部配置或订单地址
- Size Unit 默认 IN 或 CM
- Package Qty 默认包裹数量
- 尺寸与重量来自包裹明细

## 待确认
- 是否按每包裹一行
- 是否需要 Pallet 维度
- 是否允许导出时编辑字段
