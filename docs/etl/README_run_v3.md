# etl_reference_v3.py 使用说明（锚点区间截取版）
## 安装依赖
pip install pandas openpyxl

## 运行
python etl_reference_v3.py --amazon amazon_order_text.txt --internal internal_orders.xlsx --supplier supplier_quote.xlsx --kapi kapi_template.xlsx

输出：
- internal_orders.updated.xlsx
- kapi_export.xlsx

## v3 解决“未对齐”的关键
1) 顶部字段只从 header_block 解析：
   header_block = between(order_block, "订单一览", "订单内容")
   - 发货日 = between(header_block, "发货日期:", "送达日期:")
   - 送达日 = between(header_block, "送达日期:", "购买日期:")
   - 出单日期 = between(购买日期:, 配送服务/配送/销售渠道)；找不到则取购买日期所在行到行尾

2) 地址从 “配送地址” 到 “美国/地址类型/联系买家” 截取，允许多行地址，不再固定取3行：
   - name = 第一行
   - city/state/zip = 最后一行
   - 中间行全部当作地址行1/2（导出卡派时取第一条作为 Line1）

3) 电话只从 “联系买家:” 到 “更多详情/收税模型:” 区间里提取。

4) 包裹信息仍可多包裹解析，但不会影响顶部发货日对齐，因为顶部字段不再全局搜索。

如遇到某些订单页面缺少“订单一览/订单内容”等锚点，可把订单块原文贴我，我会帮你加更强的 fallback 锚点。
