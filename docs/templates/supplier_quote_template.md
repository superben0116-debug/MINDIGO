# 供应商报价单模板（仅可见字段）

## 可见字段
- 产品图（image_url）
- 尺寸单位（dimension_unit）

## 供应商填写字段
- 报价单价（quoted_unit_price）
- 报价总价（quoted_total_price）
- 交期（lead_time_days）
- 备注（supplier_remark）

## 内部隐藏字段（不展示给供应商）
- internal_order_id
- internal_quote_id
- any customer or platform identifiers

## 模板数据结构示例
{
  "internal_quote_id": "Q202502040001",
  "items": [
    {
      "image_url": "https://...",
      "dimension_unit": "cm"
    }
  ],
  "supplier_input": {
    "quoted_unit_price": null,
    "quoted_total_price": null,
    "lead_time_days": null,
    "supplier_remark": ""
  }
}
