# 实施细节（第一版）

## 供应商报价单模板
- 内部订单发起报价时，生成 supplier_quote_requests
- visible_payload 仅包含 product_image + dimension_unit + package sizes
- 供应商页面只读取 visible_payload
- 供应商提交后写入 supplier_quote_responses
- 回传价格自动更新内部报价单

## 领星同步
- 后台定时任务触发 sync_fbm_orders
- 列表分页拉取 + 详情补拉
- order_number 作为去重键
- 错误码记录 import_jobs

## 卡派导出
- 勾选内部订单
- 依据 package 展开生成导出行
- 导出记录写入 kapi_exports
