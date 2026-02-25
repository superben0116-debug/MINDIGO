# API 设计草案（第一版）

## 领星集成
- POST /integrations/lingxing/sync-fbm-orders
  - 触发拉取 FBM 订单

## 内部订单
- GET /internal-orders
  - 查询内部订单列表
- GET /internal-orders/{id}
  - 查询内部订单详情
- POST /internal-orders/{id}/quote-request
  - 发起供应商报价

## 供应商报价
- GET /supplier-quotes/{quote_no}
  - 获取报价单（仅可见字段）
- POST /supplier-quotes/{quote_no}/submit
  - 提交报价并回传价格

## 卡派导出
- GET /kapi-exports/template
  - 获取导出模板
- POST /kapi-exports
  - 生成导出批次
