# 领星集成方案（第一版）

## 接口与鉴权
- API 域名: https://openapi.lingxing.com
- 获取 token: /api/auth-server/oauth/access-token
- 续约 token: /api/auth-server/oauth/refresh
- 订单列表: /erp/sc/routing/order/Order/getOrderList
- 订单详情: /erp/sc/routing/order/Order/getOrderDetail

## 签名规则
- 参数 ASCII 排序
- 拼接 key=value 并 MD5 大写
- AES/ECB/PKCS5PADDING 加密，key 为 appId
- sign 需要 URL encode

## 拉单流程
- 拉取店铺列表 -> 获取 sid 列表
- getOrderList 分页拉取 order_number
- getOrderDetail 拉取详情并写入
- 以 order_number 去重

## 增量策略
- 每 10 分钟拉取最近 24 小时
- 对历史订单每日补拉一次

## 限流与容错
- 订单接口桶容量为 1
- 串行调用或队列排队
- 失败重试 3 次
- 错误码记录并报警

## 字段映射概览
- 订单主数据 -> internal_orders
- 订单 items -> internal_order_items
- 包裹尺寸 -> internal_order_packages

## 待确认
- 店铺列表接口是否需要对接
- 是否需要同步物流费用和利润字段
