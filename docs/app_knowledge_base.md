# 应用资料库

## 1. 核心模块
- 内部订单：`/Users/baicai/Downloads/终极 ERP/app/frontend/internal_orders.html`
- 内部订单后端：`/Users/baicai/Downloads/终极 ERP/app/backend/app/routers/internal_orders.py`
- 同步服务：`/Users/baicai/Downloads/终极 ERP/app/backend/app/services.py`
- 认证：`/Users/baicai/Downloads/终极 ERP/app/backend/app/routers/auth.py`
- 客服：`/Users/baicai/Downloads/终极 ERP/app/backend/app/routers/customer_service.py`

## 2. 订单同步流程
1. `mws/orders` 拉订单主数据。
2. `mws/orderDetail` 回补：
   - ASIN
   - SKU
   - 产品图
   - 发货日
   - 送达日
   - 购买日期
3. `pb/mp/order/v2/list` 回补：
   - 收件人
   - 地址 1/2/3
   - 联系买家
   - 电话
   - 国家
   - 地址类型

## 3. 地址持久化原则
- 已抓到的地址长期保存。
- 超过 28 天抓不到新数据时，不覆盖旧地址。
- 地址块格式：
  - 收件人
  - 地址1
  - 地址2/地址3（若有）
  - 城市, 州 ZIP
  - 国家
  - 地址类型
  - 联系买家
  - 电话

## 4. 多 SKU 规则
- 同订单号 + 同 SKU：合并数量。
- 同订单号 + 不同 SKU：拆分多行。
- 拆分后共享买家信息，但售价、标题、SKU 各自独立。

## 5. 日期规则
- 发货日 / 送达日优先使用 `mws/orderDetail`。
- UTC 时间统一转换为 `America/Los_Angeles`。
- 页面展示按 `PDT/PST` 输出。
- 不使用 `pb/mp` 平台层 `delivery_time` 反推 Amazon 送达日。

## 6. 导出规则
- 模板来源：`/Users/baicai/Downloads/终极 ERP/docs/inputs/internal_orders.xlsx`
- `BU` 列以后不输出内容与边框。
- `BP/BQ/BR` 不保留图片公式，导出留空。
- 4 行订单 `AL` 规则：
  - `AL(r)=AG(r)+AI(r)+AK(r)+AG(r+1)+AG(r+2)`
  - `AL(r+3)=AG(r+3)`
- `BD`：
  - `=Q首行+AL首行+BC首行+AL尾行`
- `BF`：
  - `=BE*汇率-BD`

## 7. 定时任务
- 每 30 分钟自动增量同步。
- 每天中国时间 09:00 全量同步。
- 新增订单后自动补全买家信息。

## 8. 设置持久化
- 配置项：`internal_orders_settings`
- 当前保存内容：
  - 汇率
  - 公式规则
  - 应用资料库
  - 代码流程快照

## 9. 恢复建议
- 若会话归档或模型崩溃，优先读取：
  - 本文件
  - `internal_orders_settings`
