# 项目进度备份

日期：2026-03-28

## 当前项目状态
- 核心模块已完成：内部订单、供应商报价、卡派导出、客服、认证、领星同步。
- 内部订单页已支持：
  - 列筛选
  - 快捷视图
  - 设置面板
  - 公式规则维护
  - 汇率维护
  - 资料库维护
- 同步链路已支持：
  - 领星 `mws/orders`
  - 领星 `mws/orderDetail`
  - 领星 `pb/mp/order/v2/list`
  - 自动补全买家信息、地址、ASIN、SKU、图片、发货/送达日期

## 当前关键设置
- 设置存储位置：`internal_orders_settings`
- 当前保存内容：
  - `exchange_rate`
  - `formula_rules`
  - `knowledge_base`
- 领星配置存储位置：`lingxing`
  - `app_id`
  - `app_secret`
  - `access_token`
  - `sid_list`
  - `start_time`
  - `end_time`
  - `chunk_days`

## 当前公式规则
### AL
- `3` 行订单：
  - `AL(r)=AG(r)+AI(r)+AK(r)`
  - `AL(r+1)` 合并
  - `AL(r+2)=AG(r+2)`
- `4` 行订单：
  - `AL(r)=AG(r)+AI(r)+AK(r)+AG(r+1)+AG(r+2)`
  - `AL(r+3)=AG(r+3)`

### 其他关键列
- `AG`：头程运费总价
- `AQ`：长 in
- `AR`：宽 in
- `AS`：高 in
- `AT`：镑重量
- `AU`：自算计费重
- `BN`：oversize 130 及 165
- `BO`：周长 < 419
- `BD`：总成本
- `BF`：利润

### 总成本与利润
- `BD = Q首行 + AL首行 + BC首行 + AL尾行`
- `BF = BE * 汇率 - BD`

## 备份原则
- 已保存的信息优先来自数据库。
- 已抓到的地址和日期信息长期保存。
- 28 天外若领星不再返回新数据，不覆盖旧值。
- 手动补过的更完整地址优先保留。
- 同订单号 + 同 SKU 合并数量；同订单号 + 不同 SKU 拆多行。

## 同步与刷新
- 每 30 分钟自动增量同步。
- 每天中国时间 09:00 全量同步。
- “一键更新订单和状态”会：
  - 清理卡住的旧任务
  - 强制拉取最近窗口
  - 同步后补全买家信息与地址

## 恢复入口
若上下文归档或需要重新接手，优先读取：
- `/Users/baicai/Downloads/终极 ERP/docs/project_progress_2026-03-28.md`
- `/Users/baicai/Downloads/终极 ERP/docs/app_knowledge_base.md`
- `/Users/baicai/Downloads/终极 ERP/app/backend/app/routers/internal_orders.py`
- `/Users/baicai/Downloads/终极 ERP/app/backend/app/services.py`
- `/Users/baicai/Downloads/终极 ERP/app/backend/app/main.py`

