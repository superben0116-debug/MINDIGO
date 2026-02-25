# Amazon订单 → 核心表(internal_orders) → 报价表(supplier_quote) → 卡派模板(kapi_template) 关联与导出规范（可直接给 Codex）

> 目标：实现一个可落地的 ETL/同步流程：  
> 1) 粘贴亚马逊订单详情文本（amazon_order_text）→ 自动解析并 **Upsert** 到 internal_orders  
> 2) 录入/导入 supplier_quote 报价 → 按 key 关联补全 internal_orders  
> 3) 选中 internal_orders 行 → 导出生成 kapi_template（卡派模板），并自动附带默认字段（模板中两行完全一致的列）

---

## 0. 你当前数据结构（来自你提供的文件）
- amazon_order_text.txt：包含 2 个订单块（以“订单编号：# <id>”开头）
- internal_orders.xlsx：核心业务表（含 订单编号、单号、产品名、客户地址、尺寸/重量、售价 等）
- supplier_quote.xlsx：报价表（含 箱唛、单价（元）、工厂内部单号、下单日期、发货日期 等）
- kapi_template.xlsx：卡派导出模板（2 行样例；大部分列为默认常量，少数列随订单变化）

---

## 1. 主键（Join Key）与实体关系

### 1.1 amazon_order_text → internal_orders
- **主键（Primary Key）**：`订单编号`（Amazon Order ID，例如 114-6194327-6726605）
- **动作**：解析 amazon 文本得到结构化字段，对 internal_orders 执行 **UPSERT**：
  - 若 internal_orders 中已存在该订单编号行：更新该行的字段
  - 若不存在：新增一行，并填充必要字段

### 1.2 supplier_quote → internal_orders
- **主键（Primary Key）**：`箱唛`（supplier_quote） ↔ `产品编码`（internal_orders）
- **产品编码提取规则**：internal_orders 的 `产品名`是“中文名称 + 换行 + 编码”，编码就是换行后那段，例如：
  - `双盆奶油色简约款吊柜浴室柜1.2米\nSPNYSSDFBV08-1.2m`
  - `木色双层简约款吊柜浴室柜1.37米\nMSSCJYKFBV28-1.37m`
- **动作**：当 supplier_quote.箱唛 命中 internal_orders 的产品编码时，把报价信息写回 internal_orders：
  - `下单日期`、`供应商出货日期`、`单价`（人民币）、`采购数量` 等

> 注意：supplier_quote 文件里有空行/备注行；仅处理 **箱唛非空且单价（元）有值** 的行。

### 1.3 internal_orders → kapi_template
- **导出粒度**：internal_orders **每条订单（每行）导出为 kapi_template 1 行**
- **默认字段**：kapi_template 样例中 **两行完全一致** 的列视为默认常量，导出时自动带出  
- **动态字段**：随订单变化的列从 internal_orders 映射/换算得到

---

## 2. 字段映射（Mapping）

## 2.1 amazon_order_text → internal_orders（UPSERT）
**Join Key：订单编号**

| amazon_order_text 解析字段 | internal_orders 目标列 | 规则 |
|---|---|---|
| 订单编号 | 订单编号 | 直接写入 |
| 发货日期 | 发货日 | 原样写入（例如 “2026年1月28日周三 PST”） |
| 送达日期区间 | 送达日 | 原样写入（例如 “2026年2月3日… 到 2026年2月11日…”） |
| 购买日期 | 出单日期 | 原样或只取日期部分（建议存原样文本） |
| 买家姓名（配送地址首行） | 客户地址（首行） | 用多行地址文本整体写入客户地址列（见解析规则） |
| 地址行1 | 客户地址（第2行） | 同上 |
| 城市州邮编行 | 客户地址（第3行） | 同上 |
| 电话 | （建议新增）电话 | 若不新增，也可在导出 kapi 时临时从 amazon 解析 |

> internal_orders 当前已包含：订单编号、客户地址、SKU、售价、发货日、送达日等列，可直接写入。

---

## 2.2 supplier_quote → internal_orders（补全报价）
**Join Key：supplier_quote.箱唛 = internal_orders 产品编码**

| supplier_quote 字段 | internal_orders 目标列 | 规则 |
|---|---|---|
| 工厂内部\n单号 | （建议新增）工厂内部单号 | internal_orders 现有“工厂内部型号(E4/E5)”与 C648/C666 不同，建议新增保存 |
| 下单日期 | 下单日期 | 直接写入 |
| 数量（套） | 采购数量 | 覆盖/比对差异 |
| 单价（元） | 单价 | 写入人民币单价 |
| 发货日期 | 供应商出货日期 | 写入 |
| （计算）数量*单价 | 总价 | 若 internal_orders 用公式则不写死；否则写入计算值 |

---

## 2.3 internal_orders → kapi_template（导出）
### 2.3.1 kapi_template 默认常量列（两行相同 → 固定带出）
导出时，从模板样例第一行读取这些列的值并写入每条导出行（默认常量）：

- Shipper Zip Code*
- Pickup Date*
- Shipper City*
- Shipper State*
- Shipper Country*
- Shipper Address Type*
- Shipper Service
- Shipper Contact Name
- Shipper Contact Phone
- Shipper Contact Email
- Shipper Address Name
- Shipper Address Line1
- Shipper Address Line2
- Pickup Time From
- Pickup Time To
- Shipper Remark
- Receiver Country*
- Receiver Address Type*
- Receiver Service
- Receiver Contact Email
- Receiver Address Line2
- Delivery Time From
- Delivery Time To
- Receiver Remark
- Size Unit*
- Name*
- Package Type*
- Package Qty*
- Pallet Type*
- Pallet Qty*
- Width*            （样例中两行一致，当前可默认；如未来变化请改为动态）
- Height*           （同上）
- NMFC
- Goods Describe
- Box Weight
- Box Length
-  Box Width
-  Box Height
-  Declared($)      （非星号列，样例默认）
- Remark

### 2.3.2 kapi_template 动态列（随订单变化）
| kapi_template 列 | 来源 internal_orders | 规则 |
|---|---|---|
| Customer orderNo | 产品编码 | 从 产品名 提取换行后编码（或独立列） |
| Ref | 单号 | 直接写入 |
| Receiver Contact Name | 客户地址首行 | 客户地址多行第 1 行 |
| Receiver Address Name | 客户地址首行 | 同上 |
| Receiver Address Line1 | 客户地址第 2 行 | 同上 |
| Receiver City* | 客户地址第 3 行解析 | 解析 CITY |
| Receiver State* | 客户地址第 3 行解析 | 解析 2 位州缩写 |
| Receiver Zip Code* | 客户地址第 3 行解析 | 解析 5 位 ZIP（可忽略 ZIP+4 后 4 位） |
| Receiver Contact Phone | 电话（amazon 解析或 internal_orders 新列） | 去掉 +1、空格、-，保留纯数字 |
| Declared($)* | 售价 | 直接写入美元售价 |
| Length* | 长cm | `长cm / 2.54`（输出英寸小数） |
| Weight* | 镑重量\n＜150lb | 直接写入 |

---

## 3. 解析规则（Regex/算法，直接可用）

### 3.1 订单块切分
- 在 amazon_order_text 里，用此正则定位每个订单开头：
  - `订单编号：#\s*(\d{3}-\d{7}-\d{7})`
- 从该位置切到下一个订单编号位置（或文本末尾）得到一个订单块。

### 3.2 提取字段（每个订单块）
**推荐使用“标签: 值”型解析：**
- 发货日期：`发货日期:\s*(.+)`
- 送达日期：`送达日期:\s*(.+)`
- 购买日期：`购买日期:\s*(.+)`
- 电话：`电话:\s*([+\d\-\s]+)`

### 3.3 提取配送地址（多行）
订单块中通常出现一段：
```
配送地址

<姓名>
<地址行1>
<CITY, ST ZIP(-ZIP4)>
美国
```
实现方式（稳妥做法）：
1) 找到“配送地址”后面的非空行
2) 读取接下来连续的 3 行（姓名、地址行1、城市州邮编行）
3) 拼成 internal_orders.客户地址 以换行保存

### 3.4 解析 CITY/STATE/ZIP
对城市州邮编行（如 `LORTON, VA 22079-1367`）使用：
- `^\s*(?P<city>[^,]+)\s*,\s*(?P<state>[A-Z]{2})\s*(?P<zip>\d{5})(?:-\d{4})?\s*$`
> 该类模式来自常见实现思路（注意真实地址格式多样，当前你的输入较规范）。  

### 3.5 从 产品名 提取产品编码（箱唛）
- 规则：取字符串最后一行（按 `\n` 分割），并 trim：
  - `code = 产品名.splitlines()[-1].strip()`

---

## 4. 导出算法（核心步骤）

1) 读取 kapi_template.xlsx 样例，识别默认列：
   - 若某列在样例 2 行中值完全一致 → 该列作为默认常量
2) 对每条要导出的 internal_orders 行：
   - 生成一行输出 dict
   - 先填充默认常量列
   - 再填充动态列（地址拆分、尺寸换算等）
3) 输出为新的 Excel（列顺序与 kapi_template 一致）

---

## 5. 质量校验（建议实现）
- 缺关键字段不允许导出：订单编号、单号、客户地址、长cm、镑重量、售价、产品编码
- 地址解析失败（解析不到 city/state/zip）→ 阻止导出并提示人工修正
- supplier_quote 匹配不到 internal_orders → 列入“未匹配报价清单”

---

## 6. 参考实现（见同目录 etl_reference.py）
该脚本包含：
- parse_amazon_orders(text) → list[dict]
- upsert_internal_orders(internal_df, parsed_orders)
- apply_supplier_quotes(internal_df, supplier_df)
- export_kapi(internal_df, kapi_template_df, selected_keys) → DataFrame

