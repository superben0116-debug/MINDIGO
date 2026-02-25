# 转换流程梳理

## 订单号识别（2组）
- 114-6194327-6726605
- 113-9738171-5070643

## 供应商报价（合并多行）
- 序号 648 工厂单号 C648 产品名: 1.37米柜体
LED智能镜柜
岩板无缝盆
- 序号 666 工厂单号 C666 产品名: 1.2米柜体
LED智能镜柜
岩板陶瓷双盆

## 卡派模板默认值（上下两行一致）
- Shipper Zip Code*: 91733
- Pickup Date*: 46056
- Shipper City*: South El Monte
- Shipper State*: CA
- Shipper Country*: US
- Shipper Address Type*: Business with dock
- Shipper Contact Name: mike
- Shipper Contact Phone: 567-227-7777
- Shipper Contact Email: chenjinrong@wedoexpress.com
- Shipper Address Name: CHAINYO SUPPLYCHAIN MANAGEMENT INC
- Shipper Address Line1: 1230 Santa Anita Ave

- Shipper Address Line2: Unit H
- Pickup Time From: 09:30
- Pickup Time To: 17:30
- Receiver Country*: US
- Receiver Address Type*: Residential
- Receiver Service: Lift-Gate；APPT
- Receiver Contact Email: chenjinrong@wedoexpress.com
- Delivery Time From: 09:00
- Delivery Time To: 16:30
- Size Unit*: in/lb
- Name*: Bathroom Vanity
- Package Type*: CRATE
- Package Qty*: 1
- Pallet Type*: PALLETS
- Pallet Qty*: 1
- Width*: 32.2834645669291
- Height*: 35.0393700787402

## 卡派模板可变字段（按订单）
- Customer orderNo: SPNYSSDFBV08-1.2m | MSSCJYKFBV28-1.37m
- Ref: 261-13-BV08-1-2M | 261-6-BV28-1-37M
- Receiver Zip Code*: 22079 | 45241
- Receiver City*: LORTON | WEST CHESTER
- Receiver State*: VA | OH
- Receiver Contact Name: Benjamin Correa | Chris & Marcina Powell
- Receiver Contact Phone: 5717898880 | 3157516688
- Receiver Address Name: Benjamin Correa | Chris & Marcina Powell
- Receiver Address Line1: 6689 HANSON LN | 7563 ROKEBY CT
- Declared($)*: 1629 | 1755
- Length*: 52.3622047244094 | 59.0551181102362
- Weight*: 271.1685798 | 291.0101832

## 映射规则（核心）
- Customer orderNo = 产品名里末尾型号（内订表产品名最后一行）
