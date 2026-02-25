# 自动继续生成说明

## 已补齐
- 配置保存与回填
- 地址字段检测与映射保存
- 测试页增加配置自检

## 建议测试顺序
1. 打开 /ui/admin_config.html 填写并保存配置
2. 在 /ui/test_page.html 点击“检查配置”确认 ok
3. 如需地址字段检测，粘贴订单详情 data 并保存映射
4. 触发 /integrations/lingxing/sync-fbm-orders 拉单
5. 打开 /ui/internal_orders.html 验证列表数据
