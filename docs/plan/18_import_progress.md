# 导入进度检测

## 新增接口
- POST /integrations/lingxing/sync-fbm-orders
  - 返回 job_id
- GET /import-jobs/{job_id}
  - 返回导入进度

## 前端测试
- 打开 /ui/test_page.html
- 点击“拉取领星订单”，获取 job_id
- 点击“查看导入进度”轮询查看
