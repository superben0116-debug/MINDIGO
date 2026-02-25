# 本地运行与页面测试

## 后端启动
- 进入目录: app/backend
- 安装依赖: pip install -r requirements.txt
- 启动服务: uvicorn app.main:app --reload --port 8000

## 访问页面
- 入口: http://localhost:8000/ui/
- 测试页: http://localhost:8000/ui/test_page.html

## 配置
- 后台配置页: http://localhost:8000/ui/admin_config.html
- 保存后会写入数据库的 app_config 表

## 领星自动 Token 与全店铺
- 只需设置 App ID 与 App Secret
- SID 列表留空或填 ALL 即抓取全部店铺
## 时间范围抓取
- 在配置页填写开始/结束日期（YYYY-MM-DD）
- 系统会按“分段天数”分片抓取，避免一次性全量超时
