def default_interface_registry() -> dict:
    return {
        "lingxing": [
            {"name": "亚马逊FBM订单列表", "path": "/erp/sc/data/mws/orders", "method": "POST", "protocol": "HTTPS", "bucket": 1},
            {"name": "亚马逊订单详情", "path": "/erp/sc/data/mws/orderDetail", "method": "POST", "protocol": "HTTPS", "bucket": 1},
            {"name": "亚马逊Listing", "path": "/erp/sc/data/mws/listing", "method": "POST", "protocol": "HTTPS", "bucket": 1},
            {"name": "订单管理列表", "path": "/pb/mp/order/v2/list", "method": "POST", "protocol": "HTTPS", "bucket": 10},
            {"name": "创建手工订单", "path": "/pb/mp/order/v2/create", "method": "POST", "protocol": "HTTPS", "bucket": 10},
            {"name": "更新待审核订单", "path": "/pb/mp/order/v2/updateOrder", "method": "POST", "protocol": "HTTPS", "bucket": 10},
            {"name": "路由订单列表", "path": "/erp/sc/routing/order/Order/getOrderList", "method": "POST", "protocol": "HTTPS", "bucket": 1},
        ],
        "kapi": [
            {"name": "卡派查询费用", "path": "/openApi/truck/queryFreight", "method": "POST", "protocol": "HTTPS"},
            {"name": "卡派创建单据", "path": "/openApi/truck/createOrder", "method": "POST", "protocol": "HTTPS"},
            {"name": "卡派获取单据", "path": "/openApi/truck/getOrders", "method": "POST", "protocol": "HTTPS"},
            {"name": "快递查询费用", "path": "/openApi/order/queryFreight", "method": "POST", "protocol": "HTTPS"},
        ],
    }

