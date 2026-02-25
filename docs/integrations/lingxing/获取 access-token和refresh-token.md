# 获取接口令牌-access\_token - Document

# [获取 access-token和refresh-token](#/docs/Authorization/GetToken?id=%e8%8e%b7%e5%8f%96-access-token%e5%92%8crefresh-token)

## [接口信息](#/docs/Authorization/GetToken?id=%e6%8e%a5%e5%8f%a3%e4%bf%a1%e6%81%af)

API Path

请求协议

请求方式

[令牌桶容量](#/docs/Guidance/newInstructions?id=_5-%e9%99%90%e6%b5%81%e7%ae%97%e6%b3%95%e8%af%b4%e6%98%8e)

`/api/auth-server/oauth/access-token`

HTTPS

POST

100

## [请求头](#/docs/Authorization/GetToken?id=%e8%af%b7%e6%b1%82%e5%a4%b4)

标签

必填

说明

类型

示例

Content-Type

是

请求内容类型

\[string\]

multipart/form-data

## [请求参数](#/docs/Authorization/GetToken?id=%e8%af%b7%e6%b1%82%e5%8f%82%e6%95%b0)

参数名

说明

必填

类型

示例

appId

AppID，在ERP开放接口菜单中获取

是

\[string\]

appSecret

AppSecret，在ERP开放接口菜单中获取

是

\[string\]

 

## [请求curl示例](#/docs/Authorization/GetToken?id=%e8%af%b7%e6%b1%82curl%e7%a4%ba%e4%be%8b)

```
curl --location 'https://openapi.lingxing.com/api/auth-server/oauth/access-token' \
--form 'appId="appId"' \
--form 'appSecret="appSecret"'
```

## [返回结果](#/docs/Authorization/GetToken?id=%e8%bf%94%e5%9b%9e%e7%bb%93%e6%9e%9c)

Json Object

参数名

说明

必填

类型

示例

code

状态码

是

\[int\]

200

msg

消息提示

是

\[string\]

data

响应数据

是

\[object\]

data>>access\_token

access\_token，请求令牌token

是

\[string\]

data>>refresh\_token

refresh\_token，可以使用它来给access\_token延续有效期

是

\[string\]

data>>expires\_in

access\_token过期时间

是

\[string\]

 

## [返回成功示例](#/docs/Authorization/GetToken?id=%e8%bf%94%e5%9b%9e%e6%88%90%e5%8a%9f%e7%a4%ba%e4%be%8b)

```
{
    "code": "200",
    "msg": "OK",
    "data": {
        "access_token": "4dcaa78e-b52d-4325-bc35-571021bb0787",
        "refresh_token": "da5b5047-e6d1-496c-ab4d-d5425a6a66e4",
        "expires_in": 7199
    }
}
```

## [返回失败示例](#/docs/Authorization/GetToken?id=%e8%bf%94%e5%9b%9e%e5%a4%b1%e8%b4%a5%e7%a4%ba%e4%be%8b)

```
{
    "code": "2001001",
    "msg": "app not exist",
    "data": null
}
```

[

上一章节

STA货件流程说明

](#/docs/Case/staProcessNew)

[

下一章节

续约接口令牌

](#/docs/Authorization/RefreshToken)

## Embedded Content