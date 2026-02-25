# 领星 API 接入指南 - Document

# [领星 API 接入指南](#/docs/Guidance/newInstructions?id=%e9%a2%86%e6%98%9f-api-%e6%8e%a5%e5%85%a5%e6%8c%87%e5%8d%97)

## [1\. 准备工作](#/docs/Guidance/newInstructions?id=_1-%e5%87%86%e5%a4%87%e5%b7%a5%e4%bd%9c)

#### [1.1. 申请AppId和AppSecret](#/docs/Guidance/newInstructions?id=_11-%e7%94%b3%e8%af%b7appid%e5%92%8cappsecret)

接入领星 API 公共服务前须先申请AppId和AppSecret，AppId和AppSecret是生成调用领星功能接口凭证token的必须参数，客户需要妥善保管，操作流程见 [如何申请企业的appid和appsecret？](#/docs/Guidance/AppID?id=_1-%e5%a6%82%e4%bd%95%e7%94%b3%e8%af%b7%e4%bc%81%e4%b8%9a%e7%9a%84appid%e5%92%8cappsecret%ef%bc%9f) ；  

#### [1.2. 配置IP白名单](#/docs/Guidance/newInstructions?id=_12-%e9%85%8d%e7%bd%aeip%e7%99%bd%e5%90%8d%e5%8d%95)

除申请AppId和AppSecret外，还需添加允许访问的外网IP白名单，二者缺一不可，操作流程见 [如何配置ip白名单？](#/docs/Guidance/AppID?id=_3-%e5%a6%82%e4%bd%95%e9%85%8d%e7%bd%aeip%e7%99%bd%e5%90%8d%e5%8d%95%ef%bc%9f) ；  

## [2\. 业务接口的调用](#/docs/Guidance/newInstructions?id=_2-%e4%b8%9a%e5%8a%a1%e6%8e%a5%e5%8f%a3%e7%9a%84%e8%b0%83%e7%94%a8)

#### [**2.1 API请求域名**](#/docs/Guidance/newInstructions?id=_21-api%e8%af%b7%e6%b1%82%e5%9f%9f%e5%90%8d)

`https://openapi.lingxing.com`

#### [**2.2 公共请求参数**](#/docs/Guidance/newInstructions?id=_22-%e5%85%ac%e5%85%b1%e8%af%b7%e6%b1%82%e5%8f%82%e6%95%b0)

参数名

类型

描述

数据来源

access\_token

string

通过接口获取的token信息

[access\_token获取](#/docs/Guidance/newInstructions?id=_31-access_token%e8%8e%b7%e5%8f%96)

app\_key

string

APP ID

[API信息的查询与配置](#/docs/Guidance/AppID?id=_2-%e5%a6%82%e4%bd%95%e6%9f%a5%e7%9c%8b%e4%bc%81%e4%b8%9a%e7%9a%84appid%e5%92%8cappsecret%ef%bc%9f)

timestamp

string

时间戳

1720408272

sign

string

接口签名

[签名sign的生成](#/docs/Guidance/newInstructions?id=_4-%e7%ad%be%e5%90%8dsign%e7%9a%84%e7%94%9f%e6%88%90)

举例说明公共请求参数在接口请求中的使用(Query Params传参)： ![token](/../../images/OpenApiImage/PublicParam.png) 如果没有传递完整的公共请求参数，则会出现[公共请求参数不完整](#/docs/Guidance/QA?id=_8-%e5%85%ac%e5%85%b1%e8%af%b7%e6%b1%82%e5%8f%82%e6%95%b0%e4%b8%8d%e5%ae%8c%e6%95%b4)的问题

> **注意**：sign作为参数，在传输时需要进行URL编码【url encode】以确保能够正常传递并被正确处理

#### [**2.3 接口请求**  
](#/docs/Guidance/newInstructions?id=_23-%e6%8e%a5%e5%8f%a3%e8%af%b7%e6%b1%82)

#### [2.3.1 Get类型请求](#/docs/Guidance/newInstructions?id=_231-get%e7%b1%bb%e5%9e%8b%e8%af%b7%e6%b1%82)

> 业务请求参数+公共请求参数，都拼接在url上面

比如发送业务参数为`offset=0,length=100`的GET请求，则url参数拼装为： `access_token=44fa2eed-xxxx-xxxx-xxxx-8c6abe5ea6a4&app_key=ak_xxxxxxxxxS&timestamp=1720429074&sign=NaUK5YE68tgyVz%2FheN8WqPvL%2F4zTp1zK7%2BX0H8iogzRNvt1hHQkYGvkzsZ%2B0e8VP&offset=0&length=100`  

举例说明Get请求如下：

```
curl --location --max-time 30000 'https://openapi.lingxing.com/xxx/xxx/xxx?access_token=44fa2eed-xxxx-xxxx-xxxx-8c6abe5ea6a4&app_key=ak_xxxxxxxxxS&timestamp=1720429074&sign=NaUK5YE68tgyVz%2FheN8WqPvL%2F4zTp1zK7%2BX0H8iogzRNvt1hHQkYGvkzsZ%2B0e8VP&offset=0&length=100'
```

#### [**2.3.2 POST类型请求**](#/docs/Guidance/newInstructions?id=_232-post%e7%b1%bb%e5%9e%8b%e8%af%b7%e6%b1%82)

> 业务请求参数（放入body，json形式传参）+ 公共请求参数
> 
> **注意**：  
> 
> 1.  body参数中若嵌套有集合需要转成string参与签名,否则会出现[生成签名不正确](#/docs/Guidance/QA?id=_2-%e7%94%9f%e6%88%90%e7%ad%be%e5%90%8d%e4%b8%8d%e6%ad%a3%e7%a1%ae)的问题  
>     
> 2.  接口文档无特殊说明的，body内参数均以json格式传输，header中需要设置 Content-type:application/json

比如发送业务参数为`{"name": "kobe", "content": { "city": "lake", "age": "133" }}`请求，  
举例说明使用 postman 发送POST请求，示例如下：  
![](/..\..\images\OpenApiImage\PostReq.png)

## [3\. access\_token获取及续约](#/docs/Guidance/newInstructions?id=_3-access_token%e8%8e%b7%e5%8f%96%e5%8f%8a%e7%bb%ad%e7%ba%a6)

#### [3.1 access\_token获取](#/docs/Guidance/newInstructions?id=_31-access_token%e8%8e%b7%e5%8f%96)

access\_token是领星API调用的凭证，调用各业务接口时都需使用access\_token，开发者需要进行妥善保存；详见接口文档 [获取接口令牌-access\_token](#/docs/Authorization/GetToken)。

![](/..\..\images\OpenApiImage\query_access_token.png)

#### [3.2 access\_token续约](#/docs/Guidance/newInstructions?id=_32-access_token%e7%bb%ad%e7%ba%a6)

如果不想生成新的token，可以根据appId和access\_token续约access\_token；在access\_token到期前调用续约接口，每次调用都会生成新的refresh\_token，详见接口文档 [续约接口令牌](#/docs/Authorization/RefreshToken)

> **注意**：refresh\_token的有效期为 2个小时，一个refresh\_token只能被使用一次给access\_token续约，如果需要再次续约，需要使用上次续约返回的refresh\_token

使用 postman 续约access\_token示例如下：

![](/..\..\images\OpenApiImage\refresh_access_token.png)

## [4\. 签名sign的生成](#/docs/Guidance/newInstructions?id=_4-%e7%ad%be%e5%90%8dsign%e7%9a%84%e7%94%9f%e6%88%90)

#### [4.1 sign的生成规则：](#/docs/Guidance/newInstructions?id=_41-sign%e7%9a%84%e7%94%9f%e6%88%90%e8%a7%84%e5%88%99%ef%bc%9a)

> a）请求包含的所有参数使用 ASII 排序【所有的业务请求入参+3个固定参数（access\_token、app\_key、timestamp）】；
> 
> b）以 key1=value1&key2=value2&...&keyn=valuen 的格式拼接起来，其中key为参数键，value为参数值【value为空不参与生成签名！（value为null会参与生成签名）】；
> 
> c）拼接之后的字符串用MD5(32位)加密后转大写；
> 
> d）用AES/ECB/PKCS5PADDING对生成的MD5值加密，其中AES加密的密钥为appId；  
> AES加密采用ECB模式，填充方式为PKCS5PADDING
> 
> **注意**：
> 
> 1.sign作为参数，在传输时需要进行URL编码【url encode】，以确保能够正常接收处理。
> 
> 2.当timestamp以固定值参与签名生成sign时，sign的有效期为 2 分钟，2分钟后签名过期，因此建议调用业务接口时使用实时的时间戳生成签名sign，不要缓存sign  
> 
> 3.当有文件上传需要生成签名时，需要文件原始名作为key，文件md5加密的值为value，key不要使用汉字，key=originFileName，value=DigestUtils.md5Hex(InputStream()))

#### [4.2 最佳实践：](#/docs/Guidance/newInstructions?id=_42-%e6%9c%80%e4%bd%b3%e5%ae%9e%e8%b7%b5%ef%bc%9a)

1.  下载相应语言的[SDK](#/docs/Guidance/newInstructions?id=_5-sdk%e4%b8%8b%e8%bd%bd)文件，以Java语言为例  
    
2.  根据自身业务进行修改，如下所示：
    
    ```
     public static void main(String[] args) throws Exception {
         String appId = "xxx";
    
         Map<String, Object> queryParam = new HashMap<>();
         queryParam.put("timestamp", 1639734344);
         queryParam.put("access_token", "59cf5437-669b-49f5-83c4-3cc1d1404680");
         queryParam.put("app_key", appId);
    
         String sign = ApiSign.sign(queryParam, appId);
         queryParam.put("sign", sign);
         log.info("sign:{}", sign);
    
         HttpRequest<Object> build = HttpRequest.builder(Object.class)
                 .method(HttpMethod.GET)
                 .endpoint("xxxx")
                 .path("erp/sc/data/local_inventory/brand")
                 .queryParams(queryParam)
                 .build();
         HttpResponse execute = HttpExecutor.create().execute(build);
         log.info("execute:{}", execute.readEntity(Object.class));
     }
    ```
    

## [5\. 限流算法说明](#/docs/Guidance/newInstructions?id=_5-%e9%99%90%e6%b5%81%e7%ae%97%e6%b3%95%e8%af%b4%e6%98%8e)

> 改进的令牌桶算法：为每一个请求提供一个令牌；当请求到达时，如果桶中有足够的令牌，则会消耗一个令牌并允许请求通过； 如果没有令牌，则请求被限流（错误码：3001008）；  
> 令牌回收是基于请求完成、异常、超时（2min）；  
> 令牌桶的维度：appId + 接口url。

限流算法示意图：

![](..\..\images\OpenApiImage\flowLimit.png)

## [6\. SDK下载](#/docs/Guidance/newInstructions?id=_6-sdk%e4%b8%8b%e8%bd%bd)

[Go](https://apidoc.lingxing.com/file/openapi-go-sdk.zip)  
[PHP](https://apidoc.lingxing.com/file/openapi-php-sdk-master-20230817.zip)  
[Java](https://apidoc.lingxing.com/file/openapi-sdk-java-20240730.zip)  
[NODE](https://apidoc.lingxing.com/file/openapi-node-sdk-master-20230515.zip)  
[Python](https://apidoc.lingxing.com/file/openapi-python3-sdk-20230419.zip)

\[领星API接入指南（旧版文档入口，新客户无需关注） 排版更新，接入方式无变化；[领星API接入指南（旧版文档入口）](#/docs/Guidance/Instructions)

[

上一章节

API信息的查询与配置

](#/docs/Guidance/AppID)

[

下一章节

常见问题案例

](#/docs/Guidance/QA)

## Embedded Content