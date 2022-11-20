---
description: Server API provides information about the storage and its state
---

# Server API

{% swagger method="get" path=" " baseUrl="/api/v1/info" summary="Get statistical information about the storage" %}
{% swagger-description %}
You can use this method to get stats of the storage and check its version. If authenticaion is enabled, the method needs a valid API token.

`curl --header "Authorization: Bearer ${API_TOKEN}" http://127.0.0.1:8383/api/v1/info`
{% endswagger-description %}

{% swagger-response status="200: OK" description="Returns information in JSON format" %}
```javascript
{
    "version": "string",
    "bucket_count": "integer",  // number of buckets in storage
    "usage": "integer",         // disk usage in bytes
    "uptime": "integer",        // server uptime in seconds
    "oldest_record": "integer", // unix timestamp of oldest record in microseconds
    "latest_record": "integer"  // unix timestamp of latest record in microseconds
    "defaults":{
    "bucket":{                  // default settings for a new bucket
        "max_block_size": "integer",            // max block content_length in bytes
        "max_block_records": "integer",         // max number of records in a block
        "quota_type": Union["NONE", "FIFO"],    // quota type
        "quota_size": "integer"                 // quota content_length in bytes
    }
}
```
{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="If authentication is enabled and the API token is invalid or empty" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}
{% endswagger %}



{% swagger method="get" path=" " baseUrl="/api/v1/list" summary="Get a list of the buckets with their stats" %}
{% swagger-description %}
You can use this method to browse the buckets of the storage. If authenticaion is enabled, the method needs a valid API token.

`curl --header "Authorization: Bearer ${API_TOKEN}" http://127.0.0.1:8383/api/v1/list`
{% endswagger-description %}

{% swagger-response status="200: OK" description="" %}
```javascript
{
    "buckets": [
      {
        "name": "string",         // name of the bucket
        "entry_count": "integer", // number of entries in the bucket
        "size": "integer",        // size of stored data in the bucket in bytes
        "oldest_record": "integer", // unix timestamp of oldest record in microseconds
        "latest_record": "integer"  // unix timestamp of latest record in microseconds

      }
    ]
}
```
{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="If authentication is enabled and the API token is invalid or empty" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}
{% endswagger %}



{% swagger method="head" path=" " baseUrl="/api/v1/alive " summary="Check if the storage engine is working" %}
{% swagger-description %}
You can use this method for health checks in Docker or Kubernetes environment. The method has anonymous access.

`curl --head http://127.0.0.1:8383/api/v1/alive`
{% endswagger-description %}

{% swagger-response status="200: OK" description="" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}
{% endswagger %}

