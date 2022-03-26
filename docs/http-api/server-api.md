---
description: Server API provides information about the storage and its state
---

# Server API

{% swagger method="get" path=" " baseUrl="/info" summary="Get statistical information about the storage" %}
{% swagger-description %}
You can use this method to get stats of the storage and check its version.
{% endswagger-description %}

{% swagger-response status="200: OK" description="Returns inforamtion in JSON format" %}
```javascript
{
    "version": "string",
    "bucket_count": "integer", // number of buckets in storage
    "usage": "integer",  // disk usage in bytes
    "uptime": "integer", // server uptime in seconds
    "oldest_record": "integer", // unix timestamp of oldest record in seconds
    "latest_record": "integer"  // unix timestamp of latest record in seconds
}
```
{% endswagger-response %}
{% endswagger %}

{% swagger method="get" path=" " baseUrl="/list" summary="Get a list of the buckets with their stats" %}
{% swagger-description %}
You can use this method to browse the buckets of the storage.
{% endswagger-description %}

{% swagger-response status="200: OK" description="" %}
```javascript
{
    "buckets": [
      {
        "name": "string",         // name of the bucket
        "entry_count": "integer", // number of entries in the bucket
        "content_length": "integer",        // content_length of stored data in the bucket in bytes
        "oldest_record": "integer", // unix timestamp of oldest record in seconds
        "latest_record": "integer"  // unix timestamp of latest record in seconds

      }
    ]
}
```
{% endswagger-response %}
{% endswagger %}

