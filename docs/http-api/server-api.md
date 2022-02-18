---
description: Server API provides information about the storage and its state
---

# Server API

{% swagger method="get" path=" " baseUrl="/info" summary="Get infromation about the storage" %}
{% swagger-description %}

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

***
