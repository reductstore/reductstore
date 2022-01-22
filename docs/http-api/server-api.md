# Server API

{% swagger method="get" path=" " baseUrl="/info" summary="Get infromation about the storage" %}
{% swagger-description %}

{% endswagger-description %}

{% swagger-response status="200: OK" description="Returns inforamtion in JSON format" %}
```javascript
{
    "version": "string",
    "bucket_count": "integer"
}
```
{% endswagger-response %}
{% endswagger %}

****

