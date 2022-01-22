# Bucket API

Before starting recording, a user has to create a bucket with the following settings:

* Maximal block size
* Quota type
* Quota size

Read more about buckets in [How does it work?](../how-does-it-work.md)

{% swagger method="get" path=" " baseUrl="/b/:bucket_name " summary="Get information about bucket" %}
{% swagger-description %}

{% endswagger-description %}

{% swagger-parameter in="path" name="bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="Information in JSON format" %}
```javascript
{
    "max_block_size": "integer",
    "quota_type": ["NONE", "FIFO"],
    "quota_size": 0
}
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Bucket doesn't exists" %}
```javascript
{
    "detail": "string"
}
```
{% endswagger-response %}
{% endswagger %}

