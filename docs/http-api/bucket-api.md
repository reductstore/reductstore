---
description: Bucket API provides HTTP methods to create, modify or delete a bucket
---

# Bucket API

Before starting recording, a user has to create a bucket with the following settings:

* Maximal block size
* Quota type
* Quota size

Read more about buckets in [How does it work?](../how-does-it-work.md)

{% swagger method="get" path=" " baseUrl="/b/:bucket_name " summary="Get information about a bucket" %}
{% swagger-description %}

{% endswagger-description %}

{% swagger-parameter in="path" name="bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="Information in JSON format" %}
```javascript
{
    "max_block_size": "integer",
    "quota_type": Union["NONE", "FIFO"],
    "quota_size": "integer"
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

{% swagger method="post" path=" " baseUrl="/b/:bucket_name  " summary="Create a new bucket" %}
{% swagger-description %}
To create a bucket, the request should contain a JSON document with some parameters or empty body. The new bucket uses default values if some parameters are empty:
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of new bucket
{% endswagger-parameter %}

{% swagger-parameter in="body" name="max_block_size" type="Integer" %}
Maximal size of a data block in bytes (default: 1Mb)
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_type" type="String" %}
Type of quota. Can have values "NONE" or "FIFO"  (default: "NONE")
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_size" type="Integer" %}
Size of quota in bytes (default: 0)
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The new bucket is created" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="409: Conflict" description="A bucket with the same name already exists" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="JSON request is invalid" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}
{% endswagger %}

{% swagger method="put" path=" " baseUrl="/b/:bucket_name " summary="Change settings of a bucket" %}
{% swagger-description %}
To update settings of a bucket, the request should have a JSON document with all the settings
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-parameter in="body" name="max_block_size" type="Integer" required="true" %}
Maximal size of a data block in bytes 
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_type" type="String" %}
Type of quota. Can have values "NONE" or "FIFO"
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_size" type="Integer" %}
Size of quota in bytes
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The settings are updated" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Bucket doesn't exists" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="JSON request is invalid" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}
{% endswagger %}

{% swagger method="delete" path=" " baseUrl="/b/:bucket_name " summary="Remove a bucket" %}
{% swagger-description %}
Remove a bucket with all 

**the stored data**
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket to remove
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The bucket is removed" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Bucket doesn't exists" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}
{% endswagger %}

