---
description: Bucket API provides HTTP methods to create, modify or delete a bucket
---

# Bucket API

Before starting recording, a user has to create a bucket with the following settings:

* Maximal block content_length
* Quota type
* Quota content_length

For more information, you can read more about buckets in [How does it work?](../how-does-it-work.md)

{% swagger method="get" path=" " baseUrl="/b/:bucket_name " summary="Get information about a bucket" %}
{% swagger-description %}
The method returns the current settings, stats, and entry list of the bucket in JSON format.
{% endswagger-description %}

{% swagger-parameter in="path" name="bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="Information about the bucket in JSON format" %}
```javascript
{
    "settings": {
        "max_block_size": "integer",            // max block content_length in bytes
        "quota_type": Union["NONE", "FIFO"],    // quota type
        "quota_size": "integer"                 // quota content_length in bytes
    }
    "info": {
        "name": "string",         // name of the bucket
        "entry_count": "integer", // number of entries in the bucket
        "content_length": "integer",        // content_length of stored data in the bucket in bytes
        "oldest_record": "integer", // unix timestamp of oldest record in seconds
        "latest_record": "integer"  // unix timestamp of latest record in seconds
    },
    "entries": List[String]        // list of entry names
}
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="The bucket doesn't exist" %}
```javascript
{
    "detail": "string"
}
```
{% endswagger-response %}
{% endswagger %}

{% swagger method="head" path=" " baseUrl="/b/:bucket_name " summary="Check if a bucket exists" %}
{% swagger-description %}

{% endswagger-description %}

{% swagger-parameter in="path" name="bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The bucket exists" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="The bucket doesn't exist" %}
```javascript
{
    // Response
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

{% swagger-parameter in="body" name="max_block_size" type="String/Integer" required="false" %}
Maximal content_length of a data block in bytes (default: 64Mb)
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_type" type="String" required="false" %}
Type of quota. Can have values "NONE" or "FIFO" (default: "NONE")
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_size" type="String/Integer" required="false" %}
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
    "detail": "string"
}
```
{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="JSON request is invalid" %}
```javascript
{
    "detail": "string"
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

{% swagger-parameter in="body" name="max_block_size" type="String/Integer" required="false" %}
Maximal content_length of a data block in bytes
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_type" type="String" required="false" %}
Type of quota. Can have values "NONE" or "FIFO"
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_size" type="String/Integer" required="false" %}
Size of quota in bytes
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The settings are updated" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Bucket doesn" %}
```javascript
{
    "detail": "string"
}
```
{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="JSON request is invalid" %}
```javascript
{
    "detail": "string"
}
```
{% endswagger-response %}
{% endswagger %}

{% swagger method="delete" path=" " baseUrl="/b/:bucket_name " summary="Remove a bucket" %}
{% swagger-description %}
Remove a bucket with

**all its entries and stored data**
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

{% swagger-response status="404: Not Found" description="Bucket doesn" %}
```javascript
{
    "detail": "string"
}
```
{% endswagger-response %}
{% endswagger %}
