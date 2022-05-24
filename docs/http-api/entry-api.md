---
description: Entry API provides methods to write, read and browse the data
---

# Entry API

{% swagger method="post" path=" " baseUrl="/b/:bucket_name/:entry_name" summary="Write a record to an entry" %}
{% swagger-description %}
The storage creates an entry at the first write operation. The record should be placed to the body of the request. The body can be empty.
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-parameter in="path" name=":entry_name" required="true" %}
Name of entry
{% endswagger-parameter %}

{% swagger-parameter in="query" name="ts" type="Integer" required="true" %}
A UNIX timestamp in microseconds
{% endswagger-parameter %}

{% swagger-parameter in="header" required="true" name="Content-Length" %}
Content-length is required to start asynchronous write operation
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The record is written" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Bucket is not found" %}
```javascript
{
    "detail": "string"
}
```
{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="Bad timestamp" %}
```javascript
{
   "detail": "string"
}
```
{% endswagger-response %}
{% endswagger %}

{% swagger method="get" path=" " baseUrl="/b/:bucket_name/:entry_name " summary="Get a record from an entry by timestamp" %}
{% swagger-description %}

{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-parameter in="path" name=":entry_name" required="true" %}
Name of entry
{% endswagger-parameter %}

{% swagger-parameter in="query" name="ts" type="Integer" required="false" %}
A UNIX timestamp in microseconds. If it is empty, returns the latest record.
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The record is found and returned in body of the response" %}
```javascript
"string"
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="The bucket or record with the timestamp doesn" %}
```javascript
{
   "detail": "string"
}
```
{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="Bad timestamp" %}
```javascript
{
   "detail": "string"
}
```
{% endswagger-response %}
{% endswagger %}

{% swagger method="get" path=" " baseUrl="/b/:bucket_name/:entry_name/list " summary="Get list of  records for a time interval" %}
{% swagger-description %}
The method responses with a JSON array that contains timestamps and sizes of the records. The time interval is [start, stop).
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-parameter in="path" name=":entry_name" required="true" %}
Name of entry
{% endswagger-parameter %}

{% swagger-parameter in="query" name="start" type="Integer" required="true" %}
A UNIX timestamp in microseconds
{% endswagger-parameter %}

{% swagger-parameter in="query" name="stop" type="Integer" required="true" %}
A UNIX timestamp in microseconds
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="Returns an JSON array with timestamps and sizes in bytes" %}
```javascript
{
    "records": [
        {
            "ts": "integer",
            "size": "integer"
        }
    ]
}
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="The bucket doesn" %}
```javascript
{
   "detail": "string"
}
```
{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="One or both timestamps are bad " %}
```javascript
{
   "detail": "string"
}
```
{% endswagger-response %}
{% endswagger %}
