+---
description: Entry API provides methods to write, read and browse the data
---

# Entry API

{% swagger method="post" path=" " baseUrl="/api/v1/b/:bucket_name/:entry_name" summary="Write a record to an entry" %}
{% swagger-description %}
The storage creates an entry on the first write operation. The record should be placed in the body of the request. The body can also be empty.
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
Content-length is required to start an asynchronous write operation
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

{% swagger-response status="409: Conflict" description="A record with the same timestamp already exists" %}
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

{% swagger method="get" path=" " baseUrl="/api/v1/b/:bucket_name/:entry_name " summary="Get a record from an entry" %}
{% swagger-description %}
The method return a content of the requested record in the body of the HTTP response. It also sends additional information in headers:

**x-reduct-time** - UNIX timestamp of the record in microseconds

**x-reduct-last** - 1 - if a record is the last record in the query&#x20;
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-parameter in="query" name="q" type="Integer" %}
A query ID to read the next record in the query. If it is set, the parameter

`ts`

 is ignored.
{% endswagger-parameter %}

{% swagger-parameter in="path" name=":entry_name" required="true" %}
Name of entry
{% endswagger-parameter %}

{% swagger-parameter in="query" name="ts" type="Integer" required="false" %}
A UNIX timestamp in microseconds. If it is empty, the latest record is returned.
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

{% swagger method="get" path="" baseUrl="/api/v1/b/:bucket_name/:entry_name/q " summary="Query records for a time interval" %}
{% swagger-description %}
The method response with a JSON document with ID which can be used to integrate records with method

**GET /b/:bucket_name/:entry_name?q=ID.**

  The time interval is [start, stop).
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-parameter in="query" name="start" type="Integer" %}
A UNIX timestamp in microseconds. If not set, the query starts from the oldest record in the entry.
{% endswagger-parameter %}

{% swagger-parameter in="query" name="stop" type="Integer" %}
A UNIX timestamp in microseconds. If not set, the query starts from the latest record in the entry.
{% endswagger-parameter %}

{% swagger-parameter in="path" name=":entry_name" required="true" %}
Name of entry
{% endswagger-parameter %}

{% swagger-parameter in="query" name="ttl" type="Integer" %}
Time To Live of the query in seconds. If a client haven't read any record for this time interval, the server removes the query and the query ID becomes invalid. Default value 5 seconds.
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="" %}
```javascript
{
   "id": "integer" // ID of query wich can be used in GET /b/:bucket/:entry request
}
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="The bucket doesn't exist or no records for the time interval" %}
```javascript
{
   "detail": "string"
}
```
{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="One or both timestamps are bad , or TTL is not a number" %}
```javascript
{
   "detail": "string"
}
```
{% endswagger-response %}
{% endswagger %}
