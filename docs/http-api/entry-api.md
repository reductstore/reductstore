---
description: HTTP methods to read, write and query entry records
---

# Entry API

The Entry API allows users to write and read data from their buckets, as well as search for specific entries using query operations.

{% swagger method="post" path=" " baseUrl="/api/v1/b/:bucket_name/:entry_name" summary="Write a record to an entry" %}
{% swagger-description %}
ReductStore creates an entry on the first write operation. The record should be placed in the body of the HTTP request. The body can also be empty.

The method needs a valid API token with write access to the entry's bucket if authentication is enabled.

The endpoint can be used with the "Expect: 100-continue" header. If the header is not set and an error occurs on the database side, the database drains the sent body and returns an HTTP status.

Since version 1.3, the database supports labels. You can assign any number of labels to a record by using headers that start with `x-reduct-label-.`
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

{% swagger-parameter in="header" name="x-reduct-label-<name>" required="false" %}
A value of a label assigned to the record
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The record is written" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="400: Bad Request" description="Posted content bigger or smaller than content-length" %}

{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is invalid or empty" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token does not have write permissions" %}

{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Bucket is not found" %}

{% endswagger-response %}

{% swagger-response status="409: Conflict" description="A record with the same timestamp already exists" %}

{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="Bad timestamp" %}

{% endswagger-response %}
{% endswagger %}

{% swagger method="get" path=" " baseUrl="/api/v1/b/:bucket_name/:entry_name " summary="Get a record from an entry" fullWidth="false" %}
{% swagger-description %}
The method finds a record for the given timestamp and sends its content in the HTTP response body. It also sends additional information in headers:

**x-reduct-time** - UNIX timestamp of the record in microseconds

**x-reduct-last** - 1 - if a record is the last record in the query (deprecated since version 1.4, use NoContent response)

**x-reduct-label-\<name>** - a value of the \<name> label

If authentication is enabled, the method needs a valid API token with read access to the entry's bucket.

Since version 1.3, the database supports labels. If a record has some labels, the method sends them as headers that start with `x-reduct-label`.
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-parameter in="query" name="q" type="Integer" required="false" %}
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

{% swagger-response status="204: No Content" description="If there is no record available for the given query" %}

{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is invalid or empty" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn't have enough permissions" %}

{% endswagger-response %}

{% swagger-response status="404: Not Found" description="The bucket or record with the timestamp doesn" %}

{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="Bad timestamp" %}

{% endswagger-response %}
{% endswagger %}

{% swagger method="head" path="" baseUrl="/api/v1/b/:bucket_name/:entry_name  " summary="Get only meta information about record" %}
{% swagger-description %}
The endpoint works exactly as

`GET /api/v1/b/:bucket_name/:entry_name`

but returns only headers with meta information with a body.
{% endswagger-description %}
{% endswagger %}

{% swagger method="get" path=" " baseUrl="/api/v1/b/:bucket_name/:entry_name/batch " summary="Get a bulk of records from an entry" %}
{% swagger-description %}
Since version 1.5, ReductStore provides a way to read a multiple records in a request. This can improve latency when you have many small records to read. The endpoint sorts all the records by time and concatenates them into a blob and sends it in the body. The meta information is sent for each record as a separate header `x-reduct-time-<timestamp>` which has a value as a CSV row. An example:

`x-reduct-time-192312381273: 100,text/plain,x=y,a="[a,b]"`

The first value is content-length, the second one is content-type, then labels as key=value pairs. If there is a comma in the value, it is escaped with double quotes.
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-parameter in="query" name="q" type="Integer" required="true" %}
A query ID to read the next record in the query
{% endswagger-parameter %}

{% swagger-parameter in="path" name=":entry_name" required="true" %}
Name of entry
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The record is found and returned in body of the response" %}
```javascript
"string"
```
{% endswagger-response %}

{% swagger-response status="204: No Content" description="If there is no record available for the given query" %}

{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is invalid or empty" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn't have enough permissions" %}

{% endswagger-response %}

{% swagger-response status="404: Not Found" description="The bucket or record with the timestamp doesn" %}

{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="Bad timestamp" %}

{% endswagger-response %}
{% endswagger %}

{% swagger method="head" path="" baseUrl="/api/v1/b/:bucket_name/:entry_name/batch  " summary="Get only meta information  in bulk" %}
{% swagger-description %}
The endpoint works exactly as

`GET /api/v1/b/:bucket_name/:entry_name/batch`

but returns only headers with meta information with a body.
{% endswagger-description %}
{% endswagger %}

{% swagger method="get" path="" baseUrl="/api/v1/b/:bucket_name/:entry_name/q " summary="Query records for a time interval" %}
{% swagger-description %}
The method responds with a JSON document containing an ID which should be used to read records with the following endpoint:

**GET /b/:bucket\_name/:entry\_name?q=ID.**

The time interval is \[start, stop).

If authentication is enabled, the method needs a valid API token with read access to the bucket of the entry.

Since version 1.3, the method also provides the `include-<label>` and `exclude-<label>` query parameters to filter records based on the values of certain labels. For example:

**GET /api/v1/:bucket/:entry/q?include-\<label1>=foo\&exclude-\<label2>=bar**

This would find all records that have `label1` equal to "foo" and excludes those that have `label2` equal to "bar".

A user can specify multiple `include` and `exclude` labels, which will be connected with an AND operator. For example:

GET /api/v1/:bucket/:entry/q?include-\<label1>=foo\&include-\<label2>=bar

This would query records that have both `label1` equal to "foo" and `label2` equal to "bar".

Since version 1.4, the method has the `continuous query` flag. If it is true, the current query will not be discarded if there are no records. A client can ask them later. The query will not be removed until its TTL has expired. The `stop` parameter is ignored for continuous queries.

Since version 1.6, the method provides the `limit` query parameter. It limits the number of record in the query. If it is not set, the query is unlimited by default.
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-parameter in="query" name="start" type="Integer" required="false" %}
A UNIX timestamp in microseconds. If not set, the query starts from the oldest record in the entry.
{% endswagger-parameter %}

{% swagger-parameter in="query" name="stop" type="Integer" required="false" %}
A UNIX timestamp in microseconds. If not set, the query starts from the latest record in the entry.
{% endswagger-parameter %}

{% swagger-parameter in="path" name=":entry_name" required="true" %}
Name of entry
{% endswagger-parameter %}

{% swagger-parameter in="query" name="ttl" type="Integer" required="false" %}
Time To Live of the query in seconds. If a client haven't read any record for this time interval, the server removes the query and the query ID becomes invalid. Default value 5 seconds.
{% endswagger-parameter %}

{% swagger-parameter in="query" name="include-<label name>" required="false" %}
Query records that have a certain value of a label.
{% endswagger-parameter %}

{% swagger-parameter in="query" name="exclude-<label name>" required="false" %}
Query records that don't have a certain value of a label.
{% endswagger-parameter %}

{% swagger-parameter in="query" name="conitnuous" type="Boolean" required="false" %}
Keep query if no records for the request
{% endswagger-parameter %}

{% swagger-parameter in="query" name="limit" type="Integer" required="false" %}
Maximum number of records in the query. Default: unlimited.
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="" %}
```javascript
{
   "id": "integer" // ID of query wich can be used in GET /b/:bucket/:entry request
}
```
{% endswagger-response %}

{% swagger-response status="204: No Content" description="No records for the time interval" %}

{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is invalid or empty" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn't have read permissions" %}

{% endswagger-response %}

{% swagger-response status="404: Not Found" description="The bucket or entry doesn't exist" %}

{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="One or both timestamps are bad , or TTL is not a number" %}

{% endswagger-response %}
{% endswagger %}



{% swagger method="delete" path="" baseUrl="/api/v1/b/:bucket_name/:entry_name  " summary="Remove entry" %}
{% swagger-description %}
Since v1.6, you can remove a specific entry from a bucket with the entire history of its records.
{% endswagger-description %}

{% swagger-response status="200: OK" description="The entry has been removed" %}

{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is invalid or empty" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn't have write permissions" %}

{% endswagger-response %}

{% swagger-response status="404: Not Found" description="The bucket or entry doesn't exist" %}

{% endswagger-response %}
{% endswagger %}
