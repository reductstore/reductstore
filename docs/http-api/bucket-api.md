---
description: HTTP methods to manage buckets with data
---

# Bucket API

The Bucket API allows users to create, modify, and delete buckets.

Before starting to record data, a user must first create a bucket and specify settings such as:

* Maximum block size
* Maximum number of records
* Quota type
* Quota size

For more information about buckets, read [here](../how-does-it-work.md#internal-structure).

{% swagger method="get" path=" " baseUrl="/api/v1/b/:bucket_name " summary="Get information about a bucket" %}
{% swagger-description %}
The method returns the current settings, stats, and entry list of the bucket in JSON format. If authenticaion is enabled, the method needs a valid API token.
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
        "max_block_records": "integer",         // max number of records in a block
        "quota_size": "integer"                 // quota content_length in bytes
    }
    "info": {
        "name": "string",         // name of the bucket
        "entry_count": "integer", // number of entries in the bucket
        "size": "integer",        // size of stored data in the bucket in bytes
        "oldest_record": "integer", // unix timestamp of oldest record in microseconds
        "latest_record": "integer",  // unix timestamp of latest record in microseconds
        "is_provisioned": "bool"  // true if the bucket provisioned and can't be changed via API
    },
    "entries": [        // list of entry stats
        {
            "name": "string",    // name of the entry
            "size": "integer",          // size of stored data in bytes
            "block_count": "integer",   // number of blocks with data
            "record_count": "integer",  // number of recods in the entry
            "oldest_record": "integer", // unix timestamp of oldest record in microseconds
            "latest_record": "integer"  // unix timestamp of latest record in microseconds

        }
    ]
}
```
{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is invalid or empty" %}

{% endswagger-response %}

{% swagger-response status="404: Not Found" description="The bucket does not exist" %}

{% endswagger-response %}
{% endswagger %}

{% swagger method="head" path=" " baseUrl="/api/v1/b/:bucket_name " summary="Check if a bucket exists" %}
{% swagger-description %}
If authenticaion is enabled, the method needs a valid API token.
{% endswagger-description %}

{% swagger-parameter in="path" name="bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The bucket exists" %}

{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is invalid or empty" %}

{% endswagger-response %}

{% swagger-response status="404: Not Found" description="The bucket does not exist" %}

{% endswagger-response %}
{% endswagger %}

{% swagger method="post" path=" " baseUrl="/api/v1/b/:bucket_name  " summary="Create a new bucket" %}
{% swagger-description %}
To create a bucket, the request should contain a JSON document with some parameters or empty body. The new bucket uses default values if some parameters are empty.

If authentication is enabled, the method needs a valid API token with full access.
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of new bucket
{% endswagger-parameter %}

{% swagger-parameter in="body" name="max_block_size" type="String/Integer" required="false" %}
Maximum size of a data block in bytes (default: 64Mb)
{% endswagger-parameter %}

{% swagger-parameter in="body" name="max_block_records" type="String/Integer" required="false" %}
Maximum number of records in a block (default 256)
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_type" type="String" required="false" %}
Type of quota. Can have values "NONE" or "FIFO" (default: "NONE")
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_size" type="String/Integer" required="false" %}
Size of quota in bytes (default: 0)
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The new bucket is created" %}

{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is invalid or empty" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn" %}

{% endswagger-response %}

{% swagger-response status="409: Conflict" description="A bucket with the same name already exists" %}

{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="JSON request is invalid" %}

{% endswagger-response %}
{% endswagger %}

{% swagger method="put" path=" " baseUrl="/api/v1/b/:bucket_name " summary="Change settings of a bucket" %}
{% swagger-description %}
To update settings of a bucket, the request should have a JSON document with all the settings.

If authenticaion is enabled, the method needs a valid API token with full access.
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket
{% endswagger-parameter %}

{% swagger-parameter in="body" name="max_block_size" type="String/Integer" required="false" %}
Maximum content_length of a data block in bytes
{% endswagger-parameter %}

{% swagger-parameter in="body" name="max_block_records" type="String/Integer" required="false" %}
Maximum number of records in a block
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_type" type="String" required="false" %}
Type of quota. Can have values "NONE" or "FIFO"
{% endswagger-parameter %}

{% swagger-parameter in="body" name="quota_size" type="String/Integer" required="false" %}
Size of quota in bytes
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The settings are updated" %}

{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is invalid or empty" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn" %}

{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Bucket doesn't exist" %}

{% endswagger-response %}

{% swagger-response status="409: Conflict" description="Bucket is provisioned" %}

{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="JSON request is invalid" %}

{% endswagger-response %}
{% endswagger %}

{% swagger method="delete" path=" " baseUrl="/api/v1/b/:bucket_name " summary="Remove a bucket" %}
{% swagger-description %}
Remove a bucket with **all its entries and stored data.**

If authentication is enabled, the method needs a valid API token with full access.
{% endswagger-description %}

{% swagger-parameter in="path" name=":bucket_name" required="true" %}
Name of bucket to remove
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="The bucket is removed" %}

{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is invalid or empty" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn" %}

{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Bucket does not exists" %}

{% endswagger-response %}

{% swagger-response status="409: Conflict" description="Bucket is provisioned" %}

{% endswagger-response %}
{% endswagger %}
