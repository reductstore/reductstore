---
description: HTTP methods to manage access tokens.
---

# Token API

{% hint style="info" %}
The database uses the token authentication when the`RS_API_TOKEN` environment is set. You should use it as a full access token to create other tokens with different permission by using the Token API
{% endhint %}

{% swagger method="get" path="" baseUrl="/api/v1/tokens" summary="Get a list of tokens" %}
{% swagger-description %}
The method returns a list of tokens with names and creation dates. To use this method, you need an access token with full access.
{% endswagger-description %}

{% swagger-response status="200: OK" description="List of tokens" %}
```javascript
{
    "tokens": [
      {
        "name": "string",  // unique name of topic
        "created_at": "string" // creation date as ISO string
      }
    ]
}
```
{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is empty or invalid" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn" %}

{% endswagger-response %}
{% endswagger %}

{% swagger method="get" path="" baseUrl="/api/v1/tokens/:token_name" summary="Show information about a token" %}
{% swagger-description %}
This method provides full information about a token expets its values. The method requers an access token with full access
{% endswagger-description %}

{% swagger-parameter in="path" name="token_name" required="true" %}
Name of token to show
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="Information about token" %}
```javascript
{
    "name": "stirng",            // unique name of topic
    "created_at": "string"       // creation date as ISO string
    "permission": {
        "full_access": "bool",
        "read": []               // list of bucket names for read access
        "write": []              // list of bucket names for write access
    }
}
```
{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is empty or invalid" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn" %}

{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Token with the given name doesn" %}

{% endswagger-response %}
{% endswagger %}

{% swagger method="post" path="/:token_name" baseUrl="/api/v1/tokens" summary="Create a new access token" %}
{% swagger-description %}
The method creates a new access token with given permissions as a JSON document in the request body . To use this method, you need an access token with full access.
{% endswagger-description %}

{% swagger-parameter in="path" name=":token_name" required="true" %}
Name of new token
{% endswagger-parameter %}

{% swagger-parameter in="body" name="full_access" type="Boolean" required="false" %}
Create a token with full acces. Default: false
{% endswagger-parameter %}

{% swagger-parameter in="body" name="read" type="String[]" required="false" %}
A list of bucket names for read access. Default: []
{% endswagger-parameter %}

{% swagger-parameter in="body" name="write" type="String[]" required="false" %}
A list of bucket names for write access. Default: []
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="Returns value and timestamp of token" %}
```javascript
{
    "value": "string",        // hash value of the token
    "created_at": "string"    // date as ISO string
}
```
{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is empty or invalid" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn" %}

{% endswagger-response %}

{% swagger-response status="409: Conflict" description="Token with the same name already exists" %}

{% endswagger-response %}

{% swagger-response status="422: Unprocessable Entity" description="Speciefied bucket doesn" %}

{% endswagger-response %}
{% endswagger %}

{% swagger method="delete" path="" baseUrl="/api/v1/tokens/:token_name" summary="Remove a  token" %}
{% swagger-description %}
This method removes or invokes a token. To use it, a client should has an access token with full access
{% endswagger-description %}

{% swagger-parameter in="path" name="token_name" required="true" %}
Name of new token
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="Token was removed" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="Access token is empty or invalid" %}

{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn" %}

{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Token with the given name doesn" %}

{% endswagger-response %}
{% endswagger %}


{% swagger method="get" path="" baseUrl="/api/v1/me" summary="Get full information about current API token" %}
{% swagger-description %}
This method takes a token from the Authentication header and returns its name, permissions and additional information
{% endswagger-description %}

{% swagger-response status="200: OK" description="Returns JSON document" %}
```javascript
{
    "name": "stirng",            // unique name of topic
    "created_at": "string"       // creation date as ISO string
    "permission": {
        "full_access": "bool",
        "read": []               // list of bucket names for read access
        "write": []              // list of bucket names for write access
    }
}
```
{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="API token is invalid" %}

{% endswagger-response %}
{% endswagger %}
