---
description: >-
  Here you can find how to access to storage with access tokens and use  the
  Token API to manage them
---

# Token API

### Access Token

Reduct Storage uses a simple authentication model with a bearer token. This token should be sent as the`Authorization` header in the following format:

```
Bearer <ACCESS_TOKEN>
```

An example of a request with CURL:

```shell
 curl   --header "Authorization: Bearer ${ACCESS_TOKEN}" -a http://127.0.0.1:8383/api/v1/info
```

{% hint style="info" %}
The storage engine uses the token authentication when the`RS_API_TOKEN` envirnoment is set. You should use it as a full access token to create other tokens with different permission by using the Token API
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
```javascript
{
    "detail": "error_message"
}
```
{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn't have full access" %}
```javascript
{
    "detail": "error_message"
}
```
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
```javascript
{
    "detail": "error_message"
}
```
{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn't have full access" %}
```javascript
{
    "detail": "error_message"
}
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Token with the given name doesn't exist" %}
```javascript
{
    "detail": "error_message"
}
```
{% endswagger-response %}
{% endswagger %}



{% swagger method="post" path="/:token_name" baseUrl="/api/v1/tokens" summary="CreateToken a new access token" %}
{% swagger-description %}
The method creates a new access token with given permissions as a JSON document in the request body . To use this method, you need an access token with full access.
{% endswagger-description %}

{% swagger-parameter in="path" name=":token_name" required="true" %}
Name of new token
{% endswagger-parameter %}

{% swagger-parameter in="body" name="full_access" type="Boolean" %}
CreateToken a token with full acces. Default: false
{% endswagger-parameter %}

{% swagger-parameter in="body" name="read" type="String[]" %}
A list of bucket names for read access. Default: []
{% endswagger-parameter %}

{% swagger-parameter in="body" name="write" type="String[]" %}
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
```javascript
{
    "detail": "error_message"
}
```
{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn't have full access" %}
```javascript
{
    "detail": "error_message"
}
```
{% endswagger-response %}

{% swagger-response status="409: Conflict" description="Token with the same name already exists" %}
```javascript
{
    "detail": "error_message"
}
```
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
```javascript
{
    "detail": "error_message"
}
```
{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn't have full access" %}
```javascript
{
    "detail": "error_message"
}
```
{% endswagger-response %}

{% swagger-response status="404: Not Found" description="Token with the given name doesn't exist" %}
```javascript
{
    "detail": "error_message"
}
```
{% endswagger-response %}
{% endswagger %}

