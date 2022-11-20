---
description: >-
  Here you can find how to access to storage with access tokens and use Token
  API to manage them
---

# Token API

### Access Token

The storage uses a simple authentication model with a bearer token. This token should be sent as the`Authorization` header in the following format:

```
Bearer <ACCESS_TOKEN>
```

An example of a request with CURL:

```shell
 curl   --header "Authorization: Bearer ${ACCESS_TOKEN}" -a http://127.0.0.1:8383/api/v1/info
```

{% hint style="info" %}
The storage engine uses the token authentication when the`RS_API_TOKEN` envirnoment is set. You should use it as a full access token to create other tokens with different permission by using Token API
{% endhint %}

{% swagger method="post" path="/:token_name" baseUrl="/api/v1/tokens" summary="Create a new access token" %}
{% swagger-description %}
The method creates a new access token with given permissions as a JSON document in the request body . To use this method, you need an access token with full access.
{% endswagger-description %}

{% swagger-parameter in="path" name=":token_name" required="true" %}
Name of new token
{% endswagger-parameter %}

{% swagger-parameter in="body" name="full_access" type="Boolean" %}
Create a token with full acces. Default: false
{% endswagger-parameter %}

{% swagger-parameter in="body" name="read" type="String[]" %}
A list of bucket names for read access. Default: []
{% endswagger-parameter %}

{% swagger-parameter in="body" name="write" type="String[]" %}
A list of bucket names for write access. Default: []
{% endswagger-parameter %}

{% swagger-response status="401: Unauthorized" description="Access token is empty or invalid" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="403: Forbidden" description="Access token doesn't have full access" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}

{% swagger-response status="409: Conflict" description="Token with the same name already exists" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}
{% endswagger %}

