---
description: Here you can find how to access to storage with token authentication
---

# Token Authentication

### Access Token

The storage uses a simple authentication model with a bearer token. This token should be sent as the`Authorization` header in the following format:

```
Bearer <ACCESS_TOKEN>
```

`ACCESS_TOKEN` should be the same as the `RS_API_TOKEN` environment variable of the server.

An example of a request with CURL:

```shell
 curl   --header "Authorization: Bearer ${API_TOKEN}" -a http://127.0.0.1:8383/info
```

{% hint style="info" %}
The storage uses the token authentication when`RS_API_TOKEN` is set.
{% endhint %}

### Refresh Token \[DEPRECATED]



{% swagger method="post" path=" " baseUrl="/auth/refresh " summary="Refresh access token" %}
{% swagger-description %}
The client has to request an access token by using this method with a header `Authorization` whith the value of the `RS_API_TOKEN` environment variable.

The access token expires 5 minutes after it was created. After that, the client will receive a 401 HTTP error for any requests and has to refresh the token.
{% endswagger-description %}

{% swagger-parameter in="header" name="Authorization" required="false" %}
Bearer
{% endswagger-parameter %}

{% swagger-response status="200: OK" description="" %}
```javascript
{
    "access_token": "string",  // access token
    "expired_at": "integer"    // expiration time in unix time
}
```
{% endswagger-response %}

{% swagger-response status="401: Unauthorized" description="" %}
```javascript
{
    // Response
}
```
{% endswagger-response %}
{% endswagger %}

#### Example in Python

```python
api_token = "SOME_TOKEN"
resp = requests.post(f'{base_url}/auth/refresh', headers={'Authorization': f'Bearer {api_token}'})
```
