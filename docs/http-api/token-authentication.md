---
description: Here you can find how to access to storage with token authentication
---

# Token Authentication

### Access Token

The storage uses a simple authentication model with a bearer token.  This token should be sent as `Authorization` header in the following format:

```
Bearer <ACCESS_TOKEN>
```

The access token expires 5 minutes after it was created. After that, the client will receive a 401 HTTP error for any requests and has to refresh the token.

{% hint style="info" %}
The storage uses the token authentication when`RS_API_TOKEN` is set.
{% endhint %}

### Refresh Token

{% swagger method="post" path=" " baseUrl="/auth/refresh " summary="Refresh access token" %}
{% swagger-description %}
The client has to request an access token by using this method with a header 

`Authorization`

which contains SHA256 hash of 

`RS_API_TOKEN.`

The token must be a hexadecimal string.
{% endswagger-description %}

{% swagger-parameter in="header" name="Authorization" %}
Bearer <SHA256 hash of RS_API_TOKEN>
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
hasher = hashlib.sha256(bytes(os.getenv("API_TOKEN"), 'utf-8'))
resp = requests.post(f'{base_url}/auth/refresh', headers={'Authorization': f'Bearer {hasher.hexdigest()}'})
```
