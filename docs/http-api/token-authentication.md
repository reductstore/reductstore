---
description: >-
  Here you can find how to access to storage with API tokens and use Token API
  to manage them
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

```
```
