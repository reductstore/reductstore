---
description: Here you can find how to access to storage with token authentication
---

# Token Authentication

### API Token

The storage uses a simple authentication model with a bearer token. This token should be sent as the`Authorization` header in the following format:

```
Bearer <ACCESS_TOKEN>
```

`ACCESS_TOKEN` should be the same as the `RS_API_TOKEN` environment variable of the server.

An example of a request with CURL:

```shell
 curl   --header "Authorization: Bearer ${API_TOKEN}" -a http://127.0.0.1:8383/api/v1/info
```

{% hint style="info" %}
The storage uses the token authentication when`RS_API_TOKEN` is set.
{% endhint %}

```
