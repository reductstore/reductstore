# ⚙ HTTP API Reference

ReductStore provides an HTTP API for interacting with the database. In order to use the API, you must first authenticate using a token, which you can be provisioned one with the `RS_API_TOKEN`[environment variable ](../#environment-variables)or created with [the Token API](token-authentication.md).

Once you have obtained a token, you can use it to authenticate your requests by including it in the `Authorization` header of your HTTP request, like this:

```
Authorization: Bearer <your-token-here>
```

An example of a request with CURL:

```shell
 curl   --header "Authorization: Bearer ${ACCESS_TOKEN}" -a http://127.0.0.1:8383/api/v1/info
```

{% hint style="info" %}
The database uses the token authentication when the`RS_API_TOKEN` envirnoment is set. You should use it as a full access token to create other tokens with different permission by using the Token API
{% endhint %}

## **Handling Errors**

If a request to ReductStore fails, the API returns an HTTP status code indicating the type of error that occurred. For example, a `404 Not Found` status code indicates that the requested resource could not be found.

Since version 1.2.0, the HTTP API also includes an error message in the `x-reduct-error` header of the response. This error message provides more detailed information about the error, which can be useful for debugging and troubleshooting.
