# ⚙ Configuration

## Settings

ReductStore uses environment variables for configuration. Here is a list of available settings:

| Name                | Default | Description                                                                               |
| ------------------- | ------- | ----------------------------------------------------------------------------------------- |
| RS\_LOG\_LEVEL      | INFO    | Logging level, can be: TRACE, DEBUG, INFO, WARNING, ERROR                                 |
| RS\_HOST            | 0.0.0.0 | Listening IP address                                                                      |
| RS\_PORT            | 8383    | Listening port                                                                            |
| RS\_API\_BASE\_PATH | /       | Prefix for all URLs of requests                                                           |
| RS\_DATA\_PATH      | /data   | Path to a folder where the storage stores the data                                        |
| RS\_API\_TOKEN      |         | If set, the storage uses [token authorization](broken-reference/)                         |
| RS\_CERT\_PATH      |         | Path to an SSL certificate. If unset, the storage uses HTTP instead of HTTPS              |
| RS\_CERT\_KEY\_PATH |         | Path to the private key of the desired SSL certificate. Should be set with RS\_CERT\_PATH |

### For Snap Users

If you use snap, you can configure the database by using the `snap set` command:

```
snap set reductstore log-level=DEBUG
```

This command changes the log level to DEBUG and restarts the database. You can check the current configuration with the `snap get reductstoret` command:

```
snap get reductstore
Key            Value
api-base       /
api-token
cert-key-path
cert-path
data-path      /var/snap/reductstore/common
host           0.0.0.0
log-level      DEBUG
port           8383
```

## Provisioning

ReductStore provides an HTTP API to create and configure resources such as buckets or access tokens. However, if you are following an Infrastructure as Code (IaC) approach, you may want to provision resources at the deployment stage and ensure that they can't be modified or deleted using the HTTP API. You can use the following environment variables to do this:

<table><thead><tr><th width="300">Name</th><th width="70">Default</th><th>Description</th></tr></thead><tbody><tr><td><strong>Bucket Provisioning</strong></td><td></td><td></td></tr><tr><td>RS_BUCKET_&#x3C;ID>_NAME</td><td></td><td>Provisioned bucket name (required)</td></tr><tr><td>RS_BUCKET_&#x3C;ID>_QUOTA_TYPE</td><td>NONE</td><td>It can be NONE or FIFO.  If it is FIFO, the bucket removes old data.</td></tr><tr><td>RS_BUCKET_&#x3C;ID>_QUOTA_SIZE</td><td>""</td><td>Size of quota to start removing old data e.g. 1 KB, 10.4 MB etc.</td></tr><tr><td>RS_BUCKET_&#x3C;ID>_MAX_BLOCK_SIZE</td><td>64Mb</td><td>Maximal block size for batched records</td></tr><tr><td>RS__BUCKET_&#x3C;ID>_MAX_BLOCK_RECORDS</td><td>256</td><td>Maximal number of batched records in a block</td></tr><tr><td><strong>Token Provisioning</strong></td><td></td><td></td></tr><tr><td>RS_TOKEN_&#x3C;ID>_NAME</td><td></td><td>Provisioned token name (required)</td></tr><tr><td>RS_TOKEN_&#x3C;ID>_VALUE</td><td></td><td>Provisioned value of token (required)</td></tr><tr><td>RS_TOKEN_&#x3C;ID>_FULL_ACCESS</td><td>false</td><td>Full access permission</td></tr><tr><td>RS_TOKEN_&#x3C;ID>_READ</td><td></td><td>List of buckets for reading</td></tr><tr><td>RS_TOKEN_&#x3C;ID>_WRITE</td><td></td><td>List of buckets for writing</td></tr></tbody></table>

{% hint style="info" %}
You can use any string value for \<ID>. It is only used to group resources of the same type.
{% endhint %}

&#x20;There is an example where we provide two buckets and a token to access them:

```
RS_BUCKET_A_NAME=bucket-1
RS_BUCKET_A_QUOTA_TYPE=FIFO
RS_BUCKET_A_QUOTA_SIZE=1Tb

RS_BUCKET_B_NAME=bucket-2

RS_TOKEN_A_NAME=token
RS_TOKEN_A_VALUE=somesecret
RS_TOKEN_A_READ=bucket-1,bucket-2
```
