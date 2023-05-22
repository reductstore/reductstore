---
description: Here you can learn how to start working with ReductStore
---

# 💡 Getting Started

### Start With Docker

The easiest way to start using ReductStore is to run Docker image:

```
docker run -p 8383:8383 -v ${PWD}/data:/data reduct/store:latest
```

The database will be available on port http://127.0.01:8383 and stores data in the `./data` directory. You may check if it's working with a simple HTTP request:

```
curl http://127.0.0.1:8383/api/v1/info
```

### Start With Snap

You can install the database with snap:

```
sudo snap install reductstore
```

ReductStore will be available on port http://127.0.01:8383 and store data in the `/var/snap/reductstore/common/data` directory.

### Start With Cargo

You can also install the database with cargo:

```
cargo install reductstore
RS_DATA_PATH=./data reductstore
```

## Configuration

ReductStore can be configured with the following environmental variables:

| Name                | Default | Description                                                                               |
|---------------------|---------|-------------------------------------------------------------------------------------------|
| RS\_LOG\_LEVEL      | INFO    | Logging level, can be: TRACE, DEBUG, INFO, WARNING, ERROR                                 |
| RS\_HOST            | 0.0.0.0 | Listening IP address                                                                      |
| RS\_PORT            | 8383    | Listening port                                                                            |
| RS\_API\_BASE\_PATH | /       | Prefix for all URLs of requests                                                           |
| RS\_DATA\_PATH      | /data   | Path to a folder where the storage stores the data                                        |
| RS\_API\_TOKEN      |         | If set, the storage uses [token authorization](broken-reference)                          |
| RS\_CERT\_PATH      |         | Path to an SSL certificate. If unset, the storage uses HTTP instead of HTTPS              |
| RS\_CERT\_KEY\_PATH |         | Path to the private key of the desired SSL certificate. Should be set with RS\_CERT\_PATH |

If you use snap, you can configure the database by using the `snap set` command:

```
snap set reductstore log-level=DEBUG
```

This command change the log level to DEBUG and restarts the database. You can check the current configuration with the `snap get reductstoret` command:

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
