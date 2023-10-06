---
description: Here, you can learn how to start working with ReductStore
---

# 💡 Getting Started

ReductStore offers many ways to easily integrate into an existing infrastructure. Choose the one that suits you best.

## Docker

The easiest way to start using ReductStore is to run Docker image:

```
docker run -p 8383:8383 -v ${PWD}/data:/data reduct/store:latest
```

The database will be available on port [http://127.0.01:8383](http://127.0.0.1:8383) and stores data in the `./data` directory. You may check if it's working with a simple HTTP request:

```
curl http://127.0.0.1:8383/api/v1/info
```

## Pre-built Binaries

You can use pre-built binaries for Linux, macOS or Windows operating systems. Download a binary for your OS and platform from [GitHub release page](https://github.com/reductstore/reductstore/releases/latest).  Extract the file and then type the commands:

On Windows:

```
set RS_DATA_PATH=C:\<PATH_TO_DATA>
.\reductstore
```

On Linux or macOS

```
RS_DATA_PATH=./data ./reductstore
```

## Cargo

**Build Requirements:**

* Minimal Rust version: 1.70
* Protobuf 3.17.3 or later

You can also install the database with cargo:

```
sudo apt install protobuf-compiler
cargo install reductstore
RS_DATA_PATH=./data reductstore
```

## Azure IoT

ReductStore is available as an Azure IoT Edge Module, so you can easily deploy it if you are using Azure IoT. Click ["Get It Now"](https://azuremarketplace.microsoft.com/en-us/marketplace/apps/reductstorellc1689939980623.reductstore?tab=Overview) and follow the instructions.

## Snap

You can install the database with snap:

```
sudo snap install reductstore
```

ReductStore will be available on port http://127.0.01:8383 and store data in the `/var/snap/reductstore/common/data` directory.

### Demo Server

If you don't want to deal with the installation, but just want to play with the database, you can use our demo server:

URL: [https://play.reduct.store](https://play.reduct.store)

API Token: reductstore

## What Is Next?

Now the database needs to ingest data. Take one of the officially supported SDKs:

* [For Rust](https://docs.rs/crate/reduct-rs/latest)
* [For Python](https://py.reduct.store/en/latest/)
* [For JavaScript/NodeJS/TypeScript](https://js.reduct.store/en/latest/)
* [For C++](https://cpp.reduct.store)

If you can't find your programming language in the list, you can still use [the ReductStore HTTP API](http-api/) or ping us. If you are serious about using it, we can help you with integration.

