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

**Build Requirements:**

* Minimal Rust version: 1.66.0
* OpenSSL 3 or later
* Protobuf 3.17.3 or later

You can also install the database with cargo:

```
sudo apt install libssl-dev protobuf-compiler pkg-config
cargo install reductstore
RS_DATA_PATH=./data reductstore
```

##
