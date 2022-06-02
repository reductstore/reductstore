# Reduct Storage

![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/reduct-storage/reduct-storage)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/reduct-storage/reduct-storage/ci)


Reduct Storage aims to solve a problem of storing data in a case where you need to write some data intensively and read it accidentally by some time interval. 
The storage engine has HTTP API and stores data as a history of blobs. Read more [here](https://docs.reduct-storage.dev/).

## Features:

* HTTP(S) API
* Storing and access blobs as time series
* Optimized for little files
* Real-time quota for buckets
* Token authentication
* Embedded Web Console
* Support EXT4, FSX filesystems

## Get started

The easiest way to start is to use Docker image:

```shell
docker run -p 8383:8383 -v ${PWD}/data:/data ghcr.io/reduct-storage/reduct-storage:main
```

or you can use the demo storage: https://play.reduct-storage.dev

## Usage Example

Reudct Storage porvides a simple HTTP API, so you can use it with `curl`:

```
# Create a bucket
curl -d "{\"quota_type\":\"FIFO\", \"quota_size\":10000}" \
  -X POST \
  -a https://play.reduct-storage.dev/b/my_data

# Write some data
curl -d "some_data" \
  -X POST \
  -a https://play.reduct-storage.dev/b/my_data/entry_1?ts=10000

# Read the data by timestamp
curl https://play.reduct-storage.dev/b/my_data/entry_1?ts=10000
```

##  Client SDKs

* [Python Client SDK](https://github.com/reduct-storage/reduct-py)
* [JavaScript Client SDK](https://github.com/reduct-storage/reduct-js)
* [C++ Client SDK](https://github.com/reduct-storage/reduct-cpp)
