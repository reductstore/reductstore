# Reduct Storage

![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/reduct-storage/reduct-storage)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/reduct-storage/reduct-storage/ci)


Reduct Storage aims to solve the problem of storing data when you need to write intensively but read it occasionally and randomly via a time interval. 
The storage engine has a simple HTTP API and stores data as a history of blobs. Read more [here](https://docs.reduct-storage.dev/).

## Features:

* HTTP(S) API
* Storing and accessing blobs as time series
* Optimized for small files
* Real-time quota for buckets
* Token authentication
* Embedded Web Console
* Support EXT4, XFS filesystems

## Get started

The easiest way to start is to use Docker image:

```shell
docker run -p 8383:8383 -v ${PWD}/data:/data ghcr.io/reduct-storage/reduct-storage:main
```

or you can use the demo storage: https://play.reduct-storage.dev

## Usage Example

Reduct Storage porvides a simple HTTP API, so you could use it with `curl`:

```
# Create a bucket
curl -d "{\"quota_type\":\"FIFO\", \"quota_size\":10000}" \
  -X POST \
  -a https://play.reduct-storage.dev/b/my_data

# Write some data
curl -d "some_data" \
  -X POST \
  -a https://play.reduct-storage.dev/b/my_data/entry_1?ts=10000

# Read the data by using its timestamp
curl https://play.reduct-storage.dev/b/my_data/entry_1?ts=10000
```

##  Client SDKs

* [Python Client SDK](https://github.com/reduct-storage/reduct-py)
* [JavaScript Client SDK](https://github.com/reduct-storage/reduct-js)
* [C++ Client SDK](https://github.com/reduct-storage/reduct-cpp)
