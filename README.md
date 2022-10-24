# Reduct Storage

![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/reduct-storage/reduct-storage)
![GitHub Workflow Status](https://img.shields.io/github/workflow/status/reduct-storage/reduct-storage/ci)
![Docker Pulls](https://img.shields.io/docker/pulls/reductstorage/engine)
![GitHub all releases](https://img.shields.io/github/downloads/reduct-storage/reduct-storage/total)

Reduct Storage is a time series database for big data. It has no limitation on the size of stored objects or the volume of stored data. It has a simple HTTP API and provides random access to data via a timestamp or time interval. Read more [here](https://docs.reduct-storage.dev/).

## Features:

* HTTP(S) API
* Storing and accessing blobs as time series
* Optimized for small files
* Real-time quota for buckets
* Token authentication
* Embedded Web Console
* Support Linux, MacOS and Windows on AMD64

## Get started

The easiest way to start is to use Docker image:

```shell
docker run -p 8383:8383 -v ${PWD}/data:/data reductstorage/engine:latest
```

or you can use the demo storage: https://play.reduct-storage.dev

## Usage Example

Reduct Storage porvides a simple HTTP API, so you could use it with `curl`:

```shell
# Take a temporal access token by using the API token
export API_TOKEN=reduct

# Create a bucket
curl -d "{\"quota_type\":\"FIFO\", \"quota_size\":10000}" \
  -X POST \
  --header "Authorization: Bearer ${API_TOKEN}"   \
  -a https://play.reduct-storage.dev/api/v1/b/my_data

# Write some data
curl -d "some_data" \
  -X POST \
  --header "Authorization: Bearer ${API_TOKEN}"   \
  -a https://play.reduct-storage.dev/api/v1/b/my_data/entry_1?ts=10000

# Read the data by using its timestamp
curl --header "Authorization: Bearer ${API_TOKEN}"   \
    https://play.reduct-storage.dev/api/v1/b/my_data/entry_1?ts=10000
```

##  Client SDKs

* [Python Client SDK](https://github.com/reduct-storage/reduct-py)
* [JavaScript Client SDK](https://github.com/reduct-storage/reduct-js)
* [C++ Client SDK](https://github.com/reduct-storage/reduct-cpp)

##  Tools

* [CLI Clinet](https://github.com/reduct-storage/reduct-cli)
* [Web Console](https://github.com/reduct-storage/web-console)
