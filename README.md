# ReductStore

[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/reductstore/reductstore)](https://github.com/reductstore/reductstore/releases/latest)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reductstore/ci.yml?branch=main)](https://github.com/reductstore/reductstore/actions)
[![Docker Pulls](https://img.shields.io/docker/pulls/reductstore/reductstore)](https://hub.docker.com/r/reductstore/reductstore)
[![GitHub all releases](https://img.shields.io/github/downloads/reductstore/reductstore/total)](https://github.com/reductstore/reductstore/releases/latest)

ReductStore is a time series database designed specifically for storing and managing large amounts of blob data. It has
high performance for writing and real-time querying, making it suitable for edge computing, computer vision, and IoT
applications. ReductStore is 100% open source under Mozilla Public License v2.0.
It has a simple HTTP API and provides random access to data via a timestamp or time interval. Read
more [here](https://docs.reduct.store/).

## Features:

* HTTP(S) API
* Storing and accessing blobs as time series
* Optimized for small files
* Real-time FIFO quota for buckets
* Token authorization
* Embedded Web Console
* Support Linux, MacOS and Windows on AMD64

## Get started

The easiest way to start is to use Docker image:

```shell
docker run -p 8383:8383 -v ${PWD}/data:/data reductstore/reductstore:latest
```

or you can use the demo storage: https://play.reduct.store/

## Usage Example

ReductStore provides a simple HTTP API, so you could use it with `curl`:

```shell
export API_TOKEN=reduct

# Create a bucket
curl -d "{\"quota_type\":\"FIFO\", \"quota_size\":10000}" \
  -X POST \
  --header "Authorization: Bearer ${API_TOKEN}"   \
  -a https://play.reduct.store/api/v1/b/my_data

# Write some data
curl -d "some_data" \
  -X POST \
  --header "Authorization: Bearer ${API_TOKEN}"   \
  -a https://play.reduct.store/api/v1/b/my_data/entry_1?ts=10000

# Read the data by using its timestamp
curl --header "Authorization: Bearer ${API_TOKEN}"   \
    https://play.reduct.store/api/v1/b/my_data/entry_1?ts=10000
```

## Client SDKs

* [Python Client SDK](https://github.com/reducstore/reduct-py)
* [JavaScript Client SDK](https://github.com/reducstore/reduct-js)
* [C++ Client SDK](https://github.com/reducstore/reduct-cpp)

## Tools

* [CLI Client](https://github.com/reducstore/reduct-cli)
* [Web Console](https://github.com/reducstore/web-console)
