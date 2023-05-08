


# ReductStore

[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/reductstore/reductstore)](https://github.com/reductstore/reductstore/releases/latest)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reductstore/ci.yml?branch=main)](https://github.com/reductstore/reductstore/actions)
[![Docker Pulls](https://img.shields.io/docker/pulls/reduct/store)](https://hub.docker.com/r/reduct/store)
[![GitHub all releases](https://img.shields.io/github/downloads/reductstore/reductstore/total)](https://github.com/reductstore/reductstore/releases/latest)
[![Discord](https://img.shields.io/discord/939475547065561088)](https://discord.gg/8wPtPGJYsn)

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
* Labeling and searching
* Embedded Web Console
* Support Linux, MacOS and Windows

## Get started

The easiest way to start is to use Docker image:

```shell
docker run -p 8383:8383 -v ${PWD}/data:/data reduct/store:latest
```

or you can use the demo instance https://play.reduct.store/ with API token `reduct`.

See [Getting Started](https://docs.reduct.store/) and [Download](https://www.reduct.store/download) pages for deail.

## Usage Example

ReductStore provides a simple HTTP API, so you could use it with `curl`:

```shell
# Create a bucket
curl -d "{\"quota_type\":\"FIFO\", \"quota_size\":10000}" \
  -X POST \
  "http://127.0.0.1:8383/api/v1/b/my_data"

# Write two records with timestamp 10000 and 20000
curl -d "some_data_1" \
  -X POST \
  --header "x-reduct-label-quality: good" \
  "http://127.0.0.1:8383/api/v1/b/my_data/entry_1?ts=10000"

curl -d "some_data_2" \
  -X POST \
  --header "x-reduct-label-quality: bad" \
  "http://127.0.0.1:8383/api/v1/b/my_data/entry_1?ts=20000"

# Query all records in the bucket (TTL of request 10000s, so you don't need to hurry)
curl "http://127.0.0.1:8383/api/v1/b/my_data/entry_1/q?ttl=10000"

# Take ID from the response and read the data until the end
curl -v "http://127.0.0.1:8383/api/v1/b/my_data/entry_1?q=<ID_FROM_RESPONSE>"
```

## Client SDKs

If you don't want to use HTTP API directly, you can use one of the client SDKs:

* [Python Client SDK](https://github.com/reductstore/reduct-py)
* [JavaScript Client SDK](https://github.com/reductstore/reduct-js)
* [C++ Client SDK](https://github.com/reductstore/reduct-cpp)

## Tools

You can use the following tools to administrate ReductStore:

* [CLI Client](https://github.com/reductstore/reduct-cli)
* [Web Console](https://github.com/reductstore/web-console)
