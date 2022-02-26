# Reduct Storage

Historian Object storage with focus on AI\ML applications.

Reduct Storage aims to solve the problem of storing data in a case where you need to write some data intensively and read it accidentally by some time interval. 
The storage uses HTTP API and stores the data as blobs. Read more [here](https://docs.reduct-storage.dev/http-api).

## Features:

* HTTP API
* Storing and access blobs as time series
* Optimized for little files
* Real-time quota for buckets
* Token authentication

## Get started

The easiest way to start is to use Docker image:

```shell
docker run -p 8383:8383 -v ${PWD}/data:/data ghcr.io/reduct-storage/reduct-storage:main
```

## API Example

Get information about the server:

```shell
curl http://127.0.0.1:8383/info #-> {"bucket_count":183,"version":"0.1.0"}
```

Create a bucket with FIFO quota:

```shell
 curl -d "{\"quot_type\":\"FIFO\", \"quota_size\":10000}" -X POST -a http://127.0.0.1:8383/b/my_data
```

Write some data with timestamp 100000:

```shell
curl -d "some_data" -X POST -a http://127.0.0.1:8383/b/my_data/entry_1?ts=10000
```

Read data by timestamp:

```shell
curl  http://127.0.0.1:8383/b/my_data/entry_1?ts=10000 #-> "some_data"
```

See [HTTP API](https://docs.reduct-storage.dev/http-api)
