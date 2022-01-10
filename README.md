# Reduct Storage

Cloud\Edge blob storage with focus on AI\ML systems.

Features: 

* HTTP API
* Storing and access blobs as time series
* Optimized for little files
* Real-time quota for buckets

## Motivation

Reduct Storage is a blob storage engine with a simple HTTP API. If the cloud storage engines like AWS S3, Minio etc. provide some kind of file system with HTTP API, Reduct Storage stores blobs as time series and provides access to the data by its timestamp. This might be useful for AI\ML applications when you have some blobs of information from a data source periodically. For example, your application captures images from a CV camera, and you need to store this data to train your model. Reduct Storage could be a good chose for this because it:
* is optimized to write data forward
* has a hard FIFO quota, so the oldest bock of data is removed immediately when you reach the quota of the bucket
* provides data by time, so you don't need to have some IDs for your blobs. You should know only time interval for the HTTP request


##  API Example

Get information about the server:
```shell
curl http://127.0.0.1:8383/info #-> {"bucket_count":183,"version":"0.1.0"}
```

Create a bucket with FIFO quota:
```shell
 curl -d "{\"quot_type\":\"FIFO\", \"quota_size\":10000}" -X POST -a http://127.0.0.1:8383/my_data
```

Write some data with timestamp 100000:
```shell
curl -d "some_data" -X POST -a http://127.0.0.1:8383/my_data/data?ts=10000
```

Reade data by timestamp:
```shell
curl  http://127.0.0.1:8383/my_data/data?ts=10000 #-> "some_data"
```

## Build

TODO
