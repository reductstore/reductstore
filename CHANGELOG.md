## Release 0.2.0 (2022-02-26)

**Features**:

* Add HEAD method to Bucket API, [PR-30](https://github.com/reduct-storage/reduct-storage/pull/30)
* PUT method of Bucket API has optional parameters, [PR-32](https://github.com/reduct-storage/reduct-storage/pull/32)
* Extends information from GET method of Server API, [PR-33](https://github.com/reduct-storage/reduct-storage/pull/33)
* Add GET /list end point to browse buckets, [PR-34](https://github.com/reduct-storage/reduct-storage/pull/34)
* Add bearer token authentication, [PR-36](https://github.com/reduct-storage/reduct-storage/pull/36)

**Bugs**:

* Fix docker build on ARM32, [PR-29](https://github.com/reduct-storage/reduct-storage/pull/29)
* Fix IBucket::List error 500 for timestamps between blocks, [PR-31](https://github.com/reduct-storage/reduct-storage/pull/31)

**Documentation**:

* Fix wrong parameters in Entry API documentation, [PR-38](https://github.com/reduct-storage/reduct-storage/pull/38)

## Release 0.1.1 (2022-02-13)

**Bugs**:

* Fix default folder for data in Docker image, [PR-23](https://github.com/reduct-storage/reduct-storage/pull/23)

## Release 0.1.0 (2022-01-24)

* Initial release with basic HTTP API and FIFO bucket quota