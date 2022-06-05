## Release 0.6.0 (in progress)

**Features**:

* Filter unfinished records in GET /b/:bucket/:entry/list endpoint, [PR-106](https://github.com/reduct-storage/reduct-storage/pull/106)
* Add content-type header to responses, [PR-107](https://github.com/reduct-storage/reduct-storage/pull/107)
* Add `max_block_records` to bucket settings, [PR-108](https://github.com/reduct-storage/reduct-storage/pull/108)

**Bugs**:

* Fix Web Console for RS_API_BASE_PATH, [PR-92](https://github.com/reduct-storage/reduct-storage/pull/92)
* Fix wasting disk space in FSX filesystem, [PR-100](https://github.com/reduct-storage/reduct-storage/pull/100)
* Fix base path in server url, [PR-105](https://github.com/reduct-storage/reduct-storage/pull/105)
* Fix updating record state in asynchronous write operation, [PR-109](https://github.com/reduct-storage/reduct-storage/pull/109)
**Other**:

* Optimise write operation, [PR-96](https://github.com/reduct-storage/reduct-storage/pull/96)
* Add benchmarks and refactor block management [PR-99](https://github.com/reduct-storage/reduct-storage/pull/99)
* Add curl to deploy image [PR-104](https://github.com/reduct-storage/reduct-storage/pull/104)

## Release 0.5.1 (2022-05-24)

**Bugs**:

* GET /b/:bucket/:entry doesn't create an empty entry, [PR-95](https://github.com/reduct-storage/reduct-storage/pull/95)

## Release 0.5.0 (2022-05-15)

**Features**:

* Integrate Web Console, [PR-77](https://github.com/reduct-storage/reduct-storage/pull/77)
* Print only HTTP errors 50x to the logs, [PR-84](https://github.com/reduct-storage/reduct-storage/issues/84)
* Add default settings for a new bucket in GET /info, [PR-87](https://github.com/reduct-storage/reduct-storage/pull/87)

**Bugs**:

* Fix CORS functionality, [PR-72](https://github.com/reduct-storage/reduct-storage/pull/72)
* Fix quota policy, [PR-83](https://github.com/reduct-storage/reduct-storage/pull/83)

**Documentation**:

* Add link to JS SDK, [PR-88](https://github.com/reduct-storage/reduct-storage/pull/88)

### Release 0.4.3 (2022-05-01)

**Bugs**:

* Fix sending big blobs [PR-80](https://github.com/reduct-storage/reduct-storage/pull/80)
* Fix handling offset in tryEnd [PR-81](https://github.com/reduct-storage/reduct-storage/pull/81)

### Release 0.4.2 (2022-04-30)

**Bugs**:

* Fix deadlock during sending data, [PR-78](https://github.com/reduct-storage/reduct-storage/pull/78)

### Release 0.4.1 (2022-04-04)

**Bugs**:

* Fix timestamp for oldest record, [PR-68](https://github.com/reduct-storage/reduct-storage/pull/68)

## Release 0.4.0 (2022-04-01)

**Features**:

* Asynchronous write/read operations with data blocks, [PR-62](https://github.com/reduct-storage/reduct-storage/pull/62)

**Bugs**:

* Fix searching start block in Entry List request, [PR-61](https://github.com/reduct-storage/reduct-storage/pull/61)
* Fix aborting GET requests, [PR-64](https://github.com/reduct-storage/reduct-storage/pull/64)

**Other**:

* Refactor block structure in entry, [PR-58](https://github.com/reduct-storage/reduct-storage/pull/58)

## Release 0.3.0 (2022-03-14)

**Features**:

* Add secure HTTP, [PR-49](https://github.com/reduct-storage/reduct-storage/pull/49)
* Extend GET /b/:bucket method with stats and list
  entries, [PR-51](https://github.com/reduct-storage/reduct-storage/pull/51)
* Add access to the latest record, [PR-53](https://github.com/reduct-storage/reduct-storage/pull/53)

**Bugs**:

* Fix sending two responses for HTTP error, [PR-48](https://github.com/reduct-storage/reduct-storage/pull/48)

**Other**:

* Replace nholmann/json with Protobuf, [PR-47](https://github.com/reduct-storage/reduct-storage/pull/47)

### Release 0.2.1 (2022-03-07)

**Bugs**:

* Fix crushing when API token is wrong, [PR-42](https://github.com/reduct-storage/reduct-storage/pull/42)
* Fix order of authentication checks, [PR-43](https://github.com/reduct-storage/reduct-storage/pull/43)

## Release 0.2.0 (2022-02-26)

**Features**:

* Add HEAD method to Bucket API, [PR-30](https://github.com/reduct-storage/reduct-storage/pull/30)
* PUT method of Bucket API has optional parameters, [PR-32](https://github.com/reduct-storage/reduct-storage/pull/32)
* Extends information from GET method of Server API, [PR-33](https://github.com/reduct-storage/reduct-storage/pull/33)
* Add GET /list end point to browse buckets, [PR-34](https://github.com/reduct-storage/reduct-storage/pull/34)
* Add bearer token authentication, [PR-36](https://github.com/reduct-storage/reduct-storage/pull/36)

**Bugs**:

* Fix docker build on ARM32, [PR-29](https://github.com/reduct-storage/reduct-storage/pull/29)
* Fix IBucket::List error 500 for timestamps between
  blocks, [PR-31](https://github.com/reduct-storage/reduct-storage/pull/31)

**Documentation**:

* Fix wrong parameters in Entry API documentation, [PR-38](https://github.com/reduct-storage/reduct-storage/pull/38)

### Release 0.1.1 (2022-02-13)

**Bugs**:

* Fix default folder for data in Docker image, [PR-23](https://github.com/reduct-storage/reduct-storage/pull/23)

## Release 0.1.0 (2022-01-24)

* Initial release with basic HTTP API and FIFO bucket quota