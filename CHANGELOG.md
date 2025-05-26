# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Pass server information to extensions, [PR-816](https://github.com/reductstore/reductstore/pull/816)
- Integrate ReductSelect v0.1.0, [PR-821](https://github.com/reductstore/reductstore/pull/821)

### Changed

- Add "@" prefix to computed labels, [PR-815](https://github.com/reductstore/reductstore/pull/815)
- Refactor Extension API for multi-line CSV processing, ReductSelect v0.2.0, [PR-823](https://github.com/reductstore/reductstore/pull/823)

### Fixed

- Fix hanging test in batch read module, [PR-830](https://github.com/reductstore/reductstore/pull/830)

## [1.15.2] - 2025-05-21

### Changed

- Update Web Console up to v1.10.1, [PR-828](https://github.com/reductstore/reductstore/pull/828)

### Fixed

- Rebuild block index if orphan block descriptor find, [PR-829](https://github.com/reductstore/reductstore/pull/829)

## [1.15.1] - 2025-05-15

### Fixed

- Fix replication of record updates, [PR-822](https://github.com/reductstore/reductstore/pull/822)

## [1.15.0] - 2025-05-07

### Added

- Python client benchmarks in CI, [PR-764](https://github.com/reductstore/reductstore/pull/764)
- RS-629: Extension API v0.1 and core implementation, [PR-762](https://github.com/reductstore/reductstore/pull/762)
- RS-622: Improve Extension API, [PR-779](https://github.com/reductstore/reductstore/pull/779)
- RS-652: `$exists\$has` operator checks multiple labels, [PR-786](https://github.com/reductstore/reductstore/pull/786)
- RS-643: Implement `$each_n` operator, [PR-788](https://github.com/reductstore/reductstore/pull/788)
- RS-644: Implement `$each_t` operator, [PR-792](https://github.com/reductstore/reductstore/pull/792)
- RS-672: Implement `$limit` operator, [PR-793](https://github.com/reductstore/reductstore/pull/793)
- RS-645: Implement `$timestamp` operator, [PR-798](https://github.com/reductstore/reductstore/pull/798)
- RS-646: Enable logging in extensions, [PR-646](https://github.com/reductstore/reductstore/pull/794)


### Changed

- Minimum Rust version to 1.85
- RS-633: Link runtime libraries statically, [PR-761](https://github.com/reductstore/reductstore/pull/761)
- RS-628: Return error if extension not found in query, [PR-780](https://github.com/reductstore/reductstore/pull/780)
- Timout for IO operation 5s, [PR-804](https://github.com/reductstore/reductstore/pull/804)
- Update Web Console up to v1.10, [PR-809](https://github.com/reductstore/reductstore/pull/809)

### Fixed

- RS-660: Fix deadlocks using `current_thread` runtime, [PR-781](https://github.com/reductstore/reductstore/pull/781)
- RS-689: Fix WAL recovery when a block wasn't saved in block index, [PR-785](https://github.com/reductstore/reductstore/pull/785)
- Fix replication recovery from broken record, [PR-795](https://github.com/reductstore/reductstore/pull/795)
- Fix deadlock in replication task, [PR-796](https://github.com/reductstore/reductstore/pull/796)
- Remove broken transaction log through file cache, [PR-799](https://github.com/reductstore/reductstore/pull/799)
- Channel timeout for read operation to prevent hanging readers, [PR-804](https://github.com/reductstore/reductstore/pull/804)

### Internal

- RS-647: Build binaries for Linux and Macos ARM64, [PR-647](https://github.com/reductstore/reductstore/pull/782)

## [1.4.8] - 2025-05-02

### Fixed

- Update Web Console up to 1.9.2 with start/stop query fix, [PR-805](https://github.com/reductstore/reductstore/pull/805)

## [1.4.7] - 2025-04-24

### Fixed

- Ignore the `Content-Length` header when updating labels, [PR-800](https://github.com/reductstore/reductstore/pull/800)

## [1.4.6] - 2025-04-17

### Fixed

- RS-692: Fix removal of replication task after update with invalid configuration, [PR-790](https://github.com/reductstore/reductstore/pull/790)
- RS-669: Fix JSON format in error messages with quotes, [PR-791](https://github.com/reductstore/reductstore/pull/791)

## [1.14.5] - 2025-04-03

### Fixed

- RS-659: Fix replication timeout for large records, [PR-774](https://github.com/reductstore/reductstore/pull/774)
- Fix double sync of block descriptor for a new block, [PR-775](https://github.com/reductstore/reductstore/pull/775)
- Update Web Console up to 1.9.1, [PR-776](https://github.com/reductstore/reductstore/pull/776)

## [1.14.4] - 2025-03-28

### Fixed

- RS-656: Fix replication lock during API HTTP iteration and batching issues, [PR-771](https://github.com/reductstore/reductstore/pull/771)

## [1.14.3] - 2025-03-10

### Fixed

- Fix hanging read query and its channel timeout, [PR-750](https://github.com/reductstore/reductstore/pull/750)

## [1.14.2] - 2025-02-27

### Fixed

- Minimum Rust version to 1.81

## [1.14.1] - 2025-02-27

### Fixed`

- Remove non-existing block from block index, [PR-744](https://github.com/reductstore/reductstore/pull/744)

## [1.14.0] - 2025-02-25

### Added

- RS-550: Support for when condition on replication task, [PR-687](https://github.com/reductstore/reductstore/pull/687)
- RS-531: Arithmetical operators in conditional query, [PR-696](https://github.com/reductstore/reductstore/pull/696)
- RS-530: String operators in conditional query, [PR-705](https://github.com/reductstore/reductstore/pull/705)
- RS-549: `$in/$nin` logical operators in conditional query, [PR-722](https://github.com/reductstore/reductstore/pull/722)
- RS-597: Configuration parameters for batch and HTTP limits, [PR-725](https://github.com/reductstore/reductstore/pull/725)
- RS-243: Configuration parameters for replication timeout and transaction log size, [PR-735](https://github.com/reductstore/reductstore/pull/735)

### Changed

- RS-598: Optimize write operation for small records, [PR-723](https://github.com/reductstore/reductstore/pull/723)
- RS-608: Bump rand to 0.9 and fix compatibility issues, [PR-736](https://github.com/reductstore/reductstore/pull/736)
- Update Web Console up to v1.9.0, [PR-743](https://github.com/reductstore/reductstore/pull/743)

### Fixed

- Fix deadlock in write operation for small records, [PR-724](https://github.com/reductstore/reductstore/pull/724)
- RS-609: Restart replication thread when task settings are updated, [PR-737](https://github.com/reductstore/reductstore/pull/737)

## [1.13.5] - 2025-02-05

### Fixed

- RS-585: Fix performance regression for querying records, [PR-721](https://github.com/reductstore/reductstore/pull/721)

## [1.13.4] - 2025-01-27

### Fixed

- RS-583: Close file descriptors before removing a folder, [PR-714](https://github.com/reductstore/reductstore/pull/714)

## [1.13.3] - 2025-01-21

### Fixed

- RS-555: Close WAL file before removal it from disk, [PR-706](https://github.com/reductstore/reductstore/pull/706)
- RS-583: Remove contents of folder before remove it, [PR-711](https://github.com/reductstore/reductstore/pull/711)

## [1.13.2] - 2025-01-15

### Fixed

- RS-577: Fix parsing of nested conditions, [PR-704](https://github.com/reductstore/reductstore/pull/704)

## [1.13.1] - 2024-12-16

### Changed

- Update Web Console up to v1.8.1 with HARD quota

## [1.13.0] - 2024-12-04

### Added

- RS-439: Snap hooks for new configuration parameters, [PR-628](https://github.com/reductstore/reductstore/pull/628)
- RS-415: Check to prevent replication to the same bucket, [PR-629](https://github.com/reductstore/reductstore/pull/629)
- RS-527: POST /:bucket/:entry/q endpoint to query with JSON request, [PR-635](https://github.com/reductstore/reductstore/pull/635)
- RS-528: Conditional query engine, [PR-640](https://github.com/reductstore/reductstore/pull/640)
- RS-524: Check naming convention before creating or renaming entry, [PR-650](https://github.com/reductstore/reductstore/pull/650)
- RS-545: Implement logical operators in conditional query, [PR-656](https://github.com/reductstore/reductstore/pull/656)
- RS-529: Implement comparison operators in conditional query, [PR-662](https://github.com/reductstore/reductstore/pull/662)
- RS-538: Add strict mode for conditional query, [PR-663](https://github.com/reductstore/reductstore/pull/663)

### Changed

- Update Web Console up to v1.8.0, [PR-655](https://github.com/reductstore/reductstore/pull/655)


### Fixed

- RS-544: Fix keeping quota error, [PR-654](https://github.com/reductstore/reductstore/pull/654)
- Fix replication crash if bucket is removed, [PR-664](https://github.com/reductstore/reductstore/pull/664)
- Fix order of condition operands in object syntax, [PR-670](https://github.com/reductstore/reductstore/pull/670)

### Internal

- Fix CI actions after Ubuntu update, [PR-604](https://github.com/reductstore/reductstore/pull/604)
- RS-536: Update README.md, [PR-649](https://github.com/reductstore/reductstore/pull/649)
- RS-193: Cross-compilation in CI/CD, [PR-651](https://github.com/reductstore/reductstore/pull/651)


## [1.12.4] - 2024-11-20

### Fixed

- RS-539: Fix transaction log file descriptor, [PR-643](https://github.com/reductstore/reductstore/pull/643)
- RS-540: Fix synchronization of a new block, [PR-652](https://github.com/reductstore/reductstore/pull/652)

## [1.12.3] - 2024-10-26

### Fixed

- RS-519: Check bucket name convention when renaming bucket, [PR-616](https://github.com/reductstore/reductstore/pull/616)
- RS-520: Check if bucket provisioned before renaming it, [PR-617](https://github.com/reductstore/reductstore/pull/617)
- RS-521: Sync bucket and entry before renaming them, [PR-521](https://github.com/reductstore/reductstore/pull/618)
- RS-525: Recovering from empty block index, [PR-620](https://github.com/reductstore/reductstore/pull/620)
- RS-523: Fix EOF error when writing and removing data in parallel, [PR-621](https://github.com/reductstore/reductstore/pull/621)

## [1.12.2] - 2024-10-21

### Fixed

- Bad file descriptor error in replication log, [PR-606](https://github.com/reductstore/reductstore/pull/606)
- Deadlock in graceful stop, [PR-607](https://github.com/reductstore/reductstore/pull/607)

## [1.12.1] - 2024-10-17

### Fixed

- Crash when querying removed entry, [PR-605](https://github.com/reductstore/reductstore/pull/605)

## [1.12.0] - 2024-10-04

### Added

- RS-418: Remove record API, [PR-560](https://github.com/reductstore/reductstore/pull/560)
- RS-389: Hard bucket quota, [PR-570](https://github.com/reductstore/reductstore/pull/570)
- RS-413: Block CRC to block index for integrity check, [PR-584](https://github.com/reductstore/reductstore/pull/584)
- RS-454: Check at least one query param to delete records, [PR-595](https://github.com/reductstore/reductstore/pull/595)
- RS-388: Rename entry API, [PR-596](https://github.com/reductstore/reductstore/pull/596)
- RS-419: Rename bucket API, [PR-597](https://github.com/reductstore/reductstore/pull/597)

### Changed

- RS-411: Refactor FileCache, [PR-551](https://github.com/reductstore/reductstore/pull/551)
- RS-412: Refactor BlockCache, [PR-556](https://github.com/reductstore/reductstore/pull/556)
- RS-380: Send uncompleted batch when query is timed out, [PR-558](https://github.com/reductstore/reductstore/pull/558)
- RS-422: Batch update records per block, [PR-559](https://github.com/reductstore/reductstore/pull/559)
- RS-448: Refactor multithreading in storage engine, [PR-573](https://github.com/reductstore/reductstore/pull/573)

### Fixed

- RS-446: Storage engine hanging during replication, [PR-564](https://github.com/reductstore/reductstore/pull/564)
- RS-468: Name of invalid batch header, [PR-586](https://github.com/reductstore/reductstore/pull/586)
- Server shutdown, [PR-557](https://github.com/reductstore/reductstore/pull/557)
- Internal 500 error after removing bucket or entry, [PR-565](https://github.com/reductstore/reductstore/pull/565)
- RS-479: Channel synchronization during write operation in HTTP API, [PR-594](https://github.com/reductstore/reductstore/pull/594)
- Deadlock in continuous query, [PR-598](https://github.com/reductstore/reductstore/pull/598)

### Security

- Bump quinn-proto from 0.11.6 to 0.11.8, [PR-577](https://github.com/reductstore/reductstore/pull/577)

### Internal

- Setup unit tests for main functions, [PR-552](https://github.com/reductstore/reductstore/pull/552)

## [1.11.2] - 2024-09-23

### Fixed

- RS-470: Fix WAL overwrite due to infrequent writing, [PR-576](https://github.com/reductstore/reductstore/pull/576)

## [1.11.1] - 2024-08-23

### Fixed

- RS-429: Fix check for existing late record, [PR-549](https://github.com/reductstore/reductstore/pull/549)
- Fix truncating of a recovered block from WAL, [PR-550](https://github.com/reductstore/reductstore/pull/550)

## [1.11.0] - 2024-08-19

### Added

- RS-31: Change labels via HTTP API, [PR-517](https://github.com/reductstore/reductstore/pull/517)
- RS-385: Add support for CORS configuration, [PR-523](https://github.com/reductstore/reductstore/pull/523)
- Buffers and timeouts for IO operations, [PR-524](https://github.com/reductstore/reductstore/pull/524)
- RS-394: Expose headers in CORS policy, [PR-530](https://github.com/reductstore/reductstore/pull/530)
- RS-359: Block index file and WAL, [PR-533](https://github.com/reductstore/reductstore/pull/533)

### Changed

- RS-333: Transfer the project to ReductSoftware UG, [PR-488](https://github.com/reductstore/reductstore/pull/488)
- Web Console v1.7.0

### Fixed

- RS-364: Mask replication token in logs, [PR-531](https://github.com/reductstore/reductstore/pull/531)
- URL without slash at the end for dest server of replication task, [PR-540](https://github.com/reductstore/reductstore/pull/540)

### Internal

- RS-232: Add replication test to CI, [PR-511](https://github.com/reductstore/reductstore/pull/511)
- RS-274: Add test coverage in CI, [PR-532](https://github.com/reductstore/reductstore/pull/532)

## [1.10.1] - 2024-07-15

### Fixed

- RS-366: Replication of a record larger than max. batch size, [PR-508](https://github.com/reductstore/reductstore/pull/508)

### Internal

- RS-231: Add migration test to CI, [PR-506](https://github.com/reductstore/reductstore/pull/506)

## [1.10.0] - 2024-06-11

### Added

- RS-261: support downsampling parameters `each_n` and `each_s` query
  options, [PR-465](https://github.com/reductstore/reductstore/pull/465)
- RS-311: support downsampling parameters `each_n` and `each_s` in replication
  tasks, [PR-480](https://github.com/reductstore/reductstore/pull/480)

### Removed

- RS-213: `reduct-cli` and dependency from `reduct-rs`, [PR-426](https://github.com/reductstore/reductstore/pull/426)
- Checking "stop before start" in query endpoint, [PR-466](https://github.com/reductstore/reductstore/pull/466)

### Security

- Bump `rustls` from 0.21.10 to 0.21.12, [PR-432](https://github.com/reductstore/reductstore/pull/432)

### Changed

- RS-262: Reduce number of open/close file operations, [PR-453](https://github.com/reductstore/reductstore/pull/453)
- RS-88: Batch records before replicating them, [PR-478](https://github.com/reductstore/reductstore/pull/478)
- Update web console up to 1.6.1, [PR-484](https://github.com/reductstore/reductstore/pull/484)

### Fixed

- Fix removing invalid blocks at start, [PR-454](https://github.com/reductstore/reductstore/pull/454)
- RS-300: mark initial token as provisioned, [PR-479](https://github.com/reductstore/reductstore/pull/479)

## [1.9.5] - 2024-04-08

### Fixed

- RS-241: Fix replication and transaction log
  initialization, [PR-431](https://github.com/reductstore/reductstore/pull/431)

### Changed

- RS-239: Override errored records if they have the same
  size, [PR-428](https://github.com/reductstore/reductstore/pull/428)

### Security

- Vulnerable to degradation of service with CONTINUATION
  Flood, [PR-430](https://github.com/reductstore/reductstore/pull/430)

## [1.9.4] - 2024-03-29

### Fixed

- RS-233: fix wrong order of timestamps in batched write
  request, [PR-421](https://github.com/reductstore/reductstore/pull/421)

## [1.9.3] - 2024-03-16

### Fixed

- RS-221: fix body draining in write a record and batch
  edpoints, [PR-418](https://github.com/reductstore/reductstore/pull/418)

## [1.9.2] - 2024-03-15

### Fixed

- RS-218: fix block migration, [PR-416](https://github.com/reductstore/reductstore/pull/416)

## [1.9.1] - 2024-03-09

### Fixed

- Fix build on ARM32, [PR-411](https://github.com/reductstore/reductstore/pull/411)

## [1.9.0] - 2024-03-08

### Added

- RS-156: Support for commercial license, [PR-400](https://github.com/reductstore/reductstore/pull/400)
- RS-187: Speed up startup time, [PR-402](https://github.com/reductstore/reductstore/pull/403/)

### Changed

- reduct-rs: Move `reduct-rs` to separated repository, [PR-395](https://github.com/reductstore/reductstore/pull/395)
- RS-95: Update `axum` up to 0.7, [PR-401](https://github.com/reductstore/reductstore/pull/401)
- Update Web Console to v1.5.0, [PR-406](https://github.com/reductstore/reductstore/pull/406)
- RS-198: improve error message for expired query id, [PR-408](https://github.com/reductstore/reductstore/pull/408)

### Fixed

- RS-195: check path and query before unwrap them in
  middleware, [PR-407](https://github.com/reductstore/reductstore/pull/407)

### Security

- Bump mio from 0.8.10 to 0.8.11, [PR-409](https://github.com/reductstore/reductstore/pull/409)

## [1.8.2] - 2024-02-10

### Fixed

- RS-162: Fix locked block by read/write operation, [PR-398](https://github.com/reductstore/reductstore/pull/398)
- RS-155: Wait for incomplete records during replication, [PR-399](https://github.com/reductstore/reductstore/pull/399)

## [1.8.1] - 2024-01-28

### Fixed

- Stuck last replication error, [PR-393](https://github.com/reductstore/reductstore/pull/393)

## [1.8.0] - 2024-01-24

### Added

- reductstore: Cargo feature `web-console` to build without Web
  Console, [PR-365](https://github.com/reductstore/reductstore/pull/365)
- reductstore: Web Console v1.4.0
- reduct-cli: `bucket` command to manage buckets, [PR-367](https://github.com/reductstore/reductstore/pull/367)
- reductstore: Data replication, [PR-377](https://github.com/reductstore/reductstore/pull/377)
- reductstore: CRUD API and diagnostics for replication, [PR-380](https://github.com/reductstore/reductstore/pull/380)
- reduct-rs: Implement replication API, [PR-391](https://github.com/reductstore/reductstore/pull/391)

### Fixed

- reductstore: Stop downloading Web Console at docs.rs
  build, [PR-366](https://github.com/reductstore/reductstore/pull/366)
- reductstore: Sync write tasks with HTTP API to avoid unfinished
  records, [PR-374](https://github.com/reductstore/reductstore/pull/374)
- reductstore: Re-create a transaction log of replication if it is
  broken, [PR-379](https://github.com/reductstore/reductstore/pull/379)
- reductstore: Status for unfinished records, [PR-381](https://github.com/reductstore/reductstore/pull/381)
- reductstore: Discard replication for any storage errors, [PR-382](https://github.com/reductstore/reductstore/pull/382)
- reductstore: CPU consumption for replication, [PR-383](https://github.com/reductstore/reductstore/pull/383)
- reductstore: GET /api/v1/replications/:replication_name empty
  diagnostics, [PR-384](https://github.com/reductstore/reductstore/pull/384)
- reductstore: Error counting in replication diagnostics, [PR-385](https://github.com/reductstore/reductstore/pull/385)
- reductstore: Discard transaction from replication log if remote bucket is
  available, [PR-386](https://github.com/reductstore/reductstore/pull/386)
- reductstore: Use only HTTP1 for replication engine, [PR-388](https://github.com/reductstore/reductstore/pull/388)
- reductstore: Mask access token for remote instance in
  replication, [PR-390](https://github.com/reductstore/reductstore/pull/390)

### Changed

- reductstore: Refactor read and write operation with tokio
  channels, [PR-370](https://github.com/reductstore/reductstore/pull/370)
- docs: update link to new website, [PR-375](https://github.com/reductstore/reductstore/pull/375)
- reductstore: Update Web Console to v1.4.1, [PR-389](https://github.com/reductstore/reductstore/pull/389)

### Removed

- docs: remove /docs with GitBook docs, [PR-376](https://github.com/reductstore/reductstore/pull/376)

## [1.7.3] - 2023-11-08

### Fixed

- reductstore: Fix entry size calculation, [PR-373](https://github.com/reductstore/reductstore/pull/373)

## [1.7.2] - 2023-11-01

### Fixed

- reduct-rs: Fix query URL in ReductClient, [PR-372](https://github.com/reductstore/reductstore/pull/372)

## [1.7.1] - 2023-10-29

### Fixed

- reductstore: Wrong size calculation if a block could not be
  removed, [PR-371](https://github.com/reductstore/reductstore/pull/371)

## [1.7.0] - 2023-10-06

### Added

- reduct-cli: `alias` and `server` commands, [PR-343](https://github.com/reductstore/reductstore/pull/343)
-

reduct-rs: `ReductClient.url`, `ReductClient.token`, `ReductCientBuilder.try_build` [PR-350](https://github.com/reductstore/reductstore/pull/350)

- reductstore: `healthcheck` to buildx.Dockerfile, [PR-350](https://github.com/reductstore/reductstore/pull/350)
- reductstore: provisioning with environment variables, [PR-352](https://github.com/reductstore/reductstore/pull/352)
- reductstore,reduct-rs: batched write API, [PR-355](https://github.com/reductstore/reductstore/pull/355)

### Changed

- reductstore: Update dependencies, min. rust v1.67.0, [PR-341](https://github.com/reductstore/reductstore/pull/341)
- reductstore: Use Web Console v1.3.0, [PR-345](https://github.com/reductstore/reductstore/pull/342)
- reductstore: Move some Python API tests in Rust part, [PR-351](https://github.com/reductstore/reductstore/pull/351)
- reduct-base: Rename `HttpError` -> `ReductError`, [PR-350](https://github.com/reductstore/reductstore/pull/350)
- docs: update Getting Started, [PR-358](https://github.com/reductstore/reductstore/pull/358)

### Fixed

- reduct-rs: Normalize instance URL
  in `ClientBuilder.url`, [PR-343](https://github.com/reductstore/reductstore/pull/343)

## [1.6.2] - 2023-09-20

### Fixed

- reductstore: Panic for a bad time interval
  in `GET /b/:bucket/:entry/q`, [PR-357](https://github.com/reductstore/reductstore/pull/357)

## [1.6.1] - 2023-08-28

### Fixed

- reductstore: README and LICENSE in reductstore crate, [PR-347](https://github.com/reductstore/reductstore/pull/347)

### Security

- reductstore: Update `rustls` with
  patched `rustls-webpki`, [PR-349](https://github.com/reductstore/reductstore/pull/349)

## [1.6.0] - 2023-08-14

### Added

- reductstore: Build docker image for ARM32 platform, [PR-328](https://github.com/reductstore/reductstore/pull/328)
- reductstore,reduct-rs: Removing entries from bucket, [PR-334](https://github.com/reductstore/reductstore/pull/334)
- reductstore,reduct-rs: Limit parameter in query request, [PR-335](https://github.com/reductstore/reductstore/pull/335)
- reduct-rs: Server API for Client SDK, [PR-321](https://github.com/reductstore/reductstore/pull/321)
- reduct-rs: Token API for Client SDK, [PR-322](https://github.com/reductstore/reductstore/pull/322)
- reduct-rs: Bucket API for Client SDK, [PR-323](https://github.com/reductstore/reductstore/pull/323)
- reduct-rs: Entry API for Client SDK, [PR-326](https://github.com/reductstore/reductstore/pull/326)
- reduct-rs: Examples and docs, [PR-333](https://github.com/reductstore/reductstore/pull/333)

### Changed

- reductstore: Refactor `http_frontend` module, [PR-306](https://github.com/reductstore/reductstore/pull/306)
- reductstore: Cache last block to reduce read operations, [PR-318](https://github.com/reductstore/reductstore/pull/318)
- reductstore: Grained HTTP components, [PR-319](https://github.com/reductstore/reductstore/pull/319)
- reductstore: Default maximum records in block 256, [PR-320](https://github.com/reductstore/reductstore/pull/320)
- reductstore: BUSL-1.1 license, [PR-337](https://github.com/reductstore/reductstore/pull/337)
- reductstore: Update README.md, [PR-338](https://github.com/reductstore/reductstore/pull/338)
- all: Organize workspaces, [PR-310](https://github.com/reductstore/reductstore/pull/310)

### Removed

- reductstore: `native-tls` dependency (only `rustls`), [PR-315](https://github.com/reductstore/reductstore/pull/315)

### Fixed

- reductstore: Partial bucket settings, [PR-325](https://github.com/reductstore/reductstore/pull/325)

## [1.5.1] - 2023-07-17

### Fixed

- Handle empty or broken block descriptor, [PR-317](https://github.com/reductstore/reductstore/pull/317)

## [1.5.0] - 2023-06-30

### Added

- `x-reduct-api` header to get quick version number in Major.Minor
  format, [PR-291](https://github.com/reductstore/reductstore/pull/291)
- `GET /api/v1/:bucket/:entry/batch` endpoint to read a bunch of
  records, [PR-294](https://github.com/reductstore/reductstore/pull/294)
- `HEAD /api/v1/b/:bucket_name/:entry_name` and `HEAD /api/v1/b/:bucket_name/:entry_name/batch`
  endpoints, [PR-296]https://github.com/reductstore/reductstore/pull/296)

### Changed

- Concise format for headers in `GET /api/v1/:bucket/:entry/batch`
  response, [PR-298](https://github.com/reductstore/reductstore/pull/298)

## [1.4.1] - 2023-06-27

### Fixed

- Stuck empty entries, [PR-302](https://github.com/reductstore/reductstore/pull/302)

## [1.4.0] - 2023-06-09

### Fixed

- Panic when an invalid utf-8 received as a label value, [PR-290](https://github.com/reductstore/reductstore/pull/290)
- Writing record for clients which don't support for Expect
  header, [PR-293](https://github.com/reductstore/reductstore/pull/293)

## [1.4.0-beta.1] - 2023-06-03

### Changed

- Update web console to 1.2.2, [PR-287](https://github.com/reductstore/reductstore/pull/287)

### Fixed

- Parsing non-string quota type, [PR-286](https://github.com/reductstore/reductstore/pull/286)
- Show file paths in logs without registry path, [PR-288](https://github.com/reductstore/reductstore/pull/288)

## [1.4.0-alpha.3] - 2023-05-29

### Fixed

- Return `init-token` to token list, [PR-280](https://github.com/reductstore/reductstore/pull/280)
- First query ID is 1, [PR-281](https://github.com/reductstore/reductstore/pull/281)
- Showing permissions in `GET /api/v1/tokens`, [PR-282](https://github.com/reductstore/reductstore/pull/282)
- Remove removed bucket from token permissions, [PR-283](https://github.com/reductstore/reductstore/pull/283)
- Build on Windows and macOS, [PR-284](https://github.com/reductstore/reductstore/pull/284)

## [1.4.0-alpha.2] - 2023-05-26

### Fixed

- Cargo package, [PR-278](https://github.com/reductstore/reductstore/pull/278)

## [1.4.0-alpha.1] - 2023-05-22

### Added

- Continuous query `GET /api/v1/:bucket/:entry/q?continuous=true|false`,
  [PR-248](https://github.com/reductstore/reductstore/pull/248)
- Build ARM64 Docker image
- Integration of Rust, [PR-251](https://github.com/reductstore/reductstore/pull/251)
- Print build commit and date in logs, [PR-271](https://github.com/reductstore/reductstore/pull/271)
- Re-build ARM64 Docker image for Rust, [PR-274](https://github.com/reductstore/reductstore/pull/274)
- Publish crate to crates.io, [PR-275](https://github.com/reductstore/reductstore/pull/275)

### Changed

- New public Docker repository `reduct/store`, [PR-246](https://github.com/reductstore/reductstore/pull/246)
- Speed up loading entries at start, [PR-250](https://github.com/reductstore/reductstore/pull/250)
- Rewrite static asset management in Rust, [PR-252](https://github.com/reductstore/reductstore/pull/252)
- Rewrite token authentication module in Rust, [PR-255](https://github.com/reductstore/reductstore/pull/255)
- Rewrite storage module in Rust, [PR-257](https://github.com/reductstore/reductstore/pull/257)
- Rewrite HTTP layer in Rust, [PR-259](https://github.com/reductstore/reductstore/pull/259)

### Removed

- Disable Windows and Macos builds because of migration on
  Rust, [PR-251](https://github.com/reductstore/reductstore/pull/251)

### Fixed

- GET /api/v1/me endpoint for disabled authentication, [PR-245](https://github.com/reductstore/reductstore/pull/245)
- Error handling when a client closes a connection, [PR-263](https://github.com/reductstore/reductstore/pull/263)
- Allow null quota type in bucket settings, [PR-264](https://github.com/reductstore/reductstore/pull/264)
- Writing belated data, [PR-265](https://github.com/reductstore/reductstore/pull/265)
- Fix searching record by timestamp, [PR-266](https://github.com/reductstore/reductstore/pull/266)
- Graceful shutdown, [PR-267](https://github.com/reductstore/reductstore/pull/267)
- Access to a block descriptor from several threads, [PR-268](https://github.com/reductstore/reductstore/pull/268)
- Handling unix SIGTERM signal, [PR-269](https://github.com/reductstore/reductstore/pull/269)
- Encoding non-text assets of Web Console, [PR-270](https://github.com/reductstore/reductstore/pull/270)
- Pass hash commit into docker image, [PR-272](https://github.com/reductstore/reductstore/pull/272)
- Build snap package in CI, [PR-273](https://github.com/reductstore/reductstore/pull/273)

## [1.3.2] - 2023-03-10

### Added

- Build and publish snap, [PR-241](https://github.com/reductstore/reductstore/pull/241)

### Changed

- Fetch Web Console from cmake, [PR-239](https://github.com/reductstore/reductstore/pull/239)
- Install snap as a daemon, [PR-240](https://github.com/reductstore/reductstore/pull/240)

### Fixed

- Begin time 0 is valid for a block, [PR-242](https://github.com/reductstore/reductstore/pull/242)

## [1.3.1] - 2023-02-03

### Fixed

- Querying when a block doesn't have records for certain
  labels, [PR-235](https://github.com/reductstore/reductstore/pull/235)

## [1.3.0] - 2023-01-26

### Added

- Labels for  `POST|GET /api/v1/:bucket/:entry` as headers with
  prefix `x-reduct-label-`, [PR-224](https://github.com/reductstore/reductstore/pull/224)
- `include-<label>` and `exclude-<label>` query parameters for query endpoint
  `GET /api/v1/:bucket/:entry/q`, [PR-226](https://github.com/reductstore/reductstore/pull/226)
- Store the `Content-Type` header received for a record while writing it, so that the record may be returned with the
  same header, [PR-231](https://github.com/reductstore/reductstore/pull/231)

### Changed

- Project license AGPLv3 to MPL-2.0, [PR-221](https://github.com/reductstore/reductstore/pull/221)
- Rename error header `-x-reduct-error`
  to `x-reduct-error`, [PR-230](https://github.com/reductstore/reductstore/pull/230)
- Update Web Console to v1.2.0, [PR-232](https://github.com/reductstore/reductstore/pull/232)

## [1.2.3] - 2023-01-02

### Fixed

- Crashing when post request is aborted by client, [PR-223](https://github.com/reductstore/reductstore/pull/223)

## [1.2.2] - 2022-12-20

### Fixed

- Token validation for anonymous access, [PR-217](https://github.com/reductstore/reductstore/pull/217)

## [1.2.1] - 2022-12-19

### Fixed

- Docker image command

## [1.2.0] - 2022-12-18

### Added:

- `GET /api/v1/me` endpoint to get current
  permissions, [PR-202](https://github.com/reductstore/reductstore/pull/208)
- Send error message in `-x-reduct-error`header [PR-213](https://github.com/reductstore/reductstore/pull/213)

### Changed:

- Consistent token and bucket management, [PR-208](https://github.com/reductstore/reductstore/pull/208)
- Rebranding: update project name, CMakeTargets and docs, [PR-215](https://github.com/reductstore/reductstore/pull/215)

### Fixed:

- HTTP statuses for `GET /api/v1/:bucket/:entry/q` and `POST /api/v1/:bucket/:entry`
  , [PR-212](https://github.com/reductstore/reductstore/pull/212)

## [1.1.1] - 2022-12-08

### Fixed:

- A crush when we handle input chunks after an HTTP
  error, [PR-206](https://github.com/reductstore/reductstore/pull/206)

## [1.1.0] - 2022-11-27

### Added:

- Implement Token API, [PR-199](https://github.com/reductstore/reductstore/pull/199)

### Fixed:

- Link to Entry API in documentation, [PR-194](https://github.com/reductstore/reductstore/pull/194)
- No error body for HEAD `/b/:bucket_name`, [PR-196](https://github.com/reductstore/reductstore/pull/196)
- Use `GET /tokens` instead of `/tokens/list`, [PR-200](https://github.com/reductstore/reductstore/pull/200)

### Changed:

- Always override init-token, [PR-201](https://github.com/reductstore/reductstore/pull/201)
- Update Web Console to v1.1.0, [PR-204](https://github.com/reductstore/reductstore/pull/204)

## [1.0.1] - 2022-10-09

### Added:

- Print 404 error to logs in debug mode, [PR-187](https://github.com/reductstore/reductstore/pull/187)

### Fixed:

- Build MacOs in pipeline, [PR-188](https://github.com/reductstore/reductstore/pull/188)
- Parsing endpoint url to print it in logs, [PR-190](https://github.com/reductstore/reductstore/pull/190)
- Handling negative timestamps, [PR-191](https://github.com/reductstore/reductstore/pull/191)

## [1.0.0] - 2022-10-03

### Added:

- Web Console v1.0.0. [PR-184](https://github.com/reductstore/reductstore/pull/184)

### Removed:

- GET `/bucketname/entryname/list` endpoint, [PR-164](https://github.com/reductstore/reductstore/pull/164)
- POST `/auth/refresh` endpoint, [PR-177](https://github.com/reductstore/reductstore/pull/177)

### Changed:

- Refactor HTTP API layer, [PR-179](https://github.com/reductstore/reductstore/pull/179)
- Prefix `/api/v1/` for all endpoints, [PR-182](https://github.com/reductstore/reductstore/pull/182)

### Fixed:

- Segfault during overriding a record, [PR-183](https://github.com/reductstore/reductstore/pull/183)
- Access to Web Console when authentication is
  enabled, [PR-185](https://github.com/reductstore/reductstore/pull/185)

### Security:

- Check bucket and entry name with regex, [PR-181](https://github.com/reductstore/reductstore/pull/181)

## [0.9.0] - 2022-09-18

### Added:

- Build a static executable for AMD64 and upload it to release from
  CI, [PR-171](https://github.com/reductstore/reductstore/pull/171)
- Build on MacOS, [PR-173](https://github.com/reductstore/reductstore/pull/173)
- Build on Windows, [PR-174](https://github.com/reductstore/reductstore/pull/174)

### Changed:

- Web Console v0.5.0, [PR-175](https://github.com/reductstore/reductstore/pull/175)

## [0.8.0] - 2022-08-26

### Added:

- Web Console v0.4.0
- `Connection` header, [PR-154](https://github.com/reductstore/reductstore/pull/154)
- Publish image to DockerHub, [PR-162](https://github.com/reductstore/reductstore/pull/162)
- Use API token as an access token, [PR-167](https://github.com/reductstore/reductstore/pull/167)

### Fixed:

- Ignoring error code after a failed bucket update, [PR-161](https://github.com/reductstore/reductstore/pull/161)
- Infinite loop in Bucket::KeepQuota, [PR-146](https://github.com/reductstore/reductstore/pull/146)
- Waiting data from HTTP client if it aborts
  connection, [PR-151](https://github.com/reductstore/reductstore/pull/151)
- Writing record when current block is broken, [PR-15-](https://github.com/reductstore/reductstore/pull/153)
- Closing uSocket, [PR-154](https://github.com/reductstore/reductstore/pull/154)
- Removing broken block when it keeps quota, [PR-155](https://github.com/reductstore/reductstore/pull/155)
- Sending headers twice, [PR-156](https://github.com/reductstore/reductstore/pull/156)
- Direction to `cd` into the `build/` directory while building the server
  locally, [PR-159](https://github.com/reductstore/reductstore/pull/159)

### Changed:

- Duplication of timestamps is not allowed, [PR-147](https://github.com/reductstore/reductstore/pull/147)
- Update dependencies, [PR-163](https://github.com/reductstore/reductstore/pull/163)

### Deprecated:

- GET `/auth/refersh` endpoint and access token, [PR-167](https://github.com/reductstore/reductstore/pull/167)

## [0.7.1] - 2022-07-30

### Fixed:

- Opening Web Console, [PR-143](https://github.com/reductstore/reductstore/pull/143)

## [0.7.0] - 2022-07-29

### Added:

- Web Console v0.3.0, [PR-133](https://github.com/reductstore/reductstore/pull/133)
- GET `/b/:bucket/:entry/q?` endpoint for iterating
  data, [PR-141](https://github.com/reductstore/reductstore/pull/141)

### Changed:

- Use `Keep a log` format for CHANGELOG, [PR-136](https://github.com/reductstore/reductstore/pull/136)
- SI for max block and read chunk sizes, [PR-137](https://github.com/reductstore/reductstore/pull/137)
- SHA256 hash for API token is optional, [PR-139](https://github.com/reductstore/reductstore/pull/139)

### Fixed:

- Typo in API documentation, [PR-124](https://github.com/reductstore/reductstore/pull/124)
- Style in documentation, [PR-129](https://github.com/reductstore/reductstore/pull/129)

## [0.6.1] - 2022-06-25

### Added:

- Use Web Console v0.2.1, [PR-120](https://github.com/reductstore/reductstore/pull/120)

## [0.6.0] - 2022-06-25

### Added:

- `Content-Type` header to responses, [PR-107](https://github.com/reductstore/reductstore/pull/107)
- `max_block_records` to bucket settings, [PR-108](https://github.com/reductstore/reductstore/pull/108)
- HEAD `/alive` method for health check, [PR-114](https://github.com/reductstore/reductstore/pull/114)

### Changed:

- Filter unfinished records in GET /b/:bucket/:entry/list
  endpoint, [PR-106](https://github.com/reductstore/reductstore/pull/106)

### Fixed:

- Web Console for RS_API_BASE_PATH, [PR-92](https://github.com/reductstore/reductstore/pull/92)
- Wasting disk space in XFS filesystem, [PR-100](https://github.com/reductstore/reductstore/pull/100)
- Base path in server url, [PR-105](https://github.com/reductstore/reductstore/pull/105)
- Updating record state in asynchronous write
  operation, [PR-109](https://github.com/reductstore/reductstore/pull/109)
- SEGFAULT when entry removed but async writer is
  alive, [PR-110](https://github.com/reductstore/reductstore/pull/110)
- Removing a block with active readers or
  writers, [PR-111](https://github.com/reductstore/reductstore/pull/111)
- Loading bucket settings from disk, [PR-112](https://github.com/reductstore/reductstore/pull/112)
- 404 error for react routes, [PR-116](https://github.com/reductstore/reductstore/pull/116)
- No token error message, [PR-118](https://github.com/reductstore/reductstore/pull/118)
- Updating bucket settings, [PR-119](https://github.com/reductstore/reductstore/pull/119)
- Benchmarks and refactor block management [PR-99](https://github.com/reductstore/reductstore/pull/99)
- CURL to deploy image [PR-104](https://github.com/reductstore/reductstore/pull/104)

### Changed:

- Optimise write operation, [PR-96](https://github.com/reductstore/reductstore/pull/96)
- Disable SSL verification in API tests, [PR-113](https://github.com/reductstore/reductstore/pull/113)

## [0.5.1] - 2022-05-24

### Fixed:

- GET `/b/:bucket/:entry` to avoid creating an empty
  entry, [PR-95](https://github.com/reductstore/reductstore/pull/95)
- Update of bucket settings, [PR-138](https://github.com/reductstore/reductstore/pull/138)

## [0.5.0] - 2022-05-15

### Added:

- Web Console, [PR-77](https://github.com/reductstore/reductstore/pull/77)
- Add default settings for a new bucket in GET /info, [PR-87](https://github.com/reductstore/reductstore/pull/87)
- Link to JS SDK to documentation, [PR-88](https://github.com/reductstore/reductstore/pull/88)

### Changed:

- Only HTTP errors 50x in the logs, [PR-84](https://github.com/reductstore/reductstore/issues/84)

### Fixed:

- CORS functionality, [PR-72](https://github.com/reductstore/reductstore/pull/72)
- Quota policy, [PR-83](https://github.com/reductstore/reductstore/pull/83)

## [0.4.3] - 2022-05-01

### Fixed:

- Sending big blobs [PR-80](https://github.com/reductstore/reductstore/pull/80)
- Handling offset in tryEnd [PR-81](https://github.com/reductstore/reductstore/pull/81)

## [0.4.2] - 2022-04-30

### Fixed:

- Deadlock during sending data, [PR-78](https://github.com/reductstore/reductstore/pull/78)

## [0.4.1] - 2022-04-04

### Fixed:

- Timestamp for oldest record, [PR-68](https://github.com/reductstore/reductstore/pull/68)

## [0.4.0] - 2022-04-01

### Added:

- Asynchronous write/read operations with data blocks, [PR-62](https://github.com/reductstore/reductstore/pull/62)

### Fixed:

- Searching start block in Entry List request, [PR-61](https://github.com/reductstore/reductstore/pull/61)
- Aborting GET requests, [PR-64](https://github.com/reductstore/reductstore/pull/64)

### Changed:

- Block structure in entry, [PR-58](https://github.com/reductstore/reductstore/pull/58)

## [0.3.0]  - 2022-03-14

### Added

- Secure HTTP, [PR-49](https://github.com/reductstore/reductstore/pull/49)
- Stats and list entries to GET /b/:bucket method with
  , [PR-51](https://github.com/reductstore/reductstore/pull/51)
- Access to the latest record, [PR-53](https://github.com/reductstore/reductstore/pull/53)

### Fixed:

- Sending two responses for HTTP error, [PR-48](https://github.com/reductstore/reductstore/pull/48)

### Changed:

- Replace nholmann/json with Protobuf, [PR-47](https://github.com/reductstore/reductstore/pull/47)

## [0.2.1] - 2022-03-07

### Fixed:

* Crushing when API token is wrong, [PR-42](https://github.com/reductstore/reductstore/pull/42)
* Order of authentication checks, [PR-43](https://github.com/reductstore/reductstore/pull/43)

## [0.2.0] - 2022-02-26

### Added:

- HEAD method to Bucket API, [PR-30](https://github.com/reductstore/reductstore/pull/30)
- Extends information from GET method of Server API, [PR-33](https://github.com/reductstore/reductstore/pull/33)
- GET /list end point to browse buckets, [PR-34](https://github.com/reductstore/reductstore/pull/34)
- Bearer token authentication, [PR-36](https://github.com/reductstore/reductstore/pull/36)

### Changed:

- PUT method of Bucket API has optional parameters, [PR-32](https://github.com/reductstore/reductstore/pull/32)

### Fixed:

- Docker build on ARM32, [PR-29](https://github.com/reductstore/reductstore/pull/29)
- IBucket::List error 500 for timestamps between
  blocks, [PR-31](https://github.com/reductstore/reductstore/pull/31)
- Wrong parameters in Entry API documentation, [PR-38](https://github.com/reductstore/reductstore/pull/38)

## [0.1.1] - 2022-02-13

### Fixed:

- Default folder for data in Docker image, [PR-23](https://github.com/reductstore/reductstore/pull/23)

## [0.1.0] - 2022-01-24

- Initial release with basic HTTP API and FIFO bucket quota


[Unreleased]: https://github.com/reductstore/reductstore/compare/v1.15.2...HEAD

[1.15.2]: https://github.com/reductstore/reductstore/compare/v1.15.1...v1.15.2

[1.15.1]: https://github.com/reductstore/reductstore/compare/v1.15.0...v1.15.1

[1.15.0]: https://github.com/reductstore/reductstore/compare/v1.14.8...v1.15.0

[1.14.8]: https://github.com/reductstore/reductstore/compare/v1.14.7...v1.14.8

[1.14.7]: https://github.com/reductstore/reductstore/compare/v1.14.6...v1.14.7

[1.14.6]: https://github.com/reductstore/reductstore/compare/v1.14.5...v1.14.6

[1.14.5]: https://github.com/reductstore/reductstore/compare/v1.14.4...v1.14.5

[1.14.4]: https://github.com/reductstore/reductstore/compare/v1.14.3...v1.14.4

[1.14.3]: https://github.com/reductstore/reductstore/compare/v1.14.2...v1.14.3

[1.14.2]: https://github.com/reductstore/reductstore/compare/v1.14.1...v1.14.2

[1.14.1]: https://github.com/reductstore/reductstore/compare/v1.14.0...v1.14.1

[1.14.0]: https://github.com/reductstore/reductstore/compare/v1.13.5...v1.14.0

[1.13.5]: https://github.com/reductstore/reductstore/compare/v1.13.4...v1.13.5

[1.13.4]: https://github.com/reductstore/reductstore/compare/v1.13.3...v1.13.4

[1.13.3]: https://github.com/reductstore/reductstore/compare/v1.13.2...v1.13.3

[1.13.2]: https://github.com/reductstore/reductstore/compare/v1.13.1...v1.13.2

[1.13.1]: https://github.com/reductstore/reductstore/compare/v1.13.0...v1.13.1

[1.13.0]: https://github.com/reductstore/reductstore/compare/v1.12.4...v1.13.0

[1.12.4]: https://github.com/reductstore/reductstore/compare/v1.12.3...v1.12.4

[1.12.3]: https://github.com/reductstore/reductstore/compare/v1.12.2...v1.12.3

[1.12.2]: https://github.com/reductstore/reductstore/compare/v1.12.1...v1.12.2

[1.12.1]: https://github.com/reductstore/reductstore/compare/v1.12.0...v1.12.1

[1.12.0]: https://github.com/reductstore/reductstore/compare/v1.11.1...v1.12.0

[1.11.2]: https://github.com/reductstore/reductstore/compare/v1.11.1...v1.11.2

[1.11.1]: https://github.com/reductstore/reductstore/compare/v1.11.0...v1.11.1

[1.11.0]: https://github.com/reductstore/reductstore/compare/v1.10.1...v1.11.0

[1.10.1]: https://github.com/reductstore/reductstore/compare/v1.10.0...v1.10.1

[1.10.0]: https://github.com/reductstore/reductstore/compare/v1.9.5...v1.10.0

[1.9.5]: https://github.com/reductstore/reductstore/compare/v1.9.4...v1.9.5

[1.9.4]: https://github.com/reductstore/reductstore/compare/v1.9.3...v1.9.4

[1.9.3]: https://github.com/reductstore/reductstore/compare/v1.9.2...v1.9.3

[1.9.2]: https://github.com/reductstore/reductstore/compare/v1.9.1...v1.9.2

[1.9.1]: https://github.com/reductstore/reductstore/compare/v1.9.0...v1.9.1

[1.9.0]: https://github.com/reductstore/reductstore/compare/v1.8.2...v1.9.0

[1.8.2]: https://github.com/reductstore/reductstore/compare/v1.8.1...v1.8.2

[1.8.1]: https://github.com/reductstore/reductstore/compare/v1.8.0...v1.8.1

[1.8.0]: https://github.com/reductstore/reductstore/compare/v1.7.3...v1.8.0

[1.7.3]: https://github.com/reductstore/reductstore/compare/v1.7.2...v1.7.3

[1.7.2]: https://github.com/reductstore/reductstore/compare/v1.7.1...v1.7.2

[1.7.1]: https://github.com/reductstore/reductstore/compare/v1.7.0...v1.7.1

[1.7.0]: https://github.com/reductstore/reductstore/compare/v1.6.2...v1.7.0

[1.6.2]: https://github.com/reductstore/reductstore/compare/v1.6.1...v1.6.2

[1.6.1]: https://github.com/reductstore/reductstore/compare/v1.6.0...v1.6.1

[1.6.0]: https://github.com/reductstore/reductstore/compare/v1.5.1...v1.6.0

[1.5.1]: https://github.com/reductstore/reductstore/compare/v1.5.0...v1.5.1

[1.5.0]: https://github.com/reductstore/reductstore/compare/v1.4.1...v1.5.0

[1.4.1]: https://github.com/reductstore/reductstore/compare/v1.4.0...v1.4.1

[1.4.0]: https://github.com/reductstore/reductstore/compare/v1.4.0-beta.1...v1.4.0

[1.4.0-beta.1]: https://github.com/reductstore/reductstore/compare/v1.4.0-alpha.2...v1.4.0-beta.1

[1.4.0-alpha.2]: https://github.com/reductstore/reductstore/compare/v1.4.0-alpha.1...v1.4.0-alpha.2

[1.4.0-alpha.1]: https://github.com/reductstore/reductstore/compare/v1.3.2...v1.4.0-alpha.1

[1.3.2]: https://github.com/reductstore/reductstore/compare/v1.3.1...v1.3.2

[1.3.1]: https://github.com/reductstore/reductstore/compare/v1.3.0...v1.3.1

[1.3.0]: https://github.com/reductstore/reductstore/compare/v1.2.3...v1.3.0

[1.2.3]: https://github.com/reductstore/reductstore/compare/v1.2.2...v1.2.3

[1.2.2]: https://github.com/reductstore/reductstore/compare/v1.2.1...v1.2.2

[1.2.1]: https://github.com/reductstore/reductstore/compare/v1.2.0...v1.2.1

[1.2.0]: https://github.com/reductstore/reductstore/compare/v1.1.1...v1.2.0

[1.1.1]: https://github.com/reductstore/reductstore/compare/v1.1.0...v1.1.1

[1.1.0]: https://github.com/reductstore/reductstore/compare/v1.0.0...v1.1.0

[1.0.1]: https://github.com/reductstore/reductstore/compare/v1.0.0...v1.0.1

[1.0.0]: https://github.com/reductstore/reductstore/compare/v0.9.0...v1.0.0

[0.9.0]: https://github.com/reductstore/reductstore/compare/v0.8.0...v0.9.0

[0.8.0]: https://github.com/reductstore/reductstore/compare/v0.7.1...v0.8.0

[0.7.1]: https://github.com/reductstore/reductstore/compare/v0.7.0...v0.7.1

[0.7.0]: https://github.com/reductstore/reductstore/compare/v0.6.1...v0.7.0

[0.6.1]: https://github.com/reductstore/reductstore/compare/v0.6.0...v0.6.1

[0.6.0]: https://github.com/reductstore/reductstore/compare/v0.5.1...v0.6.0

[0.5.1]: https://github.com/reductstore/reductstore/compare/v0.5.0...v0.5.1

[0.5.0]: https://github.com/reductstore/reductstore/compare/v0.4.3...v0.5.0

[0.4.3]: https://github.com/reductstore/reductstore/compare/v0.4.2...v0.4.3

[0.4.2]: https://github.com/reductstore/reductstore/compare/v0.4.1...v0.4.2

[0.4.1]: https://github.com/reductstore/reductstore/compare/v0.4.0...v0.4.1

[0.4.0]: https://github.com/reductstore/reductstore/compare/v0.3.0...v0.4.0

[0.3.0]: https://github.com/reductstore/reductstore/compare/v0.2.1...v0.3.0

[0.2.1]: https://github.com/reductstore/reductstore/compare/v0.2.0...v0.2.1

[0.2.0]: https://github.com/reductstore/reductstore/compare/v0.1.1...v0.2.0

[0.1.1]: https://github.com/reductstore/reductstore/compare/v0.1.0...v0.1.1

[0.1.0]: https://github.com/reductstore/reductstore/releases/tag/v0.1.0
