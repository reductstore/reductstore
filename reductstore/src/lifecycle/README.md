# Lifecycle Module

This module implements background lifecycle policies for buckets.

Supported actions:
- `delete`: remove records older than `older_than`
- `compress`: recompress eligible blocks older than `older_than`

The module is built around three layers:
- repository: stores lifecycle definitions, validates settings, and starts/stops workers
- task: runs one lifecycle policy on a schedule and writes system events
- action: executes the policy against storage and reports progress/statistics

## Files

- `action.rs`: shared action interface, action context, and action result type
- `action/delete.rs`: delete lifecycle implementation
- `action/compress.rs`: compress lifecycle implementation
- `action/progress.rs`: shared progress-window calculation and progress loading from system events
- `lifecycle_task.rs`: worker loop for one lifecycle policy
- `lifecycle_repository.rs`: builder which selects normal or read-only repository
- `lifecycle_repository/repo.rs`: mutable repository implementation persisted in `.lifecycles`
- `lifecycle_repository/read_only.rs`: replica-safe no-op repository
- `system_event_payload.rs`: lifecycle system event payload builder

## Design

The repository owns all configured lifecycle policies.

Each policy becomes a `LifecycleTask` with:
- immutable policy settings for bucket, entries, interval, and `older_than`
- a concrete action implementation selected by lifecycle type
- a shared storage handle
- optional system-event sink

The task wakes up on its configured interval, executes the action, and emits a lifecycle system event.

For primary nodes, lifecycle definitions are loaded from and saved to `.lifecycles` in the data directory.
For replica nodes, the builder returns a read-only repository and no workers are started.

## Execution Flow

```text
                    +------------------------+
API / provisioning  | LifecycleRepository    |
------------------->| - validate settings    |
                    | - persist .lifecycles  |
                    | - create LifecycleTask |
                    +-----------+------------+
                                |
                                v
                    +------------------------+
                    | LifecycleTask          |
                    | - sleeps interval      |
                    | - runs action          |
                    | - logs system event    |
                    +-----------+------------+
                                |
                    +-----------+-----------+
                    |                       |
                    v                       v
          +------------------+    +------------------+
          | DeleteAction     |    | CompressAction   |
          | query/remove     |    | estimate/compress|
          +---------+--------+    +---------+--------+
                    |                       |
                    +-----------+-----------+
                                |
                                v
                    +------------------------+
                    | progress::processing_  |
                    | window()               |
                    | - oldest matching ts   |
                    | - latest matching ts   |
                    | - saved last progress  |
                    +-----------+------------+
                                |
                                v
                    +------------------------+
                    | StorageEngine / Bucket |
                    | / Entry operations     |
                    +------------------------+
```

## Progress Model

When system events are disabled, actions process the full eligible data range:
- `start = oldest matching record`
- `stop = min(latest matching record + 1, now - older_than + 1)`

When system events are enabled, lifecycle uses persisted progress from the latest lifecycle stats event:
- progress source: `$system/lifecycle/<instance>/<policy>`
- stored field: `last_processed_ts`

The shared window calculation works like this:
1. Find the oldest matching record across selected entries.
2. Find the latest matching record across selected entries.
3. Compute `effective_cutoff_stop = min(latest_matching_record + 1, now - older_than + 1)`.
4. Load `last_processed_ts`; if absent, start from the oldest matching record.
5. Advance by a bounded data window: `24 * interval`.

Effective range for one run:
- `start = oldest matching record`, clamped so `start <= stop`
- `stop = min(last_processed_ts + 24 * interval, effective_cutoff_stop)`

Important behavior:
- tasks always scan from the oldest matching record
- tasks do not short-circuit just because historical progress reached the current stop
- this allows old data to be re-processed if later mutations make it eligible again
  for example: label updates after delete rules, or decompressed blocks after compress rules
- `caught_up` means the current run reached the effective stop for the currently visible data

## Why Tasks Always Start From The Oldest Record

Delete and compress are not purely append-only operations.

Previously processed historical data can become eligible again later:
- a compressed block can be decompressed by a later write or label update path
- an old record can become removable after label changes affecting a `when` condition

Because of that, the lifecycle worker must always query from the oldest matching record instead of
from the last processed timestamp. The progress marker only limits how far forward the task needs
to advance in the current run.

## Task Loop Semantics

`LifecycleTask` runs an inner loop with one special case:
- if a run returns `affected_records == 0` and `caught_up == false`, the worker retries after 100 ms
- this lets the task continue advancing through empty windows until it reaches the current cutoff
- once `caught_up == true` or some records were affected, the worker returns to the normal interval

This behavior is important for sparse data where many windows may contain no eligible records.

## System Events

Each run produces a `lifecycle_run` system event when a sink is configured.

Success payload fields:
- `policy_name`
- `action_type`
- `bucket`
- `duration`
- `processed_records`
- `processed_blocks`
- `caught_up`
- `last_processed_ts` when available

Error payload fields:
- the same identity fields
- `processed_records = 0`
- `processed_blocks = 0`
- `caught_up = false`
- `error_code`
- `error_message`

The same event stream is also used as the source of persisted lifecycle progress.

## Repository Responsibilities

The repository is responsible for:
- validating lifecycle settings
- enforcing minimum `older_than` and interval limits
- persisting lifecycle definitions in `.lifecycles`
- starting and stopping tasks
- switching task mode at runtime
- exposing lifecycle info and settings to the API layer

## Action Responsibilities

Delete action:
- builds a query with `QueryType::Remove` or `QueryType::Query` for dry run
- returns affected record and block counts

Compress action:
- estimates or compresses eligible blocks in the computed time range
- returns affected block and record counts

Both actions:
- share the same progress-window logic
- operate only on the configured bucket and matching entries
- report `caught_up` when the run reaches the effective current stop

## Operational Summary

In short, the lifecycle module is a persistent scheduler around storage actions:
- repository decides what should run
- task decides when it should run
- action decides what data range should be processed now
- system events record what happened and also feed the next progress calculation
