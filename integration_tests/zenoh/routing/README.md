# Zenoh Routing Integration Tests

Two files cover the indexed routing matrix:

- `subscriber_test.py` - the write path (published samples are routed to buckets)
- `queryable_test.py` - the read path (queries are served from routed buckets)

Both run against a live server started with the block matrix in `env.sh`.

## Setup

**1. Start the Zenoh router**
```bash
docker run --init -d --name zenoh-router -p 7447:7447/tcp eclipse/zenoh
```

**2. Start ReductStore** (keep this terminal open while running tests)
```bash
source integration_tests/zenoh/routing/env.sh

RS_DATA_PATH=~/data-test RS_DISABLE_AUTH=true RS_ZENOH_ENABLED=1 \
  RS_ZENOH_CONFIG="mode=client;connect/endpoints=[tcp/127.0.0.1:7447]" \
  cargo run -p reductstore --features "fs-backend web-console zenoh-api"
```

**3. Install Python dependencies (once)**
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r integration_tests/zenoh/requirements.txt
```

**4. Set test environment variables (once per terminal)**
```bash
export STORAGE_URL=http://127.0.0.1:8383
export ZENOH_CONNECT=tcp/127.0.0.1:7447
export RS_ZENOH_ROUTING_TESTS=1
cd integration_tests/zenoh
```

## Running the tests

```bash
# write path: static routing, key-prefix create/existing/no-create, fallback, allowlist
pytest -v routing/subscriber_test.py

# read path: static queryable, multi-bucket queryable, no-create queryable
pytest -v routing/queryable_test.py

# a single behavior
pytest -v routing/subscriber_test.py -k fallback
```

## The routing blocks

Each block below maps a key pattern to a bucket. `env.sh` sets these for both the
subscriber (`RS_ZENOH_SUB_*`) and the matching queryable (`RS_ZENOH_QUERY_*`).

**Block 0 - static, bucket `zenoh`**
```
KEYEXPRS='entry_$*,factory/**'   # matches entry_camera, factory/line1/temp, ...
BUCKET=zenoh                      # the full key is the entry; the bucket is fixed
```

**Block 1 - key-prefix `rt_site_*`, creation enabled**
```
KEYEXPRS='rt_$*/**'              # first chunk → bucket, rest → entry name
ROUTING=key-prefix
BUCKET_ALLOWLIST='rt_site_*'     # only bucket names matching this glob are accepted
ALLOW_BUCKET_CREATION=true       # bucket is created on first write if missing
```
`rt_london/sensors/temp` → bucket `rt_london`, entry `sensors/temp`.

**Block 2 - key-prefix `existing_site_*`, creation disabled**
```
KEYEXPRS='existing_$*/**'
ROUTING=key-prefix
BUCKET_ALLOWLIST='existing_site_*'  # ALLOW_BUCKET_CREATION unset → false
```
The bucket must already exist; writes to missing buckets are dropped.

**Block 3 - key-prefix with fallback bucket `zenoh_fallback`**
```
KEYEXPRS='fallback_$*'
ROUTING=key-prefix
BUCKET=zenoh_fallback            # single-chunk keys (no slash) fall back here
BUCKET_ALLOWLIST=zenoh_fallback
```

**Block 4 - key-prefix, no fallback**
```
KEYEXPRS='orphan_$*'
ROUTING=key-prefix
BUCKET_ALLOWLIST='orphan_*'      # no BUCKET → single-chunk keys are dropped
```

## Cleanup

```bash
docker rm -f zenoh-router
rm -rf ~/data-test
```
