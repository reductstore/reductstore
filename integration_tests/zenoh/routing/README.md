# Zenoh Routing Integration Tests

Each file in this directory tests one observable routing behavior against a live server.

---

## One-time setup

**1. Start the Zenoh router**

```bash
docker run --init -d --name zenoh-router -p 7447:7447/tcp eclipse/zenoh
```

**2. Start ReductStore with the routing configuration**

In a dedicated terminal - keep it open while you run tests:

```bash
source integration_tests/zenoh/routing/env.sh

RS_DATA_PATH=~/data-test RS_DISABLE_AUTH=true RS_ZENOH_ENABLED=1 \
  RS_ZENOH_CONFIG="mode=client;connect/endpoints=[tcp/127.0.0.1:7447]" \
  cargo run -p reductstore --features "fs-backend web-console zenoh-api"
```

The server prints one line per configured block on startup. Confirm all five subscriber and five queryable blocks appear before running tests.

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

---

## Running the tests

Run every file:
```bash
pytest -v routing/
```

Or pick one at a time from the sections below.

---

## Block 0: static routing - bucket `zenoh`

```
RS_ZENOH_SUB_0_KEYEXPRS='entry_$*,factory/**'  # matches entry_camera, factory/line1/temp, …
RS_ZENOH_SUB_0_BUCKET=zenoh                     # every matching key is stored here
```

The entire Zenoh key becomes the entry name. The bucket name is fixed; it is never derived from the key.

```bash
# Static key goes to the configured bucket, not to a bucket named after the key
pytest -v routing/static_bucket_test.py

# Both comma-separated key expressions in this block land in the same bucket
pytest -v routing/static_keyexprs_test.py

# The matching queryable reads from that same bucket
pytest -v routing/static_queryable_test.py
```

---

## Block 1: key-prefix routing - `rt_site_*`, creation enabled

```
RS_ZENOH_SUB_1_KEYEXPRS='rt_$*/**'             # matches rt_london/sensors/temp, rt_berlin/cam, …
RS_ZENOH_SUB_1_ROUTING=key-prefix              # first chunk → bucket, rest → entry name
RS_ZENOH_SUB_1_BUCKET_ALLOWLIST='rt_site_*'    # only bucket names matching this glob are accepted
RS_ZENOH_SUB_1_ALLOW_BUCKET_CREATION=true      # bucket is created on first write if missing
```

Publishing `rt_london/sensors/temp` stores payload in bucket `rt_london`, entry `sensors/temp`.

```bash
# rt_site_<id> does not exist yet; the subscriber creates it on first write
pytest -v routing/key_prefix_create_test.py

# rt_blocked_<id> does not match rt_site_*; write is dropped, no bucket is created
pytest -v routing/key_prefix_allowlist_test.py

# queryable serves data independently from multiple rt_site_* buckets
pytest -v routing/queryable_multibucket_test.py
```

---

## Block 2: key-prefix routing - `existing_site_*`, creation disabled

```
RS_ZENOH_SUB_2_KEYEXPRS='existing_$*/**'         # matches existing_site_a/sensors/temp, …
RS_ZENOH_SUB_2_ROUTING=key-prefix                # first chunk → bucket, rest → entry name
RS_ZENOH_SUB_2_BUCKET_ALLOWLIST='existing_site_*' # only matching bucket names are accepted
                                                  # ALLOW_BUCKET_CREATION not set → defaults to false
```

The target bucket must already exist. If it does not, the write is silently dropped.

```bash
# bucket is created via the API before publishing; write succeeds
pytest -v routing/key_prefix_existing_test.py

# bucket does not exist; write is dropped, bucket is not created
pytest -v routing/key_prefix_no_create_test.py

# queryable also never creates a bucket; a get() to a missing bucket returns no replies
pytest -v routing/queryable_no_create_test.py
```

---

## Block 3: key-prefix routing - fallback bucket `zenoh_fallback`

```
RS_ZENOH_SUB_3_KEYEXPRS='fallback_$*'          # matches fallback_sensor, fallback_cam, …
RS_ZENOH_SUB_3_ROUTING=key-prefix
RS_ZENOH_SUB_3_BUCKET=zenoh_fallback            # fallback: single-chunk keys land here
RS_ZENOH_SUB_3_BUCKET_ALLOWLIST=zenoh_fallback
```

A key with a slash (`fallback_a/temp`) routes normally: bucket `fallback_a`, entry `temp`.  
A key without a slash (`fallback_sensor`) has no prefix chunk, so it falls back to `zenoh_fallback` with the full key as the entry name.

```bash
# single-chunk key lands in zenoh_fallback; queryable also reads it from there
pytest -v routing/key_prefix_fallback_test.py
```

---

## Block 4: key-prefix routing - no fallback

```
RS_ZENOH_SUB_4_KEYEXPRS='orphan_$*'            # matches orphan_sensor, orphan_cam, …
RS_ZENOH_SUB_4_ROUTING=key-prefix
RS_ZENOH_SUB_4_BUCKET_ALLOWLIST='orphan_*'     # no RS_ZENOH_SUB_4_BUCKET → no fallback
```

Single-chunk keys are rejected because there is no fallback bucket. No data is stored and no bucket is created.

```bash
# single-chunk orphan key is dropped; no bucket appears
pytest -v routing/key_prefix_no_fallback_test.py
```

---

## Cleanup

```bash
docker rm -f zenoh-router
rm -rf ~/data-test
```
