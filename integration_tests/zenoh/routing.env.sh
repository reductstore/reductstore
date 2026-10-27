# Block 0: static routing
# Keys matching entry_$* or factory/** are stored in the "zenoh" bucket.
# $* matches any single chunk without slashes (e.g. entry_camera, entry_lidar).
# The full key becomes the entry name.
export RS_ZENOH_SUB_0_KEYEXPRS='entry_$*,factory/**'
export RS_ZENOH_SUB_0_BUCKET=zenoh

# Block 1: key-prefix routing, bucket creation enabled
# The first key chunk becomes the bucket name; the rest becomes the entry name.
# Only bucket names matching rt_site_* are accepted (allowlist).
# If the bucket does not exist it is created automatically on first write.
export RS_ZENOH_SUB_1_KEYEXPRS='rt_$*/**'
export RS_ZENOH_SUB_1_ROUTING=key-prefix
export RS_ZENOH_SUB_1_BUCKET_ALLOWLIST='rt_site_*'
export RS_ZENOH_SUB_1_ALLOW_BUCKET_CREATION=true

# Block 2: key-prefix routing, bucket creation disabled
# Same key-prefix logic as block 1 but the bucket must already exist.
# Writes to missing buckets are silently dropped.
export RS_ZENOH_SUB_2_KEYEXPRS='existing_$*/**'
export RS_ZENOH_SUB_2_ROUTING=key-prefix
export RS_ZENOH_SUB_2_BUCKET_ALLOWLIST='existing_site_*'

# Block 3: key-prefix routing with a fallback bucket
# Two-chunk keys (bucket/entry) route normally.
# Single-chunk keys (no slash) fall back to the "zenoh_fallback" bucket.
export RS_ZENOH_SUB_3_KEYEXPRS='fallback_$*'
export RS_ZENOH_SUB_3_ROUTING=key-prefix
export RS_ZENOH_SUB_3_BUCKET=zenoh_fallback
export RS_ZENOH_SUB_3_BUCKET_ALLOWLIST=zenoh_fallback

# Block 4: key-prefix routing with no fallback
# Single-chunk keys are rejected and dropped because no fallback bucket is set.
export RS_ZENOH_SUB_4_KEYEXPRS='orphan_$*'
export RS_ZENOH_SUB_4_ROUTING=key-prefix
export RS_ZENOH_SUB_4_BUCKET_ALLOWLIST='orphan_*'

# Queryable blocks 0-4 mirror the subscriber blocks above for the read path.
# They serve Zenoh get() queries from the same buckets; they never create buckets.
export RS_ZENOH_QUERY_0_KEYEXPRS='entry_$*,factory/**'
export RS_ZENOH_QUERY_0_BUCKET=zenoh
export RS_ZENOH_QUERY_1_KEYEXPRS='rt_$*/**'
export RS_ZENOH_QUERY_1_ROUTING=key-prefix
export RS_ZENOH_QUERY_1_BUCKET_ALLOWLIST='rt_site_*'
export RS_ZENOH_QUERY_2_KEYEXPRS='existing_$*/**'
export RS_ZENOH_QUERY_2_ROUTING=key-prefix
export RS_ZENOH_QUERY_2_BUCKET_ALLOWLIST='existing_site_*'
export RS_ZENOH_QUERY_3_KEYEXPRS='fallback_$*'
export RS_ZENOH_QUERY_3_ROUTING=key-prefix
export RS_ZENOH_QUERY_3_BUCKET=zenoh_fallback
export RS_ZENOH_QUERY_3_BUCKET_ALLOWLIST=zenoh_fallback
export RS_ZENOH_QUERY_4_KEYEXPRS='orphan_$*'
export RS_ZENOH_QUERY_4_ROUTING=key-prefix
export RS_ZENOH_QUERY_4_BUCKET_ALLOWLIST='orphan_*'

# Used by CI to write a Docker --env-file from the exports above.
ZENOH_ROUTING_ENV_VARS=(
  RS_ZENOH_SUB_0_KEYEXPRS RS_ZENOH_SUB_0_BUCKET
  RS_ZENOH_SUB_1_KEYEXPRS RS_ZENOH_SUB_1_ROUTING RS_ZENOH_SUB_1_BUCKET_ALLOWLIST RS_ZENOH_SUB_1_ALLOW_BUCKET_CREATION
  RS_ZENOH_SUB_2_KEYEXPRS RS_ZENOH_SUB_2_ROUTING RS_ZENOH_SUB_2_BUCKET_ALLOWLIST
  RS_ZENOH_SUB_3_KEYEXPRS RS_ZENOH_SUB_3_ROUTING RS_ZENOH_SUB_3_BUCKET RS_ZENOH_SUB_3_BUCKET_ALLOWLIST
  RS_ZENOH_SUB_4_KEYEXPRS RS_ZENOH_SUB_4_ROUTING RS_ZENOH_SUB_4_BUCKET_ALLOWLIST
  RS_ZENOH_QUERY_0_KEYEXPRS RS_ZENOH_QUERY_0_BUCKET
  RS_ZENOH_QUERY_1_KEYEXPRS RS_ZENOH_QUERY_1_ROUTING RS_ZENOH_QUERY_1_BUCKET_ALLOWLIST
  RS_ZENOH_QUERY_2_KEYEXPRS RS_ZENOH_QUERY_2_ROUTING RS_ZENOH_QUERY_2_BUCKET_ALLOWLIST
  RS_ZENOH_QUERY_3_KEYEXPRS RS_ZENOH_QUERY_3_ROUTING RS_ZENOH_QUERY_3_BUCKET RS_ZENOH_QUERY_3_BUCKET_ALLOWLIST
  RS_ZENOH_QUERY_4_KEYEXPRS RS_ZENOH_QUERY_4_ROUTING RS_ZENOH_QUERY_4_BUCKET_ALLOWLIST
)
