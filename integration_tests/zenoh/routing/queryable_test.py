"""Queryable (read path) routing behaviors for indexed Zenoh blocks.

Requires the server started with the routing matrix in env.sh and
RS_ZENOH_ROUTING_TESTS set. See README.md.
"""

import pytest
from reduct import ReductError

from conftest import wait_for_query_payload


async def test_static_queryable_reads_the_configured_bucket(
    client, zenoh_bucket, zenoh_session, run_id
):
    """Block 0: a static queryable serves data from its configured bucket."""
    entry_name = f"entry_{run_id}"
    payload = b"static queryable"
    bucket = await client.get_bucket(zenoh_bucket)

    await bucket.write(entry_name, payload, timestamp=1_000_000)

    assert await wait_for_query_payload(zenoh_session, entry_name, payload) == payload


async def test_key_prefix_queryable_reads_from_multiple_buckets(
    client, zenoh_session, run_id
):
    """Block 1: one queryable serves independent data from several buckets."""
    entry_name = "sensors/temp"
    buckets = [
        (f"rt_site_{run_id}_one", b"first bucket", 1_000_000),
        (f"rt_site_{run_id}_two", b"second bucket", 2_000_000),
    ]

    for bucket_name, payload, timestamp in buckets:
        bucket = await client.create_bucket(bucket_name)
        await bucket.write(entry_name, payload, timestamp=timestamp)

    for bucket_name, payload, _ in buckets:
        assert (
            await wait_for_query_payload(
                zenoh_session,
                f"{bucket_name}/{entry_name}",
                payload,
            )
            == payload
        )


async def test_key_prefix_queryable_does_not_create_a_missing_bucket(
    client, zenoh_session, run_id
):
    """Block 2: a queryable never creates a bucket; a missing one yields no reply."""
    bucket_name = f"existing_site_query_missing_{run_id}"
    key_expr = f"{bucket_name}/sensors/temp"

    replies = [reply for reply in zenoh_session.get(key_expr, timeout=1.0) if reply.ok]

    assert not replies
    with pytest.raises(ReductError):
        await client.get_bucket(bucket_name)
