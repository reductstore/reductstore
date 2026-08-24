"""Integration tests for indexed Zenoh block configuration.

These run only when the server is started with RS_ZENOH_{SUB,QUERY}_<ID>_* blocks, which
the CI action does in its `indexed` routing mode:

    RS_ZENOH_SUB_0_KEYEXPRS=entry_$*,factory/**   RS_ZENOH_SUB_0_BUCKET=zenoh
    RS_ZENOH_SUB_1_KEYEXPRS=rt_$*/**              RS_ZENOH_SUB_1_ROUTING=key-prefix
    RS_ZENOH_SUB_1_BUCKET_ALLOWLIST=rt_site_*     RS_ZENOH_SUB_1_ALLOW_BUCKET_CREATION=true
"""

import asyncio
import os
import random

import pytest
from reduct import ReductError

pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.skipif(
        not os.environ.get("RS_ZENOH_ROUTING_TESTS"),
        reason="server is not configured with indexed Zenoh blocks",
    ),
]


@pytest.fixture(name="run_id")
def _run_id() -> str:
    return f"run{random.randint(0, 1_000_000_000)}"


async def read_first_payload(bucket, entry_name):
    async for record in bucket.query(entry_name):
        return await record.read_all()
    return None


async def test_static_block_writes_to_its_bucket(client, zenoh_bucket, zenoh_session, run_id):
    """A key expression covered by the static block lands in that block's bucket."""
    entry_name = f"entry_{run_id}"
    payload = b"static routed"

    zenoh_session.put(entry_name, payload)
    await asyncio.sleep(0.5)

    bucket = await client.get_bucket(zenoh_bucket)
    assert await read_first_payload(bucket, entry_name) == payload


async def test_key_prefix_creates_allowed_bucket(client, zenoh_session, run_id):
    """The first key chunk selects the bucket, which is created on demand."""
    bucket_name = f"rt_site_{run_id}"
    payload = b"key prefix routed"

    zenoh_session.put(f"{bucket_name}/motion/welder", payload)
    await asyncio.sleep(0.5)

    bucket = await client.get_bucket(bucket_name)
    assert await read_first_payload(bucket, "motion/welder") == payload


async def test_queryable_reads_back_at_the_written_key(client, zenoh_session, run_id):
    """The read side resolves the same key expression the write side wrote."""
    bucket_name = f"rt_site_{run_id}"
    key_expr = f"{bucket_name}/sensors/temp"
    payload = b"round trip"

    zenoh_session.put(key_expr, payload)
    await asyncio.sleep(0.5)

    replies = [reply for reply in zenoh_session.get(key_expr, timeout=5.0) if reply.ok]
    assert replies
    assert bytes(replies[0].result.payload) == payload

    # the bucket exists only because the subscriber created it
    await client.get_bucket(bucket_name)


async def test_prefix_outside_allowlist_is_rejected(client, zenoh_session, run_id):
    """A prefix matching the key expression but not the allowlist creates nothing."""
    bucket_name = f"rt_blocked_{run_id}"

    zenoh_session.put(f"{bucket_name}/motion/welder", b"should not be stored")
    await asyncio.sleep(0.5)

    with pytest.raises(ReductError):
        await client.get_bucket(bucket_name)

    replies = [
        reply
        for reply in zenoh_session.get(f"{bucket_name}/motion/welder", timeout=5.0)
        if reply.ok
    ]
    assert not replies
