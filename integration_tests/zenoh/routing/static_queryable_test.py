import pytest

from .support import wait_for_query_payload

pytestmark = pytest.mark.asyncio


async def test_static_queryable_reads_the_configured_bucket(
    client, zenoh_bucket, zenoh_session, run_id
):
    entry_name = f"entry_{run_id}"
    payload = b"static queryable"
    bucket = await client.get_bucket(zenoh_bucket)

    await bucket.write(entry_name, payload, timestamp=1_000_000)

    assert await wait_for_query_payload(zenoh_session, entry_name, payload) == payload
