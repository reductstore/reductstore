import pytest

from .support import wait_for_payload, wait_for_query_payload

pytestmark = pytest.mark.asyncio


async def test_key_prefix_uses_the_fallback_bucket_for_a_single_chunk_key(
    client, zenoh_session, run_id
):
    entry_name = f"fallback_{run_id}"
    payload = b"fallback routed"

    zenoh_session.put(entry_name, payload)

    bucket = await client.get_bucket("zenoh_fallback")
    assert await wait_for_payload(bucket, entry_name, payload) == payload
    assert await wait_for_query_payload(zenoh_session, entry_name, payload) == payload
