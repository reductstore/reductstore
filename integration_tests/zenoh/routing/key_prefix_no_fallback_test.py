import pytest
from reduct import ReductError

from .support import wait_for_payload

pytestmark = pytest.mark.asyncio


async def test_key_prefix_rejects_single_chunk_keys_without_a_fallback_bucket(
    client, zenoh_bucket, zenoh_session, run_id
):
    rejected_key = f"orphan_{run_id}"
    control_entry = f"entry_{run_id}"
    control_payload = b"control routed"

    zenoh_session.put(rejected_key, b"must be discarded")
    zenoh_session.put(control_entry, control_payload)

    control_bucket = await client.get_bucket(zenoh_bucket)
    assert (
        await wait_for_payload(control_bucket, control_entry, control_payload)
        == control_payload
    )
    with pytest.raises(ReductError):
        await client.get_bucket(rejected_key)

    replies = [
        reply for reply in zenoh_session.get(rejected_key, timeout=1.0) if reply.ok
    ]
    assert not replies
