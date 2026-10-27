import pytest
from reduct import ReductError

from .support import wait_for_payload

pytestmark = pytest.mark.asyncio


async def test_static_block_writes_to_configured_bucket(
    client, zenoh_bucket, zenoh_session, run_id
):
    entry_name = f"entry_{run_id}"
    payload = b"static routed"

    zenoh_session.put(entry_name, payload)

    bucket = await client.get_bucket(zenoh_bucket)
    assert await wait_for_payload(bucket, entry_name, payload) == payload

    with pytest.raises(ReductError):
        await client.get_bucket(entry_name)
