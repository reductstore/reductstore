import pytest
from reduct import ReductError

from .support import read_first_payload, wait_until

pytestmark = pytest.mark.asyncio


async def test_key_prefix_rejects_a_bucket_outside_its_allowlist(
    client, zenoh_session, run_id
):
    blocked_bucket = f"rt_blocked_{run_id}"
    control_bucket = f"rt_site_control_{run_id}"
    control_payload = b"control routed"

    zenoh_session.put(f"{blocked_bucket}/motion/welder", b"must be discarded")
    zenoh_session.put(f"{control_bucket}/motion/welder", control_payload)

    async def control_payload_is_available() -> bytes | None:
        try:
            bucket = await client.get_bucket(control_bucket)
        except ReductError:
            return None
        actual = await read_first_payload(bucket, "motion/welder")
        return actual if actual == control_payload else None

    assert (
        await wait_until(
            control_payload_is_available,
            f"bucket '{control_bucket}' to be populated",
        )
        == control_payload
    )
    with pytest.raises(ReductError):
        await client.get_bucket(blocked_bucket)

    replies = [
        reply
        for reply in zenoh_session.get(f"{blocked_bucket}/motion/welder", timeout=1.0)
        if reply.ok
    ]
    assert not replies
