import pytest
from reduct import ReductError

from .support import read_first_payload, wait_until

pytestmark = pytest.mark.asyncio


async def test_key_prefix_creates_an_allowed_bucket(client, zenoh_session, run_id):
    bucket_name = f"rt_site_{run_id}"
    payload = b"key prefix routed"

    zenoh_session.put(f"{bucket_name}/motion/welder", payload)

    async def routed_bucket_has_payload() -> bytes | None:
        try:
            bucket = await client.get_bucket(bucket_name)
        except ReductError:
            return None
        actual = await read_first_payload(bucket, "motion/welder")
        return actual if actual == payload else None

    assert (
        await wait_until(
            routed_bucket_has_payload, f"bucket '{bucket_name}' to be populated"
        )
        == payload
    )
