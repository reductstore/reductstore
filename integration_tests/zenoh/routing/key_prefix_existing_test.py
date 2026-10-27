import pytest

from .support import wait_for_payload

pytestmark = pytest.mark.asyncio


async def test_key_prefix_writes_to_an_existing_bucket_when_creation_is_disabled(
    client, zenoh_session, run_id
):
    bucket_name = f"existing_site_{run_id}"
    payload = b"existing bucket routed"
    bucket = await client.create_bucket(bucket_name)

    zenoh_session.put(f"{bucket_name}/motion/welder", payload)

    assert await wait_for_payload(bucket, "motion/welder", payload) == payload
