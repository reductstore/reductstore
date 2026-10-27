import pytest
from reduct import ReductError

from .support import wait_for_payload

pytestmark = pytest.mark.asyncio


async def test_key_prefix_does_not_create_an_allowed_missing_bucket(
    client, zenoh_session, run_id
):
    missing_bucket = f"existing_site_missing_{run_id}"
    control_bucket_name = f"existing_site_control_{run_id}"
    control_payload = b"control routed"
    control_bucket = await client.create_bucket(control_bucket_name)

    zenoh_session.put(f"{missing_bucket}/motion/welder", b"must be discarded")
    zenoh_session.put(f"{control_bucket_name}/motion/welder", control_payload)

    assert (
        await wait_for_payload(control_bucket, "motion/welder", control_payload)
        == control_payload
    )
    with pytest.raises(ReductError):
        await client.get_bucket(missing_bucket)
