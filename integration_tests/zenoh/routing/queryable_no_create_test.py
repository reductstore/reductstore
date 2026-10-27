import pytest
from reduct import ReductError

pytestmark = pytest.mark.asyncio


async def test_key_prefix_queryable_does_not_create_a_missing_bucket(
    client, zenoh_session, run_id
):
    bucket_name = f"existing_site_query_missing_{run_id}"
    key_expr = f"{bucket_name}/sensors/temp"

    replies = [reply for reply in zenoh_session.get(key_expr, timeout=1.0) if reply.ok]

    assert not replies
    with pytest.raises(ReductError):
        await client.get_bucket(bucket_name)
