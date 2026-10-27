import pytest

from .support import wait_for_query_payload

pytestmark = pytest.mark.asyncio


async def test_key_prefix_queryable_reads_from_multiple_buckets(
    client, zenoh_session, run_id
):
    entry_name = "sensors/temp"
    buckets = [
        (f"rt_site_{run_id}_one", b"first bucket", 1_000_000),
        (f"rt_site_{run_id}_two", b"second bucket", 2_000_000),
    ]

    for bucket_name, payload, timestamp in buckets:
        bucket = await client.create_bucket(bucket_name)
        await bucket.write(entry_name, payload, timestamp=timestamp)

    for bucket_name, payload, _ in buckets:
        assert (
            await wait_for_query_payload(
                zenoh_session,
                f"{bucket_name}/{entry_name}",
                payload,
            )
            == payload
        )
