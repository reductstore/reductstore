import pytest

from .support import read_first_payload, wait_until

pytestmark = pytest.mark.asyncio


async def test_static_block_routes_each_configured_key_expression(
    client, zenoh_bucket, zenoh_session, run_id
):
    entry_name = f"entry_{run_id}"
    factory_name = f"factory/{run_id}/status"
    entry_payload = b"from entry key expression"
    factory_payload = b"from factory key expression"

    zenoh_session.put(entry_name, entry_payload)
    zenoh_session.put(factory_name, factory_payload)
    bucket = await client.get_bucket(zenoh_bucket)

    async def both_payloads_are_available() -> tuple[bytes, bytes] | None:
        entry = await read_first_payload(bucket, entry_name)
        factory = await read_first_payload(bucket, factory_name)
        if entry == entry_payload and factory == factory_payload:
            return entry, factory
        return None

    assert await wait_until(
        both_payloads_are_available,
        "both static key expression payloads",
    ) == (entry_payload, factory_payload)
