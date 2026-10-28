"""Subscriber (write path) routing behaviors for indexed Zenoh blocks.

Requires the server started with the routing matrix in env.sh and
RS_ZENOH_ROUTING_TESTS set. See README.md.
"""

import pytest
from reduct import ReductError

from conftest import (
    read_first_payload,
    wait_for_payload,
    wait_for_query_payload,
    wait_until,
)


async def test_static_block_writes_to_configured_bucket(
    client, zenoh_bucket, zenoh_session, run_id
):
    """Block 0: a static key lands in the configured bucket, not a derived one."""
    entry_name = f"entry_{run_id}"
    payload = b"static routed"

    zenoh_session.put(entry_name, payload)

    bucket = await client.get_bucket(zenoh_bucket)
    assert await wait_for_payload(bucket, entry_name, payload) == payload

    with pytest.raises(ReductError):
        await client.get_bucket(entry_name)


async def test_static_block_routes_each_configured_key_expression(
    client, zenoh_bucket, zenoh_session, run_id
):
    """Block 0: both comma-separated key expressions feed the same bucket."""
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


async def test_key_prefix_creates_an_allowed_bucket(client, zenoh_session, run_id):
    """Block 1: an allowed prefix creates the bucket on first write."""
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


async def test_key_prefix_writes_to_an_existing_bucket_when_creation_is_disabled(
    client, zenoh_session, run_id
):
    """Block 2: a pre-existing bucket accepts writes when creation is disabled."""
    bucket_name = f"existing_site_{run_id}"
    payload = b"existing bucket routed"
    bucket = await client.create_bucket(bucket_name)

    zenoh_session.put(f"{bucket_name}/motion/welder", payload)

    assert await wait_for_payload(bucket, "motion/welder", payload) == payload


async def test_key_prefix_does_not_create_an_allowed_missing_bucket(
    client, zenoh_session, run_id
):
    """Block 2: a missing bucket is not created when creation is disabled."""
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


async def test_key_prefix_uses_the_fallback_bucket_for_a_single_chunk_key(
    client, zenoh_session, run_id
):
    """Block 3: a single-chunk key falls back to the configured bucket."""
    entry_name = f"fallback_{run_id}"
    payload = b"fallback routed"

    zenoh_session.put(entry_name, payload)

    bucket = await client.get_bucket("zenoh_fallback")
    assert await wait_for_payload(bucket, entry_name, payload) == payload
    assert await wait_for_query_payload(zenoh_session, entry_name, payload) == payload


async def test_key_prefix_rejects_single_chunk_keys_without_a_fallback_bucket(
    client, zenoh_bucket, zenoh_session, run_id
):
    """Block 4: a single-chunk key without a fallback is dropped."""
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


async def test_key_prefix_rejects_a_bucket_outside_its_allowlist(
    client, zenoh_session, run_id
):
    """Block 1: a bucket outside the allowlist is dropped and not created."""
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
