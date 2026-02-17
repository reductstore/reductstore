"""Integration tests for Zenoh data ingestion into ReductStore.

In single-bucket mode, the Zenoh key expression becomes the entry name.
All data is written to the bucket configured via RS_ZENOH_BUCKET.
"""

import asyncio

import pytest


pytestmark = pytest.mark.asyncio


async def read_first_payload(bucket, entry_name):
    async for record in bucket.query(entry_name):
        return await record.read_all()
    return None


async def test_publish_simple_data(bucket, entry_name, zenoh_session):
    """Publish data via Zenoh and verify via reduct-py."""
    # In single-bucket mode: key_expr = entry_name
    key_expr = entry_name
    payload = b"hello from zenoh"

    zenoh_session.put(key_expr, payload)
    await asyncio.sleep(0.5)

    data = await read_first_payload(bucket, entry_name)
    assert data is not None, "Expected at least one stored record"
    assert data == payload


async def test_publish_with_labels(bucket, entry_name, zenoh_session, serialize_labels):
    """Publish data with labels via Zenoh attachment."""
    key_expr = entry_name
    payload = b"data with labels"
    labels = {"sensor": "imu", "unit": "m/s^2"}

    zenoh_session.put(key_expr, payload, attachment=serialize_labels(labels))
    await asyncio.sleep(0.5)

    records = [record async for record in bucket.query(entry_name)]
    assert records
    assert records[0].labels.get("sensor") == "imu"
    assert records[0].labels.get("unit") == "m/s^2"


async def test_publish_multiple_records(bucket, entry_name, zenoh_session):
    """Publish multiple records via Zenoh."""
    key_expr = entry_name

    for i in range(5):
        zenoh_session.put(key_expr, f"record_{i}".encode())
        await asyncio.sleep(0.01)

    await asyncio.sleep(0.5)

    records = [record async for record in bucket.query(entry_name)]
    assert len(records) >= 5


async def test_publish_large_payload(bucket, entry_name, zenoh_session):
    """Handle large payloads via Zenoh."""
    key_expr = entry_name
    payload = b"x" * (1024 * 1024)

    zenoh_session.put(key_expr, payload)
    await asyncio.sleep(1.0)

    data = await read_first_payload(bucket, entry_name)
    assert data is not None
    assert len(data) == len(payload)


async def test_publish_empty_payload(bucket, entry_name, zenoh_session):
    """Handle empty payloads via Zenoh."""
    key_expr = entry_name

    zenoh_session.put(key_expr, b"")
    await asyncio.sleep(0.5)

    data = await read_first_payload(bucket, entry_name)
    assert data is not None
    assert data == b""


async def test_publish_nested_key(bucket, zenoh_session):
    """Handle nested/hierarchical key expressions."""
    # Nested key becomes nested entry name
    # Note: Keys with slashes can only be read via Zenoh, not via reduct-py HTTP API
    import random

    entry_name = f"entry_{random.randint(0, 1_000_000_000)}"
    key_expr = f"factory/line1/{entry_name}"
    payload = b"temperature reading"

    zenoh_session.put(key_expr, payload)
    await asyncio.sleep(0.5)

    # Query via Zenoh queryable (not reduct-py) since entry name has slashes
    selector = f"{key_expr}?limit=1"
    replies = [reply for reply in zenoh_session.get(selector, timeout=5.0) if reply.ok]
    assert replies, "Expected at least one reply from Zenoh queryable"
    assert replies[0].ok.payload.to_bytes() == payload


async def test_publisher_stream(bucket, entry_name, zenoh_session):
    """Handle data from a Zenoh publisher."""
    key_expr = entry_name

    publisher = zenoh_session.declare_publisher(key_expr)
    for i in range(10):
        publisher.put(f"stream_{i}".encode())
        await asyncio.sleep(0.01)
    publisher.undeclare()

    await asyncio.sleep(0.5)

    records = [record async for record in bucket.query(entry_name)]
    assert len(records) >= 10


async def test_publisher_with_attachment(
    bucket,
    entry_name,
    zenoh_session,
    serialize_labels,
):
    """Handle publisher attachments for labels."""
    key_expr = entry_name

    publisher = zenoh_session.declare_publisher(key_expr)
    labels = {"type": "telemetry", "version": "1.0"}
    publisher.put(b"telemetry data", attachment=serialize_labels(labels))
    publisher.undeclare()

    await asyncio.sleep(0.5)

    records = [record async for record in bucket.query(entry_name)]
    assert records
    assert records[0].labels.get("type") == "telemetry"
    assert records[0].labels.get("version") == "1.0"
