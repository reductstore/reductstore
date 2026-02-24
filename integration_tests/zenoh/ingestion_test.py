"""Integration tests for Zenoh data ingestion into ReductStore.

All data is written to the bucket configured via RS_ZENOH_BUCKET.
"""

import asyncio

import pytest
import zenoh


pytestmark = pytest.mark.asyncio


async def read_first_payload(bucket, entry_name):
    async for record in bucket.query(entry_name):
        return await record.read_all()
    return None


async def test_publish_simple_data(bucket, entry_name, zenoh_session):
    """Publish data via Zenoh and verify via reduct-py."""
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
    import random

    entry_name = f"entry_{random.randint(0, 1_000_000_000)}"
    key_expr = f"factory/line1/{entry_name}"
    payload = b"temperature reading"

    zenoh_session.put(key_expr, payload)
    await asyncio.sleep(0.5)

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


async def test_publish_with_encoding_json(bucket, entry_name, zenoh_session):
    """Publish data with JSON encoding and verify content_type is preserved."""
    key_expr = entry_name
    payload = b'{"temperature": 23.5, "humidity": 65}'

    zenoh_session.put(key_expr, payload, encoding=zenoh.Encoding.APPLICATION_JSON)
    await asyncio.sleep(0.5)

    records = [record async for record in bucket.query(entry_name)]
    assert records, "Expected at least one stored record"
    assert records[0].content_type == "application/json"


async def test_publish_with_encoding_text_plain(bucket, entry_name, zenoh_session):
    """Publish data with text/plain encoding and verify content_type is preserved."""
    key_expr = entry_name
    payload = b"Hello, World!"

    zenoh_session.put(key_expr, payload, encoding=zenoh.Encoding.TEXT_PLAIN)
    await asyncio.sleep(0.5)

    records = [record async for record in bucket.query(entry_name)]
    assert records, "Expected at least one stored record"
    assert records[0].content_type == "text/plain"


async def test_publish_with_encoding_custom(bucket, entry_name, zenoh_session):
    """Publish data with custom MIME type encoding."""
    key_expr = entry_name
    payload = b"\x00\x01\x02\x03"

    # Custom encoding via constructor
    custom_encoding = zenoh.Encoding("application/x-custom-binary")
    zenoh_session.put(key_expr, payload, encoding=custom_encoding)
    await asyncio.sleep(0.5)

    records = [record async for record in bucket.query(entry_name)]
    assert records, "Expected at least one stored record"
    assert records[0].content_type == "application/x-custom-binary"


async def test_publish_default_encoding(bucket, entry_name, zenoh_session):
    """Default encoding should be application/octet-stream."""
    key_expr = entry_name
    payload = b"binary data"

    # No explicit encoding - should default to octet-stream
    zenoh_session.put(key_expr, payload)
    await asyncio.sleep(0.5)

    records = [record async for record in bucket.query(entry_name)]
    assert records, "Expected at least one stored record"
    # Zenoh default is zenoh/bytes which maps to application/octet-stream
    assert (
        "octet-stream" in records[0].content_type or "zenoh" in records[0].content_type
    )


async def test_publisher_with_encoding(bucket, entry_name, zenoh_session):
    """Publisher preserves encoding across multiple puts."""
    key_expr = entry_name

    publisher = zenoh_session.declare_publisher(key_expr)
    for i in range(3):
        publisher.put(
            f'{{"index": {i}}}'.encode(),
            encoding=zenoh.Encoding.APPLICATION_JSON,
        )
        await asyncio.sleep(0.01)
    publisher.undeclare()

    await asyncio.sleep(0.5)

    records = [record async for record in bucket.query(entry_name)]
    assert len(records) >= 3
    for record in records:
        assert record.content_type == "application/json"
