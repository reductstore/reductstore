"""Integration tests for error handling and edge cases.

In single-bucket mode, the Zenoh key expression becomes the entry name.
All data flows through the bucket configured via RS_ZENOH_BUCKET.
"""

import asyncio
import json

import pytest


pytestmark = pytest.mark.asyncio


async def read_first_payload(bucket, entry_name):
    async for record in bucket.query(entry_name):
        return await record.read_all()
    return None


async def test_publish_unicode_data(bucket, entry_name, zenoh_session):
    """Handle Unicode data in payload."""
    key_expr = entry_name
    payload = "Hello 世界 🌍 Привет".encode("utf-8")

    zenoh_session.put(key_expr, payload)
    await asyncio.sleep(0.5)

    data = await read_first_payload(bucket, entry_name)
    assert data is not None
    assert data == payload


async def test_publish_binary_data(bucket, entry_name, zenoh_session):
    """Handle arbitrary binary data."""
    key_expr = entry_name
    payload = bytes(range(256)) + b"\x00" * 100

    zenoh_session.put(key_expr, payload)
    await asyncio.sleep(0.5)

    data = await read_first_payload(bucket, entry_name)
    assert data is not None
    assert data == payload


async def test_rapid_fire_publishing(bucket, entry_name, zenoh_session):
    """Handle rapid consecutive publishes."""
    key_expr = entry_name

    for i in range(100):
        zenoh_session.put(key_expr, f"message_{i}".encode())

    await asyncio.sleep(2.0)

    records = [record async for record in bucket.query(entry_name)]
    assert records


async def test_concurrent_publish_to_multiple_entries(bucket, zenoh_session):
    """Handle concurrent publishes to different entries."""
    entries = ["entry_a", "entry_b", "entry_c"]

    for i in range(30):
        for entry in entries:
            key_expr = entry
            zenoh_session.put(key_expr, f"data_{i}".encode())

    await asyncio.sleep(2.0)

    for entry in entries:
        records = [record async for record in bucket.query(entry)]
        assert records


async def test_json_payload(bucket, entry_name, zenoh_session):
    """Handle JSON payload."""
    key_expr = entry_name
    data = {"sensor_id": "temp_001", "readings": [23.5, 24.1, 23.8]}
    payload = json.dumps(data).encode("utf-8")

    zenoh_session.put(key_expr, payload)
    await asyncio.sleep(0.5)

    raw = await read_first_payload(bucket, entry_name)
    assert raw is not None
    parsed = json.loads(raw.decode("utf-8"))
    assert parsed["sensor_id"] == "temp_001"


async def test_large_json_array(bucket, entry_name, zenoh_session):
    """Handle large JSON arrays."""
    key_expr = entry_name
    data = {"readings": list(range(10_000))}
    payload = json.dumps(data).encode("utf-8")

    zenoh_session.put(key_expr, payload)
    await asyncio.sleep(1.0)

    records = [record async for record in bucket.query(entry_name)]
    assert records
