"""Integration tests for querying ReductStore data via Zenoh.

In single-bucket mode, the Zenoh key expression becomes the entry name.
All data is read from the bucket configured via RS_ZENOH_BUCKET.
"""

import asyncio

import pytest


pytestmark = pytest.mark.asyncio


async def test_query_single_record(bucket, entry_name, zenoh_session):
    """Query a single record by timestamp via Zenoh."""
    timestamp = 1_000_000
    payload = b"test data for query"
    await bucket.write(entry_name, payload, timestamp=timestamp)

    # In single-bucket mode: key_expr = entry_name
    key_expr = entry_name
    selector = f"{key_expr}?ts={timestamp}"

    replies = [reply for reply in zenoh_session.get(selector, timeout=5.0) if reply.ok]

    assert replies, "No reply received from Zenoh query"
    assert replies[0].ok.payload.to_bytes() == payload

    # wait for 10s to ensure the record is expired
    import asyncio

    await asyncio.sleep(10)


async def test_query_with_labels(bucket, entry_name, zenoh_session):
    """Query a record and receive labels in attachment."""
    timestamp = 2_000_000
    payload = b"data with labels"
    labels = {"sensor": "gps", "quality": "high"}
    await bucket.write(entry_name, payload, timestamp=timestamp, labels=labels)

    key_expr = entry_name
    selector = f"{key_expr}?ts={timestamp}"

    replies = [reply for reply in zenoh_session.get(selector, timeout=5.0) if reply.ok]

    assert replies
    assert replies[0].ok.payload.to_bytes() == payload


async def test_query_time_range(bucket, entry_name, zenoh_session):
    """Query records within a time range via Zenoh."""
    base_ts = 1_000_000_000
    for i in range(5):
        ts = base_ts + i * 1_000_000
        await bucket.write(entry_name, f"record_{i}".encode(), timestamp=ts)

    key_expr = entry_name
    selector = f"{key_expr}?start={base_ts + 1_000_000}&stop={base_ts + 4_000_000}"

    replies = [reply for reply in zenoh_session.get(selector, timeout=5.0) if reply.ok]
    assert replies, "Expected at least one reply"


async def test_query_with_limit(bucket, entry_name, zenoh_session):
    """Limit the number of records returned."""
    base_ts = 2_000_000_000
    for i in range(10):
        ts = base_ts + i * 1_000_000
        await bucket.write(entry_name, f"record_{i}".encode(), timestamp=ts)

    key_expr = entry_name
    selector = f"{key_expr}?limit=3"

    replies = [reply for reply in zenoh_session.get(selector, timeout=5.0) if reply.ok]
    assert len(replies) <= 3


async def test_query_nonexistent_entry(bucket, zenoh_session):
    """Handle query for non-existent entry gracefully."""
    key_expr = "nonexistent_entry_12345"
    list(zenoh_session.get(key_expr, timeout=2.0))


async def test_query_include_label_filter(bucket, entry_name, zenoh_session):
    """Filter records by label inclusion."""
    base_ts = 3_000_000_000
    for i, label_value in enumerate(["a", "b", "a", "c", "a"]):
        ts = base_ts + i * 1_000_000
        await bucket.write(
            entry_name,
            f"record_{i}".encode(),
            timestamp=ts,
            labels={"category": label_value},
        )

    key_expr = entry_name
    selector = f"{key_expr}?include-category=a"

    replies = [reply for reply in zenoh_session.get(selector, timeout=5.0) if reply.ok]
    assert replies


async def test_query_nested_key(bucket, zenoh_session):
    """Query with a nested/hierarchical key expression."""
    # Write via Zenoh (since HTTP API can't handle slashes in entry names)
    import random

    entry_name = f"entry_{random.randint(0, 1_000_000_000)}"
    nested_key = f"factory/query/{entry_name}"
    payload = b"nested entry data"

    # Write via Zenoh subscriber
    zenoh_session.put(nested_key, payload)
    await asyncio.sleep(0.5)

    # Query via Zenoh queryable
    selector = f"{nested_key}?limit=1"
    replies = [reply for reply in zenoh_session.get(selector, timeout=5.0) if reply.ok]
    assert replies, "Expected at least one reply"
    assert replies[0].ok.payload.to_bytes() == payload
