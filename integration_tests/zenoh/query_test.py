"""Integration tests for querying ReductStore data via Zenoh."""

import asyncio
import random

import pytest
import zenoh


pytestmark = pytest.mark.asyncio

# Allow multiple records to be returned in one query response
CONSOLIDATION = zenoh.ConsolidationMode.NONE


async def test_query_single_record(bucket, entry_name, zenoh_session):
    """Write a record and query it via Zenoh."""
    ts = 1_000_000
    payload = b"test data"
    await bucket.write(entry_name, payload, timestamp=ts)

    selector = f"{entry_name}?ts={ts}"
    replies = [
        r
        for r in zenoh_session.get(selector, timeout=5.0, consolidation=CONSOLIDATION)
        if r.ok
    ]

    assert len(replies) == 1
    assert replies[0].ok.payload.to_bytes() == payload


async def test_query_time_range(bucket, entry_name, zenoh_session):
    """Write multiple records and query a time range via Zenoh."""
    base_ts = 1_000_000_000
    for i in range(5):
        await bucket.write(
            entry_name, f"record_{i}".encode(), timestamp=base_ts + i * 1_000_000
        )

    selector = f"{entry_name}?start={base_ts + 1_000_000};stop={base_ts + 4_000_000}"
    replies = [
        r
        for r in zenoh_session.get(selector, timeout=5.0, consolidation=CONSOLIDATION)
        if r.ok
    ]

    assert 2 <= len(replies) <= 3


async def test_query_with_limit(bucket, entry_name, zenoh_session):
    """Write multiple records and query with a limit via Zenoh."""
    base_ts = 2_000_000_000
    for i in range(10):
        await bucket.write(
            entry_name, f"record_{i}".encode(), timestamp=base_ts + i * 1_000_000
        )

    selector = f"{entry_name}?limit=3"
    replies = [
        r
        for r in zenoh_session.get(selector, timeout=5.0, consolidation=CONSOLIDATION)
        if r.ok
    ]

    assert len(replies) == 3


async def test_query_include_label_filter(bucket, entry_name, zenoh_session):
    """Write multiple records and query with an include label filter via Zenoh."""
    base_ts = 3_000_000_000
    for i, val in enumerate(["a", "b", "a", "c", "a"]):
        await bucket.write(
            entry_name,
            f"record_{i}".encode(),
            timestamp=base_ts + i * 1_000_000,
            labels={"cat": val},
        )

    selector = f"{entry_name}?include-cat=a"
    replies = [
        r
        for r in zenoh_session.get(selector, timeout=5.0, consolidation=CONSOLIDATION)
        if r.ok
    ]

    assert len(replies) == 3


async def test_query_exclude_label_filter(bucket, entry_name, zenoh_session):
    """Write multiple records and query with an exclude label filter via Zenoh."""
    base_ts = 4_000_000_000
    for i, val in enumerate(["a", "b", "a", "c", "a"]):
        await bucket.write(
            entry_name,
            f"record_{i}".encode(),
            timestamp=base_ts + i * 1_000_000,
            labels={"cat": val},
        )

    selector = f"{entry_name}?exclude-cat=a"
    replies = [
        r
        for r in zenoh_session.get(selector, timeout=5.0, consolidation=CONSOLIDATION)
        if r.ok
    ]

    assert len(replies) == 2


async def test_query_with_each_n(bucket, entry_name, zenoh_session):
    """Write multiple records and query with each_n."""
    base_ts = 5_000_000_000
    for i in range(12):
        await bucket.write(
            entry_name, f"record_{i}".encode(), timestamp=base_ts + i * 1_000_000
        )

    selector = f"{entry_name}?each_n=3"
    replies = [
        r
        for r in zenoh_session.get(selector, timeout=5.0, consolidation=CONSOLIDATION)
        if r.ok
    ]

    assert len(replies) == 4


async def test_query_nested_key(bucket, zenoh_session):
    """Handle nested key expressions."""
    entry_name = f"factory/query/entry_{random.randint(0, 1_000_000_000)}"
    payload = b"nested entry data"

    zenoh_session.put(entry_name, payload)
    await asyncio.sleep(0.5)

    selector = f"{entry_name}?limit=1"
    replies = [
        r
        for r in zenoh_session.get(selector, timeout=5.0, consolidation=CONSOLIDATION)
        if r.ok
    ]

    assert len(replies) == 1
    assert replies[0].ok.payload.to_bytes() == payload


async def test_query_encoding_preserved(bucket, entry_name, zenoh_session):
    """Ensure content type/encoding is preserved in query results."""
    ts = 6_000_000
    payload = b'{"status": "ok"}'
    await bucket.write(
        entry_name, payload, timestamp=ts, content_type="application/json"
    )

    selector = f"{entry_name}?ts={ts}"
    replies = [
        r
        for r in zenoh_session.get(selector, timeout=5.0, consolidation=CONSOLIDATION)
        if r.ok
    ]

    assert replies
    assert replies[0].ok.payload.to_bytes() == payload
    assert "application/json" in str(replies[0].ok.encoding)


async def test_query_last(bucket, entry_name, zenoh_session):
    """Query the last record using the last parameter."""
    base_ts = 7_000_000_000
    for i in range(5):
        await bucket.write(
            entry_name, f"record_{i}".encode(), timestamp=base_ts + i * 1_000_000
        )

    selector = f"{entry_name}?last=true"
    replies = [
        r
        for r in zenoh_session.get(selector, timeout=5.0, consolidation=CONSOLIDATION)
        if r.ok
    ]

    assert len(replies) == 1
    assert replies[0].ok.payload.to_bytes() == b"record_4"
