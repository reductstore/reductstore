"""Integration tests for querying ReductStore data via Zenoh."""

import asyncio
import json
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

    assert len(replies) == 3


async def test_query_nested_key(bucket, zenoh_session):
    """Handle nested key expressions."""
    entry_name = f"factory/query/entry_{random.randint(0, 1_000_000_000)}"
    payload = b"nested entry data"

    zenoh_session.put(entry_name, payload)
    await asyncio.sleep(0.5)

    selector = f"{entry_name}"
    replies = [
        r
        for r in zenoh_session.get(selector, timeout=5.0, consolidation=CONSOLIDATION)
        if r.ok
    ]

    assert len(replies) == 1
    assert replies[0].ok.payload.to_bytes() == payload


async def test_query_with_when_condition(bucket, entry_name, zenoh_session):
    """Query with a 'when' condition via attachment."""
    base_ts = 8_000_000_000
    for i, status in enumerate(["ok", "error", "ok", "warning", "ok"]):
        await bucket.write(
            entry_name,
            f"record_{i}".encode(),
            timestamp=base_ts + i * 1_000_000,
            labels={"status": status},
        )

    attachment = json.dumps({"when": {"&status": {"$eq": "ok"}}}).encode()

    selector = f"{entry_name}"
    replies = [
        r
        for r in zenoh_session.get(
            selector,
            timeout=5.0,
            consolidation=CONSOLIDATION,
            attachment=attachment,
        )
        if r.ok
    ]

    assert len(replies) == 3

    for reply in replies:
        labels = json.loads(reply.ok.attachment.to_bytes())
        assert labels.get("status") == "ok"
