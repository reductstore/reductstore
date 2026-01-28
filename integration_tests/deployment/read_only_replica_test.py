import asyncio
import os
import random
import time

import pytest
from reduct import Client
from reduct.error import ReductError


def requires_env(key):
    env = os.environ.get(key)
    return pytest.mark.skipif(
        env is None or env == "",
        reason=f"Not suitable environment {key} for current test",
    )


def _bucket_name():
    return f"bucket_{random.randint(0, 10_000_000)}"


async def _wait_until(predicate, timeout_s=30, step_s=0.5, label="condition"):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(step_s)
    raise AssertionError(f"Timed out waiting for {label}")


async def _bucket_exists(client: Client, name: str) -> bool:
    try:
        await client.get_bucket(name)
        return True
    except ReductError as exc:
        if exc.status_code == 404:
            return False
        raise


async def _read_record(bucket, entry_name, timestamp):
    try:
        async with bucket.read(entry_name, timestamp=timestamp) as record:
            data = await record.read_all()
            return record, data
        return None
    except ReductError as exc:
        if exc.status_code == 404:
            return None
        raise


@requires_env("PRIMARY_STORAGE_URL")
@requires_env("REPLICA_STORAGE_URL")
@pytest.mark.asyncio
async def test_read_only_replica_syncs_data():
    primary_url = os.environ["PRIMARY_STORAGE_URL"]
    replica_url = os.environ["REPLICA_STORAGE_URL"]
    token = os.getenv("API_TOKEN")
    bucket_name = _bucket_name()
    entry_name = "entry"
    ts = 1000

    async with Client(primary_url, api_token=token) as primary_client, Client(
        replica_url, api_token=token
    ) as replica_client:
        primary_bucket = await primary_client.create_bucket(bucket_name)

        await _wait_until(
            lambda: _bucket_exists(replica_client, bucket_name),
            label="bucket to appear on replica",
        )
        replica_bucket = await replica_client.get_bucket(bucket_name)

        await primary_bucket.write(
            entry_name,
            b"primary-data",
            timestamp=ts,
            labels={"version": "1"},
        )

        async def _record_present():
            record = await _read_record(replica_bucket, entry_name, ts)
            return record is not None

        await _wait_until(
            _record_present,
            label="record to appear on replica",
        )

        record, data = await _read_record(replica_bucket, entry_name, ts)
        assert data == b"primary-data"
        assert record.labels["version"] == "1"

        await primary_bucket.update(entry_name, timestamp=ts, labels={"version": "2"})

        async def _record_updated():
            result = await _read_record(replica_bucket, entry_name, ts)
            return result is not None and result[0].labels.get("version") == "2"

        await _wait_until(
            _record_updated,
            label="record update to appear on replica",
        )

        await primary_bucket.remove_entry(entry_name)

        async def _record_removed():
            return await _read_record(replica_bucket, entry_name, ts) is None

        await _wait_until(
            _record_removed,
            label="entry removal to appear on replica",
        )

        await primary_bucket.remove()

        async def _bucket_removed():
            return not await _bucket_exists(replica_client, bucket_name)

        await _wait_until(
            _bucket_removed,
            label="bucket removal to appear on replica",
        )
