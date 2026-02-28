import asyncio
import random
import time
from os import getenv

from reduct import Bucket, Client
from reduct.error import ReductError

SRC_BUCKET = getenv("SRC_BUCKET_NAME", "src")
DEST_BUCKET = getenv("DEST_BUCKET_NAME", "dest")
BASE_URL = getenv("STORAGE_URL", "http://127.0.0.1:8383")
TOKEN = getenv("RS_API_TOKEN")


async def read_record(bucket: Bucket, entry: str, ts: int):
    try:
        async with bucket.read(entry, timestamp=ts) as record:
            return await record.read_all()
    except ReductError as err:
        if err.status_code in (404, 409):
            return None
        raise


async def read_record_with_labels(bucket: Bucket, entry: str, ts: int):
    try:
        async with bucket.read(entry, timestamp=ts) as record:
            return await record.read_all(), record.labels
    except ReductError as err:
        if err.status_code in (404, 409):
            return None
        raise


async def wait_until(
    predicate, label: str, timeout_s: float = 30.0, step_s: float = 0.25
):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if await predicate():
            return
        await asyncio.sleep(step_s)
    raise AssertionError(f"Timed out waiting for {label}")


async def check():
    entry = f"meta-repl/entry-{random.randint(0, 10_000_000)}"
    meta_entry = f"{entry}/$meta"
    main_ts = 1_700_000_000_000_000
    meta_ts_1 = main_ts + 10
    meta_ts_2 = main_ts + 20

    async with Client(BASE_URL, api_token=TOKEN) as client:
        src = await client.get_bucket(SRC_BUCKET)
        dest = await client.get_bucket(DEST_BUCKET)

        print("Step 1/3: create parent entry and attachment in source")
        await src.write(entry, b"main-record", timestamp=main_ts)
        await src.write(
            meta_entry,
            b'{"version":1}',
            timestamp=meta_ts_1,
            labels={"key": "$plugin"},
        )

        async def created_on_dest():
            data = await read_record(dest, meta_entry, meta_ts_1)
            return data == b'{"version":1}'

        await wait_until(created_on_dest, "meta create replication")

        print("Step 2/3: update attachment in source")
        await src.write(
            meta_entry,
            b'{"version":2}',
            timestamp=meta_ts_2,
            labels={"key": "$plugin"},
        )

        async def updated_on_dest():
            old_data = await read_record(dest, meta_entry, meta_ts_1)
            new_data = await read_record(dest, meta_entry, meta_ts_2)
            return old_data is None and new_data == b'{"version":2}'

        await wait_until(updated_on_dest, "meta update replication")

        print("Step 3/3: remove attachment by key via remove=true update")
        await src.update(
            meta_entry,
            timestamp=meta_ts_2,
            labels={"key": "$plugin", "remove": "true"},
        )

        async def tombstone_on_source():
            record = await read_record_with_labels(src, meta_entry, meta_ts_2)
            return (
                record is not None
                and record[0] == b'{"version":2}'
                and record[1].get("key") == "$plugin"
                and record[1].get("remove") == "true"
            )

        await wait_until(tombstone_on_source, "source meta tombstone by key")

        async def tombstone_on_dest():
            main_data = await read_record(dest, entry, main_ts)
            latest_meta = await read_record_with_labels(dest, meta_entry, meta_ts_2)
            return (
                main_data == b"main-record"
                and latest_meta is not None
                and latest_meta[0] == b'{"version":2}'
                and latest_meta[1].get("key") == "$plugin"
                and latest_meta[1].get("remove") == "true"
            )

        await wait_until(tombstone_on_dest, "dest meta tombstone by key")

    print("Meta replication check passed")


if __name__ == "__main__":
    asyncio.run(check())
