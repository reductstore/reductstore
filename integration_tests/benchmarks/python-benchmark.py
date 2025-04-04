import argparse
import asyncio
import sys
import time
from datetime import datetime
from pathlib import Path

from reduct import Batch, Bucket, Client, BucketSettings

RECORD_SIZES = [1000, 100_000, 10_000_000]

MAX_BATCH_SIZE = 8_000_000
MAX_BATCH_RECORDS = 80


class Result:
    write_req_per_sec: float
    write_bytes_per_sec: float
    read_req_per_sec: float
    read_bytes_per_sec: float
    record_size: int
    record_num: int

    def __init__(self):
        self.write_req_per_sec = 0
        self.write_bytes_per_sec = 0
        self.read_req_per_sec = 0
        self.read_bytes_per_sec = 0
        self.update_req_per_sec = 0
        self.remove_req_per_sec = 0
        self.record_size = 0
        self.record_num = 0

    def __str__(self):
        return (
            f"Record size: {self.record_size // 1024} KiB, Record num: {self.record_num}, "
            f"Write req/s: {self.write_req_per_sec}, Write KiB/s: {self.write_bytes_per_sec // 1024}, "
            f"Read req/s: {self.read_req_per_sec}, Read KiB/s: {self.read_bytes_per_sec // 1024}, "
            f"Update req/s: {self.update_req_per_sec}, "
            f"Remove req/s: {self.remove_req_per_sec}"
        )

    def to_csv(self):
        return (
            f"{self.record_size},{self.record_num},{self.write_req_per_sec},"
            f"{self.write_bytes_per_sec},{self.read_req_per_sec},{self.read_bytes_per_sec},"
            f"{self.update_req_per_sec},{self.remove_req_per_sec}"
        )


async def bench(url: str, record_size: int, record_num: int) -> Result:
    """Run benchmark with given record size and record number."""

    result = Result()
    result.record_size = record_size
    result.record_num = record_num

    measure_time = time.time_ns() // 1000
    async with Client(url, api_token="token") as client:
        bucket: Bucket = await client.create_bucket(f"benchmark-{measure_time}")
        record_data = b"0" * record_size
        start_time = datetime.now()

        print("bucket name:", bucket.name)
        # Write
        batch = Batch()
        for i in range(record_num):
            batch.add(i, record_data, labels={"key": "value", "index": str(i)})
            if len(batch) >= MAX_BATCH_RECORDS or batch.size >= MAX_BATCH_SIZE:
                await bucket.write_batch("python-bench", batch)
                batch.clear()

        if len(batch) > 0:
            await bucket.write_batch("python-bench", batch)

        # Save result
        delta = (datetime.now() - start_time).total_seconds()
        result.write_req_per_sec = int(record_num / delta)
        result.write_bytes_per_sec = int(record_num * record_size / delta)

        # Read
        start_time = datetime.now()
        count = 0
        record_count = 0
        async for record in bucket.query("python-bench"):
            count += len(await record.read_all())
            record_count += 1

        if record_count != record_num:
            raise Exception(
                f"Read {record_count} records, expected {record_num} records."
            )

        if count != record_num * record_size:
            raise Exception(
                f"Read {count} bytes, expected {record_num * record_size} bytes."
            )

        # Save result
        delta = (datetime.now() - start_time).total_seconds()
        result.read_req_per_sec = int(record_num / delta)
        result.read_bytes_per_sec = int(record_num * record_size / delta)

        # Update metadata (labels)
        start_time = datetime.now()
        batch = Batch()
        for i in range(record_num):
            batch.add(i, labels={"key": "new-value"})
            if len(batch) >= MAX_BATCH_RECORDS or batch.size >= MAX_BATCH_SIZE:
                await bucket.update_batch("python-bench", batch)
                batch.clear()

        if len(batch) > 0:
            errors = await bucket.update_batch("python-bench", batch)
            assert not errors

        # Save result
        delta = (datetime.now() - start_time).total_seconds()
        result.update_req_per_sec = int(record_num / delta)

        # Remove
        start_time = datetime.now()
        removed_records = await bucket.remove_query(
            "python-bench", start=0, stop=record_num
        )
        assert removed_records == record_num

        # Save result
        delta = (datetime.now() - start_time).total_seconds()
        result.remove_req_per_sec = int(record_num / delta)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run benchmark")
    parser.add_argument("url", type=str, help="URL of the server")
    parser.add_argument("output", type=Path, help="path to the output CSV file")
    args = parser.parse_args()

    # Check and create output directory
    directory = args.output.parent
    try:
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    loop = asyncio.get_event_loop()
    with open(args.output, "w") as f:
        for record_size in RECORD_SIZES:
            num = min(10000, 1000_000_000 // record_size)
            result = loop.run_until_complete(bench(args.url, record_size, num))
            print(result)
            f.write(result.to_csv() + "\n")
