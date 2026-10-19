import argparse
import asyncio
import sys
import time
from pathlib import Path

from reduct import Batch, Bucket, Client

RECORD_SIZES = [1000, 100_000, 10_000_000]

MAX_BATCH_SIZE = 8_000_000
MAX_BATCH_RECORDS = 80
SERVER_RESTART_TIMEOUT_SECONDS = 120
SERVER_READY_TIMEOUT_SECONDS = 30


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


async def wait_until_ready(url: str) -> None:
    """Wait until a restarted server accepts API requests."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + SERVER_READY_TIMEOUT_SECONDS
    last_error = None
    while loop.time() < deadline:
        try:
            async with Client(url, api_token="token") as client:
                await client.info()
                return
        except Exception as error:
            last_error = error
            await asyncio.sleep(0.1)

    raise RuntimeError(f"ReductStore did not become ready at {url}") from last_error


async def publication_barrier(container: str, url: str) -> None:
    """Gracefully restart the server so all prior mutations are durable."""

    print(f"Waiting for publication barrier via {container} restart")
    process = await asyncio.create_subprocess_exec(
        "docker",
        "restart",
        "--timeout",
        str(SERVER_RESTART_TIMEOUT_SECONDS),
        container,
        stdout=asyncio.subprocess.DEVNULL,
    )
    if await process.wait() != 0:
        raise RuntimeError(f"Failed to restart benchmark container {container}")
    await wait_until_ready(url)


async def bench(
    url: str, record_size: int, record_num: int, restart_container: str | None
) -> Result:
    """Run benchmark with given record size and record number."""

    result = Result()
    result.record_size = record_size
    result.record_num = record_num

    measure_time = time.time_ns() // 1000
    bucket_name = f"benchmark-{measure_time}"
    async with Client(url, api_token="token") as client:
        bucket: Bucket = await client.create_bucket(bucket_name)
        record_data = b"0" * record_size
        start_time = time.perf_counter()

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
        delta = time.perf_counter() - start_time
        result.write_req_per_sec = int(record_num / delta)
        result.write_bytes_per_sec = int(record_num * record_size / delta)

    if restart_container:
        await publication_barrier(restart_container, url)

    async with Client(url, api_token="token") as client:
        bucket = await client.get_bucket(bucket_name)
        # Read after all deferred write publication has completed.
        start_time = time.perf_counter()
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
        delta = time.perf_counter() - start_time
        result.read_req_per_sec = int(record_num / delta)
        result.read_bytes_per_sec = int(record_num * record_size / delta)

        # Update metadata (labels)
        start_time = time.perf_counter()
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
        delta = time.perf_counter() - start_time
        result.update_req_per_sec = int(record_num / delta)

    if restart_container:
        await publication_barrier(restart_container, url)

    async with Client(url, api_token="token") as client:
        bucket = await client.get_bucket(bucket_name)
        # Remove
        start_time = time.perf_counter()
        removed_records = await bucket.remove_query(
            "python-bench", start=0, stop=record_num, when={"$each_n": 2}
        )
        assert removed_records == record_num / 2

        # Save result
        delta = time.perf_counter() - start_time
        result.remove_req_per_sec = int(record_num / delta)

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run benchmark")
    parser.add_argument("url", type=str, help="URL of the server")
    parser.add_argument("output", type=Path, help="path to the output CSV file")
    parser.add_argument(
        "--restart-container",
        help="Docker container to restart between mutating benchmark phases",
    )
    args = parser.parse_args()

    # Check and create output directory
    directory = args.output.parent
    try:
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    with open(args.output, "w") as f:
        for record_size in RECORD_SIZES:
            num = min(10000, 1000_000_000 // record_size)
            result = asyncio.run(
                bench(args.url, record_size, num, args.restart_container)
            )
            print(result)
            f.write(result.to_csv() + "\n")
