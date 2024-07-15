import asyncio
import json
import time
from hashlib import md5

from reduct import Client, Bucket, ServerInfo
from os import getenv
import random


async def check():
    start_at = time.time()
    with open("report.json") as f:
        report = json.load(f)

    async with Client(
        "http://127.0.0.1:8383", api_token=getenv("RS_API_TOKEN")
    ) as client:
        bucket: Bucket = await client.get_bucket(report["bucket"]["info"]["name"])

        tasks = []

        async def check_entry(entry_i):
            count = 0
            entry_name = f"entry_{entry_i}"
            async for record in bucket.query(entry_name):
                data = await record.read_all()
                md5_hash = md5(data).hexdigest()

                assert record.size == len(data)  # Check that the size is correct
                assert (
                    record.labels["md5"] == md5_hash
                )  # Check that the hash is correct
                assert (
                    record.labels["entry"] == entry_name
                )  # Check that the entry name is correct
                assert (
                    abs(float(record.labels["ts"]) - record.timestamp / 1000_000)
                    < 0.000001
                )  # Check that the timestamp is correct
                assert int(record.labels["record"]) == count
                count += 1

                if count % 100 == 0:
                    print(f"Check {count} records for {entry_name}")

            assert (
                count == report["bucket"]["entries"][entry_i]["record_count"]
            )  # Check that the number of records is correct

        for i in range(report["bucket"]["info"]["entry_count"]):
            tasks.append(check_entry(i))

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(check())
