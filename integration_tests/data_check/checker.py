import asyncio
import json
import time
from hashlib import md5
from os import getenv

from reduct import Client, Bucket

BUCKET_NAME = getenv("BUCKET_NAME", "data_check")


async def check():
    start_at = time.time()
    with open("report.json") as f:
        report = json.load(f)

    async with Client(
        "http://127.0.0.1:8383", api_token=getenv("RS_API_TOKEN")
    ) as client:
        bucket: Bucket = await client.get_bucket(BUCKET_NAME)

        tasks = []

        async def check_entry(entry_i):
            count = 0
            entry_name = f"entry_{entry_i}"
            async for record in bucket.query(entry_name):
                data = await record.read_all()
                md5_hash = md5(data).hexdigest()

                # Check that the data is correct
                if record.size != len(data):
                    print(
                        f"Size mismatch for {entry_name}: expected {len(data)}, got {record.size}"
                    )
                    exit(1)
                if record.labels["md5"] != md5_hash:
                    print(
                        f"Hash mismatch for {entry_name}: expected {md5_hash}, got {record.labels['md5']}"
                    )
                    exit(1)
                if record.labels["entry"] != entry_name:
                    print(
                        f"Entry mismatch for {entry_name}: expected {entry_name}, got {record.labels['entry']}"
                    )
                    exit(1)
                if (
                    abs(float(record.labels["ts"]) - record.timestamp / 1000_000)
                    > 0.000001
                ):
                    print(
                        f"Timestamp mismatch for {entry_name}: expected {record.timestamp / 1000_000}, got {record.labels['ts']}"
                    )
                    exit(1)
                if int(record.labels["record"]) != count:
                    print(
                        f"Record mismatch for {entry_name}: expected {count}, got {record.labels['record']}"
                    )
                    exit(1)

                count += 1

                if count % 32 == 0:
                    print(f"Check {count} records for {entry_name}")

            if count != report["bucket"]["entries"][entry_i]["record_count"]:
                print(
                    f"Total record count mismatch for {entry_name}: expected {report['bucket']['entries'][entry_i]['record_count']}, got {count}"
                )
                exit(1)

        for i in range(report["bucket"]["info"]["entry_count"]):
            tasks.append(check_entry(i))

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(check())
