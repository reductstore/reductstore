import asyncio
import json
import time
from hashlib import md5

from reduct import Client, Bucket, ServerInfo, BucketSettings
from os import getenv
import random

NUMBER_OF_ENTRIES = int(getenv("NUMBER_OF_ENTRIES", 4))
NUMBER_OF_RECORDS = int(getenv("NUMBER_OF_RECORDS", 1536))
MAX_BLOB_SIZE = int(1024 * 1024 * float(getenv("MAX_BLOB_SIZE", 1)))

BLOB = random.randbytes(MAX_BLOB_SIZE)
BUCKET_NAME = getenv("BUCKET_NAME", "data_check")


async def load():
    start_at = time.time()
    async with Client(
        "http://127.0.0.1:8383", api_token=getenv("RS_API_TOKEN")
    ) as client:
        report = {}
        bucket: Bucket = await client.create_bucket(
            BUCKET_NAME,
            exist_ok=True,
            settings=BucketSettings(
                max_block_records=NUMBER_OF_RECORDS // 4 + random.randint(-10, 10)
            ),
        )

        tasks = []

        async def write_entry(entry_i):
            for j in range(NUMBER_OF_RECORDS):
                entry_name = f"entry_{entry_i}"
                data = BLOB[: random.randint(1, MAX_BLOB_SIZE)]
                md5_hash = md5(data).hexdigest()
                ts = time.time()
                await bucket.write(
                    entry_name,
                    data,
                    timestamp=ts,
                    labels={
                        "md5": md5_hash,
                        "entry": entry_name,
                        "record": j,
                        "ts": ts,
                    },
                )

                if j % 32 == 0:
                    print(f"Written {j} records for {entry_name}")

        for i in range(NUMBER_OF_ENTRIES):
            tasks.append(write_entry(i))

        await asyncio.gather(*tasks)

        report["server"] = (await client.info()).model_dump(mode="json")
        del report["server"]["defaults"]
        report["bucket"] = (await bucket.get_full_info()).model_dump(mode="json")
        del report["bucket"]["settings"]
        report["elapsed"] = time.time() - start_at
        return report


if __name__ == "__main__":
    report = asyncio.run(load())
    with open("report.json", "w") as f:
        json.dump(report, f, indent=4)
