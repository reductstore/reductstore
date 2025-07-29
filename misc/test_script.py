from datetime import datetime

from reduct import Client, BucketSettings, QuotaType


async def main():
    async with Client("http://localhost:8383", api_token="my-token") as client:
        bucket = await client.create_bucket(
            "my-bucket",
            BucketSettings(quota_type=QuotaType.FIFO, quota_size=1_000_000_000),
            exist_ok=True,
        )

        # 3. Write some data with timestamps in the 'entry-1' entry
        now = datetime.now().timestamp()
        await bucket.write(
            "sensor-1",
            b"<Blob data>",
            timestamp=now,
            labels={"score": 10, "label2": "value2"},
        )
        await bucket.write(
            "sensor-1",
            b"<Blob data>",
            timestamp=now + 1,
            labels={"score": 20, "label2": "value2"},
        )

        when = {"#select_labels": ["score"]}
        async for record in bucket.query("sensor-1", start=now, when=when):
            print(f"Record labels: {record.labels}")
            print(await record.read_all())


# 5. Run the main function
if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
