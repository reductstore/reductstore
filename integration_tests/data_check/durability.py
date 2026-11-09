import argparse
import asyncio
import hashlib
import json
import os
import time
from pathlib import Path

from reduct import Bucket, BucketSettings, Client


BUCKET_NAME = os.getenv("BUCKET_NAME", "durability")
ENTRY_COUNT = int(os.getenv("NUMBER_OF_ENTRIES", 4))
RECORD_SIZE = int(os.getenv("RECORD_SIZE", 64 * 1024))
ACK_LOG = Path(os.getenv("ACK_LOG", "durability-acks.jsonl"))


def payload(sequence: int) -> bytes:
    seed = sequence.to_bytes(8, "big")
    return (
        seed + hashlib.sha256(seed).digest() * ((RECORD_SIZE - len(seed) + 31) // 32)
    )[:RECORD_SIZE]


async def write(duration: float) -> None:
    deadline = time.monotonic() + duration
    sequence = 0
    async with Client(
        "http://127.0.0.1:8383", api_token=os.getenv("RS_API_TOKEN")
    ) as client:
        bucket: Bucket = await client.create_bucket(
            BUCKET_NAME,
            exist_ok=True,
            settings=BucketSettings(quota_type="NONE"),
        )
        with ACK_LOG.open("a", encoding="utf-8") as acknowledgements:
            while time.monotonic() < deadline:
                entry = f"entry_{sequence % ENTRY_COUNT}"
                timestamp = time.time()
                data = payload(sequence)
                digest = hashlib.sha256(data).hexdigest()
                await bucket.write(
                    entry,
                    data,
                    timestamp=timestamp,
                    labels={"sequence": sequence, "sha256": digest},
                )
                acknowledgements.write(
                    json.dumps(
                        {
                            "entry": entry,
                            "sequence": sequence,
                            "sha256": digest,
                            "timestamp": timestamp,
                        }
                    )
                    + "\n"
                )
                acknowledgements.flush()
                os.fsync(acknowledgements.fileno())
                sequence += 1
                await asyncio.sleep(0.01)


async def verify() -> None:
    acknowledgements = [
        json.loads(line) for line in ACK_LOG.read_text(encoding="utf-8").splitlines()
    ]
    if not acknowledgements:
        raise RuntimeError("No acknowledged records were recorded before the crash")

    failures = []
    async with Client(
        "http://127.0.0.1:8383", api_token=os.getenv("RS_API_TOKEN")
    ) as client:
        bucket: Bucket = await client.get_bucket(BUCKET_NAME)
        for acknowledgement in acknowledgements:
            try:
                async with bucket.read(
                    acknowledgement["entry"], timestamp=acknowledgement["timestamp"]
                ) as record:
                    data = await record.read_all()
                    if (
                        data != payload(acknowledgement["sequence"])
                        or hashlib.sha256(data).hexdigest() != acknowledgement["sha256"]
                    ):
                        failures.append(
                            f"corrupt record {acknowledgement['entry']}#{acknowledgement['sequence']}"
                        )
            except Exception as error:
                failures.append(
                    f"missing record {acknowledgement['entry']}#{acknowledgement['sequence']}: {error}"
                )

    if failures:
        raise RuntimeError("\n".join(failures))
    print(f"Verified {len(acknowledgements)} acknowledged records after crash recovery")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("write", "verify"))
    parser.add_argument("--duration", type=float, default=45)
    args = parser.parse_args()
    if args.command == "write":
        asyncio.run(write(args.duration))
    else:
        asyncio.run(verify())


if __name__ == "__main__":
    main()
