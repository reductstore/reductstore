<p align="center">
  <a href="https://www.reduct.store">
    <img src="./docs/images/reductstore-header.jpg" alt="ReductStore banner" />
  </a>
</p>

<p align="center">
  <a href="https://github.com/reductstore/reductstore/releases/latest"><img alt="GitHub release (latest SemVer)" src="https://img.shields.io/github/v/release/reductstore/reductstore" /></a>
  <a href="https://github.com/reductstore/reductstore/actions"><img alt="GitHub Workflow Status" src="https://img.shields.io/github/actions/workflow/status/reductstore/reductstore/ci.yml?branch=main" /></a>
  <a href="https://hub.docker.com/r/reduct/store"><img alt="Docker Pulls" src="https://img.shields.io/docker/pulls/reduct/store" /></a>
  <a href="https://github.com/reductstore/reductstore/releases/latest"><img alt="GitHub all releases" src="https://img.shields.io/github/downloads/reductstore/reductstore/total" /></a>
  <a href="https://codecov.io/gh/reductstore/reductstore"><img alt="codecov" src="https://codecov.io/gh/reductstore/reductstore/branch/main/graph/badge.svg?token=8FCWEX9VSQ" /></a>
  <a href="https://community.reduct.store/signup"><img alt="Community" src="https://img.shields.io/discourse/status?server=https%3A%2F%2Fcommunity.reduct.store" /></a>
</p>

ReductStore makes robotics and industrial data queryable.

Store terabytes of images, sensor readings, logs, files, and ROS bags with timestamps and labels in one system, then query them by time range and context instead of stitching together a TSDB, object storage, and custom retention jobs.

## Why Teams Pick ReductStore

- Make binary-first robotics and industrial data queryable by time range and labels
- Built for workloads such as camera frames, sensor payloads, logs, files, and ROS bags
- Replicate only selected records to the cloud to reduce bandwidth and storage cost
- Apply quotas and lifecycle policies in the same system that stores the data

## Proof Points

<p align="center">
  <strong>🚀 60k+</strong> downloads
  &nbsp;&nbsp;•&nbsp;&nbsp;
  <strong>🏭 100+</strong> production deployments
  &nbsp;&nbsp;•&nbsp;&nbsp;
  <strong>📦 1 PB+</strong> managed data
</p>

<p align="center">
  <strong>⭐ 350+</strong> GitHub stars
  &nbsp;&nbsp;•&nbsp;&nbsp;
  <strong>🛠️ 4+</strong> years of active development
</p>

## When You Should Use It

ReductStore is ideal for scenarios where you have a continuous stream of binary data and your application has metadata that can be attached to the data as labels.
If you ingest binary data with labels into a ReductStore instance, you can use it to:

1. Query objects by time range and labels, e.g. "give me all the images from camera-1 over the last hour where the device status is 'error'".
2. Replicate only selected data to another ReductStore instance, e.g. "replicate all the data from camera-1 where the device status is 'error' to the cloud instance for further analysis".
3. Manage the data lifecycle with policies, e.g. "keep all the data from camera-1 for 30 days, then compress it and keep it for another 60 days before deleting it".

In such scenarios, ReductStore can help you manage and transfer your data efficiently based on its metadata, without combining a TSDB and blob storage. This keeps your architecture simple and efficient.

## Who It Is For

- Robotics platforms that need to replay camera frames, telemetry, and logs around incidents or model failures
- Industrial IoT pipelines that collect binary payloads at the edge and forward only filtered data upstream
- Edge AI systems that need retention, labeling, and historical retrieval without building a custom storage stack

## Get Started

The quickest way to get up and running is with Docker:

```bash
mkdir -p ./data
sudo chown -R 10001:10001 ./data
docker run -p 8383:8383 -v ${PWD}/data:/data reduct/store:latest
```

Or download a Linux binary directly from the latest release:

```bash
curl -LO https://github.com/reductstore/reductstore/releases/latest/download/reductstore.x86_64-unknown-linux-gnu.tar.gz
tar -xzf reductstore.x86_64-unknown-linux-gnu.tar.gz
mkdir -p ./data
RS_DATA_PATH=./data ./reductstore
```

For a more in-depth guide, visit the **[Getting Started](https://reduct.store/docs/)** and **[Download](https://www.reduct.store/download)** sections.

After initializing the instance, you can start writing and querying data immediately. Here's a Python sample:

```python
import asyncio

from reduct import Client

async def main():
    async with Client("http://localhost:8383") as client:
        bucket = await client.create_bucket("my-bucket", exist_ok=True)

        await bucket.write(
            "camera-1",
            b"hello, reductstore",
            timestamp="2024-01-01T10:00:00Z",
            labels={"device": "camera-1"},
        )

        async for record in bucket.query(
            "camera-1",
            start="2024-01-01T10:00:00Z",
            stop="2024-01-01T10:00:01Z",
            when={"&device": {"$eq": "camera-1"}},
        ):
            print((await record.read_all()).decode())

asyncio.run(main())
```

## Next Steps

Learn more and pick the next piece you need:

- **[Documentation](https://www.reduct.store/docs/)**
- **[Download](https://www.reduct.store/download)**
- **[Contributing Guide](./CONTRIBUTING.md)**
- **[Community Forum](https://community.reduct.store)**
- **[Good First Issues](https://github.com/reductstore/reductstore/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22good%20first%20issue%22)**

## When You Should Not Use It

ReductStore is not the best fit for every data storage problem. You should consider another solution when:

1. You have only numerical data that can be easily ingested into a time-series database.
2. You have only blob data to store and do not need to access it as historical data. In this case, object storage or a file system may be a better fit.
3. You need a message broker. Although ReductStore provides subscription and publishing functionality, it is designed for data storage and streaming, not message queueing.

## Community & Contribution

If you've found a bug, have ideas, built something with ReductStore, or want to contribute directly, here are the best places to jump in:

- **Questions and Ideas**: Join our [**Discourse community**](https://community.reduct.store) to ask questions, share ideas, and collaborate with fellow ReductStore users.
- **Bug Reports**: Open an issue on our **[GitHub repository](https://github.com/reductstore/reductstore/issues)**. Please provide as much detail as possible so we can address it effectively.
- **First Contributions**: Pick a task from [**good first issues**](https://github.com/reductstore/reductstore/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22good%20first%20issue%22) or [**help wanted**](https://github.com/reductstore/reductstore/issues?q=is%3Aissue%20is%3Aopen%20label%3A%22help%20wanted%22).
- **Pull Requests**: Open PRs only for an existing issue. Comment on the issue and get assigned before you start work. If you want to propose a new feature or workflow, open an issue first or start the discussion on the [**community forum**](https://community.reduct.store) before writing code.
- **Show Your Work**: Share your projects, benchmarks, and lessons learned on our [**Discourse community**](https://community.reduct.store).
- **Support the Project**: If ReductStore is useful to you, give us a ⭐ on GitHub.

## Contributors

Thanks to everyone who has contributed to ReductStore.

<p align="center">
  <a href="https://github.com/atimin"><img src="https://avatars.githubusercontent.com/u/67068?v=4" width="48" height="48" alt="@atimin" /></a>
  <a href="https://github.com/mother-6000"><img src="https://avatars.githubusercontent.com/u/270019311?v=4" width="48" height="48" alt="@mother-6000" /></a>
  <a href="https://github.com/AnthonyCvn"><img src="https://avatars.githubusercontent.com/u/26444489?v=4" width="48" height="48" alt="@AnthonyCvn" /></a>
  <a href="https://github.com/DibbayajyotiRoy"><img src="https://avatars.githubusercontent.com/u/125145390?v=4" width="48" height="48" alt="@DibbayajyotiRoy" /></a>
  <a href="https://github.com/rtadepalli"><img src="https://avatars.githubusercontent.com/u/105760760?v=4" width="48" height="48" alt="@rtadepalli" /></a>
  <a href="https://github.com/rohankumardubey"><img src="https://avatars.githubusercontent.com/u/82864904?v=4" width="48" height="48" alt="@rohankumardubey" /></a>
  <a href="https://github.com/tuanhungngyn"><img src="https://avatars.githubusercontent.com/u/165829382?v=4" width="48" height="48" alt="@tuanhungngyn" /></a>
  <a href="https://github.com/vbmade2000"><img src="https://avatars.githubusercontent.com/u/1904995?v=4" width="48" height="48" alt="@vbmade2000" /></a>
  <a href="https://github.com/mambaz"><img src="https://avatars.githubusercontent.com/u/3928782?v=4" width="48" height="48" alt="@mambaz" /></a>
  <a href="https://github.com/victor1234"><img src="https://avatars.githubusercontent.com/u/1102205?v=4" width="48" height="48" alt="@victor1234" /></a>
  <a href="https://github.com/aschenbecherwespe"><img src="https://avatars.githubusercontent.com/u/94011659?v=4" width="48" height="48" alt="@aschenbecherwespe" /></a>
  <a href="https://github.com/renghen"><img src="https://avatars.githubusercontent.com/u/271285?v=4" width="48" height="48" alt="@renghen" /></a>
</p>

Your support fuels our passion and drives us to keep improving.

Together, let's redefine the future of blob data storage! 🚀
