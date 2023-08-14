# ReductStore

## A high-performance time series database for blob data

[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/reductstore/reductstore)](https://github.com/reductstore/reductstore/releases/latest)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reductstore/ci.yml?branch=main)](https://github.com/reductstore/reductstore/actions)
[![Docker Pulls](https://img.shields.io/docker/pulls/reduct/store)](https://hub.docker.com/r/reduct/store)
[![GitHub all releases](https://img.shields.io/github/downloads/reductstore/reductstore/total)](https://github.com/reductstore/reductstore/releases/latest)
[![Discord](https://img.shields.io/discord/939475547065561088)](https://discord.gg/8wPtPGJYsn)


ReductStore is a time series database that is specifically designed for storing and managing large amounts of blob data. It boasts high performance for both writing and real-time querying, with the added benefit of batching data. This makes it an ideal solution for edge computing, computer vision, and IoT applications where network latency is a concern. For more information, please visit [https://www.reduct.store/](https://www.reduct.store/).

## Why Does It Exist?

There are numerous time-series databases available in the market that provide remarkable functionality and scalability. However, all of them concentrate on numeric data and have limited support for unstructured data, which may be represented as strings.

On the other hand, S3-like object storage solutions could be the best place to keep blob objects, but they don't provide an API to work with data in the time domain.

There are many kinds of applications where we need to collect unstructured data such as images, high-frequency sensor data, binary packages, or huge text documents and provide access to their history.
Many companies build a storage solution for these applications based on a combination of TSDB and Blob storage in-house. It might be a working solution; however, it is a challenging development task to keep data integrity in both databases, implement retention policies, and provide data access with good performance.

The ReductStore project aims to solve the problem of providing a complete solution for applications that require unstructured data to be stored and accessed at specific time intervals.
It guarantees that your data will not overflow your hard disk and batches records to reduce the number of critical HTTP requests for networks with high latency.

**All of these features make the database the right choice for edge computing and IoT applications if you want to avoid development costs for your in-house solution.**

## Features

- Storing and accessing unstructured data as time series
- No limit for maximum size of blob
- Real-time FIFO bucket quota based on size to avoid disk space shortage
- HTTP(S) API
- Optimized for small objects (less than 1 MB)
- Labeling data for annotation and filtering
- Iterative data querying
- Batching records in an HTTP response
- Embedded Web Console
- Token authorization for managing data access

## Get Started

The quickest way to get up and running is with our Docker image:

```
docker run -p 8383:8383 -v ${PWD}/data:/data reduct/store:latest
```

Alternatively, you can opt for Cargo:

```
apt install protobuf-compiler
cargo install reductstore
RS_DATA_PATH=./data reductstore
```

For a more in-depth guide, visit the **[Getting Started](https://docs.reduct.store/)** and **[Download](https://www.reduct.store/download)** sections.

After initializing the instance, dive in with one of our **[Client SDKs](#client-sdks)** to write or retrieve data. To illustrate, here's a Python sample:

```python
import time
import asyncio
from reduct import Client, Bucket

async def main():
    # Create a client for interacting with a ReductStore service
    async with Client("http://localhost:8383") as client:
        # Create a bucket and store a reference to it in the `bucket` variable
        bucket: Bucket = await client.create_bucket("my-bucket", exist_ok=True)

        # Write data to the bucket
        ts = time.time_ns() / 1000
        await bucket.write("entry-1", b"Hey!!", ts)

        # Read data from the bucket
        async with bucket.read("entry-1", ts) as record:
            data = await record.read_all()
            print(data)

# Run the main function
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
```

## Client SDKs

ReductStore is built with adaptability in mind. While it comes with a straightforward HTTP API that can be integrated into virtually any environment, we understand that not everyone wants to interact with the API directly.
To streamline your development process and make integrations smoother, we've developed a series of client SDKs tailored for different programming languages and environments. These SDKs wrap around the core API, offering a more intuitive and language-native way to interact with ReductStore, thus accelerating your development cycle.
Here are the client SDKs available:

- [Rust Client SDK](https://github.com/reductstore/reductstore/tree/main/reduct_rs)
- [Python Client SDK](https://github.com/reductstore/reduct-py)
- [JavaScript Client SDK](https://github.com/reductstore/reduct-js)
- [C++ Client SDK](https://github.com/reductstore/reduct-cpp)

## Tools

ReductStore is not just about data storage; it's about simplifying and enhancing your data management experience. Along with its robust core features, ReductStore offers a suite of tools to streamline administration, monitoring, and optimization. Here are the key tools you can leverage:

- [CLI Client](https://github.com/reductstore/reduct-cli) - a command-line interface for direct interactions with ReductStore
- [Web Console](https://github.com/reductstore/web-console) - a web interface to administrate a ReductStore instance

## **Feedback & Contribution**

Your input is invaluable to us! 🌟 If you've found a bug, have suggestions for improvements, or want to contribute directly to the codebase, here's how you can help:

- **Discord**: Join our [**Discord community**](https://discord.gg/BWrCncF5EP) to discuss, share ideas, and collaborate with fellow ReductStore users.
- **Feedback & Bug Reports**: Open an issue on our **[GitHub repository](https://github.com/reductstore/reductstore/issues)**. Please provide as much detail as possible so we can address it effectively.
- **Contribute**: ReductStore is an open-source project. We encourage and welcome contributions.

## **Get Involved**

We believe in the power of community and collaboration. If you've built something amazing with ReductStore, we'd love to hear about it! Share your projects, experiences, and insights on our [Discord community](https://discord.gg/BWrCncF5EP).

If you find ReductStore beneficial, give us a ⭐ on our GitHub repository.

Your support fuels our passion and drives us to keep improving.

Together, let's redefine the future of blob data storage! 🚀

## **Frequently Asked Questions (FAQ)**

**Q1: What sets ReductStore apart from other time-series databases?**

A1: ReductStore is specially designed for storing and managing large amounts of blob data, optimized for both high performance and real-time querying. Unlike other databases that focus primarily on numeric data, ReductStore excels in handling unstructured data, making it ideal for various applications like edge computing and IoT.

**Q2: How do I get started with ReductStore?**

A2: You can easily set up ReductStore using our Docker image or by using cargo. Detailed instructions are provided in the **[Getting Started](https://docs.reduct.store/)** section.

**Q3: Is there any size limitation for the blob data?**

A3: While ReductStore is optimized for small objects (less than 1 MB), there's no hard limit for the maximum size of a blob.

**Q4: Can I integrate ReductStore with my current infrastructure?**

A4: Absolutely! With our variety of client SDKs and its adaptable HTTP API, ReductStore can be integrated into almost any environment.

**Q5: I'm facing issues with the installation. Where can I get help?**

A5: We recommend checking out our **[documentation](https://docs.reduct.store/)**. If you still face issues, feel free to join our Discord community or raise an issue on our GitHub repository.
