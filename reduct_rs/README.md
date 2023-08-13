# ReductStore Client SDK for Rust


This package provides an HTTP client for interacting with the [ReductStore](https://www.reduct.store), time-series
database for unstructured data.

## Features

* Supports the [ReductStore HTTP API v1.6](https://docs.reduct.store/http-api)
* Built on top of [reqwest](https://github.com/seanmonstar/reqwest)
* Asynchronous API

## Example

```rust

use bytes::Bytes;
use reduct_rs::{ReductClient, HttpError};
use std::str::from_utf8;
use std::time::SystemTime;

use tokio;

#[tokio::main]
async fn main() -> Result<(), HttpError> {
    let client = ReductClient::builder().url("http://127.0.0.1:8383").build();

    let timestamp = SystemTime::now();

    let bucket = client.create_bucket("test").exist_ok(true).send().await?;
    bucket
        .write_record("entry-1")
        .timestamp(timestamp)
        .data(Bytes::from("Hello, World!"))
        .send()
        .await?;

    let record = bucket
        .read_record("entry-1")
        .timestamp(timestamp)
        .send()
        .await?;

    println!("Record: {:?}", record);
    println!(
        "Data: {}",
        from_utf8(&record.bytes().await?.to_vec()).unwrap()
    );

    Ok(())
}

```

## References

* Documentation - TODO
* [ReductStore HTTP API](https://docs.reduct.store/http-api)
