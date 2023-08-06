# ReductStore Client SDK for Rust

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/reductstore/reduct-rs/ci.yml?branch=main)](https://github.com/reductstore/reduct-rs/actions)

This package provides an HTTP client for interacting with the [ReductStore](https://www.reduct.store), time-series
database for unstructured data.

## Features

* Supports the [ReductStore HTTP API v1.6](https://docs.reduct.store/http-api)
* Built on top of [reqwest](https://github.com/seanmonstar/reqwest)
* Asynchronous

## Install

TODO

## Example

```rust
use std::str::from_utf8;
use std::time::SystemTime;
use bytes::Bytes;
use futures_util::StreamExt;
use reduct_rs::{ReductClient};
use tokio;
use reduct_base::error::HttpError;


#[tokio::main]
async fn main() -> Result<(), HttpError> {
    let client = ReductClient::builder()
        .url("http://127.0.0.1:8383")
        .api_token("TOKEN")
        .build();

    println!("Server v{:?}", client.server_info().await?.version);

    let bucket = client.create_bucket("test" )
        .exist_ok(true)
        .send()
        .await?;

    bucket.write_record("entry-1")
        .add_label("planet", "Earth")
        .data(Bytes::from("Hello, Earth!"))
        .send()
        .await?;

    bucket.write_record("entry-1")
        .add_label("planet", "Mars")
        .data(Bytes::from("Hello, Mars!"))
        .send()
        .await?;


    let mut query = bucket.query("entry-1")
        .add_include("planet", "Earth")
        .send().await?;

    tokio::pin!(query);

    while let Some(record) = query.next().await {
        let record = record?;
        println!("Record: {:?}", record);
        println!("Data: {}", from_utf8(&record.bytes().await?.to_vec()).unwrap());
    }

    Ok(())
}
```

## References

* Documentation - TODO
* [ReductStore HTTP API](https://docs.reduct.store/http-api)
