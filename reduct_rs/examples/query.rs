// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use bytes::Bytes;
use futures_util::StreamExt;
use reduct_rs::{ReductClient, ReductError};
use std::str::from_utf8;

#[tokio::main]
async fn main() -> Result<(), ReductError> {
    let client = ReductClient::builder().url("http://127.0.0.1:8383").build();

    println!("Server v{:?}", client.server_info().await?.version);

    let bucket = client.create_bucket("test").exist_ok(true).send().await?;

    bucket
        .write_record("entry-1")
        .add_label("planet", "Earth")
        .data(Bytes::from("Hello, Earth!"))
        .send()
        .await?;

    bucket
        .write_record("entry-1")
        .add_label("planet", "Mars")
        .data(Bytes::from("Hello, Mars!"))
        .send()
        .await?;

    let query = bucket
        .query("entry-1")
        .add_include("planet", "Earth")
        .send()
        .await?;

    tokio::pin!(query);

    while let Some(record) = query.next().await {
        let record = record?;
        println!("Record: {:?}", record);
        println!("Data: {}", from_utf8(&record.bytes().await?).unwrap());
    }

    Ok(())
}
