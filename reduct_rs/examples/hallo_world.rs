// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use bytes::Bytes;
use reduct_rs::{ReductClient, ReductError};
use std::str::from_utf8;
use std::time::SystemTime;

#[tokio::main]
async fn main() -> Result<(), ReductError> {
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
    println!("Data: {}", from_utf8(&record.bytes().await?).unwrap());

    Ok(())
}
