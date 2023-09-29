// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::Record;
use async_stream::stream;
use futures_util::StreamExt;
use reqwest::header::{HeaderValue, CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Method};
use std::collections::VecDeque;

use std::sync::Arc;

/// Builder for writing multiple records in a single request.
pub struct WriteBatchBuilder {
    bucket: String,
    entry: String,
    records: VecDeque<Record>,
    client: Arc<HttpClient>,
}

impl WriteBatchBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            bucket,
            entry,
            records: VecDeque::new(),
            client,
        }
    }

    /// Add a record to the batch.
    pub fn add_record(mut self, record: Record) -> Self {
        self.records.push_back(record);
        self
    }

    /// Add records to the batch.
    pub fn add_records(mut self, records: Vec<Record>) -> Self {
        self.records.extend(records);
        self
    }

    /// Build the request and send it to the server.
    pub async fn send(mut self) -> Result<(), crate::ReductError> {
        let request = self.client.request(
            Method::POST,
            &format!("/b/{}/{}/batch", self.bucket, self.entry),
        );

        let content_length: usize = self.records.iter().map(|r| r.content_length()).sum();

        let mut request = request
            .header(
                CONTENT_TYPE,
                HeaderValue::from_str("application/octet-stream").unwrap(),
            )
            .header(
                CONTENT_LENGTH,
                HeaderValue::from_str(&content_length.to_string()).unwrap(),
            );

        for record in &self.records {
            let mut header_values = Vec::new();
            header_values.push(record.content_length().to_string());
            header_values.push(record.content_type().to_string());
            if !record.labels().is_empty() {
                for (key, value) in record.labels() {
                    if value.contains(',') {
                        header_values.push(format!("{}=\"{}\"", key, value));
                    } else {
                        header_values.push(format!("{}={}", key, value));
                    }
                }
            }

            request = request.header(
                &format!("x-reduct-time-{}", record.timestamp_us()),
                HeaderValue::from_str(&header_values.join(",").to_string()).unwrap(),
            );
        }

        let client = Arc::clone(&self.client);

        let stream = stream! {
             while let Some(record) = self.records.pop_front() {
                 let mut stream = record.stream_bytes();
                 while let Some(bytes) = stream.next().await {
                     yield bytes;
                 }
             }
        };

        let _ = client
            .send_request(request.body(Body::wrap_stream(stream)))
            .await?;
        // todo: check response

        Ok(())
    }
}
