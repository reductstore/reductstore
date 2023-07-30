// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use bytes::Bytes;
use chrono::format::Item;
use futures::stream::Stream;
use futures::TryStream;
use reduct_base::error::HttpError;
use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Method};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

pub type Labels = HashMap<String, String>;
type StreamData = Box<
    dyn TryStream<Item = Result<Bytes, HttpError>, Ok = Bytes, Error = HttpError>
        + Send
        + Sync
        + Unpin
        + 'static,
>;

pub struct WriteRecord {
    timestamp: u64,
    labels: Labels,
    content_type: String,
    content_length: Option<u64>,
    data: Option<Body>,
}

impl Default for WriteRecord {
    fn default() -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64,
            labels: Labels::new(),
            content_type: "application/octet-stream".to_string(),
            content_length: None,
            data: None,
        }
    }
}

pub struct WriterRecordBuilder {
    record: WriteRecord,
    bucket: String,
    entry: String,
    client: Arc<HttpClient>,
}

impl WriterRecordBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            record: WriteRecord::default(),
            bucket,
            entry,
            client,
        }
    }

    pub fn timestamp(mut self, timestamp: SystemTime) -> Self {
        self.record.timestamp = timestamp
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        self
    }

    pub fn unix_timestamp(mut self, timestamp: u64) -> Self {
        self.record.timestamp = timestamp;
        self
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.record.labels = labels;
        self
    }

    pub fn content_type(mut self, content_type: String) -> Self {
        self.record.content_type = content_type;
        self
    }

    pub fn content_length(mut self, content_length: u64) -> Self {
        self.record.content_length = Some(content_length);
        self
    }

    pub fn data(mut self, data: Bytes) -> Self {
        self.record.data = Some(data.into());
        self
    }

    pub fn stream<S>(mut self, stream: S) -> Self
    where
        S: TryStream + Send + Sync + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<S::Ok>,
    {
        self.record.data = Some(Body::wrap_stream(stream));
        self
    }

    pub async fn write(self) -> Result<(), Box<dyn std::error::Error>> {
        let mut request = self.client.request(
            Method::POST,
            &format!(
                "/b/{}/{}?ts={}",
                self.bucket, self.entry, self.record.timestamp
            ),
        );
        for (key, value) in self.record.labels {
            request = request.header(key, value);
        }

        request = request.header(CONTENT_TYPE, self.record.content_type);
        if let Some(content_length) = self.record.content_length {
            request = request.header(CONTENT_LENGTH, content_length);
        }

        if let Some(data) = self.record.data {
            request = request.body(data);
        }

        self.client.send_request(request).await?;
        Ok(())
    }
}
