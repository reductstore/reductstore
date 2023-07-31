// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::record::{from_system_time, Labels};
use bytes::Bytes;

use futures::TryStream;

use reqwest::header::{CONTENT_LENGTH, CONTENT_TYPE};
use reqwest::{Body, Method};

use std::sync::Arc;
use std::time::SystemTime;

pub struct WriterRecordBuilder {
    bucket: String,
    entry: String,
    timestamp: Option<u64>,
    labels: Labels,
    content_type: String,
    content_length: Option<u64>,
    data: Option<Body>,
    client: Arc<HttpClient>,
}

impl WriterRecordBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            timestamp: None,
            labels: Labels::new(),
            content_type: "application/octet-stream".to_string(),
            content_length: None,
            data: None,
            bucket,
            entry,
            client,
        }
    }

    pub fn timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = Some(from_system_time(timestamp));
        self
    }

    pub fn unix_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn labels(mut self, labels: Labels) -> Self {
        self.labels = labels;
        self
    }

    pub fn content_type(mut self, content_type: &str) -> Self {
        self.content_type = content_type.to_string();
        self
    }

    pub fn content_length(mut self, content_length: u64) -> Self {
        self.content_length = Some(content_length);
        self
    }

    pub fn data(mut self, data: Bytes) -> Self {
        self.data = Some(data.into());
        self
    }

    pub fn stream<S>(mut self, stream: S) -> Self
    where
        S: TryStream + Send + Sync + 'static,
        S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<S::Ok>,
    {
        self.data = Some(Body::wrap_stream(stream));
        self
    }

    pub async fn write(self) -> Result<(), Box<dyn std::error::Error>> {
        let timestamp = self
            .timestamp
            .unwrap_or_else(|| from_system_time(SystemTime::now()));

        let mut request = self.client.request(
            Method::POST,
            &format!("/b/{}/{}?ts={}", self.bucket, self.entry, timestamp),
        );

        for (key, value) in self.labels {
            request = request.header(&format!("x-reduct-label-{}", key), value);
        }

        request = request.header(CONTENT_TYPE, self.content_type);
        if let Some(content_length) = self.content_length {
            request = request.header(CONTENT_LENGTH, content_length);
        }

        if let Some(data) = self.data {
            request = request.body(data);
        } else {
            request = request.header(CONTENT_LENGTH, 0);
        }

        self.client.send_request(request).await?;
        Ok(())
    }
}
