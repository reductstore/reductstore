// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::{map_error, HttpClient};
use crate::record::{from_system_time, Labels, Record};
use futures_util::StreamExt;
use reduct_base::error::ReductError;
use reqwest::Method;

use std::sync::Arc;
use std::time::SystemTime;

/// Builder for a read record request.
pub struct ReadRecordBuilder {
    bucket: String,
    entry: String,
    timestamp: Option<u64>,
    head_only: bool,
    client: Arc<HttpClient>,
}

impl ReadRecordBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            bucket,
            entry,
            timestamp: None,
            head_only: false,
            client,
        }
    }

    /// Set the timestamp of the record to read.
    pub fn timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = Some(from_system_time(timestamp));
        self
    }

    /// Set the timestamp of the record to read as a unix timestamp in microseconds.
    pub fn timestamp_us(mut self, timestamp: u64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Read only the record metadata (labels, content type, content length)
    /// default: false
    pub fn head_only(mut self, head_only: bool) -> Self {
        self.head_only = head_only;
        self
    }

    /// Send the read record request.
    ///
    /// # Returns
    ///
    /// A [`Record`] object containing the record data.
    pub async fn send(self) -> Result<Record, ReductError> {
        let mut url = format!("/b/{}/{}", self.bucket, self.entry);
        if let Some(timestamp) = self.timestamp {
            url = format!("{}?ts={}", url, &timestamp.to_string());
        }

        let method = if self.head_only {
            Method::HEAD
        } else {
            Method::GET
        };

        let request = self.client.request(method, &url);
        let response = self.client.send_request(request).await?;

        let labels: Labels = response
            .headers()
            .iter()
            .filter_map(|(key, value)| {
                if key.to_string().starts_with("x-reduct-label-") {
                    Some((
                        key.as_str()[15..].to_string(),
                        value.to_str().unwrap().to_string(),
                    ))
                } else {
                    None
                }
            })
            .collect();

        Ok(Record {
            timestamp: response
                .headers()
                .get("x-reduct-time")
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<u64>()
                .unwrap(),
            labels,
            content_type: response
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string(),
            content_length: response
                .headers()
                .get("content-length")
                .unwrap()
                .to_str()
                .unwrap()
                .parse::<usize>()
                .unwrap(),
            data: if self.head_only {
                None
            } else {
                Some(Box::pin(response.bytes_stream().map(|val| match val {
                    Ok(val) => Ok(val),
                    Err(err) => Err(map_error(err)),
                })))
            },
        })
    }
}
