// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::{map_error, HttpClient};
use crate::record::{from_system_time, Labels, Record};
use futures_util::StreamExt;
use reduct_base::error::HttpError;
use reqwest::Method;
use std::sync::Arc;
use std::time::SystemTime;

pub struct ReadRecordBuilder {
    bucket: String,
    entry: String,
    timestamp: Option<u64>,
    client: Arc<HttpClient>,
}

impl ReadRecordBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            bucket,
            entry,
            timestamp: None,
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

    pub async fn read(&self) -> Result<Record, HttpError> {
        let mut url = format!("/b/{}/{}", self.bucket, self.entry);
        if let Some(timestamp) = self.timestamp {
            url = format!("{}?ts={}", url, &timestamp.to_string());
        }

        let request = self.client.request(Method::GET, &url);
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
            labels: labels,
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
                .parse::<u64>()
                .unwrap(),
            data: Some(Box::pin(response.bytes_stream().map(|val| match val {
                Ok(val) => Ok(val),
                Err(err) => Err(map_error(err)),
            }))),
        })
    }
}
