// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::record::from_system_time;
use crate::{Labels, Record, RecordStream};
use chrono::Duration;
use futures::Stream;
use futures_util::{pin_mut, StreamExt};

use crate::http_client::HttpClient;
use async_stream::stream;
use bytes::Bytes;
use reduct_base::error::HttpError;
use reduct_base::msg::entry_api::QueryInfo;
use reqwest::header::HeaderValue;
use reqwest::{Method, Request, RequestBuilder};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::SystemTime;

pub struct Query {
    id: u64,
    bucket: String,
    entry: String,
    client: Arc<HttpClient>,
}

/// Builder for a query request.
pub struct QueryBuilder {
    start: Option<u64>,
    stop: Option<u64>,
    include: Option<Labels>,
    exclude: Option<Labels>,
    ttl: Option<Duration>,
    continuous: bool,
    head_only: bool,

    bucket: String,
    entry: String,
    client: Arc<HttpClient>,
}

impl QueryBuilder {
    pub(crate) fn new(bucket: String, entry: String, client: Arc<HttpClient>) -> Self {
        Self {
            start: None,
            stop: None,
            include: None,
            exclude: None,
            ttl: None,
            continuous: false,
            head_only: false,
            bucket,
            entry,
            client,
        }
    }

    /// Set the start time of the query.
    pub fn start(mut self, time: SystemTime) -> Self {
        self.start = Some(from_system_time(time));
        self
    }

    /// Set the start time of the query as a unix timestamp in microseconds.
    pub fn start_us(mut self, time_us: u64) -> Self {
        self.start = Some(time_us);
        self
    }

    /// Set the end time of the query.
    pub fn stop(mut self, time: SystemTime) -> Self {
        self.stop = Some(from_system_time(time));
        self
    }

    /// Set the end time of the query as a unix timestamp in microseconds.
    pub fn stop_us(mut self, time_us: u64) -> Self {
        self.stop = Some(time_us);
        self
    }

    /// Set the labels to include in the query.
    pub fn include(mut self, labels: Labels) -> Self {
        self.include = Some(labels);
        self
    }

    /// Set the labels to exclude from the query.
    pub fn exclude(mut self, labels: Labels) -> Self {
        self.exclude = Some(labels);
        self
    }

    /// Set TTL for the query.
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Set the query to be continuous.
    pub fn continuous(mut self) -> Self {
        self.continuous = true;
        self
    }

    /// Set the query to head only.
    pub fn head_only(mut self) -> Self {
        self.head_only = true;
        self
    }

    /// Set the query to be continuous.
    pub async fn send(
        mut self,
    ) -> Result<impl Stream<Item = Result<Record, HttpError>>, HttpError> {
        let mut url = format!("/b/{}/{}/q", self.bucket, self.entry);
        if let Some(start) = self.start {
            url.push_str(&format!("?start={}", start));
        }
        if let Some(stop) = self.stop {
            url.push_str(&format!("?stop={}", stop));
        }
        if let Some(ttl) = self.ttl {
            url.push_str(&format!("?ttl={}", ttl.num_seconds()));
        }
        if let Some(include) = self.include {
            for (key, value) in include {
                url.push_str(&format!("?include-{}={}", key, value));
            }
        }
        if let Some(exclude) = self.exclude {
            for (key, value) in exclude {
                url.push_str(&format!("?exclude-{}={}", key, value));
            }
        }
        if self.continuous {
            url.push_str("?continuous=true");
        }

        let response = self
            .client
            .send_and_receive_json::<(), QueryInfo>(Method::GET, &url, None)
            .await?;

        let Self {
            head_only,
            bucket,
            entry,
            client,
            ..
        } = self;
        Ok(stream! {
            let mut last = false;
            while !last {
                let request = client.request(Method::GET, &format!("/b/{}/{}/batch?q=/{}", bucket, entry, response.id));
                let response = client.send_request(request).await?;

                let stream = parse_batched_records(response, head_only).await?;
                pin_mut!(stream);
                while let Some(record) = stream.next().await {
                    let record = record?;
                    last = record.1;
                    yield Ok(record.0);
                }
            }
        })
    }
}

async fn parse_batched_records(
    response: reqwest::Response,
    head_only: bool,
) -> Result<impl Stream<Item = Result<(Record, bool), HttpError>>, HttpError> {
    let records_total = response
        .headers()
        .iter()
        .filter(|(name, _)| name.as_str().starts_with("x-reduct-time-"))
        .count();
    let mut records_count = 0;

    Ok(stream! {
        for (name, value) in response.headers().iter() {
            let name = name.as_str();
            if name.starts_with("x-reduct-time-") {
                let timestamp = name[14..].parse::<u64>().unwrap();
                let (content_length, content_type, labels) = parse_header_as_csv_row(value);

                let mut last = false;
                records_count += 1;

                let data: Option<RecordStream> = if records_count == records_total {
                    // last record in batched records read in client code
                    if response.headers().get("x-reduct-last") == Some(&HeaderValue::from_str("true").unwrap()) {
                        // last record in query
                        last = true;
                    }

                    None
                } else {
                    // batched records must be read in order, so it is safe to read them here
                    // instead of reading them in the use code with an async interator.
                    // The batched records are small if they are not the last.
                    // The last batched record is read in the async generator in chunks.
                    None
                };

                yield Ok((Record {
                    timestamp,
                    labels,
                    content_type,
                    content_length,
                    data: None
                }, last));
            }
        }
    })
}

fn parse_header_as_csv_row(header: &HeaderValue) -> (u64, String, Labels) {
    let mut items = Vec::new();
    let mut escaped = String::new();
    for item in header.to_str().unwrap().split(",") {
        if item.starts_with('"') && escaped.is_empty() {
            escaped = item[1..].to_string();
        }
        if !escaped.is_empty() {
            if item.ends_with('"') {
                escaped = escaped[..escaped.len() - 1].to_string();
                items.push(escaped.clone());
                escaped.clear();
            } else {
                escaped.push_str(item);
            }
        } else {
            items.push(item.to_string());
        }
    }

    let content_length = items[0].parse::<u64>().unwrap();
    let content_type = items[1].to_string();

    let mut labels = Labels::new();
    for label in items[2..].iter() {
        if let Some((name, value)) = label.split_once("=") {
            labels.insert(name.to_string(), value.to_string());
        }
    }

    (content_length, content_type, labels)
}
