// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::http_client::HttpClient;
use crate::record::from_system_time;
use crate::{Labels, Record, RecordStream};
use async_channel::{unbounded, Receiver};
use async_stream::stream;
use bytes::Bytes;
use bytes::BytesMut;
use chrono::Duration;
use futures::Stream;
use futures_util::{pin_mut, StreamExt};
use reduct_base::error::HttpError;
use reduct_base::msg::entry_api::QueryInfo;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Method, Request, RequestBuilder};
use std::hash::Hash;
use std::io::Read;
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

    /// Add a label to include in the query.
    pub fn add_include(mut self, key: &str, value: &str) -> Self {
        if let Some(mut labels) = self.include {
            labels.insert(key.to_string(), value.to_string());
            self.include = Some(labels);
        } else {
            let mut labels = Labels::new();
            labels.insert(key.to_string(), value.to_string());
            self.include = Some(labels);
        }
        self
    }

    /// Set the labels to exclude from the query.
    pub fn exclude(mut self, labels: Labels) -> Self {
        self.exclude = Some(labels);
        self
    }

    /// Add a label to exclude from the query.
    pub fn add_exclude(mut self, key: &str, value: &str) -> Self {
        if let Some(mut labels) = self.exclude {
            labels.insert(key.to_string(), value.to_string());
            self.exclude = Some(labels);
        } else {
            let mut labels = Labels::new();
            labels.insert(key.to_string(), value.to_string());
            self.exclude = Some(labels);
        }
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
    /// default: false
    pub fn head_only(mut self, head_only: bool) -> Self {
        self.head_only = head_only;
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
                let method = if head_only { Method::HEAD } else { Method::GET };
                let request = client.request(method, &format!("/b/{}/{}/batch?q={}", bucket, entry, response.id));
                let response = client.send_request(request).await?;

                if response.status() == reqwest::StatusCode::NO_CONTENT {
                    break;
                }

                let headers = response.headers().clone();


                let (mut tx, rx) = unbounded();
                tokio::spawn(async move {
                    let mut stream = response.bytes_stream();
                    while let Some(bytes) = stream.next().await {
                        if let Err(_) = tx.send(bytes.unwrap()).await {
                            break;
                        }
                    }
                });

                let stream = parse_batched_records(headers, rx, head_only).await?;
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
    headers: HeaderMap,
    mut rx: Receiver<Bytes>,
    head_only: bool,
) -> Result<impl Stream<Item = Result<(Record, bool), HttpError>>, HttpError> {
    //sort headers by names
    let mut sorted_headers: Vec<_> = headers
        .clone()
        .into_iter()
        .map(|(key, value)| (key.unwrap().as_str().to_string(), value))
        .collect();
    sorted_headers.sort_by(|(name1, _), (name2, _)| name1.cmp(name2));

    let records_total = sorted_headers
        .iter()
        .filter(|(name, _)| name.starts_with("x-reduct-time-"))
        .count();
    let mut records_count = 0;

    Ok(stream! {
        let mut rest_data = BytesMut::new();
        for (name, value) in sorted_headers {
            if name.starts_with("x-reduct-time-") {
                let timestamp = name[14..].parse::<u64>().unwrap();
                let (content_length, content_type, labels) = parse_header_as_csv_row(value.to_str().unwrap());
                let last =  headers.get("x-reduct-last") == Some(&HeaderValue::from_str("true").unwrap());

                records_count += 1;

                let data: Option<RecordStream> = if head_only {
                    None
                } else {
                    if records_count == records_total {
                        // last record in batched records read in client code
                        let first_chunk: Bytes = rest_data.clone().into();
                        let rx = rx.clone();

                        Some(Box::pin(stream! {
                            yield Ok(first_chunk);
                            while let Ok(bytes) = rx.recv().await {
                                yield Ok(bytes.into());
                            }
                        }))
                    } else {
                        // batched records must be read in order, so it is safe to read them here
                        // instead of reading them in the use code with an async interator.
                        // The batched records are small if they are not the last.
                        // The last batched record is read in the async generator in chunks.
                        let mut data = rest_data.clone();
                        while let Ok(bytes) = rx.recv().await {
                            data.extend_from_slice(&bytes);

                            if data.len() >= content_length {
                                break;
                            }
                        }

                        rest_data = data.split_off(content_length);
                        data.truncate(content_length);

                        Some(Box::pin(stream! {
                            yield Ok(data.into());
                        }))
                    }
                };

                yield Ok((Record {
                    timestamp,
                    labels,
                    content_type,
                    content_length,
                    data
                }, last));
            }
        }
    })
}

fn parse_header_as_csv_row(header: &str) -> (usize, String, Labels) {
    let (content_length, rest) = header.split_once(",").unwrap();
    let content_length = content_length.trim().parse::<usize>().unwrap();

    let (content_type, rest) = rest.split_once(",").unwrap_or((rest, ""));
    let content_type = content_type.trim().to_string();

    let mut labels = Labels::new();
    let mut rest = rest.to_string();
    while let Some(pair) = rest.split_once("=") {
        let (key, value) = pair;
        rest = if value.starts_with("\"") {
            let value = value[1..].to_string();
            let (value, rest) = value.split_once("\"").unwrap();
            labels.insert(key.trim().to_string(), value.trim().to_string());
            rest.trim_start_matches(",").trim().to_string()
        } else {
            if let Some(ret) = value.split_once(",") {
                let (value, rest) = ret;
                labels.insert(key.trim().to_string(), value.trim().to_string());
                rest.trim().to_string()
            } else {
                labels.insert(key.trim().to_string(), value.trim().to_string());
                break;
            }
        };
    }

    (content_length, content_type, labels)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    fn test_parse_header_as_csv_row() {
        let header = "123, text/plain, label1=value1, label2=value2";
        let (content_length, content_type, labels) = parse_header_as_csv_row(&header);
        assert_eq!(content_length, 123);
        assert_eq!(content_type, "text/plain");
        assert_eq!(labels.len(), 2);
        assert_eq!(labels.get("label1"), Some(&"value1".to_string()));
        assert_eq!(labels.get("label2"), Some(&"value2".to_string()));
    }

    #[rstest]
    fn test_parse_header_as_csv_row_quotes() {
        let header = "123, text/plain, label1=\"[1, 2, 3]\", label2=\"value2\"";
        let (content_length, content_type, labels) = parse_header_as_csv_row(&header);
        assert_eq!(content_length, 123);
        assert_eq!(content_type, "text/plain");
        assert_eq!(labels.len(), 2);
        assert_eq!(labels.get("label1"), Some(&"[1, 2, 3]".to_string()));
        assert_eq!(labels.get("label2"), Some(&"value2".to_string()));
    }

    #[rstest]
    fn test_parse_header_no_labels() {
        let header = "123, text/plain";
        let (content_length, content_type, labels) = parse_header_as_csv_row(&header);
        assert_eq!(content_length, 123);
        assert_eq!(content_type, "text/plain");
        assert_eq!(labels.len(), 0);
    }
}
