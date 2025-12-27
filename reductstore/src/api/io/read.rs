// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::MethodExtractor;
use crate::api::{ErrorCode, HttpError, StateKeeper};
use crate::auth::policy::ReadAccessPolicy;
use crate::ext::ext_repository::BoxedManageExtensions;
use crate::storage::bucket::Bucket;
use crate::storage::query::QueryRx;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http;
use axum::response::IntoResponse;
use axum_extra::headers::HeaderMap;
use bytes::Bytes;
use futures_util::Stream;
use log::debug;
use reduct_base::batch::v2::{encode_entry_name, encode_label_name, make_batched_header_name};
use reduct_base::error::ReductError;
use reduct_base::io::BoxedReadRecord;
use reduct_base::{no_content, unprocessable_entity};
use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::time::timeout;

const QUERY_ID_HEADER: &str = "x-reduct-query-id";
const LABELS_HEADER: &str = "x-reduct-labels";

fn parse_query_id(headers: &HeaderMap) -> Result<u64, HttpError> {
    let value = headers.get(QUERY_ID_HEADER).ok_or_else(|| {
        HttpError::from(unprocessable_entity!(
            "{} header is required for batched reads",
            QUERY_ID_HEADER
        ))
    })?;

    let value = value.to_str().map_err(|_| {
        HttpError::new(
            ErrorCode::UnprocessableEntity,
            "Query id header must be valid UTF-8",
        )
    })?;

    value
        .parse::<u64>()
        .map_err(|_| HttpError::new(ErrorCode::UnprocessableEntity, "Invalid query id"))
}

// GET /io/:bucket/read (query id provided in header)
pub(super) async fn read_batched_records(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    method: MethodExtractor,
) -> Result<impl IntoResponse, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let components = keeper
        .get_with_permissions(
            &headers,
            ReadAccessPolicy {
                bucket: bucket_name,
            },
        )
        .await?;

    let query_id = parse_query_id(&headers)?;

    fetch_and_response_batched_records(
        components.storage.get_bucket(bucket_name)?.upgrade()?,
        query_id,
        method.name() == "HEAD",
        &components.ext_repo,
    )
    .await
}

struct BatchedRecord {
    entry_index: usize,
    timestamp: u64,
    header_value: http::HeaderValue,
    reader: BoxedReadRecord,
}

#[derive(Clone)]
struct PrevMeta {
    content_type: String,
    labels: reduct_base::Labels,
}

#[derive(Default)]
struct LabelIndex {
    names: Vec<String>,
    lookup: HashMap<String, usize>,
}

impl LabelIndex {
    fn ensure(&mut self, name: &str) -> usize {
        if let Some(idx) = self.lookup.get(name) {
            return *idx;
        }

        let idx = self.names.len();
        self.names.push(name.to_string());
        self.lookup.insert(name.to_string(), idx);
        idx
    }

    fn as_header(&self) -> Option<http::HeaderValue> {
        if self.names.is_empty() {
            return None;
        }

        let encoded = self
            .names
            .iter()
            .map(|name| encode_label_name(name))
            .collect::<Vec<_>>()
            .join(",");
        Some(encoded.parse().unwrap())
    }
}

fn calculate_header_size(records: &[BatchedRecord], start_ts: u64) -> usize {
    records
        .iter()
        .map(|record| {
            let name = make_batched_header_name(record.entry_index, record.timestamp - start_ts);
            name.as_str().len() + record.header_value.to_str().unwrap().len() + 2
        })
        .sum()
}

fn make_record_header_value(
    meta: &reduct_base::io::RecordMeta,
    previous: Option<&PrevMeta>,
    label_index: &mut LabelIndex,
) -> http::HeaderValue {
    let mut parts: Vec<String> = vec![meta.content_length().to_string()];

    let mut content_type = String::new();
    match previous {
        Some(prev) if prev.content_type != meta.content_type() => {
            content_type = meta.content_type().to_string()
        }
        None => content_type = meta.content_type().to_string(),
        _ => {}
    }

    let labels_delta = build_label_delta(meta, previous.map(|p| &p.labels), label_index);
    let has_labels = !labels_delta.is_empty();

    if !content_type.is_empty() || has_labels {
        parts.push(content_type);
    }

    if has_labels {
        parts.push(labels_delta);
    }

    parts.join(",").parse().unwrap()
}

fn build_label_delta(
    meta: &reduct_base::io::RecordMeta,
    previous_labels: Option<&reduct_base::Labels>,
    label_index: &mut LabelIndex,
) -> String {
    let mut deltas: Vec<(usize, String)> = Vec::new();

    let format_value = |value: &str| {
        if value.contains(',') {
            format!("\"{}\"", value)
        } else {
            value.to_string()
        }
    };

    if let Some(prev) = previous_labels {
        let mut keys: Vec<String> = prev
            .keys()
            .chain(meta.labels().keys())
            .map(|k| k.to_string())
            .collect();
        keys.sort();
        keys.dedup();

        for key in keys {
            let prev_val = prev.get(&key);
            let curr_val = meta.labels().get(&key);
            match (prev_val, curr_val) {
                (Some(p), Some(c)) if p == c => continue,
                (Some(_), None) => {
                    let idx = label_index.ensure(&key);
                    deltas.push((idx, String::new()))
                }
                (_, Some(c)) => {
                    let idx = label_index.ensure(&key);
                    deltas.push((idx, format_value(c)))
                }
                _ => {}
            }
        }
    } else {
        for (k, v) in meta.labels().iter() {
            let idx = label_index.ensure(k);
            deltas.push((idx, format_value(v)));
        }
    }

    for (k, v) in meta.computed_labels() {
        let idx = label_index.ensure(&format!("@{}", k));
        deltas.push((idx, format_value(v)));
    }

    deltas.sort_by_key(|(idx, _)| *idx);
    deltas
        .into_iter()
        .map(|(idx, value)| format!("{}={}", idx, value))
        .collect::<Vec<_>>()
        .join(",")
}

async fn fetch_and_response_batched_records(
    bucket: Arc<Bucket>,
    query_id: u64,
    empty_body: bool,
    ext_repository: &BoxedManageExtensions,
) -> Result<impl IntoResponse, HttpError> {
    let (rx, io_settings) = bucket.get_query_receiver(query_id).await?;

    let mut entries: Vec<String> = Vec::new();
    let mut entry_indices: HashMap<String, usize> = HashMap::new();
    let mut records = Vec::new();
    let mut prev_meta: HashMap<String, PrevMeta> = HashMap::new();
    let mut label_index = LabelIndex::default();

    let mut header_size = 0usize;
    let mut body_size = 0u64;
    let mut start_ts: Option<u64> = None;
    let mut last = false;

    let bucket_name = bucket.name().to_string();
    let start_time = Instant::now();
    loop {
        let batch_of_readers = match next_record_readers(
            query_id,
            rx.upgrade()?,
            &format!("{}/{}", bucket_name, query_id),
            io_settings.batch_records_timeout,
            ext_repository,
        )
        .await
        {
            Some(value) => value,
            None => continue,
        };

        for reader in batch_of_readers {
            match reader {
                Ok(reader) => {
                    let meta = reader.meta().clone();
                    start_ts =
                        Some(start_ts.map_or(meta.timestamp(), |curr| curr.min(meta.timestamp())));
                    let entry_index = *entry_indices
                        .entry(meta.entry_name().to_string())
                        .or_insert_with(|| {
                            entries.push(meta.entry_name().to_string());
                            entries.len() - 1
                        });

                    let prev = prev_meta.get(meta.entry_name());
                    let header_value = make_record_header_value(&meta, prev, &mut label_index);
                    body_size += meta.content_length();

                    records.push(BatchedRecord {
                        entry_index,
                        timestamp: meta.timestamp(),
                        header_value,
                        reader,
                    });
                    header_size = calculate_header_size(&records, start_ts.unwrap());

                    prev_meta.insert(
                        meta.entry_name().to_string(),
                        PrevMeta {
                            content_type: meta.content_type().to_string(),
                            labels: meta.labels().clone(),
                        },
                    );
                }
                Err(err) => {
                    if records.is_empty() {
                        return Err(HttpError::from(err));
                    } else if err.status() == ErrorCode::NoContent {
                        last = true;
                        break;
                    } else {
                        return Err(HttpError::from(err));
                    }
                }
            }
        }

        if last {
            break;
        }

        if header_size > io_settings.batch_max_metadata_size
            || (!empty_body && body_size > io_settings.batch_max_size)
            || records.len() >= io_settings.batch_max_records
            || start_time.elapsed() > io_settings.batch_timeout
        {
            break;
        }
    }

    if records.is_empty() {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        match bucket.get_query_receiver(query_id).await {
            Err(err) if err.status() == ErrorCode::NotFound => {
                return Err(no_content!("No more records").into())
            }
            _ => { /* query is still alive */ }
        }
    }

    let start_ts = start_ts.unwrap_or_default();
    records.sort_by_key(|record| (record.entry_index, record.timestamp));

    let mut resp_headers = http::HeaderMap::new();
    resp_headers.insert(
        "x-reduct-entries",
        entries
            .iter()
            .map(|entry| encode_entry_name(entry))
            .collect::<Vec<_>>()
            .join(",")
            .parse()
            .unwrap(),
    );
    resp_headers.insert("x-reduct-start-ts", start_ts.to_string().parse().unwrap());
    if let Some(label_header) = label_index.as_header() {
        resp_headers.insert(LABELS_HEADER, label_header);
    }

    let mut readers_only = Vec::with_capacity(records.len());
    for record in records.into_iter() {
        let header_name = make_batched_header_name(record.entry_index, record.timestamp - start_ts);
        resp_headers.insert(header_name, record.header_value);
        readers_only.push(record.reader);
    }

    resp_headers.insert("content-length", body_size.to_string().parse().unwrap());
    resp_headers.insert("content-type", "application/octet-stream".parse().unwrap());
    resp_headers.insert("x-reduct-last", last.to_string().parse().unwrap());

    Ok((
        resp_headers,
        Body::from_stream(ReadersWrapper::new(readers_only, empty_body)),
    ))
}

// This function is used to get the next record from the query receiver
async fn next_record_readers(
    query_id: u64,
    rx: Arc<crate::core::sync::AsyncRwLock<QueryRx>>,
    query_path: &str,
    recv_timeout: std::time::Duration,
    ext_repository: &BoxedManageExtensions,
) -> Option<Vec<Result<BoxedReadRecord, ReductError>>> {
    if let Ok(result) = timeout(
        recv_timeout,
        ext_repository.fetch_and_process_record(query_id, rx),
    )
    .await
    {
        result
    } else {
        debug!("Timeout while waiting for record from query {}", query_path);
        None
    }
}

struct ReadersWrapper {
    readers: VecDeque<BoxedReadRecord>,
    empty_body: bool,
}

impl ReadersWrapper {
    fn new(readers: Vec<BoxedReadRecord>, empty_body: bool) -> Self {
        Self {
            readers: VecDeque::from(readers),
            empty_body,
        }
    }
}

impl Stream for ReadersWrapper {
    type Item = Result<Bytes, HttpError>;

    fn poll_next(
        mut self: Pin<&mut ReadersWrapper>,
        _ctx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.empty_body {
            return Poll::Ready(None);
        }

        if self.readers.is_empty() {
            return Poll::Ready(None);
        }

        while let Some(mut reader) = self.readers.pop_front() {
            match reader.read_chunk() {
                Some(Ok(bytes)) => {
                    self.readers.push_front(reader);
                    return Poll::Ready(Some(Ok(bytes)));
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(HttpError::from(err)))),
                None => continue,
            }
        }
        Poll::Ready(None)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::entry::QueryEntryAxum;
    use crate::api::io::query::query;
    use crate::api::tests::{headers, keeper, path_to_bucket_1};
    use axum::extract::Path;
    use axum::response::IntoResponse;
    use reduct_base::msg::entry_api::{QueryEntry, QueryType};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn reads_records_with_query_header(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        mut headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap();

        for (entry, ts, data) in [("entry-1", 1000u64, "aa"), ("entry-2", 1010u64, "bbb")] {
            let mut writer = bucket
                .begin_write(
                    entry,
                    ts,
                    data.len() as u64,
                    "text/plain".to_string(),
                    Default::default(),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from(data.to_string()))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        let request = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["entry-1".into(), "entry-2".into()]),
            ..Default::default()
        };
        let path = Path(path_to_bucket_1.0.clone());
        let response = query(
            State(keeper.clone()),
            headers.clone(),
            path,
            QueryEntryAxum(request),
        )
        .await
        .unwrap();
        let query_info: reduct_base::msg::entry_api::QueryInfo = response.into();

        headers.insert(
            QUERY_ID_HEADER,
            http::HeaderValue::from_str(&query_info.id.to_string()).unwrap(),
        );

        let response = read_batched_records(
            State(keeper.clone()),
            headers,
            path_to_bucket_1,
            MethodExtractor::new("GET"),
        )
        .await
        .unwrap()
        .into_response();

        let resp_headers = response.headers();
        assert_eq!(
            resp_headers["x-reduct-entries"],
            "batch-label-entry-1,batch-label-entry-2"
        );
        assert_eq!(resp_headers["x-reduct-start-ts"], "1000");
        assert!(resp_headers.contains_key("x-reduct-0-0"));
        assert!(resp_headers.contains_key("x-reduct-1-10"));
        assert_eq!(resp_headers["content-length"], "5");
        assert_eq!(resp_headers["x-reduct-last"], "true");
    }

    #[rstest]
    #[tokio::test]
    async fn writes_label_index_header(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        mut headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap();

        for (entry, ts, data, label) in [
            ("batch-label-entry-1", 1000u64, "aa", "foo"),
            ("batch-label-entry-2", 1010u64, "bbb", "bar"),
        ] {
            let mut writer = bucket
                .begin_write(
                    entry,
                    ts,
                    data.len() as u64,
                    "text/plain".to_string(),
                    [("label".to_string(), label.to_string())]
                        .into_iter()
                        .collect(),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(Bytes::from(data.to_string()))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        let request = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec![
                "batch-label-entry-1".into(),
                "batch-label-entry-2".into(),
            ]),
            ..Default::default()
        };
        let path = Path(path_to_bucket_1.0.clone());
        let response = query(
            State(keeper.clone()),
            headers.clone(),
            path,
            QueryEntryAxum(request),
        )
        .await
        .unwrap();
        let query_info: reduct_base::msg::entry_api::QueryInfo = response.into();

        headers.insert(
            QUERY_ID_HEADER,
            http::HeaderValue::from_str(&query_info.id.to_string()).unwrap(),
        );

        let response = read_batched_records(
            State(keeper.clone()),
            headers,
            path_to_bucket_1,
            MethodExtractor::new("GET"),
        )
        .await
        .unwrap()
        .into_response();

        let resp_headers = response.headers();
        assert_eq!(
            resp_headers["x-reduct-entries"],
            "batch-label-entry-1,batch-label-entry-2"
        );
        let label_header = resp_headers[LABELS_HEADER].to_str().unwrap();
        let label_index = label_header
            .split(',')
            .position(|label| label == "label")
            .expect("label index should be present");

        let expected_first = format!("{}=foo", label_index);
        let expected_second = format!("{}=bar", label_index);

        let entries: Vec<&str> = resp_headers["x-reduct-entries"]
            .to_str()
            .unwrap()
            .split(',')
            .collect();
        let entry1_idx = entries
            .iter()
            .position(|entry| *entry == "batch-label-entry-1")
            .unwrap();
        let entry2_idx = entries
            .iter()
            .position(|entry| *entry == "batch-label-entry-2")
            .unwrap();

        let first_header = resp_headers
            .iter()
            .find(|(name, _)| {
                name.as_str()
                    .starts_with(&format!("x-reduct-{}-", entry1_idx))
            })
            .map(|(_, value)| value.to_str().unwrap().to_string())
            .expect("header for entry-1");
        let second_header = resp_headers
            .iter()
            .find(|(name, _)| {
                name.as_str()
                    .starts_with(&format!("x-reduct-{}-", entry2_idx))
            })
            .map(|(_, value)| value.to_str().unwrap().to_string())
            .expect("header for entry-2");

        assert!(
            first_header.contains(&expected_first),
            "header: {}",
            first_header
        );
        assert!(
            second_header.contains(&expected_second),
            "header: {}",
            second_header
        );
    }

    #[rstest]
    #[tokio::test]
    async fn requires_header(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let err = read_batched_records(
            State(keeper.await),
            headers,
            path_to_bucket_1,
            MethodExtractor::new("GET"),
        )
        .await
        .err()
        .unwrap();

        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
    }
}
