// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::auth::policy::ReadAccessPolicy;
use crate::http_api::entry::MethodExtractor;
use crate::http_api::utils::ReadersWrapper;
use crate::http_api::{ErrorCode, HttpError};
use crate::storage::bucket::Bucket;

use crate::core::sync::AsyncRwLock;
use crate::ext::ext_repository::BoxedManageExtensions;
use crate::http_api::StateKeeper;
use crate::storage::query::QueryRx;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum_extra::headers::{HeaderMap, HeaderName, HeaderValue};
use log::debug;
use reduct_base::error::ReductError;
use reduct_base::io::BoxedReadRecord;
use reduct_base::{no_content, unprocessable_entity};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

// GET /:bucket/:entry/batch?q=<number>
pub(super) async fn read_batched_records(
    State(keeper): State<Arc<StateKeeper>>,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
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

    let entry_name = path.get("entry_name").unwrap();
    let query_id = match params.get("q") {
        Some(query) => query.parse::<u64>().map_err(|_| {
            HttpError::new(ErrorCode::UnprocessableEntity, "'query' must be a number")
        })?,

        None => {
            return Err(
                unprocessable_entity!("'q' parameter is required for batched reads").into(),
            );
        }
    };

    fetch_and_response_batched_records(
        components
            .storage
            .get_bucket(bucket_name)
            .await?
            .upgrade()?,
        entry_name,
        query_id,
        method.name == "HEAD",
        &components.ext_repo,
    )
    .await
}

fn make_batch_header(reader: &BoxedReadRecord) -> (HeaderName, HeaderValue) {
    let meta = reader.meta();
    let name = HeaderName::from_str(&format!("x-reduct-time-{}", meta.timestamp())).unwrap();
    let mut meta_data = vec![
        meta.content_length().to_string(),
        meta.content_type().to_string(),
    ];

    let format_labels = |(k, v): (&String, &String)| {
        if v.contains(",") {
            format!("{}=\"{}\"", k, v)
        } else {
            format!("{}={}", k, v)
        }
    };

    let mut labels: Vec<String> = meta.labels().iter().map(format_labels).collect();

    labels.extend(
        meta.computed_labels()
            .iter()
            .map(|(k, v)| format_labels((&format!("@{}", k), v))),
    );

    labels.sort();

    meta_data.append(&mut labels);
    let value: HeaderValue = meta_data.join(",").parse().unwrap();

    (name, value)
}

async fn fetch_and_response_batched_records(
    bucket: Arc<Bucket>,
    entry_name: &str,
    query_id: u64,
    empty_body: bool,
    ext_repository: &BoxedManageExtensions,
) -> Result<impl IntoResponse, HttpError> {
    let (rx, io_settings) = bucket
        .get_entry(entry_name)
        .await?
        .upgrade()?
        .get_query_receiver(query_id)
        .await?;

    let mut header_size = 0usize;
    let mut body_size = 0u64;
    let mut headers = HeaderMap::new();
    headers.reserve(io_settings.batch_max_records + 3);
    let mut readers = Vec::new();
    readers.reserve(io_settings.batch_max_records);

    let mut last = false;
    let bucket_name = bucket.name().to_string();

    let start_time = std::time::Instant::now();
    loop {
        let batch_of_readers = match next_record_readers(
            query_id,
            rx.upgrade()?,
            &format!("{}/{}/{}", bucket_name, entry_name, query_id),
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
                    {
                        let (name, value) = make_batch_header(&reader);
                        header_size += name.as_str().len() + value.to_str().unwrap().len() + 2;
                        body_size += reader.meta().content_length();
                        headers.insert(name, value);
                    }

                    readers.push(reader);
                }
                Err(err) => {
                    if readers.is_empty() {
                        return Err(HttpError::from(err));
                    } else {
                        if err.status() == ErrorCode::NoContent {
                            last = true;
                            break;
                        } else {
                            return Err(HttpError::from(err));
                        }
                    }
                }
            }
        }

        if last {
            break;
        }

        if header_size > io_settings.batch_max_metadata_size
            || (!empty_body && body_size > io_settings.batch_max_size)
            || readers.len() >= io_settings.batch_max_records
            || start_time.elapsed() > io_settings.batch_timeout
        {
            break;
        }
    }

    // TODO: it's workaround
    // check if the query is still alive
    // unfortunately, we can start using a finished query so we need to check if it's still alive again
    if readers.is_empty() {
        tokio::time::sleep(Duration::from_millis(5)).await;
        match bucket
            .get_entry(entry_name)
            .await?
            .upgrade()?
            .get_query_receiver(query_id)
            .await
        {
            Err(err) if err.status() == ErrorCode::NotFound => {
                return Err(no_content!("No more records").into());
            }
            _ => { /* query is still alive */ }
        }
    }

    headers.insert("content-length", body_size.to_string().parse().unwrap());
    headers.insert("content-type", "application/octet-stream".parse().unwrap());
    headers.insert("x-reduct-last", last.to_string().parse().unwrap());

    Ok((
        headers,
        Body::from_stream(ReadersWrapper::new(readers, empty_body)),
    ))
}

// This function is used to get the next record from the query receiver
// created for better testing
async fn next_record_readers(
    query_id: u64,
    rx: Arc<AsyncRwLock<QueryRx>>,
    query_path: &str,
    recv_timeout: Duration,
    ext_repository: &BoxedManageExtensions,
) -> Option<Vec<Result<BoxedReadRecord, ReductError>>> {
    // we need to wait for the first record
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::io::IoConfig;
    use crate::ext::ext_repository::create_ext_repository;
    use crate::http_api::entry::tests::query;
    use crate::http_api::tests::{headers, keeper, path_to_entry_1};
    use crate::storage::entry::io::record_reader::tests::MockRecord;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use bytes::Bytes;
    use reduct_base::ext::ExtSettings;
    use reduct_base::io::RecordMeta;
    use reduct_base::msg::entry_api::QueryEntry;
    use reduct_base::msg::server_api::ServerInfo;
    use reduct_base::Labels;
    use rstest::*;
    use tempfile::tempdir;
    use test_log::test as test_log;
    use tokio::time::sleep;

    #[test_log(rstest)]
    #[case("GET", "Hey!!!")]
    #[case("HEAD", "")]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_batched_read(
        #[future] keeper: Arc<StateKeeper>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
        #[case] method: String,
        #[case] _body: String,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        {
            let entry = components
                .storage
                .get_bucket("bucket-1")
                .await
                .unwrap()
                .upgrade_and_unwrap()
                .get_entry("entry-1")
                .await
                .unwrap()
                .upgrade_and_unwrap();
            for time in 10..100 {
                let mut writer = entry
                    .clone()
                    .begin_write(time, 6, "text/plain".to_string(), HashMap::new())
                    .await
                    .unwrap();
                writer.send(Ok(Some(Bytes::from("Hey!!!")))).await.unwrap();
                writer.send(Ok(None)).await.unwrap();
            }
        }

        let ttl = 1;
        let query_id = query(&path_to_entry_1, keeper.clone(), Some(ttl)).await;
        let query = Query(HashMap::from_iter(vec![(
            "q".to_string(),
            query_id.to_string(),
        )]));

        macro_rules! read_batched_records {
            () => {
                read_batched_records(
                    State(keeper.clone()),
                    Path(path_to_entry_1.clone()),
                    query.clone(),
                    headers.clone(),
                    MethodExtractor::new(method.as_str()),
                )
                .await
                .into_response()
            };
        }

        let response = read_batched_records!();
        let resp_headers = response.headers();
        assert_eq!(
            resp_headers["x-reduct-time-0"],
            "6,text/plain,b=\"[a,b]\",x=y"
        );
        assert_eq!(resp_headers["content-type"], "application/octet-stream");
        assert_eq!(resp_headers["content-length"], "510");
        assert_eq!(resp_headers["x-reduct-last"], "false");

        if method == "GET" {
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX).await.unwrap(),
                Bytes::from("Hey!!!".repeat(85))
            );
        } else {
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .len(),
                0
            );
        }

        let response = read_batched_records!();
        let resp_headers = response.headers();
        assert_eq!(resp_headers["content-length"], "36", "{:?}", resp_headers);
        assert_eq!(resp_headers["content-type"], "application/octet-stream");
        assert_eq!(resp_headers["x-reduct-time-98"], "6,text/plain");
        assert_eq!(resp_headers["x-reduct-last"], "true");

        if method == "GET" {
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX).await.unwrap(),
                Bytes::from("Hey!!!".repeat(6))
            );
        } else {
            assert_eq!(
                to_bytes(response.into_body(), usize::MAX)
                    .await
                    .unwrap()
                    .len(),
                0
            );
        }

        sleep(Duration::from_secs(ttl)).await;
        let response = read_batched_records!();
        let resp_headers = response.headers();
        assert_eq!(
            resp_headers["x-reduct-error"],
            format!(
                "Query {} not found and it might have expired. Check TTL in your query request.",
                query_id
            )
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_batched_no_entry(
        #[future] keeper: Arc<StateKeeper>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let query_id = query(&path_to_entry_1, keeper.clone(), None).await;

        components
            .storage
            .get_bucket(path_to_entry_1.get("bucket_name").unwrap())
            .await
            .unwrap()
            .upgrade()
            .unwrap()
            .remove_entry(path_to_entry_1.get("entry_name").unwrap())
            .await
            .unwrap();

        sleep(Duration::from_millis(100)).await;

        let err = read_batched_records(
            State(keeper.clone()),
            path_to_entry_1,
            Query(HashMap::from_iter(vec![(
                "q".to_string(),
                query_id.to_string(),
            )])),
            headers,
            MethodExtractor::new("GET"),
        )
        .await
        .err()
        .unwrap();

        assert!(
            err.status() == ErrorCode::NotFound || err.status() == ErrorCode::Conflict,
            "should return NotFound if the entry is deleted"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn returns_no_content_when_no_records(
        #[future] keeper: Arc<StateKeeper>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let entry = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap()
            .get_entry("entry-1")
            .await
            .unwrap()
            .upgrade()
            .unwrap();

        let query_id = entry
            .query(
                QueryEntry {
                    start: Some(1),
                    stop: Some(2),
                    ..Default::default()
                }
                .into(),
            )
            .await
            .unwrap();

        let response = read_batched_records(
            State(keeper.clone()),
            Path(path_to_entry_1.0.clone()),
            Query(HashMap::from_iter(vec![(
                "q".to_string(),
                query_id.to_string(),
            )])),
            headers,
            MethodExtractor::new("GET"),
        )
        .await
        .into_response();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }

    mod next_record_reader {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_next_record_reader_timeout(ext_repository: BoxedManageExtensions) {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            let rx = Arc::new(AsyncRwLock::new(rx));
            assert!(
                timeout(
                    Duration::from_secs(1),
                    next_record_readers(
                        1,
                        rx.clone(),
                        "",
                        Duration::from_millis(10),
                        &ext_repository
                    )
                )
                .await
                .unwrap()
                .is_none(),
                "should return None if the query is closed"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_next_record_reader_closed_tx(ext_repository: BoxedManageExtensions) {
            let (tx, rx) = tokio::sync::mpsc::channel(1);
            let rx = Arc::new(AsyncRwLock::new(rx));
            drop(tx);
            assert_eq!(
                timeout(
                    Duration::from_secs(1),
                    next_record_readers(
                        1,
                        rx.clone(),
                        "",
                        Duration::from_millis(0),
                        &ext_repository
                    )
                )
                .await
                .unwrap()
                .unwrap()[0]
                    .as_ref()
                    .err()
                    .unwrap()
                    .status(),
                ErrorCode::NoContent,
                "should return None if the query is closed"
            );
        }
    }

    #[rstest]
    fn test_batch_compute_labels() {
        let mut record = MockRecord::new();
        let meta = RecordMeta::builder()
            .timestamp(1000u64)
            .labels(Labels::from_iter(vec![("a".to_string(), "b".to_string())]))
            .computed_labels(Labels::from_iter(vec![("x".to_string(), "y".to_string())]))
            .content_length(100u64)
            .content_type("text/plain".to_string())
            .build();
        record.expect_meta().return_const(meta);

        let record: BoxedReadRecord = Box::new(record);

        let (name, value) = make_batch_header(&record);
        assert_eq!(name, HeaderName::from_static("x-reduct-time-1000"));
        assert_eq!(value.to_str().unwrap(), "100,text/plain,@x=y,a=b");
    }

    mod batch_parameters {
        use super::*;
        use crate::cfg::Cfg;
        use crate::http_api::Components;
        use reduct_base::msg::entry_api::QueryEntry;
        use serde_json::{json, Value};

        #[rstest]
        #[tokio::test]
        async fn max_records_from_query(#[future] keeper: Arc<StateKeeper>) {
            let components = keeper.await.get_anonymous().await.unwrap();
            let resp =
                build_bucket_and_query(components.clone(), json!({"#batch_records": 10}), false)
                    .await;

            let count = resp
                .into_response()
                .headers()
                .iter()
                .filter(|(name, _)| name.as_str().starts_with("x-reduct-time-"))
                .count();
            assert_eq!(count, 10);
        }

        #[rstest]
        #[tokio::test]
        async fn max_records_from_settings(#[future] keeper: Arc<StateKeeper>) {
            let components = keeper.await.get_anonymous().await.unwrap();
            let resp = build_bucket_and_query(components.clone(), json!({}), false).await;
            let count = resp
                .into_response()
                .headers()
                .iter()
                .filter(|(name, _)| name.as_str().starts_with("x-reduct-time-"))
                .count();
            assert_eq!(count, Cfg::default().io_conf.batch_max_records);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn max_metadata_size_from_query(#[future] keeper: Arc<StateKeeper>) {
            let components = keeper.await.get_anonymous().await.unwrap();
            let resp =
                build_bucket_and_query(components, json!({"#batch_metadata_size": 100}), false)
                    .await;
            let body = to_bytes(resp.into_response().into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(body.len() <= 100);
        }

        #[rstest]
        #[tokio::test(flavor = "multi_thread")]
        async fn max_size_from_settings(#[future] keeper: Arc<StateKeeper>) {
            let components = keeper.await.get_anonymous().await.unwrap();
            let resp = build_bucket_and_query(components.clone(), json!({}), false).await;
            let body = to_bytes(resp.into_response().into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(body.len() <= Cfg::default().io_conf.batch_max_size as usize);
        }

        #[rstest]
        #[tokio::test]
        async fn ignore_max_size_if_no_body(#[future] keeper: Arc<StateKeeper>) {
            let components = keeper.await.get_anonymous().await.unwrap();
            let resp = build_bucket_and_query(components.clone(), json!({"#batch_size": 1}), true)
                .await
                .into_response();

            let header_size: usize = resp.headers().iter().count();
            let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();

            assert_eq!(body.len(), 0);
            assert_eq!(
                header_size, 88,
                "should sand 85 records in headers + 3 default headers"
            );
        }

        async fn build_bucket_and_query(
            components: Arc<Components>,
            condition: Value,
            only_metadata: bool,
        ) -> impl IntoResponse {
            let bucket = components
                .storage
                .get_bucket("bucket-1")
                .await
                .unwrap()
                .upgrade()
                .unwrap();
            let entry = bucket
                .get_entry("entry-1")
                .await
                .unwrap()
                .upgrade()
                .unwrap();

            for time in 10..100 {
                let mut writer = entry
                    .clone()
                    .begin_write(time, 6, "text/plain".to_string(), HashMap::new())
                    .await
                    .unwrap();
                writer.send(Ok(Some(Bytes::from("Hey!!!")))).await.unwrap();
                writer.send(Ok(None)).await.unwrap();
            }

            let query_id = entry
                .query(
                    QueryEntry {
                        when: Some(condition),
                        only_metadata: Some(only_metadata),
                        ..QueryEntry::default()
                    }
                    .into(),
                )
                .await
                .unwrap();

            fetch_and_response_batched_records(
                bucket,
                "entry-1",
                query_id,
                only_metadata,
                &components.ext_repo,
            )
            .await
            .unwrap()
        }
    }

    #[fixture]
    fn ext_repository() -> BoxedManageExtensions {
        create_ext_repository(
            Some(tempdir().unwrap().keep()),
            vec![],
            ExtSettings::builder()
                .server_info(ServerInfo::default())
                .build(),
            IoConfig::default(),
        )
        .unwrap()
    }

    mod stream_wrapper {
        use super::*;
        use futures_util::Stream;

        #[rstest]
        fn test_size_hint() {
            let wrapper = ReadersWrapper::new(vec![], false);
            assert_eq!(wrapper.size_hint(), (0, None));
        }
    }
}
