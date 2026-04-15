// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::links::derive_key_from_secret;
use crate::api::http::utils::{make_headers_from_reader, RangeRecordStream, RecordStream};
use crate::api::http::{Components, HttpError, StateKeeper};
use crate::auth::policy::{Policy, ReadAccessPolicy};
use crate::auth::token_repository::ManageTokens;
use crate::core::sync::AsyncRwLock as RwLock;
use crate::ext::ext_repository::ManageExtensions;
use crate::storage::query::QueryRx;
use aes_siv::aead::{Aead, KeyInit};
use aes_siv::{Aes128SivAead, Nonce};
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header::CONTENT_RANGE, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum_extra::headers::{AcceptRanges, ContentLength, HeaderMap, HeaderMapExt, Range};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use flate2::read::ZlibDecoder;
use reduct_base::error::ErrorCode::NoContent;
use reduct_base::error::ReductError;
use reduct_base::io::BoxedReadRecord;
use reduct_base::msg::query_link_api::QueryLinkCreateRequest;
use reduct_base::{not_found, unauthorized, unprocessable_entity};
use std::collections::{Bound, HashMap, VecDeque};
use std::io::{Cursor, Read, Seek, SeekFrom};
use std::ops::Bound::Included;
use std::sync::Arc;
use tokio::sync::Mutex;

// GET /api/v1/links/:file_name&ct=...&s=...&i=...&n=...&ts=...&e=...
pub(super) async fn get(
    State(keeper): State<Arc<StateKeeper>>,
    header_map: HeaderMap,
    Path(_file_name): Path<String>, // we need the file_name to have a name when downloading
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, HttpError> {
    // first anonymous access to decrypt the query
    let components = keeper.get_anonymous().await?;
    let (token_name, query) = decrypt_query(&components, params.clone()).await?;
    check_permissions_from_token_name(&components, &token_name, &query.bucket).await?;

    let range = if header_map.contains_key("Range") {
        Some(header_map.typed_get::<Range>().unwrap())
    } else {
        None
    };

    let mut cache_lock = components.query_link_cache.write().await?;
    let key = cache_key(&params, &query)?;

    if let Some(cached) = cache_lock.get(&key) {
        // reset reader to the beginning if was used before from cache
        cached
            .lock()
            .await
            .seek(SeekFrom::Start(0))
            .map_err(|e| ReductError::from(e))?;
        prepare_response(&components, range, Arc::clone(cached)).await
    } else {
        let bucket = components
            .storage
            .get_bucket(&query.bucket)
            .await?
            .upgrade()?;

        let repo_ext = &components.ext_repo;

        // Get entries from query.entries first, fallback to query.entry from CreateLink request
        let mut query_request = query.query.clone();
        // We need for compatibility with v1.18 -> remove at some point
        let entry_name = if let Some(ref entries) = query_request.entries {
            if !entries.is_empty() {
                entries.first().cloned().unwrap_or_default()
            } else {
                // entries is empty, use the entry from CreateLink request
                query_request.entries = Some(vec![query.entry.clone()]);
                query.entry.clone()
            }
        } else {
            // entries is None, use the entry from CreateLink request
            query_request.entries = Some(vec![query.entry.clone()]);
            query.entry.clone()
        };

        // Execute the query at bucket level with multi-entry API
        let id = bucket.query(query_request.clone()).await?;
        repo_ext
            .register_query(id, &query.bucket, &entry_name, query_request)
            .await?;

        let record_entry = query
            .record_entry
            .as_deref()
            .expect("record_entry should be validated in decrypt_query");
        let record_timestamp = query
            .record_timestamp
            .expect("record_timestamp should be validated in decrypt_query");
        let (rx, _): (_, _) = bucket.get_query_receiver(id).await?;
        let record = process_query_and_fetch_record_by_identity(
            record_entry,
            record_timestamp,
            repo_ext,
            id,
            rx.upgrade()?,
        )
        .await?;

        let record = Arc::new(Mutex::new(record));
        cache_lock.insert(key, Arc::clone(&record));
        prepare_response(&components, range, record).await
    }
}

fn cache_key(
    params: &HashMap<String, String>,
    query: &QueryLinkCreateRequest,
) -> Result<String, ReductError> {
    let ct = params
        .get("ct")
        .ok_or_else(|| unprocessable_entity!("Missing 'ct' parameter"))?;
    if let (Some(record_entry), Some(record_timestamp)) =
        (query.record_entry.as_ref(), query.record_timestamp)
    {
        Ok(format!("{ct}:{record_entry}:{record_timestamp}"))
    } else {
        Err(unprocessable_entity!(
            "Both 'record_entry' and 'record_timestamp' must be provided in payload or URL parameters"
        ))
    }
}

async fn check_permissions_from_token_name(
    components: &Arc<Components>,
    token_name: &str,
    bucket: &str,
) -> Result<(), ReductError> {
    let mut token_repo = components.token_repo.write().await?;
    check_permissions_with_token_repo(token_repo.as_mut(), token_name, bucket).await
}

async fn check_permissions_with_token_repo(
    token_repo: &mut (dyn ManageTokens + Send),
    token_name: &str,
    bucket: &str,
) -> Result<(), ReductError> {
    if token_repo.get_token_list().await?.is_empty() {
        // Authentication is disabled.
        return Ok(());
    }

    let token = token_repo.get_token(token_name).await?.clone();
    if let Some(expiry) = token.expires_at {
        if chrono::Utc::now() >= expiry {
            return Err(unauthorized!("Token has expired"));
        }
    }

    ReadAccessPolicy { bucket }.validate(Ok(token))
}

async fn decrypt_query(
    components: &Arc<Components>,
    params: HashMap<String, String>,
) -> Result<(String, QueryLinkCreateRequest), ReductError> {
    let ciphertxt_b64 = params
        .get("ct")
        .ok_or_else(|| unprocessable_entity!("Missing 'ct' parameter"))?;
    let salt_b64 = params
        .get("s")
        .ok_or_else(|| unprocessable_entity!("Missing 's' parameter"))?;
    let nonce_b64 = params
        .get("n")
        .ok_or_else(|| unprocessable_entity!("Missing 'n' parameter"))?;
    let issuer = params
        .get("i")
        .ok_or_else(|| unprocessable_entity!("Missing 'i' parameter"))?;

    let mut token_repo = components.token_repo.write().await?;
    let token_secret = if token_repo.get_token_list().await?.is_empty() {
        // Authentication is disabled, use empty token
        ""
    } else {
        token_repo.get_token(issuer).await?.value.as_str()
    };

    let ciphertxt = URL_SAFE_NO_PAD
        .decode(ciphertxt_b64)
        .map_err(|e| unprocessable_entity!("Invalid base64 in 'ct' parameter: {}", e))?;

    let salt = URL_SAFE_NO_PAD
        .decode(salt_b64)
        .map_err(|e| unprocessable_entity!("Invalid base64 in 's' parameter: {}", e))?;

    let key = derive_key_from_secret(token_secret.as_bytes(), &salt);
    let cipher = Aes128SivAead::new_from_slice(&key).unwrap();

    let nonce_bytes = URL_SAFE_NO_PAD
        .decode(nonce_b64)
        .map_err(|e| unprocessable_entity!("Invalid base64 in 'n' parameter: {}", e))?;

    let compressed_text = cipher
        .decrypt(&Nonce::from_iter(nonce_bytes), ciphertxt.as_ref())
        .map_err(|e| unprocessable_entity!("Failed to decrypt query: {}", e))?;

    // decompress the query
    let mut decoder = ZlibDecoder::new(Cursor::new(compressed_text));
    let mut query = Vec::new();
    decoder
        .read_to_end(&mut query)
        .map_err(|e| unprocessable_entity!("Failed to decompress query: {}", e))?;

    // parse the query
    let mut query: QueryLinkCreateRequest = serde_json::from_slice(&query)
        .map_err(|e| unprocessable_entity!("Failed to parse query: {}", e))?;

    // Check expiration
    if query.expire_at < chrono::Utc::now() {
        return Err(unprocessable_entity!("Query link has expired").into());
    }

    let query_entry = params.get("e");
    let query_timestamp = params.get("ts");
    match (query_entry, query_timestamp) {
        (Some(entry), Some(ts)) => {
            let ts = ts
                .parse::<u64>()
                .map_err(|e| unprocessable_entity!("Invalid 'ts' parameter: {}", e))?;
            query.record_entry = Some(entry.clone());
            query.record_timestamp = Some(ts);
        }
        (None, None) => {}
        _ => {
            return Err(unprocessable_entity!(
                "Both 'e' and 'ts' parameters must be provided together"
            ))
        }
    }

    if query.record_entry.is_none() || query.record_timestamp.is_none() {
        return Err(unprocessable_entity!(
            "Both 'record_entry' and 'record_timestamp' must be provided in payload or URL parameters"
        ));
    }

    Ok((issuer.to_string(), query))
}

async fn process_query_and_fetch_record_by_identity(
    record_entry: &str,
    record_timestamp: u64,
    repo_ext: &Box<dyn ManageExtensions + Send + Sync>,
    id: u64,
    rx: Arc<RwLock<QueryRx>>,
) -> Result<BoxedReadRecord, ReductError> {
    loop {
        let Some(readers) = repo_ext.fetch_and_process_record(id, rx.clone()).await else {
            continue;
        };

        for reader in readers {
            let reader = match reader {
                Ok(r) => r,
                Err(ReductError {
                    status: NoContent, ..
                }) => {
                    return Err(not_found!(
                        "Record {}/{} not found in query result",
                        record_entry,
                        record_timestamp
                    )
                    .into())
                }
                Err(e) => return Err(e.into()),
            };

            let is_match = {
                let meta = reader.meta();
                meta.entry_name() == record_entry && meta.timestamp() == record_timestamp
            };
            if is_match {
                return Ok(reader);
            }
        }
    }
}

async fn prepare_response(
    components: &Arc<Components>,
    range: Option<Range>,
    reader: Arc<Mutex<BoxedReadRecord>>,
) -> Result<impl IntoResponse, HttpError> {
    let mut headers = make_headers_from_reader(reader.lock().await.meta());
    headers.typed_insert(AcceptRanges::bytes());

    if let Some(range) = range {
        let initial_content_length = reader.lock().await.meta().content_length();

        let mut ranges = range
            .satisfiable_ranges(initial_content_length)
            .collect::<VecDeque<_>>();

        if ranges.is_empty() {
            headers.insert(
                CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{}", initial_content_length)).unwrap(),
            );
            return Ok((StatusCode::RANGE_NOT_SATISFIABLE, headers, Body::empty()));
        }

        ranges.retain(|range| resolve_content_range(range, initial_content_length).is_some());
        if ranges.is_empty() {
            headers.insert(
                CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{}", initial_content_length)).unwrap(),
            );
            return Ok((StatusCode::RANGE_NOT_SATISFIABLE, headers, Body::empty()));
        }

        let content_length = ranges
            .iter()
            .filter_map(|range| resolve_content_range(range, initial_content_length))
            .map(|(start, end)| end - start + 1)
            .sum();

        components.limits.check_egress(content_length).await?;

        if let Some((start, end)) =
            resolve_content_range(ranges.front().unwrap(), initial_content_length)
        {
            headers.insert(
                CONTENT_RANGE,
                HeaderValue::from_str(&format!(
                    "bytes {}-{}/{}",
                    start, end, initial_content_length
                ))
                .unwrap(),
            );
        }

        headers.typed_insert(ContentLength(content_length));

        Ok((
            StatusCode::PARTIAL_CONTENT,
            headers,
            Body::from_stream(RangeRecordStream::new(reader, ranges)),
        ))
    } else {
        let content_length = reader.lock().await.meta().content_length();
        components.limits.check_egress(content_length).await?;

        Ok((
            StatusCode::OK,
            headers,
            Body::from_stream(RecordStream::new(reader, false)),
        ))
    }
}

fn resolve_content_range(
    range: &(Bound<u64>, Bound<u64>),
    initial_content_length: u64,
) -> Option<(u64, u64)> {
    match range {
        (Included(s), Included(e)) if e >= s => Some((*s, *e)),
        (Included(s), Bound::Unbounded) if *s < initial_content_length => {
            Some((*s, initial_content_length.saturating_sub(1)))
        }
        (Bound::Unbounded, Included(e)) => Some((0, *e)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::links::create::create;
    use crate::api::http::links::tests::create_query_link;
    use crate::api::http::links::QueryLinkCreateRequestAxum;
    use crate::api::http::tests::{egress_limited_keeper, headers, keeper};
    use crate::auth::token_repository::TokenRepositoryBuilder;
    use crate::cfg::Cfg;
    use crate::storage::entry::io::record_reader::tests::MockRecord;
    use axum::body::to_bytes;
    use chrono::{Duration, Utc};
    use mockall::predicate::eq;
    use reduct_base::error::ErrorCode;
    use reduct_base::io::RecordMeta;
    use reduct_base::msg::entry_api::{QueryEntry, QueryType};
    use reduct_base::msg::query_link_api::QueryLinkCreateRequest;
    use reduct_base::unauthorized;
    use rstest::rstest;
    use tempfile::tempdir;

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_query_link(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();

        let link = create_query_link(
            headers,
            keeper.clone(),
            QueryEntry {
                query_type: QueryType::Query,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap()
        .0
        .link;

        let params: HashMap<String, String> =
            url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                .into_owned()
                .collect();
        let response = get(
            State(Arc::clone(&keeper)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(params),
        )
        .await
        .unwrap();

        let resp = response.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers()["content-type"], "text/plain");
        assert_eq!(resp.headers()["content-length"], "6");
        assert!(!resp.headers().contains_key("content-range"));
        assert_eq!(resp.headers()["x-reduct-label-x"], "y");

        let body_bytes = to_bytes(resp.into_body(), 1000).await.unwrap();
        assert_eq!(
            String::from_utf8_lossy(body_bytes.iter().as_slice()),
            "Hey!!!"
        );

        assert!(
            components
                .query_link_cache
                .write()
                .await
                .unwrap()
                .get(&get_cache_key_from_params(
                    &url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                        .into_owned()
                        .collect::<HashMap<String, String>>(),
                ))
                .is_some(),
            "Query link should be cached"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_query_link_cached(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let link = create_query_link(
            headers,
            keeper.clone(),
            QueryEntry {
                query_type: QueryType::Query,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap()
        .0
        .link;

        let mut mock = MockRecord::new();
        mock.expect_meta().return_const(
            RecordMeta::builder()
                .content_length(0)
                .content_type("text/plain".to_string())
                .build(),
        );
        mock.expect_seek()
            .with(eq(SeekFrom::Start(0)))
            .returning(|_| Ok(0));

        let params = &url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
            .into_owned()
            .collect();
        components.query_link_cache.write().await.unwrap().insert(
            get_cache_key_from_params(params),
            Arc::new(Mutex::new(Box::new(mock))),
        );

        let response = get(
            State(Arc::clone(&keeper)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(params.clone()),
        )
        .await
        .unwrap();

        let resp = response.into_response();
        assert_eq!(resp.headers()["content-type"], "text/plain");
        assert_eq!(resp.headers()["content-length"], "0");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_query_link_cache_is_scoped_by_record_identity(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        let mut writer = bucket
            .begin_write(
                "entry-1",
                1,
                5,
                "text/plain".to_string(),
                Default::default(),
            )
            .await
            .unwrap();
        writer
            .send(Ok(Some(bytes::Bytes::from("Later"))))
            .await
            .unwrap();
        writer.send(Ok(None)).await.unwrap();

        let link = create_query_link(
            headers,
            keeper.clone(),
            QueryEntry {
                query_type: QueryType::Query,
                entries: Some(vec!["entry-1".to_string()]),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap()
        .0
        .link;

        let params: HashMap<String, String> =
            url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                .into_owned()
                .collect();

        let first = get(
            State(Arc::clone(&keeper)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(params.clone()),
        )
        .await
        .unwrap()
        .into_response();
        let first_body = to_bytes(first.into_body(), 1000).await.unwrap();
        assert_eq!(first_body, bytes::Bytes::from("Hey!!!"));

        let mut second_params = params;
        second_params.insert("e".to_string(), "entry-1".to_string());
        second_params.insert("ts".to_string(), "1".to_string());
        let second = get(
            State(Arc::clone(&keeper)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(second_params),
        )
        .await
        .unwrap()
        .into_response();
        let second_body = to_bytes(second.into_body(), 1000).await.unwrap();
        assert_eq!(second_body, bytes::Bytes::from("Later"));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_query_link_uses_record_identity_for_preview(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        for (entry, ts, data) in [
            ("entry-a", 10u64, "A1"),
            ("entry-a", 30u64, "A3"),
            ("entry-b", 20u64, "B2"),
        ] {
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
                .send(Ok(Some(bytes::Bytes::from(data.to_string()))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();
        }

        let response = create(
            State(Arc::clone(&keeper)),
            headers,
            Path("file.txt".to_string()),
            QueryLinkCreateRequestAxum(QueryLinkCreateRequest {
                expire_at: Utc::now() + chrono::Duration::hours(1),
                bucket: "bucket-1".to_string(),
                entry: "entry-a".to_string(),
                record_entry: Some("entry-a".to_string()),
                record_timestamp: Some(30),
                query: QueryEntry {
                    query_type: QueryType::Query,
                    entries: Some(vec!["entry-a".to_string(), "entry-b".to_string()]),
                    start: Some(0),
                    stop: Some(100),
                    ..Default::default()
                },
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;

        let params: HashMap<String, String> =
            url::form_urlencoded::parse(response.link.split('?').nth(1).unwrap().as_bytes())
                .into_owned()
                .collect();
        let resp = get(
            State(Arc::clone(&keeper)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(params),
        )
        .await
        .unwrap()
        .into_response();

        let body = to_bytes(resp.into_body(), 1000).await.unwrap();
        assert_eq!(body, bytes::Bytes::from("A3"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_query_link_record_not_found_by_identity(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let link = create_query_link(
            headers,
            keeper.clone(),
            QueryEntry {
                query_type: QueryType::Query,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap()
        .0
        .link;

        let mut params: HashMap<String, String> =
            url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                .into_owned()
                .collect();
        params.insert("e".to_string(), "entry-1".to_string());
        params.insert("ts".to_string(), "10".to_string());
        let result = get(
            State(Arc::clone(&keeper)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(params),
        )
        .await;
        let err: ReductError = result.err().unwrap().into();
        assert_eq!(
            err,
            not_found!("Record entry-1/10 not found in query result")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_expire_at_in_past(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper.await;
        let link = create_query_link(
            headers,
            keeper.clone(),
            QueryEntry {
                query_type: QueryType::Query,
                ..Default::default()
            },
            Some(Utc::now() - chrono::Duration::hours(1)),
        )
        .await
        .unwrap()
        .0;

        let params: HashMap<String, String> =
            url::form_urlencoded::parse(link.link.split('?').nth(1).unwrap().as_bytes())
                .into_owned()
                .collect();
        let err = get(
            State(Arc::clone(&keeper)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(params),
        )
        .await
        .err()
        .unwrap();
        let err: ReductError = err.into();
        assert_eq!(err, unprocessable_entity!("Query link has expired"));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_query_link_range(#[future] keeper: Arc<StateKeeper>, mut headers: HeaderMap) {
        let keeper = keeper.await;
        let link = create_query_link(
            headers.clone(),
            keeper.clone(),
            QueryEntry {
                query_type: QueryType::Query,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap()
        .0
        .link;

        let params: HashMap<String, String> =
            url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                .into_owned()
                .collect();
        headers.typed_insert(Range::bytes(0..3).unwrap()); // Request first
        let response = get(
            State(Arc::clone(&keeper)),
            headers,
            Path("file.txt".to_string()),
            Query(params),
        )
        .await
        .unwrap();

        let resp = response.into_response();
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(resp.headers()["content-type"], "text/plain");
        assert_eq!(resp.headers()["content-length"], "3");
        assert_eq!(resp.headers()["content-range"], "bytes 0-2/6");
        assert_eq!(resp.headers()["x-reduct-label-x"], "y");

        let body_bytes = to_bytes(resp.into_body(), 1000).await.unwrap();
        assert_eq!(String::from_utf8_lossy(body_bytes.iter().as_slice()), "Hey");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_query_link_range_not_satisfiable(
        #[future] keeper: Arc<StateKeeper>,
        mut headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let link = create_query_link(
            headers.clone(),
            keeper.clone(),
            QueryEntry {
                query_type: QueryType::Query,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap()
        .0
        .link;

        let params: HashMap<String, String> =
            url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                .into_owned()
                .collect();

        headers.insert("range", HeaderValue::from_static("bytes=6-5"));
        let response = get(
            State(Arc::clone(&keeper)),
            headers,
            Path("file.txt".to_string()),
            Query(params),
        )
        .await
        .unwrap();

        let resp = response.into_response();
        assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(resp.headers()["content-range"], "bytes */6");

        let body_bytes = to_bytes(resp.into_body(), 1000).await.unwrap();
        assert_eq!(body_bytes.len(), 0);
    }

    #[test]
    fn test_resolve_content_range_variants() {
        assert_eq!(
            resolve_content_range(&(Included(1), Bound::Unbounded), 6),
            Some((1, 5))
        );
        assert_eq!(
            resolve_content_range(&(Bound::Unbounded, Included(2)), 6),
            Some((0, 2))
        );
        assert_eq!(resolve_content_range(&(Included(3), Included(2)), 6), None);
        assert_eq!(
            resolve_content_range(&(Included(6), Bound::Unbounded), 6),
            None
        );
        assert_eq!(
            resolve_content_range(&(Bound::Excluded(1), Included(2)), 6),
            None
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_query_link_egress_rate_limit(
        #[future] egress_limited_keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = egress_limited_keeper.await;
        let link = create_query_link(
            headers,
            keeper.clone(),
            QueryEntry {
                query_type: QueryType::Query,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap()
        .0
        .link;

        let params: HashMap<String, String> =
            url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                .into_owned()
                .collect();
        let err = get(
            State(Arc::clone(&keeper)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(params),
        )
        .await
        .err()
        .unwrap();
        let err: ReductError = err.into();
        assert_eq!(err.status, ErrorCode::TooManyRequests);
        assert!(err.message.contains("egress bytes"));
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_query_link_range_egress_rate_limit(
        #[future] egress_limited_keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = egress_limited_keeper.await;
        let link = create_query_link(
            headers,
            keeper.clone(),
            QueryEntry {
                query_type: QueryType::Query,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap()
        .0
        .link;

        let params: HashMap<String, String> =
            url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                .into_owned()
                .collect();

        let mut request_headers = HeaderMap::new();
        request_headers.typed_insert(Range::bytes(0..6).unwrap());
        let err = get(
            State(Arc::clone(&keeper)),
            request_headers,
            Path("file.txt".to_string()),
            Query(params),
        )
        .await
        .err()
        .unwrap();
        let err: ReductError = err.into();
        assert_eq!(err.status, ErrorCode::TooManyRequests);
        assert!(err.message.contains("egress bytes"));
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_query_link_with_entries_specified(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;

        // Create a query link with entries specified in QueryEntry
        let link = create_query_link(
            headers,
            keeper.clone(),
            QueryEntry {
                query_type: QueryType::Query,
                entries: Some(vec!["entry-1".to_string()]),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap()
        .0
        .link;

        let params: HashMap<String, String> =
            url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                .into_owned()
                .collect();
        let response = get(
            State(Arc::clone(&keeper)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(params),
        )
        .await
        .unwrap();

        let resp = response.into_response();
        assert_eq!(resp.headers()["content-type"], "text/plain");
        assert_eq!(resp.headers()["content-length"], "6");

        let body_bytes = to_bytes(resp.into_body(), 1000).await.unwrap();
        assert_eq!(
            String::from_utf8_lossy(body_bytes.iter().as_slice()),
            "Hey!!!"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_query_link_with_empty_entries_falls_back_to_entry(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;

        let link = create_query_link(
            headers,
            keeper.clone(),
            QueryEntry {
                query_type: QueryType::Query,
                entries: Some(vec![]),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap()
        .0
        .link;

        let params: HashMap<String, String> =
            url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                .into_owned()
                .collect();
        let response = get(
            State(Arc::clone(&keeper)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(params),
        )
        .await
        .unwrap();

        let resp = response.into_response();
        assert_eq!(resp.headers()["content-type"], "text/plain");
        assert_eq!(resp.headers()["content-length"], "6");

        let body_bytes = to_bytes(resp.into_body(), 1000).await.unwrap();
        assert_eq!(
            String::from_utf8_lossy(body_bytes.iter().as_slice()),
            "Hey!!!"
        );
    }

    #[tokio::test]
    async fn test_check_permissions_with_token_repo_auth_disabled() {
        let cfg = Cfg::default();
        let mut repo = TokenRepositoryBuilder::new(cfg)
            .build(tempdir().unwrap().keep())
            .await;

        check_permissions_with_token_repo(repo.as_mut(), "any-token", "bucket-1")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_check_permissions_with_token_repo_expired_token() {
        let cfg = Cfg {
            api_token: "init-token".to_string(),
            ..Cfg::default()
        };
        let path = tempdir().unwrap().keep();
        let mut repo = TokenRepositoryBuilder::new(cfg).build(path).await;

        let mut init_token = repo.get_token("init-token").await.unwrap().clone();
        init_token.expires_at = Some(Utc::now() - Duration::seconds(1));
        repo.update_token(init_token).await.unwrap();

        let err = check_permissions_with_token_repo(repo.as_mut(), "init-token", "bucket-1")
            .await
            .unwrap_err();
        assert_eq!(err, unauthorized!("Token has expired"));
    }

    mod validation {
        use super::*;
        use rstest::rstest;

        #[test]
        fn test_cache_key_missing_ct_param() {
            let params = HashMap::new();
            let query = QueryLinkCreateRequest {
                record_entry: Some("entry-1".to_string()),
                record_timestamp: Some(0),
                ..Default::default()
            };

            let err = cache_key(&params, &query).unwrap_err();
            assert_eq!(err, unprocessable_entity!("Missing 'ct' parameter"));
        }

        #[test]
        fn test_cache_key_missing_record_identity() {
            let params = HashMap::from([("ct".to_string(), "cipher".to_string())]);
            let query = QueryLinkCreateRequest::default();

            let err = cache_key(&params, &query).unwrap_err();
            assert_eq!(
                err,
                unprocessable_entity!(
                    "Both 'record_entry' and 'record_timestamp' must be provided in payload or URL parameters"
                )
            );
        }

        #[rstest]
        #[case("ct", "XXX", "Invalid base64 in 'ct' parameter")]
        #[case("s", "XXX", "Invalid base64 in 's' parameter")]
        #[case("n", "XXX", "Invalid base64 in 'n' parameter")]
        #[tokio::test]
        async fn test_get_query_link_invalid_base64(
            #[future] keeper: Arc<StateKeeper>,
            headers: HeaderMap,
            #[case] key: &str,
            #[case] value: &str,
            #[case] _error_msg: &str,
        ) {
            let keeper = keeper.await;
            let link = create_query_link(
                headers,
                keeper.clone(),
                QueryEntry {
                    query_type: QueryType::Query,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
            .0
            .link;

            let params: HashMap<String, String> =
                url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                    .into_owned()
                    .collect();

            let mut modified_params = params.clone();
            modified_params.insert(key.to_string(), value.to_string());
            let result = get(
                State(Arc::clone(&keeper)),
                HeaderMap::new(),
                Path("file.txt".to_string()),
                Query(modified_params),
            )
            .await;
            assert!(result
                .err()
                .unwrap()
                .into_inner()
                .to_string()
                .contains(&format!("Invalid base64 in '{}' parameter", key)));
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_query_link_invalid_ts_param(
            #[future] keeper: Arc<StateKeeper>,
            headers: HeaderMap,
        ) {
            let keeper = keeper.await;
            let link = create_query_link(
                headers,
                keeper.clone(),
                QueryEntry {
                    query_type: QueryType::Query,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
            .0
            .link;

            let mut params: HashMap<String, String> =
                url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                    .into_owned()
                    .collect();
            params.insert("e".to_string(), "entry-1".to_string());
            params.insert("ts".to_string(), "invalid".to_string());
            let result = get(
                State(Arc::clone(&keeper)),
                HeaderMap::new(),
                Path("file.txt".to_string()),
                Query(params),
            )
            .await;
            let err: ReductError = result.err().unwrap().into();
            assert!(err.message.contains("Invalid 'ts' parameter"));
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_query_link_partial_identity_params(
            #[future] keeper: Arc<StateKeeper>,
            headers: HeaderMap,
        ) {
            let keeper = keeper.await;
            let link = create_query_link(
                headers,
                keeper.clone(),
                QueryEntry {
                    query_type: QueryType::Query,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
            .0
            .link;

            let mut params: HashMap<String, String> =
                url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                    .into_owned()
                    .collect();
            params.insert("ts".to_string(), "0".to_string());
            let result = get(
                State(Arc::clone(&keeper)),
                HeaderMap::new(),
                Path("file.txt".to_string()),
                Query(params),
            )
            .await;
            let err: ReductError = result.err().unwrap().into();
            assert_eq!(
                err,
                unprocessable_entity!("Both 'e' and 'ts' parameters must be provided together")
            );
        }

        #[rstest]
        #[tokio::test]
        #[case("ct")]
        #[case("s")]
        #[case("n")]
        #[case("i")]
        async fn test_get_query_link_missing_params(
            #[future] keeper: Arc<StateKeeper>,
            headers: HeaderMap,
            #[case] key: &str,
        ) {
            let keeper = keeper.await;
            let link = create_query_link(
                headers,
                keeper.clone(),
                QueryEntry {
                    query_type: QueryType::Query,
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
            .0
            .link;

            let params: HashMap<String, String> =
                url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                    .into_owned()
                    .collect();

            let mut modified_params = params.clone();
            modified_params.remove(key);
            let result = get(
                State(Arc::clone(&keeper)),
                HeaderMap::new(),
                Path("file.txt".to_string()),
                Query(modified_params),
            )
            .await;
            let err: ReductError = result.err().unwrap().into();
            assert_eq!(err, unprocessable_entity!("Missing '{}' parameter", key));
        }
    }

    fn get_cache_key_from_params(params: &HashMap<String, String>) -> String {
        let ct = params.get("ct").unwrap();
        let entry = params.get("e").map(String::as_str).unwrap_or("entry-1");
        let ts = params.get("ts").map(String::as_str).unwrap_or("0");
        format!("{ct}:{entry}:{ts}")
    }

    mod fetching {
        use super::*;
        use reduct_base::internal_server_error;
        use tokio::sync::mpsc::channel;

        #[rstest]
        #[tokio::test]
        async fn test_fetch_query_error(#[future] keeper: Arc<StateKeeper>) {
            let components = keeper.await.get_anonymous().await.unwrap();
            let ext_repo = &components.ext_repo;
            let (tx, rx) = channel(1);
            tx.send(Err(internal_server_error!("Oops"))).await.unwrap();
            let rx = Arc::new(RwLock::new(rx));
            let id = 1;

            let err = process_query_and_fetch_record_by_identity("entry-1", 0, ext_repo, id, rx)
                .await
                .err()
                .unwrap();
            assert_eq!(err, internal_server_error!("Oops"));
        }
    }
}
