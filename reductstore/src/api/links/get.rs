// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::links::derive_key_from_secret;
use crate::api::middleware::check_permissions;
use crate::api::utils::{make_headers_from_reader, RecordStream};
use crate::api::{Components, HttpError};
use crate::auth::policy::ReadAccessPolicy;
use crate::ext::ext_repository::ManageExtensions;
use crate::storage::query::QueryRx;
use aes_siv::aead::{Aead, KeyInit};
use aes_siv::{Aes128SivAead, Nonce};
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::header::AUTHORIZATION;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum_extra::headers::{AcceptRanges, ContentLength, HeaderMap, HeaderMapExt, Range};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use flate2::read::ZlibDecoder;
use futures_util::Stream;
use log::info;
use reduct_base::error::ErrorCode::NoContent;
use reduct_base::error::ReductError;
use reduct_base::io::ReadRecord;
use reduct_base::msg::query_link_api::QueryLinkCreateRequest;
use reduct_base::{not_found, unprocessable_entity};
use std::collections::{Bound, HashMap, VecDeque};
use std::io::SeekFrom::Start;
use std::io::{Cursor, Read, Seek};
use std::ops::Bound::Included;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::RwLock;

// GET /api/v1/links/:file_name&ct=...&s=...&i=...&r=...
pub(super) async fn get(
    State(components): State<Arc<Components>>,
    header_map: HeaderMap,
    Path(_file_name): Path<String>, // we need the file_name to have a name when downloading
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, HttpError> {
    let (record_num, token, query) = decrypt_query(&components, params).await?;

    check_permissions(
        &components,
        &HeaderMap::from_iter([(AUTHORIZATION, format!("Bearer {}", token).parse().unwrap())]),
        ReadAccessPolicy {
            bucket: &query.bucket,
        },
    )
    .await?;

    let entry = components
        .storage
        .get_bucket(&query.bucket)?
        .upgrade()?
        .get_entry(&query.entry)?
        .upgrade()?;

    let repo_ext = &components.ext_repo;

    // Execute the query with extension mechanism
    let id = entry.query(query.query.clone()).await?;
    repo_ext
        .register_query(id, &query.bucket, &query.entry, query.query)
        .await?;

    let rx = entry.get_query_receiver(id.clone())?.upgrade()?;

    let range = if header_map.contains_key("Range") {
        Some(header_map.typed_get::<Range>().unwrap())
    } else {
        None
    };
    process_query_and_fetch_record(record_num, repo_ext, id, rx, range).await
}

async fn decrypt_query(
    components: &Arc<Components>,
    params: HashMap<String, String>,
) -> Result<(u64, String, QueryLinkCreateRequest), ReductError> {
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
    let record_num = params
        .get("r")
        .unwrap_or(&"0".to_string())
        .parse::<u64>()
        .map_err(|e| unprocessable_entity!("Invalid 'r' parameter: {}", e))?;

    let token_repo = components.token_repo.read().await;
    let token = if token_repo.get_token_list()?.is_empty() {
        // Authentication is disabled, use empty token
        ""
    } else {
        token_repo.get_token(issuer)?.value.as_str()
    };

    let ciphertxt = URL_SAFE_NO_PAD
        .decode(ciphertxt_b64)
        .map_err(|e| unprocessable_entity!("Invalid base64 in 'ct' parameter: {}", e))?;

    let salt = URL_SAFE_NO_PAD
        .decode(salt_b64)
        .map_err(|e| unprocessable_entity!("Invalid base64 in 's' parameter: {}", e))?;

    let key = derive_key_from_secret(token.as_bytes(), &salt);
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
    let query: QueryLinkCreateRequest = serde_json::from_slice(&query)
        .map_err(|e| unprocessable_entity!("Failed to parse query: {}", e))?;

    // Check expiration
    if query.expire_at < chrono::Utc::now() {
        return Err(unprocessable_entity!("Query link has expired").into());
    }
    Ok((record_num, token.to_string(), query))
}

async fn process_query_and_fetch_record(
    record_num: u64,
    repo_ext: &Box<dyn ManageExtensions + Send + Sync>,
    id: u64,
    rx: Arc<RwLock<QueryRx>>,
    range: Option<Range>,
) -> Result<impl IntoResponse, HttpError> {
    let mut count = 0;
    loop {
        let Some(readers) = repo_ext.fetch_and_process_record(id, rx.clone()).await else {
            continue;
        };

        for reader in readers {
            let reader = match reader {
                Ok(r) => r,
                Err(ReductError {
                    status: NoContent, ..
                }) => return Err(not_found!("Record number out of range").into()),
                Err(e) => return Err(e.into()),
            };

            if count == record_num {
                let mut headers = make_headers_from_reader(reader.meta());
                headers.typed_insert(AcceptRanges::bytes());

                return if let Some(range) = range {
                    // TODO: Cash record on backend side to avoid double reading if we have range request

                    let content_length = range
                        .satisfiable_ranges(reader.meta().content_length())
                        .map(|(start, end)| match (start, end) {
                            (Included(s), Included(e)) => e - s + 1,
                            (Included(s), Bound::Unbounded) => reader.meta().content_length() - s,
                            (Bound::Unbounded, Included(e)) => e + 1,
                            _ => 0,
                        })
                        .sum();

                    headers.typed_insert(ContentLength(content_length));
                    headers.typed_insert(range.clone());

                    Ok((
                        StatusCode::PARTIAL_CONTENT,
                        headers,
                        Body::from_stream(RangeRecordStream::new(reader, range)),
                    ))
                } else {
                    Ok((
                        StatusCode::OK,
                        headers,
                        Body::from_stream(RecordStream::new(reader, false)),
                    ))
                };
            }
        }

        count += 1;
    }
}

struct RangeRecordStream {
    reader: Box<dyn ReadRecord + Send>,
    ranges: VecDeque<(Bound<u64>, Bound<u64>)>,
}

impl RangeRecordStream {
    pub fn new(reader: Box<dyn ReadRecord + Send>, range: Range) -> Self {
        let ranges = range
            .satisfiable_ranges(reader.meta().content_length())
            .collect();
        Self { reader, ranges }
    }
}

impl Stream for RangeRecordStream {
    type Item = Result<Bytes, HttpError>;

    fn poll_next(
        mut self: Pin<&mut RangeRecordStream>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let Some(range) = self.ranges.pop_front() else {
            return Poll::Ready(None);
        };

        let (start, end) = match range {
            (Included(s), Included(e)) => (s, e + 1),
            (Included(s), Bound::Unbounded) => (s, self.reader.meta().content_length()),
            (Bound::Unbounded, Included(e)) => (0, e + 1),
            _ => return Poll::Ready(Some(Err(unprocessable_entity!("Invalid range").into()))),
        };

        // TODO: Ranges can be very large, we should read in chunks instead of allocating a big buffer
        let mut buf = vec![0; (end - start) as usize];
        self.reader.seek(Start(start)).unwrap();
        let read = self.reader.read(&mut buf);
        match read {
            Ok(0) => Poll::Ready(None),
            Ok(n) => Poll::Ready(Some(Ok(Bytes::from(buf[..n].to_vec())))),
            Err(e) => Poll::Ready(Some(Err(unprocessable_entity!("Read error: {}", e).into()))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::links::tests::create_query_link;
    use crate::api::tests::{components, headers};
    use axum::body::to_bytes;
    use chrono::Utc;
    use reduct_base::msg::entry_api::{QueryEntry, QueryType};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_get_query_link(#[future] components: Arc<Components>, headers: HeaderMap) {
        let components = components.await;

        let link = create_query_link(
            headers,
            components.clone(),
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

        let response = get(
            State(Arc::clone(&components)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(
                url::form_urlencoded::parse(link.split('?').nth(1).unwrap().as_bytes())
                    .into_owned()
                    .collect(),
            ),
        )
        .await
        .unwrap();

        let resp = response.into_response();
        assert_eq!(resp.headers()["content-type"], "text/plain");
        assert_eq!(resp.headers()["content-length"], "6");
        assert_eq!(resp.headers()["x-reduct-label-x"], "y");

        let body_bytes = to_bytes(resp.into_body(), 1000).await.unwrap();
        assert_eq!(
            String::from_utf8_lossy(body_bytes.iter().as_slice()),
            "Hey!!!"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_query_link_record_out_of_range(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let link = create_query_link(
            headers,
            components.clone(),
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
        params.insert("r".to_string(), "10".to_string()); // out of range
        let result = get(
            State(Arc::clone(&components)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(params),
        )
        .await;
        assert_eq!(
            result.err().unwrap().0,
            not_found!("Record number out of range")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_expire_at_in_past(#[future] components: Arc<Components>, headers: HeaderMap) {
        let components = components.await;
        let link = create_query_link(
            headers,
            components.clone(),
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
            State(Arc::clone(&components)),
            HeaderMap::new(),
            Path("file.txt".to_string()),
            Query(params),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(err.0, unprocessable_entity!("Query link has expired"));
    }
    mod validation {
        use super::*;
        use rstest::rstest;
        #[rstest]
        #[case("ct", "XXX", "Invalid base64 in 'ct' parameter")]
        #[case("s", "XXX", "Invalid base64 in 's' parameter")]
        #[case("n", "XXX", "Invalid base64 in 'n' parameter")]
        #[tokio::test]
        async fn test_get_query_link_invalid_base64(
            #[future] components: Arc<Components>,
            headers: HeaderMap,
            #[case] key: &str,
            #[case] value: &str,
            #[case] _error_msg: &str,
        ) {
            let components = components.await;
            let link = create_query_link(
                headers,
                components.clone(),
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
                State(Arc::clone(&components)),
                HeaderMap::new(),
                Path("file.txt".to_string()),
                Query(modified_params),
            )
            .await;
            assert!(result
                .err()
                .unwrap()
                .0
                .to_string()
                .contains(&format!("Invalid base64 in '{}' parameter", key)));
        }

        #[rstest]
        #[tokio::test]
        #[case("ct")]
        #[case("s")]
        #[case("n")]
        #[case("i")]
        async fn test_get_query_link_missing_params(
            #[future] components: Arc<Components>,
            headers: HeaderMap,
            #[case] key: &str,
        ) {
            let components = components.await;
            let link = create_query_link(
                headers,
                components.clone(),
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
                State(Arc::clone(&components)),
                HeaderMap::new(),
                Path("file.txt".to_string()),
                Query(modified_params),
            )
            .await;
            assert_eq!(
                result.err().unwrap().0,
                unprocessable_entity!("Missing '{}' parameter", key)
            );
        }
    }

    mod fetching {
        use super::*;
        use reduct_base::internal_server_error;
        use tokio::sync::mpsc::channel;

        #[rstest]
        #[tokio::test]
        async fn test_fetch_query_error(#[future] components: Arc<Components>) {
            let components = components.await;
            let ext_repo = &components.ext_repo;
            let (tx, rx) = channel(1);
            tx.send(Err(internal_server_error!("Oops"))).await.unwrap();
            let rx = Arc::new(RwLock::new(rx));
            let id = 1;

            let err = process_query_and_fetch_record(0, ext_repo, id, rx, None)
                .await
                .err()
                .unwrap();
            assert_eq!(err.0, internal_server_error!("Oops"));
        }
    }
}
