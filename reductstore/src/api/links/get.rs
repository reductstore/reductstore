// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::links::derive_key_from_secret;
use crate::api::middleware::check_permissions;
use crate::api::utils::{make_headers_from_reader, RecordStream};
use crate::api::{Components, HttpError};
use crate::auth::policy::ReadAccessPolicy;
use aes_siv::aead::{Aead, KeyInit};
use aes_siv::{Aes128SivAead, Nonce};
use axum::body::Body;
use axum::extract::{Query, State};
use axum::http::header::AUTHORIZATION;
use axum::response::IntoResponse;
use axum_extra::headers::HeaderMap;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use flate2::read::ZlibDecoder;
use reduct_base::error::ErrorCode::NoContent;
use reduct_base::error::ReductError;
use reduct_base::msg::query_link_api::QueryLinkCreateRequest;
use reduct_base::{no_content, not_found, unprocessable_entity};
use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::sync::Arc;

// GET /api/v1/links&ct=...&s=...&i=...&r=...
pub(super) async fn get(
    State(components): State<Arc<Components>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse, HttpError> {
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
    if let Some(exp) = query.expire_at {
        if exp < chrono::Utc::now() {
            return Err(unprocessable_entity!("Query link has expired").into());
        }
    }

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

    let id = entry.query(query.query).await?;
    let rx = entry.get_query_receiver(id.clone())?;

    let mut count = 0;
    while let Some(reader) = rx.upgrade()?.write().await.recv().await {
        let reader = match reader {
            Ok(r) => r,
            Err(ReductError {
                status: NoContent, ..
            }) => break, // end of stream without data
            Err(e) => return Err(e.into()),
        };
        if count == record_num {
            let headers = make_headers_from_reader(&reader);
            return Ok((headers, Body::from_stream(RecordStream::new(reader, false))));
        }
        count += 1;
    }

    Err(not_found!("Record number out of range").into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::links::tests::create_query_link;
    use crate::api::tests::{components, headers};
    use axum::body::to_bytes;
    use chrono::Utc;
    use mockall::Any;
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
        let result = get(State(Arc::clone(&components)), Query(params)).await;
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
        let err = get(State(Arc::clone(&components)), Query(params))
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
            #[case] error_msg: &str,
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
            let result = get(State(Arc::clone(&components)), Query(modified_params)).await;
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
            let result = get(State(Arc::clone(&components)), Query(modified_params)).await;
            assert_eq!(
                result.err().unwrap().0,
                unprocessable_entity!("Missing '{}' parameter", key)
            );
        }
    }
}
