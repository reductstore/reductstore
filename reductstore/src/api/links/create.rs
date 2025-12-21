// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::links::{
    derive_key_from_secret, QueryLinkCreateRequestAxum, QueryLinkCreateResponseAxum,
};
use crate::api::{HttpError, StateKeeper};
use crate::auth::policy::ReadAccessPolicy;
use aes_siv::aead::{Aead, KeyInit};
use aes_siv::Aes128SivAead;
use axum::extract::{Path, State};
use axum::http::header::AUTHORIZATION;
use axum_extra::headers::HeaderMap;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use rand::rngs::OsRng;
use rand::TryRngCore;
use reduct_base::error::ReductError;
use reduct_base::msg::query_link_api::QueryLinkCreateResponse;
use reduct_base::{internal_server_error, unprocessable_entity};
use std::io::Write;
use std::sync::Arc;

// POST /api/v1/links/
pub(super) async fn create(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(file_name): Path<String>,
    params: QueryLinkCreateRequestAxum,
) -> Result<QueryLinkCreateResponseAxum, HttpError> {
    let components = keeper
        .get_with_permissions(
            &headers,
            ReadAccessPolicy {
                bucket: &params.0.bucket,
            },
        )
        .await?;

    if params.0.query.query_type != reduct_base::msg::entry_api::QueryType::Query {
        return Err(unprocessable_entity!("Only 'Query' type is supported for query links").into());
    }

    // check and normalize base URL if provided
    let url = check_and_normalize_base_url(&params, components.cfg.public_url.clone())?;

    // find current token
    let token = components.token_repo.write().await?.validate_token(
        headers
            .get(AUTHORIZATION.as_str())
            .map(|header| header.to_str().unwrap_or("invalid-token")),
    )?;

    // compress the query to make the link shorter
    let query_string = serde_json::to_string(&params.0)?;
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
    encoder
        .write_all(query_string.as_bytes())
        .map_err(|e| internal_server_error!("Failed to compress query for query link: {}", e))?;

    // use token to encrypt the query
    let mut salt = [0u8; 16];
    OsRng
        .try_fill_bytes(&mut salt)
        .map_err(|e| internal_server_error!("Failed to generate salt for query link: {}", e))?;

    let key = derive_key_from_secret(token.value.as_bytes(), &salt);
    let cipher = Aes128SivAead::new_from_slice(&key).unwrap();

    let mut nonce_bytes = [0u8; 16];
    OsRng
        .try_fill_bytes(&mut nonce_bytes)
        .map_err(|e| internal_server_error!("Failed to generate nonce for query link: {}", e))?;

    let compressed_query = encoder.finish().map_err(|e| {
        internal_server_error!("Failed to finish compression for query link: {}", e)
    })?;

    let ct = cipher
        .encrypt(&nonce_bytes.into(), compressed_query.as_slice())
        .map_err(|e| -> ReductError { internal_server_error!("Failed to encrypt query: {}", e) })?;

    // encode to base64
    let ct_b64 = URL_SAFE_NO_PAD.encode(&ct);
    let salt_b64 = URL_SAFE_NO_PAD.encode(&salt);
    let nonce_b64 = URL_SAFE_NO_PAD.encode(&nonce_bytes);

    let link = format!(
        "{}api/v1/links/{}?ct={}&s={}&i={}&n={}&r={}",
        url,
        file_name,
        ct_b64,
        salt_b64,
        token.name.as_str(),
        nonce_b64,
        params.0.index.unwrap_or(0)
    );
    Ok(QueryLinkCreateResponse { link }.into())
}

fn check_and_normalize_base_url(
    params: &QueryLinkCreateRequestAxum,
    default_url: String,
) -> Result<String, HttpError> {
    let Some(url) = &params.0.base_url else {
        return Ok(default_url);
    };

    let parsed_url = url::Url::parse(url)
        .map_err(|e| unprocessable_entity!("Invalid base_url provided for query link: {}", e))?;
    if !parsed_url.has_host() {
        return Err(unprocessable_entity!(
            "Invalid base_url provided for query link: missing host"
        )
        .into());
    }

    if !url.ends_with('/') {
        Ok(format!("{}/", url))
    } else {
        Ok(url.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::links::tests::create_query_link;
    use crate::api::tests::{headers, keeper};
    use reduct_base::msg::entry_api::{QueryEntry, QueryType};
    use rstest::rstest;
    use std::sync::Arc;
    use url::Url;

    #[rstest]
    #[tokio::test]
    async fn test_create_query_link(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let keeper = keeper.await;
        let response = create_query_link(
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
        .0;

        let url = Url::parse(&response.link).unwrap();
        let params: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();

        assert!(params.contains_key("ct"));
        assert!(params.contains_key("s"));
        assert!(params.contains_key("i"));
        assert!(params.contains_key("n"));
        assert_eq!(params.get("r").unwrap(), "0");
    }

    #[rstest]
    #[tokio::test]
    async fn test_create_query_link_invalid_type(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let err = create_query_link(
            headers,
            keeper,
            QueryEntry {
                query_type: QueryType::Remove,
                ..Default::default()
            },
            None,
        )
        .await
        .err()
        .unwrap();
        let err: ReductError = err.into();
        assert_eq!(
            err,
            unprocessable_entity!("Only 'Query' type is supported for query links")
        );
    }

    mod check_and_normalize_base_url {
        use super::*;
        use reduct_base::msg::query_link_api::QueryLinkCreateRequest;

        #[rstest]
        fn test_valid_base_url() {
            let params = QueryLinkCreateRequestAxum(QueryLinkCreateRequest {
                base_url: Some("https://example.com/base/".to_string()),
                ..Default::default()
            });
            let result =
                check_and_normalize_base_url(&params, "https://default.com/".to_string()).unwrap();
            assert_eq!(result, "https://example.com/base/");
        }

        #[rstest]
        fn test_base_url_missing_trailing_slash() {
            let params = QueryLinkCreateRequestAxum(QueryLinkCreateRequest {
                base_url: Some("https://example.com/base".to_string()),
                ..Default::default()
            });
            let result =
                check_and_normalize_base_url(&params, "https://default.com/".to_string()).unwrap();
            assert_eq!(result, "https://example.com/base/");
        }

        #[rstest]
        fn test_invalid_base_url() {
            let params = QueryLinkCreateRequestAxum(QueryLinkCreateRequest {
                base_url: Some("ht!tp://invalid-url".to_string()),
                ..Default::default()
            });
            let err = check_and_normalize_base_url(&params, "https://default.com/".to_string())
                .err()
                .unwrap();
            let err: ReductError = err.into();
            assert_eq!(
                err,
                unprocessable_entity!(
                    "Invalid base_url provided for query link: relative URL without a base"
                )
            );
        }

        #[rstest]
        fn test_base_url_default() {
            let params = QueryLinkCreateRequestAxum(QueryLinkCreateRequest {
                base_url: None,
                ..Default::default()
            });
            let result =
                check_and_normalize_base_url(&params, "https://default.com/".to_string()).unwrap();
            assert_eq!(result, "https://default.com/");
        }
    }
}
