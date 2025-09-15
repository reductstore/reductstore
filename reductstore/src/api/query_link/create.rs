// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::{FullAccessPolicy, ReadAccessPolicy};
use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use reduct_base::msg::query_link_api::{QueryLinkCreateRequest, QueryLinkCreateResponse};
use reqwest::header::HeaderMap;
use ring::aead::{UnboundKey, AES_256_GCM};
use std::sync::Arc;

// POST /api/v1/query_links/
pub(super) async fn create(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
    params: QueryLinkCreateRequest,
) -> Result<QueryLinkCreateResponse, HttpError> {
    check_permissions(
        &components,
        &headers,
        ReadAccessPolicy {
            bucket: &params.bucket,
        },
    )
    .await?;

    // find current token
    let token = components.token_repo.write().await.validate_token(
        headers
            .get(AUTHORIZATION.as_str())
            .map(|header| header.to_str().unwrap_or("invalid-token")),
    )?;

    // use token to encrypt the query
    let query_string = serde_json::to_string(&params.query)?;
    let mut key_bytes = vec![0; AES_256_GCM.key_len()];
    key_bytes[..token.value.len()].copy_from_slice(token.value.as_bytes());

    let unbound_key = UnboundKey::new(&AES_256_GCM, &key_bytes).unwrap();
    todo!()
}
