// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::query_link::{QueryLinkCreateRequestAxum, QueryLinkCreateResponseAxum};
use crate::api::{Components, HttpError};
use crate::auth::policy::ReadAccessPolicy;
use aes_siv::aead::{Aead, KeyInit};
use aes_siv::{Aes128SivAead, Aes256SivAead};
use axum::debug_handler;
use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use axum_extra::headers::HeaderMap;
use base64::Engine;
use rand::rngs::OsRng;
use rand::TryRngCore;
use reduct_base::error::ReductError;
use reduct_base::internal_server_error;
use reduct_base::msg::query_link_api::QueryLinkCreateResponse;
use std::sync::Arc;

// POST /api/v1/query_links/
#[debug_handler]
pub(super) async fn create(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
    params: QueryLinkCreateRequestAxum,
) -> Result<QueryLinkCreateResponseAxum, HttpError> {
    check_permissions(
        &components,
        &headers,
        ReadAccessPolicy {
            bucket: &params.0.bucket,
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
    let query_string = serde_json::to_string(&params.0)?;
    let mut salt = [0u8; 16];
    OsRng.try_fill_bytes(&mut salt).map_err(|e| {
        internal_server_error!(&format!("Failed to generate salt for query link: {}", e))
    })?;

    let key = derive_key_from_secret(token.value.as_bytes(), &salt);
    let cipher = Aes128SivAead::new_from_slice(&key).unwrap();

    let ct = cipher
        .encrypt(&salt.into(), query_string.as_bytes())
        .map_err(|e| -> ReductError {
            internal_server_error!(&format!("Failed to encrypt query: {}", e))
        })?;

    // encode to base64
    let ct_b64 = base64::engine::general_purpose::STANDARD.encode(&ct);
    let salt_b64 = base64::engine::general_purpose::STANDARD.encode(&salt);

    let link = format!(
        "/api/v1/query_links?ct={}&s={}&i={}",
        ct_b64,
        salt_b64,
        token.name.as_str()
    );
    Ok(QueryLinkCreateResponse { link }.into())
}

fn derive_key_from_secret(secret: &[u8], salt: &[u8]) -> [u8; 32] {
    use argon2::Argon2;
    let argon2 = Argon2::default();
    let mut key = [0u8; 32];
    argon2.hash_password_into(secret, salt, &mut key).unwrap();
    key
}
