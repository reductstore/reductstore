// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::query_link::{
    derive_key_from_secret, QueryLinkCreateRequestAxum, QueryLinkCreateResponseAxum,
};
use crate::api::utils::{make_headers_from_reader, RecordStream};
use crate::api::{Components, HttpError};
use crate::auth::policy::ReadAccessPolicy;
use aes_siv::aead::{Aead, KeyInit};
use aes_siv::{Aes128SivAead, Aes256SivAead, Nonce};
use axum::body::Body;
use axum::debug_handler;
use axum::extract::{Query, State};
use axum::http::header::AUTHORIZATION;
use axum::response::IntoResponse;
use axum_extra::headers::HeaderMap;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use futures_util::StreamExt;
use log::info;
use rand::rngs::OsRng;
use rand::TryRngCore;
use reduct_base::error::ReductError;
use reduct_base::msg::query_link_api::{QueryLinkCreateRequest, QueryLinkCreateResponse};
use reduct_base::{internal_server_error, unprocessable_entity};
use std::collections::HashMap;
use std::sync::Arc;

// GET /api/v1/query_links&ct=...&s=...&i=...&r=...
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
        .ok_or_else(|| unprocessable_entity!("Missing 'r' parameter"))?
        .parse::<u64>()
        .map_err(|e| unprocessable_entity!("Invalid 'r' parameter: {}", e))?;

    let token_repo = components.token_repo.read().await;
    let token = token_repo.get_token(issuer)?;

    let ciphertxt = URL_SAFE_NO_PAD
        .decode(ciphertxt_b64)
        .map_err(|e| unprocessable_entity!("Invalid base64 in 'ct' parameter: {}", e))?;

    let salt = URL_SAFE_NO_PAD
        .decode(salt_b64)
        .map_err(|e| unprocessable_entity!("Invalid base64 in 's' parameter: {}", e))?;

    let key = derive_key_from_secret(token.value.as_bytes(), &salt);
    let cipher = Aes128SivAead::new_from_slice(&key).unwrap();

    let nonce_bytes = URL_SAFE_NO_PAD
        .decode(nonce_b64)
        .map_err(|e| unprocessable_entity!("Invalid base64 in 'n' parameter: {}", e))?;

    let plaintext = cipher
        .decrypt(&Nonce::from_iter(nonce_bytes), ciphertxt.as_ref())
        .map_err(|e| unprocessable_entity!("Failed to decrypt query: {}", e))?;

    let query: QueryLinkCreateRequest = serde_json::from_slice(&plaintext)
        .map_err(|e| unprocessable_entity!("Failed to parse decrypted query: {}", e))?;

    check_permissions(
        &components,
        &HeaderMap::from_iter([(
            AUTHORIZATION,
            format!("Bearer {}", token.value).parse().unwrap(),
        )]),
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
        let reader = reader?;
        if count == record_num {
            let headers = make_headers_from_reader(&reader);
            return Ok((headers, Body::from_stream(RecordStream::new(reader, false))));
        }
        count += 1;
    }

    Err(unprocessable_entity!("Record number out of range").into())
}
