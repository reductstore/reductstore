// Copyright 2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::{HttpError, StateKeeper};
use crate::audit::{AuditEvent, AuditQuery, AUDIT_BUCKET_NAME};
use crate::auth::policy::ReadAccessPolicy;
use axum::extract::{Path, Query, State};
use axum::routing::get;
use axum::Json;
use axum::Router;
use axum_extra::headers::HeaderMap;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
pub(super) struct AuditQueryParams {
    start: Option<u64>,
    end: Option<u64>,
    endpoint: Option<String>,
}

pub(crate) fn create_audit_api_routes() -> Router<Arc<StateKeeper>> {
    Router::new().route("/{token_name}", get(get_audit_events))
}

pub(super) async fn get_audit_events(
    State(keeper): State<Arc<StateKeeper>>,
    Path(token_name): Path<String>,
    Query(params): Query<AuditQueryParams>,
    headers: HeaderMap,
) -> Result<Json<Vec<AuditEvent>>, HttpError> {
    let components = keeper
        .get_with_permissions(
            &headers,
            ReadAccessPolicy {
                bucket: AUDIT_BUCKET_NAME,
            },
        )
        .await?;

    let filter = AuditQuery {
        start: params.start,
        end: params.end,
        endpoint: params.endpoint,
    };

    let events = components
        .audit_repo
        .write()
        .await?
        .query_token_events(&token_name, filter)
        .await?;

    Ok(Json(events))
}
