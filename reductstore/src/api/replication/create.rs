// Copyright 2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::replication::ReplicationSettingsAxum;
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use std::sync::Arc;

// POST /api/v1/replications/:replication_name
pub(crate) async fn create_replication(
    State(components): State<Arc<Components>>,
    Path(replication_name): Path<String>,
    headers: HeaderMap,
    permissions: ReplicationSettingsAxum,
) -> Result<(), HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;
    Ok(())
}
