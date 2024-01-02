// Copyright 2024 ReductStore
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::token::{PermissionsAxum, TokenCreateResponseAxum};
use crate::api::{Components, HttpError};
use crate::auth::policy::FullAccessPolicy;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;

use std::sync::Arc;

// POST /api/v1/replications/:replication_name
// pub(crate) async fn create_replication(
//     State(components): State<Arc<Components>>,
//     Path(token_name): Path<String>,
//     headers: HeaderMap,
//     permissions: PermissionsAxum,
// ) -> Result<TokenCreateResponseAxum, HttpError> {
//     check_permissions(&components, headers, FullAccessPolicy {}).await?;
//
//     Ok(TokenCreateResponseAxum(
//         components
//             .token_repo
//             .write()
//             .await
//             .generate_token(&token_name, permissions.into())?,
//     ))
// }
