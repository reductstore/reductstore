// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod write;

use crate::api::io::write::write_batched_records;
use crate::api::StateKeeper;
use axum::routing::post;
use std::sync::Arc;

pub(super) fn create_io_api_routes() -> axum::Router<Arc<StateKeeper>> {
    axum::Router::new().route("/{bucket_name}/write", post(write_batched_records))
}
