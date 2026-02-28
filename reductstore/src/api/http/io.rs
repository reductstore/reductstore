// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

mod query;
mod read;
mod remove;
mod update;
mod write;

use crate::api::http::io::query::query;
use crate::api::http::io::read::read_batched_records;
use crate::api::http::io::remove::remove_batched_records;
use crate::api::http::io::update::update_batched_records;
use crate::api::http::io::write::write_batched_records;
use crate::api::http::StateKeeper;
use axum::routing::{delete, get, patch, post};
use std::sync::Arc;

pub(super) fn create_io_api_routes() -> axum::Router<Arc<StateKeeper>> {
    axum::Router::new()
        .route("/{bucket_name}/write", post(write_batched_records))
        .route(
            "/{bucket_name}/read",
            get(read_batched_records).head(read_batched_records),
        )
        .route("/{bucket_name}/remove", delete(remove_batched_records))
        .route("/{bucket_name}/update", patch(update_batched_records))
        .route("/{bucket_name}/q", post(query))
}
