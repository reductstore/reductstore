// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::server::ServerInfoAxum;
use crate::api::http::{HttpError, StateKeeper};
use crate::auth::policy::AuthenticatedPolicy;
use axum::extract::State;
use axum_extra::headers::HeaderMap;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

static INFO_DIAGNOSTIC_ID: AtomicU64 = AtomicU64::new(1);

// GET /info
pub(super) async fn info(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
) -> Result<ServerInfoAxum, HttpError> {
    let id = INFO_DIAGNOSTIC_ID.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    log::warn!("info_diag id={} stage=authorization event=start", id);
    let components = keeper
        .get_with_permissions(&headers, AuthenticatedPolicy {})
        .await;
    log::warn!(
        "info_diag id={} stage=authorization event=end elapsed={:?} success={}",
        id,
        started.elapsed(),
        components.is_ok()
    );
    let components = components?;
    let started = Instant::now();
    log::warn!("info_diag id={} stage=storage_info event=start", id);
    let info = components.storage.info().await;
    log::warn!(
        "info_diag id={} stage=storage_info event=end elapsed={:?} success={}",
        id,
        started.elapsed(),
        info.is_ok()
    );
    Ok(info?.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::http::tests::{headers, keeper};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_info(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let info = info(State(keeper.await), headers).await.unwrap();
        assert_eq!(info.0.instance_name, "unknown");
        assert_eq!(info.0.instance_role, "PRIMARY");
        assert_eq!(info.0.bucket_count, 2);
    }
}
