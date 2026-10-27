// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::api::http::HttpError;
use crate::api::http::StateKeeper;
use crate::auth::policy::FullAccessPolicy;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;

use std::sync::Arc;

// DELETE /b/:bucket_name
pub(super) async fn remove_bucket(
    State(keeper): State<Arc<StateKeeper>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    let components = keeper
        .get_with_permissions(&headers, FullAccessPolicy {})
        .await?;
    components.storage.remove_bucket(&bucket_name).await?;
    components
        .token_repo
        .write()
        .await
        .unwrap()
        .remove_bucket_from_tokens(&bucket_name)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::api::http::tests::{headers, keeper};
    use crate::storage::bucket::DeletionAttemptHook;
    use crate::storage::engine::StorageEngine;
    use reduct_base::error::{ErrorCode, ReductError};
    use rstest::rstest;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::{sleep, timeout};

    async fn wait_for_bucket_removal(storage: &Arc<StorageEngine>, name: &str) {
        timeout(Duration::from_secs(5), async {
            loop {
                match storage.get_bucket(name).await {
                    Err(err) if err.status() == ErrorCode::NotFound => break,
                    _ => sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("bucket was not removed");
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        remove_bucket(State(keeper.await), Path("bucket-1".to_string()), headers)
            .await
            .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_not_found(#[future] keeper: Arc<StateKeeper>, headers: HeaderMap) {
        let err = remove_bucket(State(keeper.await), Path("not-found".to_string()), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Bucket 'not-found' is not found",)
        )
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_from_permission(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let token = components
            .token_repo
            .write()
            .await
            .unwrap()
            .get_token("test")
            .await
            .unwrap()
            .clone();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-1".to_string(), "bucket-2".to_string()]
        );

        remove_bucket(
            State(keeper.clone()),
            Path("bucket-1".to_string()),
            headers.clone(),
        )
        .await
        .unwrap();

        let components = keeper.get_anonymous().await.unwrap();
        let token = components
            .token_repo
            .write()
            .await
            .unwrap()
            .get_token("test")
            .await
            .unwrap()
            .clone();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-2".to_string()]
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_bucket_repeated_delete_during_active_worker(
        #[future] keeper: Arc<StateKeeper>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let storage = components.storage.clone();
        let bucket = storage
            .get_bucket("bucket-1")
            .await
            .unwrap()
            .upgrade_and_unwrap();

        let attempts = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let hook_attempts = Arc::clone(&attempts);
        let hook_release = Arc::clone(&release);
        let hook: DeletionAttemptHook = Arc::new(
            move |attempt| -> futures_util::future::BoxFuture<'static, Result<(), ReductError>> {
                let attempts = Arc::clone(&hook_attempts);
                let release = Arc::clone(&hook_release);
                Box::pin(async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt == 1 {
                        release.notified().await;
                    }
                    Ok(())
                })
            },
        );
        bucket.set_deletion_attempt_hook(Some(hook)).await;

        remove_bucket(
            State(keeper.clone()),
            Path("bucket-1".to_string()),
            headers.clone(),
        )
        .await
        .unwrap();
        remove_bucket(
            State(keeper.clone()),
            Path("bucket-1".to_string()),
            headers.clone(),
        )
        .await
        .unwrap();

        let components = keeper.get_anonymous().await.unwrap();
        let token = components
            .token_repo
            .write()
            .await
            .unwrap()
            .get_token("test")
            .await
            .unwrap()
            .clone();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-2".to_string()]
        );

        release.notify_one();
        wait_for_bucket_removal(&storage, "bucket-1").await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);

        let components = keeper.get_anonymous().await.unwrap();
        let token = components
            .token_repo
            .write()
            .await
            .unwrap()
            .get_token("test")
            .await
            .unwrap()
            .clone();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-2".to_string()]
        );
    }
}
