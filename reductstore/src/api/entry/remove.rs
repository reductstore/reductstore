// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use std::collections::HashMap;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;

use std::sync::Arc;

// DELETE /b/:bucket_name/:entry_name
pub(crate) async fn remove_entry(
    State(components): State<Arc<Components>>,
    Path(path): Path<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();

    check_permissions(
        &components,
        headers,
        WriteAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )
    .await?;

    components
        .storage
        .write()
        .await
        .get_bucket_mut(bucket_name)?
        .remove_entry(entry_name)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers};
    use reduct_base::error::ErrorCode;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_remove_entry(#[future] components: Arc<Components>, headers: HeaderMap) {
        let components = components.await;
        let path = HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]);
        remove_entry(State(Arc::clone(&components)), Path(path), headers)
            .await
            .unwrap();

        assert_eq!(
            components
                .storage
                .read()
                .await
                .get_bucket("bucket-1")
                .unwrap()
                .get_entry("entry-1")
                .err()
                .unwrap()
                .status(),
            ErrorCode::NotFound
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let path = HashMap::from_iter(vec![
            ("bucket_name".to_string(), "XXX".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]);
        let err = remove_entry(State(Arc::clone(&components)), Path(path), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            HttpError::new(ErrorCode::NotFound, "Bucket 'XXX' is not found",)
        )
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_entry_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let path = HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "XXX".to_string()),
        ]);
        let err = remove_entry(State(Arc::clone(&components)), Path(path), headers)
            .await
            .err()
            .unwrap();
        assert_eq!(
            err,
            HttpError::new(
                ErrorCode::NotFound,
                "Entry 'XXX' not found in bucket 'bucket-1'",
            )
        )
    }
}
