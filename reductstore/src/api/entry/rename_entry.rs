// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::WriteAccessPolicy;
use std::collections::HashMap;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;

use axum::Json;
use reduct_base::msg::entry_api::RenameEnry;
use std::sync::Arc;

// PUT /b/:bucket_name/:entry_name
pub(crate) async fn rename_entry(
    State(components): State<Arc<Components>>,
    Path(path): Path<HashMap<String, String>>,
    headers: HeaderMap,
    request: Json<RenameEnry>,
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
        .get_bucket(bucket_name)?
        .upgrade()?
        .rename_entry(entry_name, &request.new_name.clone())
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers};
    use reduct_base::error::ErrorCode;
    use rstest::{fixture, rstest};

    #[rstest]
    #[tokio::test]
    async fn test_rename_entry(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        request: RenameEnry,
    ) {
        let components = components.await;
        let path = HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]);

        rename_entry(
            State(Arc::clone(&components)),
            Path(path),
            headers,
            request.into(),
        )
        .await
        .unwrap();

        assert_eq!(
            components
                .storage
                .get_bucket("bucket-1")
                .unwrap()
                .upgrade_and_unwrap()
                .get_entry("entry-1")
                .err()
                .unwrap()
                .status(),
            ErrorCode::NotFound
        );

        assert_eq!(
            components
                .storage
                .get_bucket("bucket-1")
                .unwrap()
                .upgrade_and_unwrap()
                .get_entry("entry-2")
                .unwrap()
                .upgrade_and_unwrap()
                .name(),
            "entry-2"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_rename_bucket_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        request: RenameEnry,
    ) {
        let components = components.await;
        let path = HashMap::from_iter(vec![
            ("bucket_name".to_string(), "XXX".to_string()),
            ("entry_name".to_string(), "entry-1".to_string()),
        ]);
        let err = rename_entry(
            State(Arc::clone(&components)),
            Path(path),
            headers,
            request.into(),
        )
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
    async fn test_rename_entry_not_found(
        #[future] components: Arc<Components>,
        headers: HeaderMap,
        request: RenameEnry,
    ) {
        let components = components.await;
        let path = HashMap::from_iter(vec![
            ("bucket_name".to_string(), "bucket-1".to_string()),
            ("entry_name".to_string(), "XXX".to_string()),
        ]);
        let err = rename_entry(
            State(Arc::clone(&components)),
            Path(path),
            headers,
            request.into(),
        )
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

    #[fixture]
    fn request() -> RenameEnry {
        RenameEnry {
            new_name: "entry-2".to_string(),
        }
    }
}
