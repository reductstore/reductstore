// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::FullAccessPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use reduct_base::error::HttpError;

use axum::extract::{Path, State};
use axum::headers::HeaderMap;

use std::sync::Arc;

// DELETE /b/:bucket_name
pub async fn remove_bucket(
    State(components): State<Arc<HttpServerState>>,
    Path(bucket_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    check_permissions(&components, headers, FullAccessPolicy {}).await?;
    components
        .storage
        .write()
        .await
        .remove_bucket(&bucket_name)?;
    components
        .token_repo
        .write()
        .await
        .remove_bucket_from_tokens(&bucket_name)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::http_frontend::HttpServerState;

    use crate::http_frontend::tests::{components, headers};

    use rstest::rstest;

    use std::sync::Arc;

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket(components: Arc<HttpServerState>, headers: HeaderMap) {
        remove_bucket(State(components), Path("bucket-1".to_string()), headers)
            .await
            .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_bucket_from_permission(
        components: Arc<HttpServerState>,
        headers: HeaderMap,
    ) {
        let token = components
            .token_repo
            .read()
            .await
            .find_by_name("test")
            .unwrap();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-1".to_string(), "bucket-2".to_string()]
        );

        remove_bucket(
            State(components.clone()),
            Path("bucket-1".to_string()),
            headers.clone(),
        )
        .await
        .unwrap();

        let token = components
            .token_repo
            .read()
            .await
            .find_by_name("test")
            .unwrap();
        assert_eq!(
            token.permissions.unwrap().read,
            vec!["bucket-2".to_string()]
        );
    }
}
