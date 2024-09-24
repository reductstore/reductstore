// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;

use reduct_base::batch::sort_headers_by_time;

use crate::api::entry::common::err_to_batched_header;
use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::WriteAccessPolicy;

// DELETE /:bucket/:entry/batch
pub(crate) async fn remove_batched_records(
    State(components): State<Arc<Components>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    _: Body,
) -> Result<HeaderMap, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    check_permissions(
        &components,
        headers.clone(),
        WriteAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )
    .await?;

    let entry_name = path.get("entry_name").unwrap();
    let record_headers: Vec<_> = sort_headers_by_time(&headers)?;

    let err_map = {
        let entry = components
            .storage
            .get_bucket(bucket_name)?
            .upgrade()?
            .get_entry(entry_name)?
            .upgrade()?;
        entry
            .remove_records(record_headers.iter().map(|(time, _)| *time).collect())
            .await?
    };

    let mut headers = HeaderMap::new();
    err_map.iter().for_each(|(time, err)| {
        err_to_batched_header(&mut headers, *time, err);
    });

    Ok(headers.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, empty_body, headers, path_to_entry_1};
    use reduct_base::error::{ErrorCode, ReductError};
    use reduct_base::not_found;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_remove_record_bad_timestamp(
        #[future] components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        headers.insert("content-length", "0".parse().unwrap());
        headers.insert("x-reduct-time-xxx", "10".parse().unwrap());

        let err = remove_batched_records(
            State(components.await),
            headers,
            path_to_entry_1,
            empty_body.await,
        )
        .await
        .err()
        .unwrap();

        assert_eq!(
            err,
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "Invalid header'x-reduct-time-xxx': must be an unix timestamp in microseconds",
            )
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_batched_records(
        #[future] components: Arc<Components>,
        mut headers: HeaderMap,
        path_to_entry_1: Path<HashMap<String, String>>,
        #[future] empty_body: Body,
    ) {
        let components = components.await;
        headers.insert("x-reduct-time-0", "".parse().unwrap());
        headers.insert("x-reduct-time-1", "".parse().unwrap());

        let err_map = remove_batched_records(
            State(Arc::clone(&components)),
            headers,
            path_to_entry_1,
            empty_body.await,
        )
        .await
        .unwrap();

        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade()
            .unwrap();

        let err = bucket
            .get_entry("entry-1")
            .unwrap()
            .upgrade()
            .unwrap()
            .begin_read(0)
            .await
            .err()
            .unwrap();
        assert_eq!(err, not_found!("No record with timestamp 0"));

        assert_eq!(err_map.len(), 1);
        assert_eq!(
            err_map.get("x-reduct-error-1").unwrap(),
            "404,No record with timestamp 1"
        );
    }
}
