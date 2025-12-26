use crate::api::entry::QueryInfoAxum;
use crate::api::HttpError;
use crate::api::StateKeeper;
use crate::auth::policy::ReadAccessPolicy;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use reduct_base::msg::entry_api::{QueryEntry, QueryInfo};
use std::collections::HashMap;
use std::sync::Arc;

// POST /io/:bucket/query
pub(super) async fn query(
    State(keeper): State<Arc<StateKeeper>>,
    Path(path): Path<HashMap<String, String>>,
    request: QueryEntry,
    headers: HeaderMap,
) -> Result<QueryInfoAxum, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();
    let components = keeper
        .get_with_permissions(
            &headers,
            ReadAccessPolicy {
                bucket: bucket_name,
            },
        )
        .await?;

    let bucket = components.storage.get_bucket(bucket_name)?.upgrade()?;
    let id = bucket.entry_query(request.clone())?;

    components
        .ext_repo
        .register_query(id, bucket_name, entry_name, request)
        .await?;

    Ok(QueryInfoAxum::from(QueryInfo { id }))
}
