use crate::api::entry::{QueryEntryAxum, QueryInfoAxum};
use crate::api::HttpError;
use crate::api::StateKeeper;
use crate::auth::policy::ReadAccessPolicy;

use axum::extract::{Path, State};
use axum_extra::headers::HeaderMap;
use reduct_base::msg::entry_api::QueryInfo;
use std::collections::HashMap;
use std::sync::Arc;

// POST /io/:bucket/q
pub(super) async fn query(
    State(keeper): State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    request: QueryEntryAxum,
) -> Result<QueryInfoAxum, HttpError> {
    let request = request.0;
    let bucket_name = path.get("bucket_name").unwrap();
    let components = keeper
        .get_with_permissions(
            &headers,
            ReadAccessPolicy {
                bucket: bucket_name,
            },
        )
        .await?;

    let entry_name = request
        .entries
        .as_ref()
        .and_then(|entries| entries.first())
        .cloned()
        .unwrap_or_default();

    let bucket = components.storage.get_bucket(bucket_name)?.upgrade()?;
    let id = bucket.query(request.clone())?;

    components
        .ext_repo
        .register_query(id, bucket_name, &entry_name, request)
        .await?;

    Ok(QueryInfoAxum::from(QueryInfo { id }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper, path_to_bucket_1};
    use crate::core::sync::AsyncRwLock;
    use crate::core::weak::Weak;
    use crate::storage::bucket::Bucket;
    use crate::storage::query::QueryRx;
    use axum::extract::Path;
    use bytes::Bytes;
    use reduct_base::error::ErrorCode;
    use reduct_base::io::ReadRecord;
    use reduct_base::msg::entry_api::{QueryEntry, QueryType};
    use rstest::rstest;

    async fn write_record(bucket: &Arc<Bucket>, entry: &str, timestamp: u64, data: &str) {
        let mut writer = bucket
            .begin_write(
                entry,
                timestamp,
                data.len() as u64,
                "text/plain".to_string(),
                Default::default(),
            )
            .await
            .unwrap();
        writer
            .send(Ok(Some(Bytes::from(data.to_string()))))
            .await
            .unwrap();
        writer.send(Ok(None)).await.unwrap();
    }

    async fn collect_records(rx: Weak<AsyncRwLock<QueryRx>>) -> Vec<(String, u64)> {
        let rx = rx.upgrade().unwrap();
        let mut rx = rx.write().await.unwrap();
        let mut records = Vec::new();

        while let Some(result) = rx.recv().await {
            match result {
                Ok(reader) => {
                    let meta = reader.meta().clone();
                    records.push((meta.entry_name().to_string(), meta.timestamp()));
                }
                Err(err) => {
                    assert_eq!(err.status(), ErrorCode::NoContent);
                    break;
                }
            }
        }

        records
    }

    #[rstest]
    #[tokio::test]
    async fn aggregates_entries_from_bucket(
        #[future] keeper: Arc<StateKeeper>,
        path_to_bucket_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let keeper = keeper.await;
        let components = keeper.get_anonymous().await.unwrap();
        let bucket = components
            .storage
            .get_bucket("bucket-1")
            .unwrap()
            .upgrade_and_unwrap();

        write_record(&bucket, "entry-a", 20, "aa").await;
        write_record(&bucket, "entry-b", 10, "bb").await;
        write_record(&bucket, "entry-a", 30, "cc").await;

        let request = QueryEntry {
            query_type: QueryType::Query,
            entries: Some(vec!["entry-a".into(), "entry-b".into()]),
            ..Default::default()
        };

        let response = query(
            State(keeper.clone()),
            headers,
            path_to_bucket_1,
            QueryEntryAxum(request),
        )
        .await
        .unwrap();
        let QueryInfo { id } = response.into();

        let (rx, _) = bucket.get_query_receiver(id).await.unwrap();
        let records = collect_records(rx).await;

        assert_eq!(
            records,
            vec![
                ("entry-b".to_string(), 10),
                ("entry-a".to_string(), 20),
                ("entry-a".to_string(), 30)
            ]
        );
    }
}
