// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::asset::asset_manager::ManageStaticAsset;
use crate::ext::ext_repository::{BoxedManageExtensions, ExtRepository, ManageExtensions};
use crate::storage::query::QueryRx;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::ext::{BoxedReadRecord, ExtSettings};
use reduct_base::msg::entry_api::QueryEntry;
use reduct_base::no_content;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock as AsyncRwLock;

pub fn create_ext_repository(
    external_path: Option<PathBuf>,
    embedded_extensions: Vec<Box<dyn ManageStaticAsset + Sync + Send>>,
    settings: ExtSettings,
) -> Result<BoxedManageExtensions, ReductError> {
    if external_path.is_some() || !embedded_extensions.is_empty() {
        let mut paths = if let Some(path) = external_path {
            vec![path]
        } else {
            Vec::new()
        };

        for embedded in &embedded_extensions {
            if let Ok(path) = embedded.absolut_path("") {
                paths.push(path);
            }
        }

        Ok(Box::new(ExtRepository::try_load(
            paths,
            embedded_extensions,
            settings,
        )?))
    } else {
        // Dummy extension repository if
        struct NoExtRepository;

        #[async_trait]
        impl ManageExtensions for NoExtRepository {
            async fn register_query(
                &self,
                _query_id: u64,
                _bucket_name: &str,
                _entry_name: &str,
                _query: QueryEntry,
            ) -> Result<(), ReductError> {
                Ok(())
            }

            async fn fetch_and_process_record(
                &self,
                _query_id: u64,
                query_rx: Arc<AsyncRwLock<QueryRx>>,
            ) -> Option<Result<Vec<BoxedReadRecord>, ReductError>> {
                let result = query_rx
                    .write()
                    .await
                    .recv()
                    .await
                    .map(|record| record.map(|r| vec![Box::new(r) as BoxedReadRecord]));

                if result.is_none() {
                    // If no record is available, return a no content error to finish the query.
                    return Some(Err(no_content!("")));
                }

                result
            }
        }

        Ok(Box::new(NoExtRepository))
    }
}

#[cfg(test)]
mod tests {
    use crate::ext::ext_repository::create_ext_repository;
    use reduct_base::error::ErrorCode::NoContent;
    use reduct_base::ext::ExtSettings;
    use reduct_base::msg::server_api::ServerInfo;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::sync::RwLock as AsyncRwLock;

    #[tokio::test]
    async fn test_no_content_error_returned() {
        // Create the dummy extension repository
        let ext_repo = create_ext_repository(
            None,
            vec![],
            ExtSettings::builder()
                .server_info(ServerInfo::default())
                .build(),
        )
        .unwrap();

        let (tx, rx) = mpsc::channel(1);
        let rx = Arc::new(AsyncRwLock::new(rx));
        drop(tx); // Close the sender to simulate no records being available

        // Call fetch_and_process_record, which should return None
        let result = ext_repo.fetch_and_process_record(1, rx.clone()).await;
        assert!(
            result.is_some(),
            "Should return Some if no record is available"
        );
        let err = result.unwrap().err().unwrap();
        assert_eq!(err.status(), NoContent);
    }
}
