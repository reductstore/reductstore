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
            ) -> Option<Result<BoxedReadRecord, ReductError>> {
                let result = query_rx
                    .write()
                    .await
                    .recv()
                    .await
                    .map(|record| record.map(|r| Box::new(r) as BoxedReadRecord));

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
