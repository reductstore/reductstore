// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::io::IoConfig;
use crate::ext::ext_repository::{BoxedManageExtensions, ExtRepository};
use crate::storage::engine::StorageEngine;
use reduct_base::error::ReductError;
use reduct_base::ext::{ExtSettings, IoExtension};
use std::path::PathBuf;
use std::sync::Arc;

pub fn create_ext_repository(
    external_path: Option<PathBuf>,
    static_extensions: Vec<Box<dyn IoExtension + Send + Sync>>,
    settings: ExtSettings,
    io_config: IoConfig,
    storage: Option<Arc<StorageEngine>>,
) -> Result<BoxedManageExtensions, ReductError> {
    let paths = if let Some(path) = external_path {
        vec![path]
    } else {
        Vec::new()
    };

    Ok(Box::new(ExtRepository::try_load(
        paths,
        static_extensions,
        settings,
        io_config,
        storage,
    )?))
}

#[cfg(test)]
mod tests {
    use crate::cfg::io::IoConfig;
    use crate::core::sync::AsyncRwLock;
    use crate::ext::ext_repository::create_ext_repository;
    use reduct_base::error::ErrorCode;
    use reduct_base::error::ErrorCode::NoContent;
    use reduct_base::ext::ExtSettings;
    use reduct_base::msg::entry_api::QueryEntry;
    use reduct_base::msg::server_api::ServerInfo;
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_no_content_error_returned() {
        // Create extension repository without loading any extensions
        let ext_repo = create_ext_repository(
            None,
            vec![],
            ExtSettings::builder()
                .server_info(ServerInfo::default())
                .build(),
            IoConfig::default(),
            None,
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
        assert_eq!(
            result.unwrap()[0].as_ref().err().unwrap().status(),
            NoContent
        );
    }

    #[tokio::test]
    async fn test_query_with_unknown_extension_returns_error() {
        let ext_repo = create_ext_repository(
            None,
            vec![],
            ExtSettings::builder()
                .server_info(ServerInfo::default())
                .build(),
            IoConfig::default(),
            None,
        )
        .unwrap();

        let err = ext_repo
            .register_query(
                1,
                "bucket-1",
                "entry-1",
                QueryEntry {
                    ext: Some(json!({"test-ext": {"param": 1}})),
                    ..Default::default()
                },
            )
            .await
            .err()
            .unwrap();

        assert_eq!(err.status(), ErrorCode::UnprocessableEntity);
        assert!(err.message().starts_with("Unknown extension 'test-ext'"));
    }
}
