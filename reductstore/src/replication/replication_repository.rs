// Copyright 2023-204 ReductStore
// Licensed under the Business Source License 1.1

use crate::replication::replication::Replication;
use crate::replication::{ManageReplications, TransactionNotification};
use async_trait::async_trait;

use crate::storage::storage::Storage;
use reduct_base::error::ReductError;
use reduct_base::msg::replication_api::ReplicationSettings;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use url::Url;

pub(crate) struct ReplicationRepository {
    replications: HashMap<String, Replication>,
    storage: Arc<RwLock<Storage>>,
}

#[async_trait]
impl ManageReplications for ReplicationRepository {
    async fn create_replication(
        &mut self,
        name: &str,
        settings: ReplicationSettings,
    ) -> Result<(), ReductError> {
        // check if replication already exists
        if self.replications.contains_key(name) {
            return Err(ReductError::conflict(&format!(
                "Replication '{}' already exists",
                name
            )));
        }

        // check if destination host is valid
        if Url::parse(&settings.dst_host).is_err() {
            return Err(ReductError::bad_request(&format!(
                "Invalid destination host '{}'",
                settings.dst_host
            )));
        }

        // check if source bucket exists
        let storage = self.storage.read().await;
        if storage.get_bucket(&settings.src_bucket).is_err() {
            return Err(ReductError::not_found(&format!(
                "Source bucket '{}' for replication '{}' does not exist",
                settings.src_bucket, name
            )));
        }

        let replication = Replication::new(name.to_string(), settings, Arc::clone(&self.storage));
        self.replications.insert(name.to_string(), replication);
        Ok(())
    }

    fn replications(&self) -> HashMap<String, ReplicationSettings> {
        self.replications
            .iter()
            .map(|(name, replication)| (name.clone(), replication.settings().clone()))
            .collect()
    }

    async fn notify(&self, notification: TransactionNotification) -> Result<(), ReductError> {
        for (_, replication) in self.replications.iter() {
            let _ = replication.notify(notification.clone()).await?;
        }
        Ok(())
    }
}

impl ReplicationRepository {
    pub(crate) fn new(storage: Arc<RwLock<Storage>>) -> Self {
        Self {
            replications: HashMap::new(),
            storage,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::replication::replication_repository::ReplicationRepository;
    use crate::replication::ManageReplications;
    use crate::storage::storage::Storage;
    use reduct_base::error::ReductError;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::replication_api::ReplicationSettings;
    use reduct_base::Labels;
    use rstest::{fixture, rstest};
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[rstest]
    #[tokio::test]
    async fn create_replication(storage: Arc<RwLock<Storage>>, settings: ReplicationSettings) {
        let mut repo = ReplicationRepository::new(Arc::clone(&storage));
        repo.create_replication("test", settings).await.unwrap();

        assert_eq!(repo.replications().len(), 1);
    }

    #[rstest]
    #[tokio::test]
    async fn create_replication_with_same_name(
        storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) {
        let mut repo = ReplicationRepository::new(Arc::clone(&storage));
        repo.create_replication("test", settings.clone())
            .await
            .unwrap();

        assert_eq!(repo.replications().len(), 1);
        assert_eq!(
            repo.create_replication("test", settings).await,
            Err(ReductError::conflict("Replication 'test' already exists")),
            "Should not create replication with the same name"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn create_replication_with_invalid_url(
        storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) {
        let mut repo = ReplicationRepository::new(Arc::clone(&storage));
        let mut settings = settings;
        settings.dst_host = "invalid_url".to_string();

        assert_eq!(
            repo.create_replication("test", settings).await,
            Err(ReductError::bad_request(
                "Invalid destination host 'invalid_url'"
            )),
            "Should not create replication with invalid url"
        );
    }

    #[fixture]
    fn settings() -> ReplicationSettings {
        ReplicationSettings {
            src_bucket: "bucket-1".to_string(),
            dst_bucket: "bucket-2".to_string(),
            dst_host: "http://localhost".to_string(),
            dst_token: "token".to_string(),
            entries: vec!["entry-1".to_string()],
            include: Labels::default(),
            exclude: Labels::default(),
        }
    }

    #[fixture]
    fn storage() -> Arc<RwLock<Storage>> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::new(tmp_dir.into_path());
        storage
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap();
        Arc::new(RwLock::new(storage))
    }
}
