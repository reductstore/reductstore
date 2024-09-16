// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::replication::proto::replication_repo::Item;
use crate::replication::proto::{
    Label as ProtoLabel, ReplicationRepo as ProtoReplicationRepo,
    ReplicationSettings as ProtoReplicationSettings,
};
use crate::replication::replication_task::ReplicationTask;
use crate::replication::{ManageReplications, TransactionNotification};
use crate::storage::storage::Storage;
use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, error};
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::msg::replication_api::{
    FullReplicationInfo, ReplicationInfo, ReplicationSettings,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use url::Url;

const REPLICATION_REPO_FILE_NAME: &str = ".replications";

impl From<ReplicationSettings> for ProtoReplicationSettings {
    fn from(settings: ReplicationSettings) -> Self {
        Self {
            src_bucket: settings.src_bucket,
            dst_bucket: settings.dst_bucket,
            dst_host: settings.dst_host,
            dst_token: settings.dst_token,
            entries: settings.entries,
            include: settings
                .include
                .into_iter()
                .map(|(k, v)| ProtoLabel { name: k, value: v })
                .collect(),
            exclude: settings
                .exclude
                .into_iter()
                .map(|(k, v)| ProtoLabel { name: k, value: v })
                .collect(),
            each_s: settings.each_s.unwrap_or(0.0),
            each_n: settings.each_n.unwrap_or(0),
        }
    }
}

impl From<ProtoReplicationSettings> for ReplicationSettings {
    fn from(settings: ProtoReplicationSettings) -> Self {
        Self {
            src_bucket: settings.src_bucket,
            dst_bucket: settings.dst_bucket,
            dst_host: settings.dst_host,
            dst_token: settings.dst_token,
            entries: settings.entries,
            include: settings
                .include
                .into_iter()
                .map(|label| (label.name, label.value))
                .collect(),
            exclude: settings
                .exclude
                .into_iter()
                .map(|label| (label.name, label.value))
                .collect(),
            each_s: if settings.each_s > 0.0 {
                Some(settings.each_s)
            } else {
                None
            },
            each_n: if settings.each_n > 0 {
                Some(settings.each_n)
            } else {
                None
            },
        }
    }
}

/// A repository for managing replications from HTTP API

pub(crate) struct ReplicationRepository {
    replications: HashMap<String, ReplicationTask>,
    storage: Arc<Storage>,
    config_path: PathBuf,
}

impl ManageReplications for ReplicationRepository {
    fn create_replication(
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

        self.check_and_create_replication(&name, settings)
    }

    fn update_replication(
        &mut self,
        name: &str,
        settings: ReplicationSettings,
    ) -> Result<(), ReductError> {
        // check if replication exists and not provisioned
        match self.replications.get(name) {
            Some(replication) => {
                if replication.is_provisioned() {
                    Err(ReductError::conflict(&format!(
                        "Can't update provisioned replication '{}'",
                        name
                    )))
                } else {
                    Ok(())
                }
            }
            None => Err(ReductError::not_found(&format!(
                "Replication '{}' does not exist",
                name
            ))),
        }?;

        self.replications.remove(name); // remove old replication because it may have a different connection configuration
        self.check_and_create_replication(&name, settings)
    }

    fn replications(&self) -> Vec<ReplicationInfo> {
        let mut replications = Vec::new();
        for (_, replication) in self.replications.iter() {
            replications.push(replication.info());
        }
        replications
    }

    fn get_info(&self, name: &str) -> Result<FullReplicationInfo, ReductError> {
        let replication = self.get_replication(name)?;
        let info = FullReplicationInfo {
            info: replication.info(),
            settings: replication.masked_settings().clone(),
            diagnostics: replication.diagnostics(),
        };
        Ok(info)
    }

    fn get_replication(&self, name: &str) -> Result<&ReplicationTask, ReductError> {
        self.replications.get(name).ok_or_else(|| {
            ReductError::not_found(&format!("Replication '{}' does not exist", name))
        })
    }

    fn get_mut_replication(&mut self, name: &str) -> Result<&mut ReplicationTask, ReductError> {
        self.replications.get_mut(name).ok_or_else(|| {
            ReductError::not_found(&format!("Replication '{}' does not exist", name))
        })
    }

    fn remove_replication(&mut self, name: &str) -> Result<(), ReductError> {
        let repl = self.get_replication(name)?;
        if repl.is_provisioned() {
            return Err(ReductError::conflict(&format!(
                "Can't remove provisioned replication '{}'",
                name
            )));
        }
        self.replications.remove(name);
        self.save_repo()
    }

    fn notify(&mut self, notification: TransactionNotification) -> Result<(), ReductError> {
        for (_, replication) in self.replications.iter_mut() {
            let _ = replication.notify(notification.clone())?;
        }
        Ok(())
    }
}

impl ReplicationRepository {
    pub(crate) fn load_or_create(storage: Arc<Storage>) -> Self {
        let config_path = storage.data_path().join(REPLICATION_REPO_FILE_NAME);

        let mut repo = Self {
            replications: HashMap::new(),
            storage,
            config_path,
        };

        match std::fs::read(&repo.config_path) {
            Ok(data) => {
                debug!(
                    "Reading replication repository from {}",
                    repo.config_path.as_os_str().to_str().unwrap_or("...")
                );
                let proto_repo = ProtoReplicationRepo::decode(&mut Bytes::from(data))
                    .expect("Error decoding replication repository");
                for item in proto_repo.replications {
                    if let Err(err) =
                        repo.create_replication(&item.name, item.settings.unwrap().into())
                    {
                        error!("Failed to load replication '{}': {}", item.name, err);
                    }
                }
            }
            Err(_) => {
                debug!(
                    "Creating a new token repository {}",
                    repo.config_path.as_os_str().to_str().unwrap_or("...")
                );
                repo.save_repo()
                    .expect("Failed to create a new token repository");
            }
        }
        repo
    }

    fn save_repo(&self) -> Result<(), ReductError> {
        let proto_repo = ProtoReplicationRepo {
            replications: self
                .replications
                .iter()
                .map(|(name, replication)| Item {
                    name: name.clone(),
                    settings: Some(replication.settings().clone().into()),
                })
                .collect(),
        };

        let mut buf = Vec::new();
        proto_repo
            .encode(&mut buf)
            .expect("Error encoding replication repository");

        std::fs::write(&self.config_path, buf).map_err(|e| {
            ReductError::internal_server_error(&format!(
                "Failed to write replication repository to {}: {}",
                self.config_path.as_os_str().to_str().unwrap_or("..."),
                e
            ))
        })
    }

    fn check_and_create_replication(
        &mut self,
        name: &&str,
        settings: ReplicationSettings,
    ) -> Result<(), ReductError> {
        // check if destination host is valid
        if Url::parse(&settings.dst_host).is_err() {
            return Err(ReductError::unprocessable_entity(&format!(
                "Invalid destination host '{}'",
                settings.dst_host
            )));
        }

        // check if source bucket exists
        if self.storage.get_bucket(&settings.src_bucket).is_err() {
            return Err(ReductError::not_found(&format!(
                "Source bucket '{}' for replication '{}' does not exist",
                settings.src_bucket, name
            )));
        }
        let replication =
            ReplicationTask::new(name.to_string(), settings, Arc::clone(&self.storage));
        self.replications.insert(name.to_string(), replication);
        self.save_repo()
    }
}

#[cfg(test)]
mod tests {
    use crate::replication::replication_repository::ReplicationRepository;
    use crate::replication::{ManageReplications, TransactionNotification};
    use crate::storage::storage::Storage;

    use crate::replication::Transaction::WriteRecord;
    use reduct_base::error::ReductError;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::replication_api::ReplicationSettings;
    use reduct_base::Labels;
    use rstest::{fixture, rstest};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::RwLock;
    use tokio::time::sleep;

    #[rstest]
    #[tokio::test]
    async fn create_replication(
        #[future] storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) {
        let mut repo = ReplicationRepository::load_or_create(Arc::clone(&storage.await)).await;
        repo.create_replication("test", settings.clone())
            .await
            .unwrap();

        let repls = repo.replications().await;
        assert_eq!(repls.len(), 1);
        assert_eq!(repls[0].name, "test");
        assert_eq!(
            repo.get_replication("test").unwrap().settings(),
            &settings,
            "Should create replication with the same name and settings"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn create_replication_with_same_name(
        #[future] storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) {
        let mut repo = ReplicationRepository::load_or_create(Arc::clone(&storage.await)).await;
        repo.create_replication("test", settings.clone())
            .await
            .unwrap();

        assert_eq!(
            repo.create_replication("test", settings).await,
            Err(ReductError::conflict("Replication 'test' already exists")),
            "Should not create replication with the same name"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn create_replication_with_invalid_url(
        #[future] storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) {
        let mut repo = ReplicationRepository::load_or_create(Arc::clone(&storage.await)).await;
        let mut settings = settings;
        settings.dst_host = "invalid_url".to_string();

        assert_eq!(
            repo.create_replication("test", settings).await,
            Err(ReductError::unprocessable_entity(
                "Invalid destination host 'invalid_url'"
            )),
            "Should not create replication with invalid url"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn create_and_load_replications(
        #[future] storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) {
        let storage = storage.await;
        let mut repo = ReplicationRepository::load_or_create(Arc::clone(&storage)).await;
        repo.create_replication("test", settings.clone())
            .await
            .unwrap();

        let repo = ReplicationRepository::load_or_create(Arc::clone(&storage)).await;
        assert_eq!(repo.replications().await.len(), 1);
        assert_eq!(
            repo.get_replication("test").unwrap().settings(),
            &settings,
            "Should load replication from file"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_replication(
        #[future] storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) {
        let mut repo = ReplicationRepository::load_or_create(Arc::clone(&storage.await)).await;
        repo.create_replication("test", settings.clone())
            .await
            .unwrap();

        let mut settings = settings;
        settings.dst_bucket = "bucket-3".to_string();
        repo.update_replication("test", settings.clone())
            .await
            .unwrap();

        let replication = repo.get_replication("test").unwrap();
        assert_eq!(replication.settings().dst_bucket, "bucket-3");
    }

    #[rstest]
    #[tokio::test]
    async fn test_update_provisioned_replication(
        #[future] storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) {
        let mut repo = ReplicationRepository::load_or_create(Arc::clone(&storage.await)).await;
        repo.create_replication("test", settings.clone())
            .await
            .unwrap();

        let replication = repo.get_mut_replication("test").unwrap();
        replication.set_provisioned(true);

        assert_eq!(
            repo.update_replication("test", settings).await,
            Err(ReductError::conflict(
                "Can't update provisioned replication 'test'"
            )),
            "Should not update provisioned replication"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_replication(
        #[future] storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) {
        let storage = storage.await;
        let mut repo = ReplicationRepository::load_or_create(Arc::clone(&storage)).await;
        repo.create_replication("test", settings.clone())
            .await
            .unwrap();

        repo.remove_replication("test").unwrap();
        assert_eq!(repo.replications().await.len(), 0);

        // check if replication is removed from file
        let repo = ReplicationRepository::load_or_create(Arc::clone(&storage)).await;
        assert_eq!(
            repo.replications().await.len(),
            0,
            "Should remove replication permanently"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_non_existing_replication(#[future] storage: Arc<RwLock<Storage>>) {
        let mut repo = ReplicationRepository::load_or_create(Arc::clone(&storage.await)).await;
        assert_eq!(
            repo.remove_replication("test-2"),
            Err(ReductError::not_found(
                "Replication 'test-2' does not exist"
            )),
            "Should not remove non existing replication"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_remove_provioned_replication(
        #[future] storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) {
        let mut repo = ReplicationRepository::load_or_create(Arc::clone(&storage.await)).await;
        repo.create_replication("test", settings.clone())
            .await
            .unwrap();

        let replication = repo.get_mut_replication("test").unwrap();
        replication.set_provisioned(true);

        assert_eq!(
            repo.remove_replication("test"),
            Err(ReductError::conflict(
                "Can't remove provisioned replication 'test'"
            )),
            "Should not remove provisioned replication"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_get_replication(
        #[future] storage: Arc<RwLock<Storage>>,
        settings: ReplicationSettings,
    ) {
        let mut repo = ReplicationRepository::load_or_create(Arc::clone(&storage.await)).await;
        repo.create_replication("test", settings.clone())
            .await
            .unwrap();
        {
            let repl = repo.get_mut_replication("test").unwrap();
            repl.notify(TransactionNotification {
                bucket: "bucket-1".to_string(),
                entry: "entry-1".to_string(),
                labels: Vec::new(),
                event: WriteRecord(0),
            })
            .await
            .unwrap();
            sleep(Duration::from_millis(100)).await;
        }

        let info = repo.get_info("test").await.unwrap();
        let repl = repo.get_replication("test").unwrap();
        assert_eq!(info.settings, repl.masked_settings().clone());
        assert_eq!(info.info, repl.info().await);
        assert_eq!(info.diagnostics, repl.diagnostics().await);
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
            each_n: None,
            each_s: None,
        }
    }

    #[fixture]
    async fn storage() -> Arc<RwLock<Storage>> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::load(tmp_dir.into_path(), None).await;
        let bucket = storage
            .create_bucket("bucket-1", BucketSettings::default())
            .unwrap();
        let _ = bucket.get_or_create_entry("entry-1").unwrap();
        Arc::new(RwLock::new(storage))
    }
}
