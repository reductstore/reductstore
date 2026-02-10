// Copyright 2023-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::{Cfg, DEFAULT_PORT};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::replication::proto::replication_repo::Item;
use crate::replication::proto::{
    Label as ProtoLabel, ReplicationMode as ProtoReplicationMode,
    ReplicationRepo as ProtoReplicationRepo, ReplicationSettings as ProtoReplicationSettings,
};
use crate::replication::replication_task::ReplicationTask;
use crate::replication::{ManageReplications, TransactionNotification};
use crate::storage::engine::StorageEngine;
use crate::storage::query::condition::Parser;
use crate::storage::query::filters::WhenFilter;
use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, error, warn};
use prost::Message;
use reduct_base::error::ReductError;
use reduct_base::msg::replication_api::{
    FullReplicationInfo, ReplicationInfo, ReplicationMode, ReplicationSettings,
};
use reduct_base::{not_found, unprocessable_entity};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::SeekFrom::Start;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::task::JoinHandle;
use url::Url;

const REPLICATION_REPO_FILE_NAME: &str = ".replications";

impl From<ReplicationSettings> for ProtoReplicationSettings {
    fn from(settings: ReplicationSettings) -> Self {
        Self {
            src_bucket: settings.src_bucket,
            dst_bucket: settings.dst_bucket,
            dst_host: settings.dst_host,
            dst_token: settings.dst_token.unwrap_or_default(),
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
            when: settings.when.map(|value| value.to_string()),
            mode: ProtoReplicationMode::from(&settings.mode) as i32,
        }
    }
}

impl From<ProtoReplicationSettings> for ReplicationSettings {
    fn from(settings: ProtoReplicationSettings) -> Self {
        Self {
            src_bucket: settings.src_bucket,
            dst_bucket: settings.dst_bucket,
            dst_host: settings.dst_host,
            dst_token: if settings.dst_token.is_empty() {
                None
            } else {
                Some(settings.dst_token)
            },
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
            when: if let Some(when) = settings.when {
                match serde_json::from_str(&when) {
                    Ok(value) => Some(value),
                    Err(err) => {
                        error!(
                            "Failed to parse 'when' field: {} in replication settings: {}",
                            err, when
                        );
                        None
                    }
                }
            } else {
                None
            },
            mode: ProtoReplicationMode::try_from(settings.mode)
                .unwrap_or(ProtoReplicationMode::Enabled)
                .into(),
        }
    }
}

impl From<&ReplicationMode> for ProtoReplicationMode {
    fn from(mode: &ReplicationMode) -> Self {
        match mode {
            ReplicationMode::Enabled => ProtoReplicationMode::Enabled,
            ReplicationMode::Paused => ProtoReplicationMode::Paused,
            ReplicationMode::Disabled => ProtoReplicationMode::Disabled,
        }
    }
}

impl From<ProtoReplicationMode> for ReplicationMode {
    fn from(mode: ProtoReplicationMode) -> Self {
        match mode {
            ProtoReplicationMode::Enabled => ReplicationMode::Enabled,
            ProtoReplicationMode::Paused => ReplicationMode::Paused,
            ProtoReplicationMode::Disabled => ReplicationMode::Disabled,
        }
    }
}

/// A repository for managing replications from HTTP API

enum NotificationCommand {
    Notify(TransactionNotification),
    Stop,
}

pub(crate) struct ReplicationRepository {
    replications: Arc<AsyncRwLock<HashMap<String, ReplicationTask>>>,
    storage: Arc<StorageEngine>,
    repo_path: PathBuf,
    config: Cfg,
    started: bool,
    notification_tx: UnboundedSender<NotificationCommand>,
    notification_worker: Option<JoinHandle<()>>,
}

#[async_trait]
impl ManageReplications for ReplicationRepository {
    async fn create_replication(
        &mut self,
        name: &str,
        settings: ReplicationSettings,
    ) -> Result<(), ReductError> {
        // check if replication already exists
        if self.replications.read().await?.contains_key(name) {
            return Err(ReductError::conflict(&format!(
                "Replication '{}' already exists",
                name
            )));
        }

        self.create_or_update_replication_task(&name, settings)
            .await
    }

    async fn update_replication(
        &mut self,
        name: &str,
        settings: ReplicationSettings,
    ) -> Result<(), ReductError> {
        // check if replication exists and not provisioned
        match self.replications.read().await?.get(name) {
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

        self.create_or_update_replication_task(&name, settings)
            .await
    }

    async fn replications(&self) -> Result<Vec<ReplicationInfo>, ReductError> {
        let mut replications = Vec::new();
        let guard = self.replications.read().await?;
        for (_, replication) in guard.iter() {
            replications.push(replication.info().await?);
        }
        Ok(replications)
    }

    async fn get_info(&self, name: &str) -> Result<FullReplicationInfo, ReductError> {
        let guard = self.replications.read().await?;
        let replication = guard.get(name).ok_or_else(|| {
            ReductError::not_found(&format!("Replication '{}' does not exist", name))
        })?;
        let info = FullReplicationInfo {
            info: replication.info().await?,
            settings: replication.masked_settings().clone(),
            diagnostics: replication.diagnostics().await?,
        };
        Ok(info)
    }

    async fn get_replication_settings(
        &self,
        name: &str,
    ) -> Result<ReplicationSettings, ReductError> {
        let guard = self.replications.read().await?;
        guard
            .get(name)
            .map(|replication| replication.settings().clone())
            .ok_or_else(|| {
                ReductError::not_found(&format!("Replication '{}' does not exist", name))
            })
    }

    async fn is_replication_running(&self, name: &str) -> Result<bool, ReductError> {
        let guard = self.replications.read().await?;
        guard
            .get(name)
            .map(|replication| replication.is_running())
            .ok_or_else(|| {
                ReductError::not_found(&format!("Replication '{}' does not exist", name))
            })
    }

    async fn set_replication_provisioned(
        &mut self,
        name: &str,
        provisioned: bool,
    ) -> Result<(), ReductError> {
        let mut guard = self.replications.write().await?;
        let replication = guard.get_mut(name).ok_or_else(|| {
            ReductError::not_found(&format!("Replication '{}' does not exist", name))
        })?;
        replication.set_provisioned(provisioned);
        Ok(())
    }

    async fn remove_replication(&mut self, name: &str) -> Result<(), ReductError> {
        let mut guard = self.replications.write().await?;
        let repl = guard.get(name).ok_or_else(|| {
            ReductError::not_found(&format!("Replication '{}' does not exist", name))
        })?;
        if repl.is_provisioned() {
            return Err(ReductError::conflict(&format!(
                "Can't remove provisioned replication '{}'",
                name
            )));
        }
        let removed = guard.remove(name);
        drop(guard);
        if let Some(mut repl) = removed {
            repl.stop().await;
        }
        self.save_repo().await
    }

    async fn set_mode(&mut self, name: &str, mode: ReplicationMode) -> Result<(), ReductError> {
        let mut guard = self.replications.write().await?;
        let replication = guard.get_mut(name).ok_or_else(|| {
            ReductError::not_found(&format!("Replication '{}' does not exist", name))
        })?;
        replication.set_mode(mode);
        drop(guard);
        self.save_repo().await
    }

    async fn notify(&mut self, notification: TransactionNotification) -> Result<(), ReductError> {
        let should_enqueue = {
            let guard = self.replications.read().await?;
            guard
                .iter()
                .any(|(_, replication)| replication.settings().src_bucket == notification.bucket)
        };
        if should_enqueue {
            self.notification_tx
                .send(NotificationCommand::Notify(notification))
                .map_err(|_| {
                    ReductError::internal_server_error("Failed to enqueue replication notification")
                })?;
        }
        Ok(())
    }

    fn start(&mut self) {
        self.start_all();
    }

    async fn stop(&mut self) {
        let _ = self.notification_tx.send(NotificationCommand::Stop);
        if let Some(worker) = self.notification_worker.take() {
            if let Err(err) = worker.await {
                error!("Failed to join replication notification worker: {:?}", err);
            }
        }

        let mut guard = self.replications.write().await.unwrap();
        for (_, task) in guard.iter_mut() {
            task.stop().await;
        }
    }
}

impl ReplicationRepository {
    pub(crate) async fn load_or_create(storage: Arc<StorageEngine>, config: Cfg) -> Self {
        let repo_path = storage.data_path().join(REPLICATION_REPO_FILE_NAME);
        let replications = Arc::new(AsyncRwLock::new(HashMap::<String, ReplicationTask>::new()));
        let (notification_tx, mut notification_rx) = unbounded_channel::<NotificationCommand>();
        let worker_replications = Arc::clone(&replications);
        let notification_worker = tokio::spawn(async move {
            while let Some(command) = notification_rx.recv().await {
                match command {
                    NotificationCommand::Notify(notification) => {
                        let mut replications = match worker_replications.write().await {
                            Ok(guard) => guard,
                            Err(err) => {
                                error!("Failed to lock replication map: {}", err);
                                continue;
                            }
                        };

                        for (_, replication) in replications.iter_mut() {
                            if replication.settings().src_bucket != notification.bucket {
                                continue;
                            }

                            if let Err(err) = replication.notify(notification.clone()).await {
                                error!("Failed to notify replication task: {}", err);
                            }
                        }
                    }
                    NotificationCommand::Stop => break,
                }
            }
        });

        let mut repo = Self {
            replications,
            storage,
            repo_path,
            config,
            started: false,
            notification_tx,
            notification_worker: Some(notification_worker),
        };

        let read_conf_file = async || {
            let mut lock = FILE_CACHE
                .write_or_create(&repo.repo_path, Start(0))
                .await?;

            let mut buf = Vec::new();
            lock.read_to_end(&mut buf)?;
            Ok::<Vec<u8>, ReductError>(buf)
        };

        match read_conf_file().await {
            Ok(buf) => {
                debug!(
                    "Reading replication repository from {}",
                    repo.repo_path.as_os_str().to_str().unwrap_or("...")
                );
                let proto_repo = ProtoReplicationRepo::decode(&mut Bytes::from(buf))
                    .expect("Error decoding replication repository");
                for item in proto_repo.replications {
                    if let Err(err) = repo
                        .create_replication(&item.name, item.settings.unwrap().into())
                        .await
                    {
                        error!("Failed to load replication '{}': {}", item.name, err);
                    }
                }
            }
            Err(err) => {
                warn!(
                    "Failed to read replication repository from {}: {}",
                    repo.repo_path.as_os_str().to_str().unwrap_or("..."),
                    err
                );
            }
        }
        repo
    }

    async fn save_repo(&self) -> Result<(), ReductError> {
        let replications = self.replications.read().await?;
        let proto_repo = ProtoReplicationRepo {
            replications: replications
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

        let mut file = FILE_CACHE
            .write_or_create(&self.repo_path, Start(0))
            .await?;
        file.set_len(0)?;
        file.write_all(&buf)?;
        file.sync_all().await?;

        Ok(())
    }

    async fn create_or_update_replication_task(
        &mut self,
        name: &str,
        settings: ReplicationSettings,
    ) -> Result<(), ReductError> {
        // check if destination host is valid
        let dest_url = match Url::parse(&settings.dst_host) {
            Ok(url) => url,

            Err(_) => {
                return Err(unprocessable_entity!(
                    "Invalid destination host '{}'",
                    settings.dst_host
                ))
            }
        };

        // check if source bucket exists
        if self.storage.get_bucket(&settings.src_bucket).await.is_err() {
            return Err(not_found!(
                "Source bucket '{}' for replication '{}' does not exist",
                settings.src_bucket,
                name
            ));
        }

        // check if target and source buckets are the same
        if settings.src_bucket == settings.dst_bucket
            && self.config.replication_conf.listening_port
                == dest_url.port_or_known_default().unwrap_or(DEFAULT_PORT)
            && ["127.0.0.1", "localhost", "0.0.0.0"].contains(&dest_url.host_str().unwrap_or(""))
        {
            return Err(unprocessable_entity!(
                "Source and destination buckets must be different",
            ));
        }

        // check syntax of when condition
        let mut conf = self.config.clone();
        if let Some(when) = &settings.when {
            let (cond, directives) = match Parser::new().parse(when.clone()) {
                Ok((cond, dirs)) => (cond, dirs),
                Err(err) => {
                    return Err(unprocessable_entity!(
                        "Invalid replication condition: {}",
                        err.message
                    ))
                }
            };

            let filer = WhenFilter::<TransactionNotification>::try_new(
                cond,
                directives,
                self.config.io_conf.clone(),
                true,
            )?;
            conf.io_conf = filer.io_config().clone();
        }

        // remove old replication because before creating new one
        let mut removed = self.replications.write().await?.remove(name);

        // we keep the old token if the new one is empty (meaning not updated)
        let init_token = settings.dst_token.clone().or_else(|| {
            removed
                .as_ref()
                .and_then(|r| r.settings().dst_token.clone())
        });

        if let Some(mut old) = removed.take() {
            old.stop().await;
        }

        let mut settings = settings;
        settings.dst_token = init_token;

        let replication =
            ReplicationTask::new(name.to_string(), settings, conf, Arc::clone(&self.storage));
        let mut replication = replication;
        if self.started {
            replication.start();
        }
        self.replications
            .write()
            .await?
            .insert(name.to_string(), replication);
        self.save_repo().await
    }
}

impl ReplicationRepository {
    pub(crate) fn start_all(&mut self) {
        if self.started {
            return;
        }

        if let Some(mut replications) = self.replications.try_write() {
            for (_, task) in replications.iter_mut() {
                task.start();
            }
        } else {
            let replications = Arc::clone(&self.replications);
            tokio::spawn(async move {
                let mut replications = match replications.write().await {
                    Ok(guard) => guard,
                    Err(err) => {
                        error!("Failed to lock replication map: {}", err);
                        return;
                    }
                };
                for (_, task) in replications.iter_mut() {
                    task.start();
                }
            });
        }
        self.started = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::sync::{reset_rwlock_config, set_rwlock_timeout};
    use crate::replication::Transaction::WriteRecord;
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::{conflict, internal_server_error, not_found, Labels};
    use rstest::*;
    use serial_test::serial;
    use tokio::time::{sleep, Duration};

    mod create {
        use super::*;
        #[rstest]
        #[tokio::test]
        async fn create_replication(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let repls = repo.replications().await.unwrap();
            assert_eq!(repls.len(), 1);
            assert_eq!(repls[0].name, "test");
            assert_eq!(
                repo.get_replication_settings("test").await.unwrap(),
                settings,
                "Should create replication with the same name and settings"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn create_replication_with_same_name(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            assert_eq!(
                repo.create_replication("test", settings).await,
                Err(conflict!("Replication 'test' already exists")),
                "Should not create replication with the same name"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn create_replication_with_invalid_url(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            let mut settings = settings;
            settings.dst_host = "invalid_url".to_string();

            assert_eq!(
                repo.create_replication("test", settings).await,
                Err(unprocessable_entity!(
                    "Invalid destination host 'invalid_url'"
                )),
                "Should not create replication with invalid url"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn create_replication_to_same_bucket(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            let mut settings = settings;
            settings.dst_host = format!("http://localhost:{}", DEFAULT_PORT);
            settings.dst_bucket = "bucket-1".to_string();

            assert_eq!(
                repo.create_replication("test", settings).await,
                Err(unprocessable_entity!(
                    "Source and destination buckets must be different"
                )),
                "Should not create replication to the same bucket"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_replication_src_bucket_not_found(
            #[future] mut repo: ReplicationRepository,
            mut settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            settings.src_bucket = "bucket-2".to_string();
            assert_eq!(
                repo.create_replication("test", settings).await,
                Err(not_found!(
                    "Source bucket 'bucket-2' for replication 'test' does not exist"
                )),
                "Should not create replication with non existing source bucket"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_replication_with_invalid_when_condition(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            let mut settings = settings;
            settings.when = Some(serde_json::json!({"$UNKNOWN_OP": ["&x", "y"]}));
            assert_eq!(
                repo.create_replication("test", settings).await,
                Err(unprocessable_entity!(
                    "Invalid replication condition: Operator '$UNKNOWN_OP' not supported"
                )),
                "Should not create replication with invalid when condition"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn create_and_load_replications(
            #[future] storage: Arc<StorageEngine>,
            settings: ReplicationSettings,
        ) {
            let storage = storage.await;
            let mut repo =
                ReplicationRepository::load_or_create(Arc::clone(&storage), Cfg::default()).await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let repo =
                ReplicationRepository::load_or_create(Arc::clone(&storage), Cfg::default()).await;
            assert_eq!(repo.replications().await.unwrap().len(), 1);
            assert_eq!(
                repo.get_replication_settings("test").await.unwrap(),
                settings,
                "Should load replication from file"
            );
        }
    }

    mod update {
        use super::*;
        #[rstest]
        #[tokio::test]
        async fn test_update_replication(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let mut settings = settings;
            settings.dst_bucket = "bucket-3".to_string();
            repo.update_replication("test", settings.clone())
                .await
                .unwrap();

            let replication = repo.get_replication_settings("test").await.unwrap();
            assert_eq!(replication.dst_bucket, "bucket-3");
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_provisioned_replication(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            repo.set_replication_provisioned("test", true)
                .await
                .unwrap();

            assert_eq!(
                repo.update_replication("test", settings).await,
                Err(conflict!("Can't update provisioned replication 'test'")),
                "Should not update provisioned replication"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_non_existing_replication(#[future] mut repo: ReplicationRepository) {
            let mut repo = repo.await;
            assert_eq!(
                repo.update_replication("test-2", ReplicationSettings::default())
                    .await,
                Err(not_found!("Replication 'test-2' does not exist")),
                "Should not update non existing replication"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_replication_with_invalid_url(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let mut settings = settings;
            settings.dst_host = "invalid_url".to_string();

            assert_eq!(
                repo.update_replication("test", settings).await,
                Err(unprocessable_entity!(
                    "Invalid destination host 'invalid_url'"
                )),
                "Should not update replication with invalid url"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_replication_to_same_bucket(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let mut settings = settings;
            settings.dst_host = format!("http://localhost:{}", DEFAULT_PORT);
            settings.dst_bucket = "bucket-1".to_string();

            assert_eq!(
                repo.update_replication("test", settings).await,
                Err(unprocessable_entity!(
                    "Source and destination buckets must be different"
                )),
                "Should not update replication to the same bucket"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_replication_src_bucket_not_found(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let mut settings = settings;
            settings.src_bucket = "bucket-2".to_string();

            assert_eq!(
                repo.update_replication("test", settings).await,
                Err(not_found!(
                    "Source bucket 'bucket-2' for replication 'test' does not exist"
                )),
                "Should not update replication with non existing source bucket"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_old_replication_only_for_valid(
            #[future] mut repo: ReplicationRepository,
            mut settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();
            settings.when = Some(serde_json::json!({"$not-exist": [true, true]}));

            let err = repo
                .update_replication("test", settings)
                .await
                .err()
                .unwrap();
            assert_eq!(
                err,
                unprocessable_entity!(
                    "Invalid replication condition: Operator '$not-exist' not supported"
                ),
                "Should not update replication with invalid when condition"
            );

            assert!(repo.get_info("test").await.is_ok(), "Was not removed");
        }

        #[rstest]
        #[tokio::test]
        async fn test_update_replication_keep_dst_token_if_not_set(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let mut updated = settings;
            updated.dst_bucket = "bucket-3".to_string();
            updated.dst_token = None;
            repo.update_replication("test", updated).await.unwrap();

            let replication = repo.get_replication_settings("test").await.unwrap();
            assert_eq!(replication.dst_bucket, "bucket-3");
            assert_eq!(replication.dst_token, Some("token".to_string()));
        }
    }

    mod remove {
        use super::*;
        #[rstest]
        #[tokio::test]
        async fn test_remove_replication(
            #[future] mut repo: ReplicationRepository,
            #[future] storage: Arc<StorageEngine>,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            let storage = storage.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            repo.remove_replication("test").await.unwrap();
            assert_eq!(repo.replications().await.unwrap().len(), 0);

            // check if replication is removed from file
            let repo =
                ReplicationRepository::load_or_create(Arc::clone(&storage), Cfg::default()).await;
            assert_eq!(
                repo.replications().await.unwrap().len(),
                0,
                "Should remove replication permanently"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_non_existing_replication(#[future] mut repo: ReplicationRepository) {
            let mut repo = repo.await;
            assert_eq!(
                repo.remove_replication("test-2").await,
                Err(not_found!("Replication 'test-2' does not exist")),
                "Should not remove non existing replication"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_provisioned_replication(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            repo.set_replication_provisioned("test", true)
                .await
                .unwrap();

            assert_eq!(
                repo.remove_replication("test").await,
                Err(conflict!("Can't remove provisioned replication 'test'")),
                "Should not remove provisioned replication"
            );
        }
    }

    mod get {
        use super::*;
        use reduct_base::io::RecordMeta;

        #[rstest]
        #[tokio::test]
        async fn test_get_replication(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();
            {
                repo.notify(TransactionNotification {
                    bucket: "bucket-1".to_string(),
                    entry: "entry-1".to_string(),
                    meta: RecordMeta::builder().build(),
                    event: WriteRecord(0),
                })
                .await
                .unwrap();
                sleep(Duration::from_millis(100)).await;
            }

            let info = repo.get_info("test").await.unwrap();
            assert_eq!(info.settings.src_bucket, settings.src_bucket);
            assert_eq!(info.info.name, "test");
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_non_existing_replication(#[future] repo: ReplicationRepository) {
            let repo = repo.await;
            assert_eq!(
                repo.get_info("test-2").await.err(),
                Some(not_found!("Replication 'test-2' does not exist")),
                "Should not get non existing replication"
            );
            assert_eq!(
                repo.get_replication_settings("test-2").await.err(),
                Some(not_found!("Replication 'test-2' does not exist")),
                "Should not get settings for non existing replication"
            );
            assert_eq!(
                repo.is_replication_running("test-2").await.err(),
                Some(not_found!("Replication 'test-2' does not exist")),
                "Should not get running state for non existing replication"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_get_mut_non_existing_replication(#[future] mut repo: ReplicationRepository) {
            let mut repo = repo.await;
            assert_eq!(
                repo.set_replication_provisioned("test-2", true).await.err(),
                Some(not_found!("Replication 'test-2' does not exist")),
                "Should not get non existing replication"
            );
        }
    }

    mod notify {
        use super::*;
        use reduct_base::io::RecordMeta;
        use tokio::time::{sleep, Duration};

        #[rstest]
        #[tokio::test]
        async fn test_notify_replication(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let notification = TransactionNotification {
                bucket: "bucket-1".to_string(),
                entry: "entry-1".to_string(),
                meta: RecordMeta::builder().build(),
                event: WriteRecord(0),
            };

            repo.notify(notification.clone()).await.unwrap();
            sleep(Duration::from_millis(50)).await;
            assert_eq!(repo.get_info("test").await.unwrap().info.pending_records, 1);
        }

        #[rstest]
        #[tokio::test]
        async fn test_notify_wrong_bucket(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let notification = TransactionNotification {
                bucket: "bucket-2".to_string(),
                entry: "entry-1".to_string(),
                meta: RecordMeta::builder().build(),
                event: WriteRecord(0),
            };

            repo.notify(notification).await.unwrap();
            sleep(Duration::from_millis(50)).await;
            assert_eq!(
                repo.get_info("test").await.unwrap().info.pending_records,
                0,
                "Should not notify replication for wrong bucket"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_notify_after_stop_returns_error(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test", settings).await.unwrap();
            repo.stop().await;

            let notification = TransactionNotification {
                bucket: "bucket-1".to_string(),
                entry: "entry-1".to_string(),
                meta: RecordMeta::builder().build(),
                event: WriteRecord(0),
            };

            assert_eq!(
                repo.notify(notification).await.err(),
                Some(internal_server_error!(
                    "Failed to enqueue replication notification"
                ))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_notify_skips_non_matching_replications(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            let mut settings_1 = settings.clone();
            settings_1.src_bucket = "bucket-1".to_string();
            repo.create_replication("test-1", settings_1).await.unwrap();

            repo.storage
                .create_bucket("bucket-2", BucketSettings::default())
                .await
                .unwrap();
            let mut settings_2 = settings;
            settings_2.src_bucket = "bucket-2".to_string();
            settings_2.dst_bucket = "bucket-1".to_string();
            repo.create_replication("test-2", settings_2).await.unwrap();

            let notification = TransactionNotification {
                bucket: "bucket-1".to_string(),
                entry: "entry-1".to_string(),
                meta: RecordMeta::builder().build(),
                event: WriteRecord(0),
            };

            repo.notify(notification).await.unwrap();
            sleep(Duration::from_millis(50)).await;
            assert_eq!(
                repo.get_info("test-1").await.unwrap().info.pending_records,
                1
            );
            assert_eq!(
                repo.get_info("test-2").await.unwrap().info.pending_records,
                0
            );
        }

        #[rstest]
        #[tokio::test]
        #[serial]
        async fn test_notify_worker_lock_timeout(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            struct ResetGuard;
            impl Drop for ResetGuard {
                fn drop(&mut self) {
                    reset_rwlock_config();
                }
            }
            let _reset = ResetGuard;

            let mut repo = repo.await;
            repo.create_replication("test", settings).await.unwrap();

            set_rwlock_timeout(Duration::from_millis(20));
            let replications = Arc::clone(&repo.replications);
            let guard = replications.read().await.unwrap();

            let notification = TransactionNotification {
                bucket: "bucket-1".to_string(),
                entry: "entry-1".to_string(),
                meta: RecordMeta::builder().build(),
                event: WriteRecord(0),
            };
            repo.notify(notification).await.unwrap();
            sleep(Duration::from_millis(50)).await;
            drop(guard);

            assert_eq!(repo.get_info("test").await.unwrap().info.pending_records, 0);
        }
    }

    mod start {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_start_all(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test-1", settings.clone())
                .await
                .unwrap();
            repo.create_replication("test-2", settings.clone())
                .await
                .unwrap();

            repo.start();
            assert!(
                repo.is_replication_running("test-1").await.unwrap(),
                "Replication 'test-1' should be running"
            );
            assert!(
                repo.is_replication_running("test-2").await.unwrap(),
                "Replication 'test-2' should be running"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_double_start(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test-1", settings.clone())
                .await
                .unwrap();

            repo.start();
            repo.start(); // second start should have no effect

            assert!(
                repo.is_replication_running("test-1").await.unwrap(),
                "Replication 'test-1' should be running"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_start_all_when_lock_contended(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test-1", settings.clone())
                .await
                .unwrap();
            repo.create_replication("test-2", settings).await.unwrap();

            let replications = Arc::clone(&repo.replications);
            let guard = replications.write().await.unwrap();
            repo.start();
            drop(guard);
            sleep(Duration::from_millis(50)).await;

            assert!(repo.is_replication_running("test-1").await.unwrap());
            assert!(repo.is_replication_running("test-2").await.unwrap());
        }

        #[rstest]
        #[tokio::test]
        #[serial]
        async fn test_start_all_lock_timeout(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            struct ResetGuard;
            impl Drop for ResetGuard {
                fn drop(&mut self) {
                    reset_rwlock_config();
                }
            }
            let _reset = ResetGuard;

            let mut repo = repo.await;
            repo.create_replication("test-1", settings).await.unwrap();

            set_rwlock_timeout(Duration::from_millis(20));
            let replications = Arc::clone(&repo.replications);
            let guard = replications.read().await.unwrap();
            repo.start();
            sleep(Duration::from_millis(50)).await;
            drop(guard);

            assert!(!repo.is_replication_running("test-1").await.unwrap());
        }
    }

    mod set_mode {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_set_mode(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            repo.create_replication("test-1", settings.clone())
                .await
                .unwrap();
            repo.set_mode("test-1", ReplicationMode::Paused)
                .await
                .unwrap();

            assert_eq!(
                repo.get_info("test-1").await.unwrap().info.mode,
                ReplicationMode::Paused
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_set_mode_non_existing(#[future] mut repo: ReplicationRepository) {
            let mut repo = repo.await;
            assert_eq!(
                repo.set_mode("test-1", ReplicationMode::Paused).await,
                Err(not_found!("Replication 'test-1' does not exist"))
            );
        }
    }

    #[fixture]
    fn settings() -> ReplicationSettings {
        ReplicationSettings {
            src_bucket: "bucket-1".to_string(),
            dst_bucket: "bucket-2".to_string(),
            dst_host: "http://localhost".to_string(),
            dst_token: Some("token".to_string()),
            entries: vec!["entry-1".to_string()],
            include: Labels::default(),
            exclude: Labels::default(),
            each_n: None,
            each_s: None,
            when: None,
            mode: ReplicationMode::Enabled,
        }
    }

    #[fixture]
    async fn storage() -> Arc<StorageEngine> {
        let tmp_dir = tempfile::tempdir().unwrap();
        let cfg = Cfg {
            data_path: tmp_dir.keep(),
            ..Cfg::default()
        };
        let storage = StorageEngine::builder()
            .with_data_path(cfg.data_path.clone())
            .with_cfg(cfg)
            .build()
            .await;

        let bucket = storage
            .create_bucket("bucket-1", BucketSettings::default())
            .await
            .unwrap()
            .upgrade_and_unwrap();
        let _ = bucket.get_or_create_entry("entry-1").await.unwrap();
        Arc::new(storage)
    }

    #[fixture]
    async fn repo(#[future] storage: Arc<StorageEngine>) -> ReplicationRepository {
        let storage = storage.await;
        ReplicationRepository::load_or_create(storage, Cfg::default()).await
    }
}
