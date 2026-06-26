// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{Cfg, DEFAULT_PORT};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::replication::proto::replication_repo::Item;
use crate::replication::proto::{
    Label as ProtoLabel, ReplicationCompression as ProtoReplicationCompression,
    ReplicationMode as ProtoReplicationMode, ReplicationRepo as ProtoReplicationRepo,
    ReplicationSettings as ProtoReplicationSettings,
};
use crate::replication::replication_task::ReplicationTask;
use crate::replication::{ManageReplications, TransactionNotification};
use crate::storage::engine::StorageEngine;
use crate::storage::query::condition::Parser;
use crate::storage::query::filters::WhenFilter;
use crate::syslog::SystemEventSink;
use async_trait::async_trait;
use bytes::Bytes;
use log::{debug, error, warn};
use prost::Message;
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::replication_api::{
    FullReplicationInfo, ReplicationCompression, ReplicationInfo, ReplicationMode,
    ReplicationSettings,
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
            dst_prefix: settings.dst_prefix,
            include: Vec::new(),
            exclude: settings
                .exclude
                .into_iter()
                .map(|(k, v)| ProtoLabel { name: k, value: v })
                .collect(),
            each_n: settings.each_n.unwrap_or(0),
            each_s: 0.0, // Deprecated field, always set to 0.0 (migration to $each_t in when condition)
            when: settings.when.map(|value| value.to_string()),
            mode: ProtoReplicationMode::from(&settings.mode) as i32,
            compression: ProtoReplicationCompression::from(&settings.compression) as i32,
        }
    }
}

impl ProtoReplicationSettings {
    /// Convert ProtoReplicationSettings to ReplicationSettings with migration support
    /// Returns (ReplicationSettings, migrated: bool)
    fn into_settings(self) -> (ReplicationSettings, bool) {
        let mut migrated = false;

        // Parse the when condition first
        let mut when: Option<serde_json::Value> = if let Some(when_str) = self.when {
            match serde_json::from_str(&when_str) {
                Ok(value) => Some(value),
                Err(err) => {
                    error!(
                        "Failed to parse 'when' field: {} in replication settings: {}",
                        err, when_str
                    );
                    None
                }
            }
        } else {
            None
        };

        // Migrate deprecated each_s to $each_t by injecting it into the when condition
        if self.each_s > 0.0 {
            migrated = true;
            warn!(
                "The 'each_s' field is deprecated and will be migrated to 'when' condition using $each_t operator. Value: {}",
                self.each_s
            );

            if let Some(when_value) = &mut when {
                // Inject $each_t into the existing when condition
                if let Some(obj) = when_value.as_object_mut() {
                    obj.insert("$each_t".to_string(), serde_json::json!(self.each_s));
                } else {
                    error!(
                        "Existing 'when' condition is not an object, cannot inject $each_t. Using only $each_t condition."
                    );
                    when = Some(serde_json::json!({"$each_t": self.each_s}));
                }
            } else {
                // No when condition exists, create one with just $each_t
                when = Some(serde_json::json!({"$each_t": self.each_s}));
            }
        }

        // Migrate deprecated include to $in by injecting it into the when condition
        if !self.include.is_empty() {
            migrated = true;
            warn!(
                "The 'include' field is deprecated and will be migrated to 'when' condition using $in operator. Value: {:?}",
                self.include
            );
            for include in &self.include {
                if let Some(when_value) = &mut when {
                    // Inject $in into the existing when condition
                    if let Some(obj) = when_value.as_object_mut() {
                        obj.insert(
                            "$in".to_string(),
                            serde_json::json!([&include.name, &include.value]),
                        );
                    } else {
                        error!(
                            "Existing 'when' condition is not an object, cannot inject $in. Using only $in condition."
                        );
                        when = Some(
                            serde_json::json!({"$in": serde_json::json!([&include.name, &include.value])}),
                        );
                    }
                } else {
                    // No when condition exists, create one with just $in
                    when = Some(
                        serde_json::json!({"$in": serde_json::json!([&include.name, &include.value])}),
                    );
                }
            }
        }

        let settings = ReplicationSettings {
            src_bucket: self.src_bucket,
            dst_bucket: self.dst_bucket,
            dst_host: self.dst_host,
            dst_token: if self.dst_token.is_empty() {
                None
            } else {
                Some(self.dst_token)
            },
            entries: self.entries,
            dst_prefix: self.dst_prefix,
            exclude: self
                .exclude
                .into_iter()
                .map(|label| (label.name, label.value))
                .collect(),
            each_n: if self.each_n > 0 {
                Some(self.each_n)
            } else {
                None
            },
            when,
            mode: ProtoReplicationMode::try_from(self.mode)
                .unwrap_or(ProtoReplicationMode::Enabled)
                .into(),
            compression: ProtoReplicationCompression::try_from(self.compression)
                .unwrap_or(ProtoReplicationCompression::None)
                .into(),
        };

        (settings, migrated)
    }
}

impl From<ProtoReplicationSettings> for ReplicationSettings {
    fn from(settings: ProtoReplicationSettings) -> Self {
        settings.into_settings().0
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

impl From<&ReplicationCompression> for ProtoReplicationCompression {
    fn from(compression: &ReplicationCompression) -> Self {
        match compression {
            ReplicationCompression::None => ProtoReplicationCompression::None,
            ReplicationCompression::Zstd => ProtoReplicationCompression::Zstd,
            ReplicationCompression::Gzip => ProtoReplicationCompression::Gzip,
        }
    }
}

impl From<ProtoReplicationCompression> for ReplicationCompression {
    fn from(compression: ProtoReplicationCompression) -> Self {
        match compression {
            ProtoReplicationCompression::None => ReplicationCompression::None,
            ProtoReplicationCompression::Zstd => ReplicationCompression::Zstd,
            ProtoReplicationCompression::Gzip => ReplicationCompression::Gzip,
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
    system_event_sink: Option<SystemEventSink>,
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

        self.create_or_update_replication_task(name, settings).await
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

        self.create_or_update_replication_task(name, settings).await
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
        self.save_repo().await?;
        self.remove_transaction_logs_for_replication(name).await
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
    pub(crate) async fn load_or_create(
        storage: Arc<StorageEngine>,
        config: Cfg,
        system_event_sink: Option<SystemEventSink>,
    ) -> Self {
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
            system_event_sink,
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
                    let (settings, _migrated) = item.settings.unwrap().into_settings();
                    if let Err(err) = repo.create_replication(&item.name, settings).await {
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

        let replication = ReplicationTask::new(
            name.to_string(),
            settings,
            conf,
            Arc::clone(&self.storage),
            self.system_event_sink.clone(),
        )?;
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

    async fn remove_transaction_logs_for_replication(
        &self,
        replication_name: &str,
    ) -> Result<(), ReductError> {
        let log_file_name = format!("{}.log", replication_name);
        let mut dirs = vec![self.storage.data_path().clone()];

        while let Some(dir) = dirs.pop() {
            let mut entries = match tokio::fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err.into()),
            };

            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let file_type = entry.file_type().await?;
                if file_type.is_dir() {
                    dirs.push(path);
                    continue;
                }

                if path.file_name().and_then(|name| name.to_str()) != Some(log_file_name.as_str()) {
                    continue;
                }

                match FILE_CACHE.remove(&path).await {
                    Ok(()) => {}
                    Err(err) if err.status() == ErrorCode::NotFound => {}
                    Err(err) => return Err(err),
                }
            }
        }

        Ok(())
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
            mut settings: ReplicationSettings,
        ) {
            settings.dst_prefix = "robot-1".to_string();
            let storage = storage.await;
            let mut repo =
                ReplicationRepository::load_or_create(Arc::clone(&storage), Cfg::default(), None)
                    .await;
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let repo =
                ReplicationRepository::load_or_create(Arc::clone(&storage), Cfg::default(), None)
                    .await;
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
            let storage = Arc::clone(&repo.storage);
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let bucket = storage
                .get_bucket("bucket-1")
                .await
                .unwrap()
                .upgrade_and_unwrap();
            let mut writer = bucket
                .begin_write("entry-1", 1, 4, "text/plain".to_string(), Labels::default())
                .await
                .unwrap();
            writer
                .send(Ok(Some(bytes::Bytes::from_static(b"data"))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();

            let log_path = storage
                .data_path()
                .join("bucket-1")
                .join("entry-1")
                .join("test.log");
            std::fs::write(&log_path, b"pending transactions").unwrap();

            let mut updated = settings;
            updated.dst_bucket = "bucket-3".to_string();
            updated.dst_token = None;
            repo.update_replication("test", updated).await.unwrap();

            let replication = repo.get_replication_settings("test").await.unwrap();
            assert_eq!(replication.dst_bucket, "bucket-3");
            assert_eq!(replication.dst_token, Some("token".to_string()));
            assert!(
                log_path.exists(),
                "Should keep transaction log when replication settings change"
            );
        }
    }

    mod remove {
        use super::*;
        #[rstest]
        #[tokio::test]
        async fn test_remove_replication(
            #[future] mut repo: ReplicationRepository,
            settings: ReplicationSettings,
        ) {
            let mut repo = repo.await;
            let storage = Arc::clone(&repo.storage);
            repo.create_replication("test", settings.clone())
                .await
                .unwrap();

            let bucket = storage
                .get_bucket("bucket-1")
                .await
                .unwrap()
                .upgrade_and_unwrap();
            let mut writer = bucket
                .begin_write("entry-1", 1, 4, "text/plain".to_string(), Labels::default())
                .await
                .unwrap();
            writer
                .send(Ok(Some(bytes::Bytes::from_static(b"data"))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();

            let log_path = storage
                .data_path()
                .join("bucket-1")
                .join("entry-1")
                .join("test.log");
            std::fs::write(&log_path, b"pending transactions").unwrap();

            repo.remove_replication("test").await.unwrap();
            assert_eq!(repo.replications().await.unwrap().len(), 0);
            assert!(!log_path.exists(), "Should remove transaction log");

            // check if replication is removed from file
            let repo =
                ReplicationRepository::load_or_create(Arc::clone(&storage), Cfg::default(), None)
                    .await;
            assert_eq!(
                repo.replications().await.unwrap().len(),
                0,
                "Should remove replication permanently"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_transaction_logs_for_replication(
            #[future] repo: ReplicationRepository,
        ) {
            let repo = repo.await;
            let storage = Arc::clone(&repo.storage);
            storage
                .create_system_bucket("$system", BucketSettings::default())
                .await
                .unwrap();

            let bucket = storage
                .get_bucket("bucket-1")
                .await
                .unwrap()
                .upgrade_and_unwrap();
            let mut writer = bucket
                .begin_write("entry-1", 1, 4, "text/plain".to_string(), Labels::default())
                .await
                .unwrap();
            writer
                .send(Ok(Some(bytes::Bytes::from_static(b"data"))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();

            let log_path = storage
                .data_path()
                .join("bucket-1")
                .join("entry-1")
                .join("test.log");
            std::fs::write(&log_path, b"pending transactions").unwrap();

            let mut writer = storage
                .begin_write(
                    "$system",
                    "replications/instance/test",
                    1,
                    4,
                    "application/json".to_string(),
                    Labels::default(),
                )
                .await
                .unwrap();
            writer
                .send(Ok(Some(bytes::Bytes::from_static(b"data"))))
                .await
                .unwrap();
            writer.send(Ok(None)).await.unwrap();

            let system_transaction_log_paths = [
                storage
                    .data_path()
                    .join("$system")
                    .join("replications")
                    .join("instance")
                    .join("test")
                    .join("test.log"),
                storage
                    .data_path()
                    .join("$system")
                    .join("usage")
                    .join("instance")
                    .join("total")
                    .join("test.log"),
                storage
                    .data_path()
                    .join("$system")
                    .join("audit")
                    .join("instance")
                    .join("api")
                    .join("test.log"),
            ];
            for path in &system_transaction_log_paths {
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, b"pending transactions").unwrap();
            }

            let system_event_data_path = storage
                .data_path()
                .join("$system")
                .join("usage")
                .join("instance")
                .join("total")
                .join("1.blk");
            std::fs::write(&system_event_data_path, b"system event").unwrap();

            repo.remove_transaction_logs_for_replication("test")
                .await
                .unwrap();

            assert!(!log_path.exists(), "Should remove transaction log");
            for path in &system_transaction_log_paths {
                assert!(!path.exists(), "Should remove diagnostics transaction log");
            }
            assert!(
                system_event_data_path.exists(),
                "Should keep diagnostics data"
            );
            assert!(
                storage
                    .get_bucket("$system")
                    .await
                    .unwrap()
                    .upgrade_and_unwrap()
                    .get_entry("replications/instance/test")
                    .await
                    .is_ok(),
                "Should keep system event entry"
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

    mod include_migration {
        use super::*;
        use crate::replication::proto::{
            Label as ProtoLabel, ReplicationSettings as ProtoReplicationSettings,
        };

        fn create_proto_settings(
            include: Vec<ProtoLabel>,
            when: Option<String>,
        ) -> ProtoReplicationSettings {
            ProtoReplicationSettings {
                src_bucket: "bucket-1".to_string(),
                dst_bucket: "bucket-2".to_string(),
                dst_host: "http://localhost".to_string(),
                dst_token: "token".to_string(),
                entries: vec![],
                dst_prefix: String::new(),
                include,
                exclude: vec![],
                each_n: 0,
                each_s: 0.0,
                when,
                mode: ProtoReplicationMode::Enabled as i32,
                compression: ProtoReplicationCompression::None as i32,
            }
        }

        #[rstest]
        #[tokio::test]
        async fn test_include_migrated_to_in_without_when() {
            let proto_settings = create_proto_settings(
                vec![ProtoLabel {
                    name: "sensor".to_string(),
                    value: "temp".to_string(),
                }],
                None,
            );

            let (settings, migrated) = proto_settings.into_settings();

            assert!(migrated, "Should indicate migration occurred");
            assert_eq!(
                settings.when,
                Some(serde_json::json!({"$in": ["sensor", "temp"]})),
                "Should migrate include to $in in when condition"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_include_migrated_to_in_with_existing_when() {
            let proto_settings = create_proto_settings(
                vec![ProtoLabel {
                    name: "sensor".to_string(),
                    value: "temp".to_string(),
                }],
                Some(r#"{"$eq": ["&status", "active"]}"#.to_string()),
            );

            let (settings, migrated) = proto_settings.into_settings();

            assert!(migrated, "Should indicate migration occurred");
            assert_eq!(
                settings.when,
                Some(serde_json::json!({
                    "$eq": ["&status", "active"],
                    "$in": ["sensor", "temp"]
                })),
                "Should inject $in into existing when condition"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_include_with_multiple_labels() {
            let proto_settings = create_proto_settings(
                vec![
                    ProtoLabel {
                        name: "sensor".to_string(),
                        value: "temp".to_string(),
                    },
                    ProtoLabel {
                        name: "location".to_string(),
                        value: "warehouse".to_string(),
                    },
                ],
                None,
            );

            let (settings, migrated) = proto_settings.into_settings();

            assert!(migrated, "Should indicate migration occurred");
            // Note: The current implementation overwrites with each label,
            // so only the last one will be present. This is the expected behavior.
            assert_eq!(
                settings.when,
                Some(serde_json::json!({"$in": ["location", "warehouse"]})),
                "Should migrate last include label to $in"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_no_migration_when_include_empty() {
            let proto_settings = create_proto_settings(
                vec![],
                Some(r#"{"$eq": ["&status", "active"]}"#.to_string()),
            );

            let (settings, migrated) = proto_settings.into_settings();

            assert!(
                !migrated,
                "Should not indicate migration when include is empty"
            );
            assert_eq!(
                settings.when,
                Some(serde_json::json!({"$eq": ["&status", "active"]})),
                "Should preserve original when condition"
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
            dst_prefix: String::new(),
            exclude: Labels::default(),
            each_n: None,
            when: None,
            mode: ReplicationMode::Enabled,
            compression: Default::default(),
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
        ReplicationRepository::load_or_create(storage, Cfg::default(), None).await
    }

    mod each_s_migration {

        use super::*;

        #[rstest]
        #[tokio::test]
        async fn test_each_s_migrated_to_each_t_without_when() {
            let proto_settings = get_proto_replication_settings(2.5, None).await;

            let (settings, migrated) = proto_settings.into_settings();

            assert!(migrated, "Should mark as migrated");
            assert_eq!(settings.when, Some(serde_json::json!({"$each_t": 2.5})));
        }

        #[rstest]
        #[tokio::test]
        async fn test_each_s_migrated_to_each_t_with_existing_when() {
            let proto_settings =
                get_proto_replication_settings(2.0, Some(r#"{"&label": {"$eq": 1}}"#.to_string()))
                    .await;

            let (settings, migrated) = proto_settings.into_settings();

            assert!(migrated, "Should mark as migrated");
            assert_eq!(
                settings.when,
                Some(serde_json::json!({
                    "&label": {"$eq": 1},
                    "$each_t": 2.0
                }))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_no_migration_when_each_s_is_zero() {
            let proto_settings =
                get_proto_replication_settings(0.0, Some(r#"{"&label": {"$eq": 1}}"#.to_string()))
                    .await;

            let (settings, migrated) = proto_settings.into_settings();

            assert!(!migrated, "Should not mark as migrated");
            assert_eq!(
                settings.when,
                Some(serde_json::json!({"&label": {"$eq": 1}}))
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_when_parsing_error_with_each_s() {
            let proto_settings =
                get_proto_replication_settings(1.5, Some("invalid json".to_string())).await;

            let (settings, migrated) = proto_settings.into_settings();

            assert!(migrated, "Should mark as migrated");
            // When parsing fails, should use only $each_t
            assert_eq!(settings.when, Some(serde_json::json!({"$each_t": 1.5})));
        }

        #[rstest]
        #[tokio::test]
        async fn test_when_parsing_error_without_each_s() {
            let proto_settings =
                get_proto_replication_settings(0.0, Some("invalid json".to_string())).await;

            let (settings, migrated) = proto_settings.into_settings();

            assert!(!migrated, "Should not mark as migrated");
            assert_eq!(settings.when, None);
        }

        #[rstest]
        #[tokio::test]
        async fn test_when_is_not_object_with_each_s() {
            let proto_settings = get_proto_replication_settings(
                3.0,
                Some(r#"["array", "not", "object"]"#.to_string()),
            )
            .await;

            let (settings, migrated) = proto_settings.into_settings();

            assert!(migrated, "Should mark as migrated");
            // When existing condition is not an object, replace with $each_t
            assert_eq!(settings.when, Some(serde_json::json!({"$each_t": 3.0})));
        }

        async fn get_proto_replication_settings(
            each_s: f64,
            when: ::core::option::Option<::prost::alloc::string::String>,
        ) -> ProtoReplicationSettings {
            ProtoReplicationSettings {
                src_bucket: "bucket-1".to_string(),
                dst_bucket: "bucket-2".to_string(),
                dst_host: "http://localhost".to_string(),
                dst_token: "".to_string(),
                dst_prefix: "".to_string(),
                entries: vec![],
                include: vec![],
                exclude: vec![],
                each_n: 0,
                each_s: each_s,
                when: when,
                mode: ProtoReplicationMode::Enabled as i32,
                compression: ProtoReplicationCompression::None as i32,
            }
        }
    }
}
