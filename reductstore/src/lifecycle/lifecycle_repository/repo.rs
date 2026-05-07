// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::Cfg;
use crate::core::duration::parse_duration_to_micros;
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::lifecycle::action::{build_lifecycle_action, LifecycleContext};
use crate::lifecycle::lifecycle_task::LifecycleTask;
use crate::lifecycle::ManageLifecycles;
use crate::storage::engine::StorageEngine;
use crate::storage::query::condition::Parser;
use async_trait::async_trait;
use log::{debug, error, warn};
use reduct_base::error::ReductError;
use reduct_base::msg::lifecycle_api::{FullLifecycleInfo, LifecycleInfo, LifecycleSettings};
use reduct_base::{conflict, not_found, unprocessable_entity};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::SeekFrom::Start;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;

const LIFECYCLE_REPO_FILE_NAME: &str = ".lifecycles";

#[derive(Serialize, Deserialize, Default)]
struct LifecycleRepoData {
    lifecycles: Vec<LifecycleRepoItem>,
}

#[derive(Serialize, Deserialize)]
struct LifecycleRepoItem {
    name: String,
    settings: LifecycleSettings,
}

pub(crate) struct LifecycleRepository {
    lifecycles: Arc<AsyncRwLock<HashMap<String, LifecycleTask>>>,
    storage: Arc<StorageEngine>,
    repo_path: PathBuf,
    config: Cfg,
    started: bool,
}

#[async_trait]
impl ManageLifecycles for LifecycleRepository {
    async fn create_lifecycle(
        &mut self,
        name: &str,
        settings: LifecycleSettings,
    ) -> Result<(), ReductError> {
        if self.lifecycles.read().await?.contains_key(name) {
            return Err(conflict!("Lifecycle '{}' already exists", name));
        }

        self.create_or_update_lifecycle_task(name, settings).await
    }

    async fn update_lifecycle(
        &mut self,
        name: &str,
        settings: LifecycleSettings,
    ) -> Result<(), ReductError> {
        match self.lifecycles.read().await?.get(name) {
            Some(lifecycle) => {
                if lifecycle.is_provisioned() {
                    Err(conflict!("Can't update provisioned lifecycle '{}'", name))
                } else {
                    Ok(())
                }
            }
            None => Err(not_found!("Lifecycle '{}' does not exist", name)),
        }?;

        self.create_or_update_lifecycle_task(name, settings).await
    }

    async fn lifecycles(&self) -> Result<Vec<LifecycleInfo>, ReductError> {
        let guard = self.lifecycles.read().await?;
        Ok(guard.values().map(|lifecycle| lifecycle.info()).collect())
    }

    async fn get_info(&self, name: &str) -> Result<FullLifecycleInfo, ReductError> {
        let guard = self.lifecycles.read().await?;
        let lifecycle = guard
            .get(name)
            .ok_or_else(|| not_found!("Lifecycle '{}' does not exist", name))?;
        Ok(FullLifecycleInfo {
            info: lifecycle.info(),
            settings: lifecycle.settings().clone(),
        })
    }

    async fn get_lifecycle_settings(&self, name: &str) -> Result<LifecycleSettings, ReductError> {
        let guard = self.lifecycles.read().await?;
        guard
            .get(name)
            .map(|lifecycle| lifecycle.settings().clone())
            .ok_or_else(|| not_found!("Lifecycle '{}' does not exist", name))
    }

    async fn is_lifecycle_running(&self, name: &str) -> Result<bool, ReductError> {
        let guard = self.lifecycles.read().await?;
        guard
            .get(name)
            .map(|lifecycle| lifecycle.is_running())
            .ok_or_else(|| not_found!("Lifecycle '{}' does not exist", name))
    }

    async fn set_lifecycle_provisioned(
        &mut self,
        name: &str,
        provisioned: bool,
    ) -> Result<(), ReductError> {
        let mut guard = self.lifecycles.write().await?;
        let lifecycle = guard
            .get_mut(name)
            .ok_or_else(|| not_found!("Lifecycle '{}' does not exist", name))?;
        lifecycle.set_provisioned(provisioned);
        Ok(())
    }

    async fn remove_lifecycle(&mut self, name: &str) -> Result<(), ReductError> {
        let mut guard = self.lifecycles.write().await?;
        let lifecycle = guard
            .get(name)
            .ok_or_else(|| not_found!("Lifecycle '{}' does not exist", name))?;
        if lifecycle.is_provisioned() {
            return Err(conflict!("Can't remove provisioned lifecycle '{}'", name));
        }

        let removed = guard.remove(name);
        drop(guard);
        if let Some(mut lifecycle) = removed {
            lifecycle.stop().await;
        }
        self.save_repo().await
    }

    fn start(&mut self) {
        self.start_all();
    }

    async fn stop(&mut self) {
        let mut guard = self.lifecycles.write().await.unwrap();
        for (_, task) in guard.iter_mut() {
            task.stop().await;
        }
        self.started = false;
    }
}

impl LifecycleRepository {
    pub(crate) async fn load_or_create(storage: Arc<StorageEngine>, config: Cfg) -> Self {
        let repo_path = storage.data_path().join(LIFECYCLE_REPO_FILE_NAME);
        let mut repo = Self {
            lifecycles: Arc::new(AsyncRwLock::new(HashMap::new())),
            storage,
            repo_path,
            config,
            started: false,
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
            Ok(buf) if !buf.is_empty() => {
                debug!(
                    "Reading lifecycle repository from {}",
                    repo.repo_path.as_os_str().to_str().unwrap_or("...")
                );
                match serde_json::from_slice::<LifecycleRepoData>(&buf) {
                    Ok(data) => {
                        for item in data.lifecycles {
                            if let Err(err) = repo.create_lifecycle(&item.name, item.settings).await
                            {
                                error!("Failed to load lifecycle '{}': {}", item.name, err);
                            }
                        }
                    }
                    Err(err) => error!(
                        "Failed to decode lifecycle repository from {}: {}",
                        repo.repo_path.as_os_str().to_str().unwrap_or("..."),
                        err
                    ),
                }
            }
            Ok(_) => {}
            Err(err) => {
                warn!(
                    "Failed to read lifecycle repository from {}: {}",
                    repo.repo_path.as_os_str().to_str().unwrap_or("..."),
                    err
                );
            }
        }

        repo
    }

    async fn save_repo(&self) -> Result<(), ReductError> {
        let lifecycles = self.lifecycles.read().await?;
        let data = LifecycleRepoData {
            lifecycles: lifecycles
                .iter()
                .map(|(name, lifecycle)| LifecycleRepoItem {
                    name: name.clone(),
                    settings: lifecycle.settings().clone(),
                })
                .collect(),
        };

        let buf = serde_json::to_vec_pretty(&data)
            .map_err(|err| ReductError::internal_server_error(&err.to_string()))?;
        let mut file = FILE_CACHE
            .write_or_create(&self.repo_path, Start(0))
            .await?;
        file.set_len(0)?;
        file.write_all(&buf)?;
        file.sync_all().await?;
        Ok(())
    }

    async fn create_or_update_lifecycle_task(
        &mut self,
        name: &str,
        settings: LifecycleSettings,
    ) -> Result<(), ReductError> {
        if self.storage.get_bucket(&settings.bucket).await.is_err() {
            return Err(not_found!(
                "Bucket '{}' for lifecycle '{}' does not exist",
                settings.bucket,
                name
            ));
        }

        parse_duration_to_micros(&settings.max_age).map_err(|err| {
            unprocessable_entity!("Invalid lifecycle max age '{}': {}", settings.max_age, err)
        })?;

        if let Some(when) = &settings.when {
            let (_, directives) = Parser::new().parse(when.clone()).map_err(|err| {
                unprocessable_entity!("Invalid lifecycle condition: {}", err.message)
            })?;
            if directives.contains_key("#ext") {
                return Err(unprocessable_entity!(
                    "Lifecycle condition cannot use '#ext' directive"
                ));
            }
        }

        let action = build_lifecycle_action(settings.lifecycle_type);
        let mut removed = self.lifecycles.write().await?.remove(name);
        if let Some(mut old) = removed.take() {
            old.stop().await;
        }

        let mut lifecycle = LifecycleTask::new(
            name.to_string(),
            settings,
            self.config.lifecycle_conf.interval,
            action,
            LifecycleContext::new(Arc::clone(&self.storage)),
        );
        if self.started {
            lifecycle.start();
        }
        self.lifecycles
            .write()
            .await?
            .insert(name.to_string(), lifecycle);
        self.save_repo().await
    }

    fn start_all(&mut self) {
        if self.started {
            return;
        }

        if let Some(mut lifecycles) = self.lifecycles.try_write() {
            for (_, task) in lifecycles.iter_mut() {
                task.start();
            }
        } else {
            let lifecycles = Arc::clone(&self.lifecycles);
            tokio::spawn(async move {
                let mut lifecycles = match lifecycles.write().await {
                    Ok(guard) => guard,
                    Err(err) => {
                        error!("Failed to lock lifecycle map: {}", err);
                        return;
                    }
                };
                for (_, task) in lifecycles.iter_mut() {
                    task.start();
                }
            });
        }
        self.started = true;
    }
}
