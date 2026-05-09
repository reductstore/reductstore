// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::Cfg;
use crate::core::duration::parse_duration_to_micros;
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use crate::lifecycle::action::{build_lifecycle_action, LifecycleContext};
use crate::lifecycle::lifecycle_task::LifecycleTask;
use crate::lifecycle::{LifecycleAuditSink, ManageLifecycles};
use crate::storage::engine::StorageEngine;
use crate::storage::query::condition::Parser;
use async_trait::async_trait;
use log::{debug, error, warn};
use reduct_base::error::ReductError;
use reduct_base::msg::lifecycle_api::{
    FullLifecycleInfo, LifecycleInfo, LifecycleMode, LifecycleSettings, LifecycleType,
};
use reduct_base::{conflict, internal_server_error, not_found, unprocessable_entity};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::SeekFrom::Start;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

const LIFECYCLE_REPO_FILE_NAME: &str = ".lifecycles";
const MIN_LIFECYCLE_MAX_AGE_US: i64 = 60 * 60 * 1_000_000;
#[cfg(debug_assertions)]
const MIN_LIFECYCLE_INTERVAL_US: i64 = 10 * 1_000_000;
#[cfg(not(debug_assertions))]
const MIN_LIFECYCLE_INTERVAL_US: i64 = 10 * 60 * 1_000_000;
#[cfg(debug_assertions)]
const MIN_LIFECYCLE_INTERVAL_LABEL: &str = "10s";
#[cfg(not(debug_assertions))]
const MIN_LIFECYCLE_INTERVAL_LABEL: &str = "10m";

type LifecycleActionBuilder = Arc<
    dyn Fn(LifecycleType) -> Arc<dyn crate::lifecycle::action::LifecycleAction + Send + Sync>
        + Send
        + Sync,
>;

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
    started: bool,
    action_builder: LifecycleActionBuilder,
    audit_sink: Option<LifecycleAuditSink>,
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

    async fn set_mode(&mut self, name: &str, mode: LifecycleMode) -> Result<(), ReductError> {
        let mut guard = self.lifecycles.write().await?;
        let lifecycle = guard
            .get_mut(name)
            .ok_or_else(|| not_found!("Lifecycle '{}' does not exist", name))?;
        lifecycle.set_mode(mode);
        drop(guard);
        self.save_repo().await
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

    async fn start(&mut self) -> Result<(), ReductError> {
        self.start_all().await
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
    pub(crate) async fn load_or_create(
        storage: Arc<StorageEngine>,
        _config: Cfg,
        audit_sink: Option<LifecycleAuditSink>,
    ) -> Self {
        let repo_path = storage.data_path().join(LIFECYCLE_REPO_FILE_NAME);
        let mut repo = Self {
            lifecycles: Arc::new(AsyncRwLock::new(HashMap::new())),
            storage,
            repo_path,
            started: false,
            action_builder: Arc::new(build_lifecycle_action),
            audit_sink,
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

        let max_age_us = parse_duration_to_micros(&settings.max_age).map_err(|err| {
            unprocessable_entity!("Invalid lifecycle max age '{}': {}", settings.max_age, err)
        })?;

        if max_age_us < MIN_LIFECYCLE_MAX_AGE_US {
            return Err(unprocessable_entity!(
                "Lifecycle max age '{}' is shorter than minimum allowed value of 1h",
                settings.max_age
            ));
        }

        let interval_us = parse_duration_to_micros(&settings.interval).map_err(|err| {
            unprocessable_entity!(
                "Invalid lifecycle interval '{}': {}",
                settings.interval,
                err
            )
        })?;

        if interval_us < MIN_LIFECYCLE_INTERVAL_US {
            return Err(unprocessable_entity!(
                "Lifecycle interval '{}' is shorter than minimum allowed value of {}",
                settings.interval,
                MIN_LIFECYCLE_INTERVAL_LABEL
            ));
        }

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

        let action = (self.action_builder)(settings.lifecycle_type);
        let mut removed = self.lifecycles.write().await?.remove(name);
        if let Some(mut old) = removed.take() {
            old.stop().await;
        }

        let interval = Duration::from_micros(interval_us.max(0) as u64);
        let mut lifecycle = LifecycleTask::new(
            name.to_string(),
            settings,
            interval,
            action,
            LifecycleContext::new(Arc::clone(&self.storage)),
            self.audit_sink.clone(),
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

    #[cfg(test)]
    fn with_action_builder(mut self, action_builder: LifecycleActionBuilder) -> Self {
        self.action_builder = action_builder;
        self
    }

    async fn start_all(&mut self) -> Result<(), ReductError> {
        if self.started {
            return Ok(());
        }

        let mut lifecycles = self
            .lifecycles
            .write()
            .await
            .map_err(|err| internal_server_error!("Failed to lock lifecycle map: {}", err))?;
        for (_, task) in lifecycles.iter_mut() {
            task.start();
        }
        self.started = true;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::action::{LifecycleAction, LifecycleRunResult};
    use reduct_base::msg::bucket_api::BucketSettings;
    use reduct_base::msg::lifecycle_api::{LifecycleMode, LifecycleType};
    use reduct_base::{conflict, not_found, unprocessable_entity};
    use rstest::{fixture, rstest};
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    mockall::mock! {
        Action {}

        #[async_trait]
        impl LifecycleAction for Action {
            fn lifecycle_type(&self) -> LifecycleType;

            async fn run(
                &self,
                name: &str,
                settings: &LifecycleSettings,
                context: LifecycleContext,
            ) -> Result<LifecycleRunResult, ReductError>;
        }
    }

    #[rstest]
    #[tokio::test]
    async fn creates_lifecycle(
        #[future] mut repo: LifecycleRepository,
        settings: LifecycleSettings,
    ) {
        let mut repo = repo.await;
        repo.create_lifecycle("test", settings.clone())
            .await
            .unwrap();

        let lifecycles = repo.lifecycles().await.unwrap();
        assert_eq!(lifecycles.len(), 1);
        assert_eq!(lifecycles[0].name, "test");
        assert!(!lifecycles[0].is_provisioned);
        assert!(!lifecycles[0].is_running);
        assert_eq!(repo.get_lifecycle_settings("test").await.unwrap(), settings);
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_duplicate_lifecycle(
        #[future] mut repo: LifecycleRepository,
        settings: LifecycleSettings,
    ) {
        let mut repo = repo.await;
        repo.create_lifecycle("test", settings.clone())
            .await
            .unwrap();

        assert_eq!(
            repo.create_lifecycle("test", settings).await,
            Err(conflict!("Lifecycle 'test' already exists"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn persists_lifecycle(
        #[future] storage: Arc<StorageEngine>,
        settings: LifecycleSettings,
    ) {
        let storage = storage.await;
        let mut repo =
            LifecycleRepository::load_or_create(Arc::clone(&storage), Cfg::default(), None).await;
        repo.create_lifecycle("test", settings.clone())
            .await
            .unwrap();

        let repo = LifecycleRepository::load_or_create(storage, Cfg::default(), None).await;
        assert_eq!(repo.lifecycles().await.unwrap().len(), 1);
        assert_eq!(repo.get_lifecycle_settings("test").await.unwrap(), settings);
    }

    #[rstest]
    #[tokio::test]
    async fn updates_lifecycle(
        #[future] mut repo: LifecycleRepository,
        settings: LifecycleSettings,
    ) {
        let mut repo = repo.await;
        repo.create_lifecycle("test", settings).await.unwrap();

        let updated = LifecycleSettings {
            entries: vec!["entry-2".to_string()],
            max_age: "2d".to_string(),
            when: Some(serde_json::json!({"$eq": ["&label", "value"]})),
            ..settings_fixture()
        };
        repo.update_lifecycle("test", updated.clone())
            .await
            .unwrap();

        assert_eq!(repo.get_lifecycle_settings("test").await.unwrap(), updated);
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_update_for_missing_lifecycle(#[future] mut repo: LifecycleRepository) {
        let mut repo = repo.await;
        assert_eq!(
            repo.update_lifecycle("missing", settings_fixture()).await,
            Err(not_found!("Lifecycle 'missing' does not exist"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_update_for_provisioned_lifecycle(
        #[future] mut repo: LifecycleRepository,
        settings: LifecycleSettings,
    ) {
        let mut repo = repo.await;
        repo.create_lifecycle("test", settings.clone())
            .await
            .unwrap();
        repo.set_lifecycle_provisioned("test", true).await.unwrap();

        assert_eq!(
            repo.update_lifecycle("test", settings).await,
            Err(conflict!("Can't update provisioned lifecycle 'test'"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn sets_mode(#[future] mut repo: LifecycleRepository, settings: LifecycleSettings) {
        let mut repo = repo.await;
        repo.create_lifecycle("test", settings).await.unwrap();

        repo.set_mode("test", LifecycleMode::Disabled)
            .await
            .unwrap();
        assert_eq!(
            repo.get_info("test").await.unwrap().info.mode,
            LifecycleMode::Disabled
        );
        assert_eq!(
            repo.get_lifecycle_settings("test").await.unwrap().mode,
            LifecycleMode::Disabled
        );

        repo.set_mode("test", LifecycleMode::Enabled).await.unwrap();
        assert_eq!(
            repo.get_info("test").await.unwrap().info.mode,
            LifecycleMode::Enabled
        );
        assert_eq!(
            repo.get_lifecycle_settings("test").await.unwrap().mode,
            LifecycleMode::Enabled
        );
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_set_mode_for_missing_lifecycle(#[future] mut repo: LifecycleRepository) {
        let mut repo = repo.await;

        assert_eq!(
            repo.set_mode("missing", LifecycleMode::Disabled).await,
            Err(not_found!("Lifecycle 'missing' does not exist"))
        );
    }

    #[rstest]
    #[case::missing_bucket(
        LifecycleSettings {
            bucket: "missing".to_string(),
            ..settings_fixture()
        },
        not_found!("Bucket 'missing' for lifecycle 'test' does not exist")
    )]
    #[case::bad_max_age(
        LifecycleSettings {
            max_age: "30days".to_string(),
            ..settings_fixture()
        },
        unprocessable_entity!(
            "Invalid lifecycle max age '30days': [UnprocessableEntity] Invalid duration unit: days"
        )
    )]
    #[case::too_short_max_age(
        LifecycleSettings {
            max_age: "30m".to_string(),
            ..settings_fixture()
        },
        unprocessable_entity!("Lifecycle max age '30m' is shorter than minimum allowed value of 1h")
    )]
    #[case::bad_when(
        LifecycleSettings {
            when: Some(serde_json::json!({"$UNKNOWN_OP": ["&x", "y"]})),
            ..settings_fixture()
        },
        unprocessable_entity!("Invalid lifecycle condition: Operator '$UNKNOWN_OP' not supported")
    )]
    #[case::too_short_interval(
        LifecycleSettings {
            interval: "5s".to_string(),
            ..settings_fixture()
        },
        too_short_interval_error()
    )]
    #[case::ext_when(
        LifecycleSettings {
            when: Some(serde_json::json!({"#ext": {"name": "pipe"}})),
            ..settings_fixture()
        },
        unprocessable_entity!("Lifecycle condition cannot use '#ext' directive")
    )]
    #[tokio::test]
    async fn rejects_invalid_settings(
        #[future] mut repo: LifecycleRepository,
        #[case] settings: LifecycleSettings,
        #[case] expected: ReductError,
    ) {
        let mut repo = repo.await;
        assert_eq!(repo.create_lifecycle("test", settings).await, Err(expected));
        assert!(repo.lifecycles().await.unwrap().is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn removes_lifecycle(
        #[future] mut repo: LifecycleRepository,
        #[future] storage: Arc<StorageEngine>,
        settings: LifecycleSettings,
    ) {
        let mut repo = repo.await;
        let storage = storage.await;
        repo.create_lifecycle("test", settings).await.unwrap();

        repo.remove_lifecycle("test").await.unwrap();
        assert!(repo.lifecycles().await.unwrap().is_empty());

        let repo = LifecycleRepository::load_or_create(storage, Cfg::default(), None).await;
        assert!(repo.lifecycles().await.unwrap().is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_remove_for_provisioned_lifecycle(
        #[future] mut repo: LifecycleRepository,
        settings: LifecycleSettings,
    ) {
        let mut repo = repo.await;
        repo.create_lifecycle("test", settings).await.unwrap();
        repo.set_lifecycle_provisioned("test", true).await.unwrap();

        assert_eq!(
            repo.remove_lifecycle("test").await,
            Err(conflict!("Can't remove provisioned lifecycle 'test'"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn reports_missing_lifecycle(#[future] repo: LifecycleRepository) {
        let repo = repo.await;
        assert_eq!(
            repo.get_info("missing").await.err(),
            Some(not_found!("Lifecycle 'missing' does not exist"))
        );
        assert_eq!(
            repo.get_lifecycle_settings("missing").await.err(),
            Some(not_found!("Lifecycle 'missing' does not exist"))
        );
        assert_eq!(
            repo.is_lifecycle_running("missing").await.err(),
            Some(not_found!("Lifecycle 'missing' does not exist"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_provisioned_flag_for_missing_lifecycle(
        #[future] mut repo: LifecycleRepository,
    ) {
        let mut repo = repo.await;
        assert_eq!(
            repo.set_lifecycle_provisioned("missing", true).await.err(),
            Some(not_found!("Lifecycle 'missing' does not exist"))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn starts_worker_and_calls_action(
        #[future] storage: Arc<StorageEngine>,
        mut settings: LifecycleSettings,
    ) {
        settings.interval = "100ms".to_string();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut action = MockAction::new();
        action
            .expect_lifecycle_type()
            .return_const(LifecycleType::Delete);
        action
            .expect_run()
            .times(1)
            .returning(move |name, settings, context| {
                let bucket_name = settings.bucket.clone();
                let tx = tx.clone();
                assert!(Arc::strong_count(&context.storage) > 0);
                tx.send((name.to_string(), bucket_name)).unwrap();
                Ok(LifecycleRunResult {
                    affected_records: 1,
                })
            });
        let action: Arc<dyn LifecycleAction + Send + Sync> = Arc::new(action);
        let action_builder: LifecycleActionBuilder = Arc::new(move |lifecycle_type| {
            assert_eq!(lifecycle_type, LifecycleType::Delete);
            Arc::clone(&action)
        });

        let storage = storage.await;
        let mut repo = LifecycleRepository::load_or_create(storage, lifecycle_cfg(), None)
            .await
            .with_action_builder(action_builder);
        repo.create_lifecycle("test", settings).await.unwrap();

        repo.start().await.unwrap();
        let call = timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        repo.stop().await;

        assert_eq!(call, ("test".to_string(), "bucket-1".to_string()));
        assert!(!repo.is_lifecycle_running("test").await.unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn starts_new_lifecycle_when_repo_is_already_started(
        #[future] storage: Arc<StorageEngine>,
        mut settings: LifecycleSettings,
    ) {
        settings.interval = "100ms".to_string();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut action = MockAction::new();
        action
            .expect_lifecycle_type()
            .return_const(LifecycleType::Delete);
        action.expect_run().times(1).returning(move |name, _, _| {
            tx.send(name.to_string()).unwrap();
            Ok(LifecycleRunResult::default())
        });
        let action: Arc<dyn LifecycleAction + Send + Sync> = Arc::new(action);
        let action_builder: LifecycleActionBuilder = Arc::new(move |_| Arc::clone(&action));

        let storage = storage.await;
        let mut repo = LifecycleRepository::load_or_create(storage, lifecycle_cfg(), None)
            .await
            .with_action_builder(action_builder);
        repo.start().await.unwrap();
        repo.create_lifecycle("test", settings).await.unwrap();

        assert_eq!(
            timeout(Duration::from_secs(1), rx.recv()).await.unwrap(),
            Some("test".to_string())
        );
        repo.stop().await;
    }

    #[rstest]
    #[tokio::test]
    async fn disabled_worker_does_not_run_action(
        #[future] storage: Arc<StorageEngine>,
        settings: LifecycleSettings,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut action = MockAction::new();
        action
            .expect_lifecycle_type()
            .return_const(LifecycleType::Delete);
        action.expect_run().returning(move |name, _, _| {
            tx.send(name.to_string()).unwrap();
            Ok(LifecycleRunResult::default())
        });
        let action: Arc<dyn LifecycleAction + Send + Sync> = Arc::new(action);
        let action_builder: LifecycleActionBuilder = Arc::new(move |_| Arc::clone(&action));

        let storage = storage.await;
        let mut repo = LifecycleRepository::load_or_create(storage, lifecycle_cfg(), None)
            .await
            .with_action_builder(action_builder);

        let settings = LifecycleSettings {
            mode: LifecycleMode::Disabled,
            ..settings
        };
        repo.create_lifecycle("test", settings).await.unwrap();

        repo.start().await.unwrap();
        assert!(timeout(Duration::from_millis(300), rx.recv())
            .await
            .is_err());
        repo.stop().await;
    }

    #[rstest]
    #[tokio::test]
    async fn persists_mode_across_reload(
        #[future] storage: Arc<StorageEngine>,
        settings: LifecycleSettings,
    ) {
        let storage = storage.await;
        let mut repo =
            LifecycleRepository::load_or_create(Arc::clone(&storage), Cfg::default(), None).await;
        repo.create_lifecycle("test", settings).await.unwrap();
        repo.set_mode("test", LifecycleMode::Disabled)
            .await
            .unwrap();

        let repo = LifecycleRepository::load_or_create(storage, Cfg::default(), None).await;
        let info = repo.get_info("test").await.unwrap();
        assert_eq!(info.info.mode, LifecycleMode::Disabled);
        assert_eq!(info.settings.mode, LifecycleMode::Disabled);
    }

    #[rstest]
    #[tokio::test]
    async fn sets_mode_on_provisioned_lifecycle(
        #[future] mut repo: LifecycleRepository,
        settings: LifecycleSettings,
    ) {
        let mut repo = repo.await;
        repo.create_lifecycle("test", settings).await.unwrap();
        repo.set_lifecycle_provisioned("test", true).await.unwrap();

        repo.set_mode("test", LifecycleMode::Disabled)
            .await
            .unwrap();

        let info = repo.get_info("test").await.unwrap();
        assert_eq!(info.info.mode, LifecycleMode::Disabled);
    }

    #[fixture]
    fn settings() -> LifecycleSettings {
        settings_fixture()
    }

    fn settings_fixture() -> LifecycleSettings {
        LifecycleSettings {
            lifecycle_type: LifecycleType::Delete,
            bucket: "bucket-1".to_string(),
            entries: vec!["entry-1".to_string()],
            max_age: "1h".to_string(),
            interval: "1h".to_string(),
            when: None,
            mode: LifecycleMode::Enabled,
        }
    }

    #[cfg(debug_assertions)]
    fn too_short_interval_error() -> ReductError {
        unprocessable_entity!(
            "Lifecycle interval '5s' is shorter than minimum allowed value of 10s"
        )
    }

    #[cfg(not(debug_assertions))]
    fn too_short_interval_error() -> ReductError {
        unprocessable_entity!(
            "Lifecycle interval '5s' is shorter than minimum allowed value of 10m"
        )
    }

    fn lifecycle_cfg() -> Cfg {
        Cfg::default()
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
        storage
            .create_bucket("bucket-1", BucketSettings::default())
            .await
            .unwrap();
        Arc::new(storage)
    }

    #[fixture]
    async fn repo(#[future] storage: Arc<StorageEngine>) -> LifecycleRepository {
        LifecycleRepository::load_or_create(storage.await, lifecycle_cfg(), None).await
    }
}
