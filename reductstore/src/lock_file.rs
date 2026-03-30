// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{Cfg, InstanceRole};
use crate::core::file_cache::FILE_CACHE;
use crate::core::sync::AsyncRwLock;
use async_trait::async_trait;
use log::{error, info, warn};
use reduct_base::error::ReductError;
use std::io::Read;
use std::io::SeekFrom::Start;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, PartialEq)]
pub enum State {
    Waiting,
    Locked,
    Failed,
}

#[derive(Debug, PartialEq, Clone, Default)]
pub enum FailureAction {
    Proceed,
    #[default]
    Abort,
}

#[async_trait]
pub trait LockFile {
    async fn is_locked(&self) -> Result<bool, ReductError>;
    async fn is_failed(&self) -> Result<bool, ReductError>;
    async fn is_waiting(&self) -> Result<bool, ReductError>;
}
pub type BoxedLockFile = Box<dyn LockFile + Sync + Send>;
struct ImplLockFile {
    path: PathBuf,
    stop_on_drop: Arc<AtomicBool>,
    handle: tokio::task::JoinHandle<()>,
    state: Arc<AsyncRwLock<State>>,
}

pub(crate) struct LockFileBuilder {
    path_buf: PathBuf,
    config: Cfg,
}
impl LockFileBuilder {
    pub fn noop() -> BoxedLockFile {
        Box::new(NoopLockFile {})
    }

    pub fn new(path_buf: PathBuf) -> Self {
        Self {
            path_buf,
            config: Cfg::default(),
        }
    }

    pub fn with_config(mut self, config: Cfg) -> Self {
        self.config = config;
        self
    }

    pub fn build(self) -> BoxedLockFile {
        Self::from_config(self.path_buf, self.config)
    }

    fn from_config(path: PathBuf, cfg: Cfg) -> BoxedLockFile {
        let role = cfg.role;
        let cfg = cfg.lock_file_config;

        // Atomic flag to signal the background task to stop
        let stop_on_drop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop_on_drop);
        let file_path = path.clone();
        let state = Arc::new(AsyncRwLock::new(State::Waiting));
        let state_clone = Arc::clone(&state);

        let mut this = Box::new(ImplLockFile {
            path,
            stop_on_drop,
            handle: tokio::spawn(async {}),
            state,
        });

        let handle = tokio::spawn(async move {
            if let Err(err) =
                Self::run_lock_task(file_path, cfg, role, state_clone, stop_flag).await
            {
                error!("Lock file task failed: {}", err);
            }
        });

        this.handle = handle;
        this
    }

    async fn run_lock_task(
        file_path: PathBuf,
        cfg: crate::cfg::lock_file::LockFileConfig,
        role: InstanceRole,
        state: Arc<AsyncRwLock<State>>,
        stop_flag: Arc<AtomicBool>,
    ) -> Result<(), ReductError> {
        // Each process instance keeps its own owner token in the lock file.
        let unique_id = format!("{}-{}", std::process::id(), uuid::Uuid::new_v4());

        // Main loop to acquire and maintain the lock

        let mut locked = false;
        while !(locked || stop_flag.load(std::sync::atomic::Ordering::SeqCst)) {
            // Check if the file is already locked
            let time_start = std::time::Instant::now();
            while FILE_CACHE.try_exists(&file_path).await?
                && !stop_flag.load(std::sync::atomic::Ordering::SeqCst)
            {
                if let Some(last_modified) = FILE_CACHE
                    .get_stats(&file_path)
                    .await?
                    .and_then(|meta| meta.modified_time)
                {
                    // elapsed can fail if system time is changed backwards, so we default to 0 duration
                    if last_modified.elapsed().unwrap_or(Duration::from_secs(0)) > cfg.ttl
                        && cfg.ttl.as_secs() > 0
                    {
                        warn!(
                            "Lock file is stale (last modified over {:?} ago), removing: {:?}",
                            last_modified.elapsed().unwrap(),
                            file_path
                        );
                        if let Err(err) = FILE_CACHE.remove(&file_path).await {
                            error!("Failed to remove stale lock file: {:?}", err);
                        }
                        break;
                    }
                }

                if time_start.elapsed() > cfg.timeout && cfg.timeout.as_secs() > 0 {
                    match cfg.failure_action {
                        FailureAction::Proceed => {
                            warn!(
                                "Timeout while waiting for lock file, proceeding anyway: {:?}",
                                file_path
                            );
                            break;
                        }
                        FailureAction::Abort => {
                            error!(
                                "Timeout while waiting for lock file, aborting: {:?}",
                                file_path
                            );
                            *state.write().await? = State::Failed;
                            return Ok(());
                        }
                    }
                }

                tokio::time::sleep(cfg.polling_interval).await;
            }

            match role {
                InstanceRole::Primary => {
                    locked = true;
                }
                InstanceRole::Secondary => {
                    // Secondary instance waits a bit to ensure the primary has created the lock file
                    tokio::time::sleep(cfg.polling_interval * 3).await;
                    if !FILE_CACHE.try_exists(&file_path).await? {
                        locked = true;
                    } else {
                        info!("Secondary instance could not acquire lock file (already held by primary): {:?}", file_path);
                    }
                }
                InstanceRole::Replica | InstanceRole::Standalone => {}
            }
        }

        // we need to sync the file to ensure that the lock is visible to other processes
        // With Blue/Green deployments, the file system is shared between instances
        // so we need to keep the file locked as long as the process is running and recreate it if it gets deleted
        // during deployments
        while !stop_flag.load(std::sync::atomic::Ordering::SeqCst) {
            if let Err(e) = Self::write_lock_owner_id(&file_path, &unique_id).await {
                error!("Error while recreating lock file: {}", e);
            }

            if locked {
                *state.write().await? = State::Locked;
                locked = false; // don't touch the lock
            }

            tokio::time::sleep(cfg.polling_interval).await;
            Self::check_lock_owner_id(&file_path, &unique_id, role.clone()).await?;
        }

        Ok(())
    }

    async fn read_lock_owner_id(file_path: &PathBuf) -> Result<Option<String>, ReductError> {
        // we need to download lockfile in case it is cached with stale content
        if let Err(err) = FILE_CACHE.invalidate_local_cache_file(file_path).await {
            warn!(
                "Failed to invalidate local cache for lock file {:?}: {}",
                file_path, err
            );
        }

        let mut file = match FILE_CACHE.read(file_path, Start(0)).await {
            Ok(file) => file,
            Err(err) => {
                warn!("Failed to read lock file {:?}: {}", file_path, err);
                return Ok(None);
            }
        };

        let mut content = Vec::new();
        file.read_to_end(&mut content)?;
        let content = String::from_utf8_lossy(&content);
        let content = content.trim();
        if content.is_empty() {
            Ok(None)
        } else {
            Ok(Some(content.to_string()))
        }
    }

    async fn write_lock_owner_id(file_path: &PathBuf, owner_id: &str) -> Result<(), ReductError> {
        let mut file = FILE_CACHE.write_or_create(file_path, Start(0)).await?;
        file.set_len(0)?;
        file.write_all(owner_id.as_bytes())?;
        file.sync_all().await?;
        Ok(())
    }

    async fn check_lock_owner_id(
        file_path: &PathBuf,
        unique_id: &str,
        role: InstanceRole,
    ) -> Result<(), ReductError> {
        if let Some(owner_id) = Self::read_lock_owner_id(file_path).await? {
            if owner_id != unique_id {
                match role {
                    InstanceRole::Primary => {
                        error!(
                            "Lock file {:?} is owned by '{}', but primary is overwriting it with '{}'",
                            file_path, owner_id, unique_id
                        );
                    }
                    InstanceRole::Secondary => {
                        error!(
                            "Lock file {:?} is owned by '{}', secondary cannot acquire it",
                            file_path, owner_id
                        );
                        panic!(
                            "Lock file {:?} is owned by '{}', secondary cannot acquire it",
                            file_path, owner_id
                        );
                    }
                    InstanceRole::Replica | InstanceRole::Standalone => {}
                }
            }
        }

        Ok(())
    }
}

impl ImplLockFile {
    async fn remove_lock_file(path: &PathBuf) {
        if let Err(err) = FILE_CACHE.remove(path).await {
            error!("Failed to remove lock file: {:?}", err);
        }
    }
}

#[async_trait]
impl LockFile for ImplLockFile {
    async fn is_locked(&self) -> Result<bool, ReductError> {
        Ok(*self.state.read().await? == State::Locked)
    }

    async fn is_failed(&self) -> Result<bool, ReductError> {
        Ok(*self.state.read().await? == State::Failed)
    }

    async fn is_waiting(&self) -> Result<bool, ReductError> {
        Ok(*self.state.read().await? == State::Waiting)
    }
}

impl Drop for ImplLockFile {
    fn drop(&mut self) {
        self.stop_on_drop
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let path = self.path.clone();
        // Use block_in_place to handle async cleanup in drop
        let handle =
            tokio::runtime::Handle::try_current().expect("Failed to get current Tokio handle");
        let _ = std::thread::spawn(move || {
            handle.block_on(async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                ImplLockFile::remove_lock_file(&path).await;
            });
        })
        .join();
    }
}

struct NoopLockFile;

#[async_trait]
impl LockFile for NoopLockFile {
    async fn is_locked(&self) -> Result<bool, ReductError> {
        Ok(true)
    }

    async fn is_failed(&self) -> Result<bool, ReductError> {
        Ok(false)
    }

    async fn is_waiting(&self) -> Result<bool, ReductError> {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::lock_file::LockFileConfig;
    use crate::cfg::{Cfg, InstanceRole};
    use rstest::{fixture, rstest};
    use std::fs;
    use tempfile::tempdir;
    use test_log::test as test_log;
    use tokio::time::error::Elapsed;
    use tokio::time::timeout;

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_lock_file_acquire_and_drop(lock_file_path: PathBuf) {
        {
            let lock_file = LockFileBuilder::new(lock_file_path.clone()).build();

            // The lock may be acquired quickly; only wait if we observe it waiting.
            if lock_file.is_waiting().await.unwrap() {
                let acquired = wait_new_state(&lock_file).await;
                assert!(acquired.is_ok(), "Lock file was not acquired in time");
            }
            assert!(lock_file.is_locked().await.unwrap());
            assert!(!lock_file.is_failed().await.unwrap());
            assert!(!lock_file.is_waiting().await.unwrap());

            assert_ne!(
                fs::read_to_string(&lock_file_path).unwrap(),
                "dummy",
                "Lock file must be overwritten"
            );
        }

        wait_for_lock_file_cleanup(&lock_file_path).await;
        assert!(
            !lock_file_path.exists(),
            "Lock file must be deleted on drop"
        );
    }

    #[test_log(rstest)]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_lock_file_timeout_abort(lock_file_path: PathBuf) {
        fs::write(&lock_file_path, "dummy").unwrap();
        let lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(500),
                    timeout: Duration::from_secs(2),
                    ..Default::default()
                },
                InstanceRole::Primary,
            ))
            .build();

        // Initially, the lock file should be in waiting state
        assert!(lock_file.is_waiting().await.unwrap());

        // Wait for the lock to fail due to timeout
        let failed = wait_new_state(&lock_file).await;
        assert!(failed.is_ok(), "Lock file did not fail in time");
        assert!(lock_file.is_failed().await.unwrap());
        assert!(!lock_file.is_locked().await.unwrap());
        assert!(!lock_file.is_waiting().await.unwrap());

        assert_eq!(
            fs::read_to_string(&lock_file_path).unwrap(),
            "dummy",
            "Lock file must not be overwritten"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_lock_file_timeout_proceed(lock_file_path: PathBuf) {
        fs::write(&lock_file_path, "dummy").unwrap();
        let lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(500),
                    timeout: Duration::from_secs(2),
                    failure_action: FailureAction::Proceed,
                    ..Default::default()
                },
                InstanceRole::Primary,
            ))
            .build();

        // Initially, the lock file should be in waiting state
        assert!(lock_file.is_waiting().await.unwrap());

        // Wait for the lock to be acquired despite the timeout
        let acquired = wait_new_state(&lock_file).await;
        assert!(acquired.is_ok(), "Lock file was not acquired in time");
        assert!(lock_file.is_locked().await.unwrap());
        assert!(!lock_file.is_failed().await.unwrap());
        assert!(!lock_file.is_waiting().await.unwrap());

        assert_ne!(
            fs::read_to_string(&lock_file_path).unwrap(),
            "dummy",
            "Lock file must be overwritten"
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_secondary_instance_waits(lock_file_path: PathBuf) {
        let secondary_lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(500),
                    ..Default::default()
                },
                InstanceRole::Secondary,
            ))
            .build();

        {
            let primary_lock_file = LockFileBuilder::new(lock_file_path.clone())
                .with_config(test_cfg(
                    LockFileConfig {
                        polling_interval: Duration::from_millis(500),
                        ..Default::default()
                    },
                    InstanceRole::Primary,
                ))
                .build();

            // Wait for the primary to acquire the lock
            let primary_acquired = wait_new_state(&primary_lock_file).await;
            assert!(
                primary_acquired.is_ok(),
                "Primary lock file was acquired in time"
            );
            assert!(primary_lock_file.is_locked().await.unwrap());

            // Secondary should wait while primary lock exists.
            let secondary_acquired = wait_new_state(&secondary_lock_file).await;
            assert!(
                secondary_acquired.is_err(),
                "Secondary lock file was not acquired in time"
            );
            assert!(secondary_lock_file.is_waiting().await.unwrap());
        }

        wait_for_lock_file_cleanup(&lock_file_path).await;

        let secondary_acquired = wait_new_state(&secondary_lock_file).await;
        assert!(
            secondary_acquired.is_ok(),
            "Secondary lock file was acquired in time"
        );
        assert!(secondary_lock_file.is_locked().await.unwrap());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_ttl_removes_stale_lock(lock_file_path: PathBuf) {
        fs::write(&lock_file_path, "dummy").unwrap();
        let lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(500),
                    ttl: Duration::from_secs(1),
                    ..Default::default()
                },
                InstanceRole::Primary,
            ))
            .build();

        // Initially, the lock file should be in waiting state
        assert!(lock_file.is_waiting().await.unwrap());

        // Wait for the lock to be acquired after TTL expires
        let acquired = wait_new_state(&lock_file).await;
        assert!(acquired.is_ok(), "Lock file was not acquired in time");
        assert!(lock_file.is_locked().await.unwrap());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_lock_owner_id_returns_none_for_empty_file(lock_file_path: PathBuf) {
        fs::write(&lock_file_path, "").unwrap();

        assert_eq!(
            LockFileBuilder::read_lock_owner_id(&lock_file_path)
                .await
                .unwrap(),
            None
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_lock_owner_id_returns_none_when_read_fails(lock_file_path: PathBuf) {
        let _ = fs::remove_file(&lock_file_path);

        assert_eq!(
            LockFileBuilder::read_lock_owner_id(&lock_file_path)
                .await
                .unwrap(),
            None
        );
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_check_lock_owner_id_allows_same_owner(lock_file_path: PathBuf) {
        let owner_id = format!("owner-{}", uuid::Uuid::new_v4());
        fs::write(&lock_file_path, &owner_id).unwrap();

        LockFileBuilder::check_lock_owner_id(&lock_file_path, &owner_id, InstanceRole::Secondary)
            .await
            .unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    #[should_panic(expected = "secondary cannot acquire it")]
    async fn test_check_lock_owner_id_panics_for_secondary_with_foreign_uid(
        lock_file_path: PathBuf,
    ) {
        fs::write(&lock_file_path, format!("foreign-{}", uuid::Uuid::new_v4())).unwrap();
        let _ = LockFileBuilder::check_lock_owner_id(
            &lock_file_path,
            &format!("secondary-{}", uuid::Uuid::new_v4()),
            InstanceRole::Secondary,
        )
        .await
        .unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_check_lock_owner_id_allows_primary_to_overwrite_foreign_uid(
        lock_file_path: PathBuf,
    ) {
        let owner_id = format!("primary-{}", uuid::Uuid::new_v4());
        fs::write(&lock_file_path, format!("foreign-{}", uuid::Uuid::new_v4())).unwrap();

        LockFileBuilder::check_lock_owner_id(&lock_file_path, &owner_id, InstanceRole::Primary)
            .await
            .unwrap();
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_secondary_acquires_if_file_missing(lock_file_path: PathBuf) {
        let lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(100),
                    ..Default::default()
                },
                InstanceRole::Secondary,
            ))
            .build();

        // Secondary should grab the lock if file does not exist after grace period.
        let acquired = wait_new_state(&lock_file).await;
        assert!(acquired.is_ok(), "Secondary failed to acquire lock");
        assert!(lock_file.is_locked().await.unwrap());
    }

    #[rstest]
    #[tokio::test]
    async fn test_run_lock_task_secondary_timeout_proceed_does_not_acquire_when_file_exists(
        lock_file_path: PathBuf,
    ) {
        fs::write(&lock_file_path, "dummy").unwrap();

        let stop_flag = Arc::new(AtomicBool::new(false));
        let state = Arc::new(AsyncRwLock::new(State::Waiting));

        let cfg = LockFileConfig {
            polling_interval: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
            failure_action: FailureAction::Proceed,
            ..Default::default()
        };

        let task = tokio::spawn(LockFileBuilder::run_lock_task(
            lock_file_path.clone(),
            cfg,
            InstanceRole::Secondary,
            Arc::clone(&state),
            Arc::clone(&stop_flag),
        ));

        tokio::time::sleep(Duration::from_millis(1300)).await;
        assert_eq!(*state.read().await.unwrap(), State::Waiting);

        stop_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        timeout(Duration::from_secs(2), task)
            .await
            .expect("run_lock_task must stop")
            .unwrap()
            .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_run_lock_task_replica_runs_once_and_stops(lock_file_path: PathBuf) {
        let _ = fs::remove_file(&lock_file_path);
        let _ = fs::remove_dir_all(&lock_file_path);

        let stop_flag = Arc::new(AtomicBool::new(false));
        let state = Arc::new(AsyncRwLock::new(State::Waiting));
        let cfg = LockFileConfig {
            polling_interval: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
            ..Default::default()
        };

        let task = tokio::spawn(LockFileBuilder::run_lock_task(
            lock_file_path,
            cfg,
            InstanceRole::Replica,
            state,
            Arc::clone(&stop_flag),
        ));

        tokio::task::yield_now().await;
        stop_flag.store(true, std::sync::atomic::Ordering::SeqCst);

        timeout(Duration::from_secs(2), task)
            .await
            .expect("run_lock_task must stop")
            .unwrap()
            .unwrap();
    }

    #[rstest]
    #[tokio::test]
    async fn test_run_lock_task_recreate_error_when_path_is_directory(lock_file_path: PathBuf) {
        let _ = fs::remove_file(&lock_file_path);
        fs::create_dir(&lock_file_path).unwrap();

        let stop_flag = Arc::new(AtomicBool::new(false));
        let state = Arc::new(AsyncRwLock::new(State::Waiting));

        let cfg = LockFileConfig {
            polling_interval: Duration::from_millis(10),
            timeout: Duration::from_secs(1),
            failure_action: FailureAction::Proceed,
            ..Default::default()
        };

        let task = tokio::spawn(LockFileBuilder::run_lock_task(
            lock_file_path,
            cfg,
            InstanceRole::Primary,
            Arc::clone(&state),
            Arc::clone(&stop_flag),
        ));

        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(*state.read().await.unwrap(), State::Waiting);

        stop_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        timeout(Duration::from_secs(2), task)
            .await
            .expect("run_lock_task must stop")
            .unwrap()
            .unwrap();
    }

    async fn wait_new_state(lock_file: &BoxedLockFile) -> Result<(), Elapsed> {
        let acquired = timeout(Duration::from_secs(10), async {
            loop {
                if !lock_file.is_waiting().await.unwrap() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await;
        acquired
    }

    async fn wait_for_lock_file_cleanup(lock_file_path: &PathBuf) {
        timeout(Duration::from_secs(3), async {
            while lock_file_path.exists() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("Lock file must be cleaned up in time");
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_lock_file_when_path_exists(lock_file_path: PathBuf) {
        fs::write(&lock_file_path, "lock").unwrap();

        ImplLockFile::remove_lock_file(&lock_file_path).await;

        assert!(!lock_file_path.exists());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_lock_file_when_already_removed(lock_file_path: PathBuf) {
        let _ = fs::remove_file(&lock_file_path);

        ImplLockFile::remove_lock_file(&lock_file_path).await;

        assert!(!lock_file_path.exists());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_lock_file_when_path_is_directory(lock_file_path: PathBuf) {
        let _ = fs::remove_file(&lock_file_path);
        fs::create_dir(&lock_file_path).unwrap();

        ImplLockFile::remove_lock_file(&lock_file_path).await;

        assert!(lock_file_path.exists());
    }

    #[rstest]
    #[tokio::test(flavor = "multi_thread")]
    async fn test_drops_lock_file(lock_file_path: PathBuf) {
        let lock_file = LockFileBuilder::new(lock_file_path.clone())
            .with_config(test_cfg(
                LockFileConfig {
                    polling_interval: Duration::from_millis(500),
                    ..Default::default()
                },
                InstanceRole::Primary,
            ))
            .build();

        // Wait for the lock to be acquired
        let acquired = wait_new_state(&lock_file).await;

        assert!(acquired.is_ok(), "Lock file was not acquired in time");
        assert!(lock_file.is_locked().await.unwrap());

        // Drop the lock file and wait for cleanup.
        drop(lock_file);
        wait_for_lock_file_cleanup(&lock_file_path).await;

        assert!(
            !lock_file_path.exists(),
            "Lock file must be deleted on drop"
        );
    }

    #[fixture]
    fn lock_file_path() -> PathBuf {
        let dir = tempdir().unwrap().keep();

        let filepath = dir.join("test.lock");
        filepath
    }

    fn test_cfg(lock_file_config: LockFileConfig, role: InstanceRole) -> Cfg {
        Cfg {
            lock_file_config,
            role,
            ..Default::default()
        }
    }
}
