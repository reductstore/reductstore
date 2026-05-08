// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::lifecycle::ManageLifecycles;
use async_trait::async_trait;
use reduct_base::error::ReductError;
use reduct_base::forbidden;
use reduct_base::msg::lifecycle_api::{
    FullLifecycleInfo, LifecycleInfo, LifecycleMode, LifecycleSettings,
};

pub(super) struct ReadOnlyLifecycleRepository;

impl ReadOnlyLifecycleRepository {
    pub(super) fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ManageLifecycles for ReadOnlyLifecycleRepository {
    async fn create_lifecycle(
        &mut self,
        _name: &str,
        _settings: LifecycleSettings,
    ) -> Result<(), ReductError> {
        Err(forbidden!("Cannot create lifecycle in read-only mode"))
    }

    async fn update_lifecycle(
        &mut self,
        _name: &str,
        _settings: LifecycleSettings,
    ) -> Result<(), ReductError> {
        Err(forbidden!("Cannot update lifecycle in read-only mode"))
    }

    async fn lifecycles(&self) -> Result<Vec<LifecycleInfo>, ReductError> {
        Ok(vec![])
    }

    async fn get_info(&self, _name: &str) -> Result<FullLifecycleInfo, ReductError> {
        Err(forbidden!("Cannot get lifecycle info in read-only mode"))
    }

    async fn get_lifecycle_settings(&self, _name: &str) -> Result<LifecycleSettings, ReductError> {
        Err(forbidden!(
            "Cannot get lifecycle settings in read-only mode"
        ))
    }

    async fn is_lifecycle_running(&self, _name: &str) -> Result<bool, ReductError> {
        Err(forbidden!("Cannot get lifecycle in read-only mode"))
    }

    async fn set_mode(&mut self, _name: &str, _mode: LifecycleMode) -> Result<(), ReductError> {
        Err(forbidden!("Cannot set lifecycle mode in read-only mode"))
    }

    async fn set_lifecycle_provisioned(
        &mut self,
        _name: &str,
        _provisioned: bool,
    ) -> Result<(), ReductError> {
        Err(forbidden!(
            "Cannot set lifecycle provisioned state in read-only mode"
        ))
    }

    async fn remove_lifecycle(&mut self, _name: &str) -> Result<(), ReductError> {
        Err(forbidden!("Cannot remove lifecycle in read-only mode"))
    }

    fn start(&mut self) {
        // No-op
    }

    async fn stop(&mut self) {
        // No-op
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::forbidden;
    use rstest::{fixture, rstest};

    #[fixture]
    fn repo() -> ReadOnlyLifecycleRepository {
        ReadOnlyLifecycleRepository::new()
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_create(mut repo: ReadOnlyLifecycleRepository) {
        let err = repo
            .create_lifecycle("test", LifecycleSettings::default())
            .await
            .err()
            .unwrap();
        assert_eq!(err, forbidden!("Cannot create lifecycle in read-only mode"));
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_update(mut repo: ReadOnlyLifecycleRepository) {
        let err = repo
            .update_lifecycle("test", LifecycleSettings::default())
            .await
            .err()
            .unwrap();
        assert_eq!(err, forbidden!("Cannot update lifecycle in read-only mode"));
    }

    #[rstest]
    #[tokio::test]
    async fn returns_empty_list(repo: ReadOnlyLifecycleRepository) {
        assert!(repo.lifecycles().await.unwrap().is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_getters(repo: ReadOnlyLifecycleRepository) {
        assert_eq!(
            repo.get_info("test").await.err().unwrap(),
            forbidden!("Cannot get lifecycle info in read-only mode")
        );
        assert_eq!(
            repo.get_lifecycle_settings("test").await.err().unwrap(),
            forbidden!("Cannot get lifecycle settings in read-only mode")
        );
        assert_eq!(
            repo.is_lifecycle_running("test").await.err().unwrap(),
            forbidden!("Cannot get lifecycle in read-only mode")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn rejects_mutations(mut repo: ReadOnlyLifecycleRepository) {
        assert_eq!(
            repo.set_lifecycle_provisioned("test", true)
                .await
                .err()
                .unwrap(),
            forbidden!("Cannot set lifecycle provisioned state in read-only mode")
        );
        assert_eq!(
            repo.remove_lifecycle("test").await.err().unwrap(),
            forbidden!("Cannot remove lifecycle in read-only mode")
        );
        assert_eq!(
            repo.set_mode("test", LifecycleMode::Disabled)
                .await
                .err()
                .unwrap(),
            forbidden!("Cannot set lifecycle mode in read-only mode")
        );
    }

    #[rstest]
    #[tokio::test]
    async fn start_and_stop_are_noops(mut repo: ReadOnlyLifecycleRepository) {
        repo.start();
        repo.stop().await;
    }
}
