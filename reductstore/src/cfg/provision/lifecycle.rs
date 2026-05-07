// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{CfgParser, ExtCfgBounds, ProvisionedLifecycle};
use crate::core::duration::parse_duration_to_micros;
use crate::core::env::{Env, GetEnv};
use crate::lifecycle::{LifecycleRepoBuilder, ManageLifecycles};
use crate::storage::engine::StorageEngine;
use log::{error, info};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::lifecycle_api::{LifecycleSettings, LifecycleType};
use std::collections::HashMap;
use std::sync::Arc;

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(in crate::cfg) async fn provision_lifecycle_repo(
        &self,
        storage: Arc<StorageEngine>,
    ) -> Result<Box<dyn ManageLifecycles + Send + Sync>, ReductError> {
        let mut repo = LifecycleRepoBuilder::new(self.cfg.clone())
            .build(Arc::clone(&storage))
            .await;

        for (name, lifecycle) in &self.cfg.lifecycles {
            if let Err(err) = repo
                .create_lifecycle(name, lifecycle.settings.clone())
                .await
            {
                if err.status() == ErrorCode::Conflict {
                    repo.update_lifecycle(name, lifecycle.settings.clone())
                        .await?;
                } else {
                    error!("Failed to provision lifecycle '{}': {}", name, err);
                    continue;
                }
            }

            repo.set_lifecycle_provisioned(name, true).await?;
            let info_data = repo.get_info(name).await?;
            info!(
                "Provisioned lifecycle '{}' with {:?}",
                name, info_data.settings
            );
        }

        Ok(repo)
    }

    pub(in crate::cfg) fn parse_lifecycles(
        env: &mut Env<EnvGetter>,
    ) -> HashMap<String, ProvisionedLifecycle> {
        let mut lifecycles = HashMap::<String, (String, ProvisionedLifecycle)>::new();
        for (id, name) in env.matches("RS_LIFECYCLE_(.*)_NAME") {
            lifecycles.insert(
                id,
                (
                    name,
                    ProvisionedLifecycle {
                        settings: LifecycleSettings::default(),
                    },
                ),
            );
        }

        let mut unfinished_lifecycles = vec![];
        for (id, (name, lifecycle)) in &mut lifecycles {
            if let Some(lifecycle_type) =
                env.get_optional::<String>(&format!("RS_LIFECYCLE_{}_TYPE", id))
            {
                match lifecycle_type.to_lowercase().as_str() {
                    "delete" => lifecycle.settings.lifecycle_type = LifecycleType::Delete,
                    _ => {
                        error!(
                            "Lifecycle '{}' has invalid type '{}'. Drop it.",
                            name, lifecycle_type
                        );
                        unfinished_lifecycles.push(id.clone());
                        continue;
                    }
                }
            }

            if let Some(bucket) = env.get_optional::<String>(&format!("RS_LIFECYCLE_{}_BUCKET", id))
            {
                lifecycle.settings.bucket = bucket;
            } else {
                error!("Lifecycle '{}' has no bucket. Drop it.", name);
                unfinished_lifecycles.push(id.clone());
                continue;
            }

            if let Some(max_age) =
                env.get_optional::<String>(&format!("RS_LIFECYCLE_{}_MAX_AGE", id))
            {
                if let Err(err) = parse_duration_to_micros(&max_age) {
                    error!(
                        "Lifecycle '{}' has invalid max age '{}': {}. Drop it.",
                        name, max_age, err
                    );
                    unfinished_lifecycles.push(id.clone());
                    continue;
                }
                lifecycle.settings.max_age = max_age;
            } else {
                error!("Lifecycle '{}' has no max age. Drop it.", name);
                unfinished_lifecycles.push(id.clone());
                continue;
            }

            if let Some(interval) =
                env.get_optional::<u64>(&format!("RS_LIFECYCLE_{}_INTERVAL", id))
            {
                lifecycle.settings.interval = interval;
            }

            if let Some(entries) =
                env.get_optional::<String>(&format!("RS_LIFECYCLE_{}_ENTRIES", id))
            {
                lifecycle.settings.entries = entries
                    .split(',')
                    .map(|entry| entry.trim())
                    .filter(|entry| !entry.is_empty())
                    .map(|entry| entry.to_string())
                    .collect();
            }

            if let Some(when) = env.get_optional::<String>(&format!("RS_LIFECYCLE_{}_WHEN", id)) {
                match serde_json::from_str::<serde_json::Value>(&when) {
                    Ok(when) => lifecycle.settings.when = Some(when),
                    Err(err) => {
                        error!(
                            "Lifecycle '{}' has invalid when condition: {}. Drop it.",
                            name, err
                        );
                        unfinished_lifecycles.push(id.clone());
                    }
                }
            }
        }

        lifecycles
            .into_iter()
            .filter(|(id, _)| !unfinished_lifecycles.contains(id))
            .map(|(_, (name, lifecycle))| (name, lifecycle))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::msg::lifecycle_api::LifecycleInfo;
    use rstest::{fixture, rstest};
    use std::collections::BTreeMap;
    use std::env::VarError;
    use std::path::PathBuf;

    #[derive(Clone)]
    struct TestEnvGetter {
        values: BTreeMap<String, String>,
    }

    impl TestEnvGetter {
        fn new(values: &[(&str, &str)]) -> Self {
            Self {
                values: values
                    .iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            }
        }
    }

    impl GetEnv for TestEnvGetter {
        fn get(&self, key: &str) -> Result<String, VarError> {
            self.values.get(key).cloned().ok_or(VarError::NotPresent)
        }

        fn all(&self) -> BTreeMap<String, String> {
            self.values.clone()
        }
    }

    #[fixture]
    fn path() -> PathBuf {
        tempfile::tempdir().unwrap().keep()
    }

    fn lifecycle_env(path: PathBuf, overrides: &[(&str, &str)]) -> TestEnvGetter {
        let mut values = BTreeMap::from([
            (
                "RS_DATA_PATH".to_string(),
                path.to_str().unwrap().to_string(),
            ),
            ("RS_BUCKET_1_NAME".to_string(), "telemetry".to_string()),
            (
                "RS_LIFECYCLE_A_NAME".to_string(),
                "purge-sensors-30d".to_string(),
            ),
            ("RS_LIFECYCLE_A_TYPE".to_string(), "delete".to_string()),
            ("RS_LIFECYCLE_A_BUCKET".to_string(), "telemetry".to_string()),
            (
                "RS_LIFECYCLE_A_ENTRIES".to_string(),
                "sensors/*, env/temp ,,env/humidity".to_string(),
            ),
            ("RS_LIFECYCLE_A_MAX_AGE".to_string(), "30d".to_string()),
            ("RS_LIFECYCLE_A_INTERVAL".to_string(), "600".to_string()),
            (
                "RS_LIFECYCLE_A_WHEN".to_string(),
                r#"{"$eq":["&label","true"]}"#.to_string(),
            ),
        ]);

        for (key, value) in overrides {
            values.insert(key.to_string(), value.to_string());
        }

        TestEnvGetter { values }
    }

    async fn lifecycle_infos(
        env_getter: TestEnvGetter,
    ) -> (Vec<LifecycleInfo>, Option<LifecycleSettings>, bool) {
        let components = CfgParser::from_env(env_getter, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let audit_enabled = components.cfg.audit_conf.enabled;
        let repo = components.lifecycle_repo.read().await.unwrap();
        let lifecycles = repo.lifecycles().await.unwrap();
        let settings = match lifecycles.first() {
            Some(info) => Some(repo.get_lifecycle_settings(&info.name).await.unwrap()),
            None => None,
        };
        (lifecycles, settings, audit_enabled)
    }

    #[rstest]
    #[tokio::test]
    async fn parses_lifecycle_settings_from_env(path: PathBuf) {
        let (_, settings, audit_enabled) = lifecycle_infos(lifecycle_env(path, &[])).await;
        let settings = settings.unwrap();

        assert!(audit_enabled);
        assert_eq!(settings.lifecycle_type, LifecycleType::Delete);
        assert_eq!(settings.bucket, "telemetry");
        assert_eq!(
            settings.entries,
            vec!["sensors/*", "env/temp", "env/humidity"]
        );
        assert_eq!(settings.max_age, "30d");
        assert_eq!(settings.interval, 600);
        assert_eq!(
            settings.when,
            Some(serde_json::json!({"$eq": ["&label", "true"]}))
        );
    }

    #[rstest]
    #[tokio::test]
    async fn defaults_lifecycle_type_to_delete(path: PathBuf) {
        let mut env_getter = lifecycle_env(path, &[]);
        env_getter.values.remove("RS_LIFECYCLE_A_TYPE");

        let (_, settings, _) = lifecycle_infos(env_getter).await;
        assert_eq!(settings.unwrap().lifecycle_type, LifecycleType::Delete);
    }

    #[rstest]
    #[case("RS_LIFECYCLE_A_TYPE", "archive")]
    #[case("RS_LIFECYCLE_A_MAX_AGE", "30days")]
    #[case("RS_LIFECYCLE_A_WHEN", r#"{"$eq":["&label","true"]"#)]
    #[tokio::test]
    async fn drops_lifecycle_with_invalid_cfg(
        path: PathBuf,
        #[case] key: &str,
        #[case] value: &str,
    ) {
        let (lifecycles, settings, _) = lifecycle_infos(lifecycle_env(path, &[(key, value)])).await;

        assert!(lifecycles.is_empty());
        assert!(settings.is_none());
    }

    #[rstest]
    fn parse_lifecycles_trims_entries_and_parses_when() {
        let getter = TestEnvGetter::new(&[
            ("RS_LIFECYCLE_A_NAME", "purge-sensors-30d"),
            ("RS_LIFECYCLE_A_TYPE", "delete"),
            ("RS_LIFECYCLE_A_BUCKET", "telemetry"),
            (
                "RS_LIFECYCLE_A_ENTRIES",
                "sensors/*, env/temp ,,env/humidity",
            ),
            ("RS_LIFECYCLE_A_MAX_AGE", "30d"),
            ("RS_LIFECYCLE_A_INTERVAL", "600"),
            ("RS_LIFECYCLE_A_WHEN", r#"{"$eq":["&label","true"]}"#),
        ]);
        let mut env = Env::new(getter);
        let lifecycles = CfgParser::<TestEnvGetter>::parse_lifecycles(&mut env);

        let lifecycle = lifecycles.get("purge-sensors-30d").unwrap();
        assert_eq!(lifecycle.settings.lifecycle_type, LifecycleType::Delete);
        assert_eq!(lifecycle.settings.bucket, "telemetry");
        assert_eq!(
            lifecycle.settings.entries,
            vec!["sensors/*", "env/temp", "env/humidity"]
        );
        assert_eq!(lifecycle.settings.max_age, "30d");
        assert_eq!(lifecycle.settings.interval, 600);
        assert_eq!(
            lifecycle.settings.when,
            Some(serde_json::json!({"$eq": ["&label", "true"]}))
        );
    }
}
