// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use crate::cfg::{CfgParser, ExtCfgBounds, ProvisionedReplication};
use crate::core::env::{Env, GetEnv};
use crate::replication::{ManageReplications, ReplicationRepoBuilder};
use crate::storage::engine::StorageEngine;
use crate::syslog::SystemEventSink;
use log::{error, info, warn};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::replication_api::{
    ReplicationCompression, ReplicationMode, ReplicationSettings,
};
use reduct_base::Labels;
use std::collections::HashMap;
use std::sync::Arc;

impl<EnvGetter: GetEnv, ExtCfg: ExtCfgBounds> CfgParser<EnvGetter, ExtCfg> {
    pub(in crate::cfg) async fn provision_replication_repo(
        &self,
        storage: Arc<StorageEngine>,
        system_event_sink: SystemEventSink,
    ) -> Result<Box<dyn ManageReplications + Send + Sync>, ReductError> {
        let mut repo = ReplicationRepoBuilder::new(self.cfg.clone())
            .with_system_event_sink(system_event_sink)
            .build(Arc::clone(&storage))
            .await;
        for (name, replication) in &self.cfg.replications {
            if let Err(e) = repo
                .create_replication(name, replication.settings.clone())
                .await
            {
                if e.status() == ErrorCode::Conflict {
                    let mut settings = replication.settings.clone();
                    if replication.mode_override.is_none() {
                        if let Ok(info) = repo.get_info(name).await {
                            settings.mode = info.info.mode;
                        }
                    }
                    repo.update_replication(name, settings).await?;
                } else {
                    error!("Failed to provision replication '{}': {}", name, e);
                    continue;
                }
            }

            repo.set_replication_provisioned(name, true).await?;

            let info_data = repo.get_info(name).await?;
            info!(
                "Provisioned replication '{}' with {:?}",
                name, info_data.settings
            );
        }
        Ok(repo)
    }

    pub(in crate::cfg) fn parse_replications(
        env: &mut Env<EnvGetter>,
    ) -> HashMap<String, ProvisionedReplication> {
        let mut replications = HashMap::<String, (String, ProvisionedReplication)>::new();
        for (id, name) in env.matches("RS_REPLICATION_(.*)_NAME") {
            let replication = ProvisionedReplication {
                settings: ReplicationSettings {
                    src_bucket: "".to_string(),
                    dst_bucket: "".to_string(),
                    dst_host: "http://localhost".to_string(),
                    dst_token: None,
                    entries: vec![],
                    dst_prefix: String::new(),
                    each_n: None,
                    when: None,
                    mode: ReplicationMode::Enabled,
                    compression: ReplicationCompression::None,
                },
                mode_override: None,
            };
            replications.insert(id, (name, replication));
        }

        let mut unfinished_replications = vec![];
        for (id, (name, replication)) in &mut replications {
            if let Some(src_bucket) =
                env.get_optional::<String>(&format!("RS_REPLICATION_{}_SRC_BUCKET", id))
            {
                replication.settings.src_bucket = src_bucket;
            } else {
                error!("Replication '{}' has no source bucket. Drop it.", name);
                unfinished_replications.push(id.clone());
                continue;
            }

            if let Some(remote_bucket) =
                env.get_optional::<String>(&format!("RS_REPLICATION_{}_DST_BUCKET", id))
            {
                replication.settings.dst_bucket = remote_bucket;
            } else {
                error!("Replication '{}' has no destination bucket. Drop it.", name);
                unfinished_replications.push(id.clone());
                continue;
            }

            if let Some(remote_host) =
                env.get_optional::<String>(&format!("RS_REPLICATION_{}_DST_HOST", id))
            {
                match url::Url::parse(&remote_host) {
                    Ok(url) => replication.settings.dst_host = url.to_string(),
                    Err(err) => {
                        error!(
                            "Replication '{}' has invalid remote host: {}. Drop it.",
                            name, err
                        );
                        unfinished_replications.push(id.clone());
                        continue;
                    }
                }
            } else {
                error!("Replication '{}' has no remote host. Drop it.", name);
                unfinished_replications.push(id.clone());
                continue;
            }

            let token = env
                .get_masked::<String>(&format!("RS_REPLICATION_{}_DST_TOKEN", id), "".to_string());
            replication.settings.dst_token = if token.is_empty() { None } else { Some(token) };

            if let Some(entries) =
                env.get_optional::<String>(&format!("RS_REPLICATION_{}_ENTRIES", id))
            {
                replication.settings.entries = entries.split(",").map(|s| s.to_string()).collect();
            }

            if let Some(dst_prefix) =
                env.get_optional::<String>(&format!("RS_REPLICATION_{}_DST_PREFIX", id))
            {
                replication.settings.dst_prefix = dst_prefix;
            }

            if let Some(each_n) = env.get_optional::<u64>(&format!("RS_REPLICATION_{}_EACH_N", id))
            {
                replication.settings.each_n = Some(each_n);
            }

            // Parse the when condition first
            if let Some(when) =
                env.get_optional::<serde_json::Value>(&format!("RS_REPLICATION_{}_WHEN", id))
            {
                replication.settings.when = Some(when);
            }

            // Migrate deprecated each_s to $each_t by injecting it into the when condition
            if let Some(each_s) = env.get_optional::<f64>(&format!("RS_REPLICATION_{}_EACH_S", id))
            {
                warn!(
                    "The 'RS_REPLICATION_{}_EACH_S' environment variable is deprecated and will be migrated to 'when' condition using $each_t operator. Value: {}",
                    id, each_s
                );

                if let Some(when) = &mut replication.settings.when {
                    // Inject $each_t into the existing when condition
                    if let Some(obj) = when.as_object_mut() {
                        obj.insert("$each_t".to_string(), serde_json::json!(each_s));
                    }
                } else {
                    // No when condition exists, create one with just $each_t
                    replication.settings.when = Some(serde_json::json!({"$each_t": each_s}));
                }
            }

            // Migrate deprecated include to $in by injecting it into the when condition
            for (key, value) in
                env.matches::<String>(&format!("RS_REPLICATION_{}_INCLUDE_(.*)", id))
            {
                warn!(
                    "The include parameter is deprecated. Use 'RS_REPLICATION_{}_WHEN' instead.",
                    id
                );

                if let Some(when) = &mut replication.settings.when {
                    if let Some(obj) = when.as_object_mut() {
                        obj.insert("$in".to_string(), serde_json::json!([&key, &value]));
                    }
                } else {
                    // No when condition exists, create one with just $in
                    replication.settings.when =
                        Some(serde_json::json!({"$in": serde_json::json!([&key, &value])}));
                }
            }

            // Migrate deprecated exclude to $nin by injecting it into the when condition
            for (key, value) in
                env.matches::<String>(&format!("RS_REPLICATION_{}_EXCLUDE_(.*)", id))
            {
                warn!(
                    "The exclude parameter is deprecated. Use 'RS_REPLICATION_{}_WHEN' instead.",
                    id
                );

                if let Some(when) = &mut replication.settings.when {
                    if let Some(obj) = when.as_object_mut() {
                        obj.insert("$nin".to_string(), serde_json::json!([&key, &value]));
                    }
                } else {
                    // No when condition exists, create one with just $nin
                    replication.settings.when =
                        Some(serde_json::json!({"$nin": serde_json::json!([&key, &value])}));
                }
            }

            if let Some(compression) =
                env.get_optional::<String>(&format!("RS_REPLICATION_{}_COMPRESSION", id))
            {
                match compression.to_lowercase().as_str() {
                    "none" => replication.settings.compression = ReplicationCompression::None,
                    "zstd" => replication.settings.compression = ReplicationCompression::Zstd,
                    "gzip" => replication.settings.compression = ReplicationCompression::Gzip,
                    _ => {
                        error!(
                            "Replication '{}' has invalid compression '{}'. Drop it.",
                            name, compression
                        );
                        unfinished_replications.push(id.clone());
                        continue;
                    }
                }
            }

            if let Some(mode) = env.get_optional::<String>(&format!("RS_REPLICATION_{}_MODE", id)) {
                replication.mode_override = Some(mode.clone());
                match mode.to_lowercase().as_str() {
                    "enabled" => replication.settings.mode = ReplicationMode::Enabled,
                    "paused" => replication.settings.mode = ReplicationMode::Paused,
                    "disabled" => replication.settings.mode = ReplicationMode::Disabled,
                    _ => {
                        error!(
                            "Replication '{}' has invalid mode '{}'. Drop it.",
                            name, mode
                        );
                        unfinished_replications.push(id.clone());
                    }
                }
            }
        }

        replications
            .into_iter()
            .filter(|(id, _)| !unfinished_replications.contains(id))
            .map(|(_, (name, replication))| (name, replication))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::replication::ReplicationConfig;
    use crate::cfg::tests::MockEnvGetter;
    use crate::cfg::Cfg;
    use crate::replication::{ManageReplications, ReplicationRepoBuilder};
    use crate::storage::engine::StorageEngine;
    use mockall::predicate::eq;
    use rstest::{fixture, rstest};
    use std::collections::BTreeMap;
    use std::env::VarError;
    use std::path::PathBuf;
    use std::sync::Arc;
    use test_log::test as log_test;

    // Local helper to create a replication repo for tests
    async fn create_replication_repo(
        storage: Arc<StorageEngine>,
        cfg: Cfg,
    ) -> Box<dyn ManageReplications + Send + Sync> {
        ReplicationRepoBuilder::new(cfg).build(storage).await
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_replications(mut env_with_replications: MockEnvGetter) {
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Ok("bucket1".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_BUCKET"))
            .return_const(Ok("bucket2".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_HOST"))
            .return_const(Ok("http://localhost".to_string()));

        env_with_replications
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let components = CfgParser::from_env(env_with_replications, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let repo = components.replication_repo.read().await.unwrap();
        let replication = repo.get_replication_settings("replication1").await.unwrap();
        let repl_info = repo.get_info("replication1").await.unwrap();

        assert_eq!(replication.src_bucket, "bucket1");
        assert_eq!(replication.dst_bucket, "bucket2");
        assert_eq!(replication.dst_host, "http://localhost/");
        assert_eq!(replication.dst_token, Some("TOKEN".to_string()));
        assert_eq!(replication.entries, vec!["entry1", "entry2"]);
        assert_eq!(replication.dst_prefix, "robot-1");
        assert_eq!(replication.each_n, Some(10));
        // The when condition should include the original $and plus migrated $in and $nin
        assert_eq!(
            replication.when,
            Some(serde_json::json!({
                "$and": [true, false],
                "$in": ["KEY", "value"],
                "$nin": ["KEY", "value"]
            }))
        );
        assert!(repl_info.info.is_provisioned);
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_replications_with_mode(mut env_with_replications: MockEnvGetter) {
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Ok("bucket1".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_BUCKET"))
            .return_const(Ok("bucket2".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_HOST"))
            .return_const(Ok("http://localhost".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_MODE"))
            .return_const(Ok("paused".to_string()));

        env_with_replications
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let components = CfgParser::from_env(env_with_replications, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let repo = components.replication_repo.read().await.unwrap();
        let repl_info = repo.get_info("replication1").await.unwrap();

        assert_eq!(repl_info.info.mode, ReplicationMode::Paused);
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_replications_drop_invalid_mode(mut env_with_replications: MockEnvGetter) {
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Ok("bucket1".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_BUCKET"))
            .return_const(Ok("bucket2".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_HOST"))
            .return_const(Ok("http://localhost".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_MODE"))
            .return_const(Ok("bogus".to_string()));

        env_with_replications
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let components = CfgParser::from_env(env_with_replications, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let repo = components.replication_repo.read().await.unwrap();

        assert_eq!(repo.replications().await.unwrap().len(), 0);
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_replications_needs_src_bucket(mut env_with_replications: MockEnvGetter) {
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Err(VarError::NotPresent));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_BUCKET"))
            .return_const(Ok("bucket2".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_HOST"))
            .return_const(Ok("http://localhost".to_string()));

        env_with_replications
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let components = CfgParser::from_env(env_with_replications, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let repo = components.replication_repo.read().await.unwrap();
        assert_eq!(repo.replications().await.unwrap().len(), 0);
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_replications_src_not_exist(mut env_with_replications: MockEnvGetter) {
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Ok("NOT-EXIST".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_BUCKET"))
            .return_const(Ok("bucket2".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_HOST"))
            .return_const(Ok("http://localhost".to_string()));

        env_with_replications
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let components = CfgParser::from_env(env_with_replications, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let repo = components.replication_repo.read().await.unwrap();
        assert_eq!(repo.replications().await.unwrap().len(), 0);
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_replications_needs_dst_bucket(mut env_with_replications: MockEnvGetter) {
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Err(VarError::NotPresent));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_BUCKET"))
            .return_const(Err(VarError::NotPresent));
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_HOST"))
            .return_const(Ok("http://localhost".to_string()));

        env_with_replications
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let components = CfgParser::from_env(env_with_replications, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let repo = components.replication_repo.read().await.unwrap();
        assert_eq!(repo.replications().await.unwrap().len(), 0);
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_replications_needs_dst_host(mut env_with_replications: MockEnvGetter) {
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Err(VarError::NotPresent));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_BUCKET"))
            .return_const(Ok("bucket2".to_string()));
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_HOST"))
            .return_const(Err(VarError::NotPresent));

        env_with_replications
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let components = CfgParser::from_env(env_with_replications, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let repo = components.replication_repo.read().await.unwrap();
        assert_eq!(repo.replications().await.unwrap().len(), 0);
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_replications_needs_valid_dst_host(mut env_with_replications: MockEnvGetter) {
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Ok("bucket1".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_BUCKET"))
            .return_const(Ok("bucket2".to_string()));
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_HOST"))
            .return_const(Ok("invalid-url".to_string()));

        env_with_replications
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let components = CfgParser::from_env(env_with_replications, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let repo = components.replication_repo.read().await.unwrap();
        assert_eq!(repo.replications().await.unwrap().len(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_replications_update_existing(mut env_with_replications: MockEnvGetter) {
        let cfg = Cfg {
            data_path: env_with_replications.get("RS_DATA_PATH").unwrap().into(),
            ..Default::default()
        };
        let storage = StorageEngine::builder()
            .with_data_path(cfg.data_path.clone())
            .with_cfg(cfg.clone())
            .build()
            .await;
        storage
            .create_bucket("bucket1", Default::default())
            .await
            .unwrap();
        let mut repo = create_replication_repo(
            Arc::new(storage),
            Cfg {
                replication_conf: ReplicationConfig {
                    connection_timeout: std::time::Duration::from_secs(10),
                    replication_log_size: 500,
                    verify_ssl: true,
                    ca_path: None,
                    listening_port: 8080,
                },
                ..Default::default()
            },
        )
        .await;
        repo.create_replication(
            "replication1",
            ReplicationSettings {
                src_bucket: "bucket1".to_string(),
                dst_bucket: "bucket2".to_string(),
                dst_host: "http://localhost".to_string(),
                dst_token: None,
                entries: vec![],
                dst_prefix: String::new(),
                each_n: None,
                when: None,
                mode: ReplicationMode::Enabled,
                compression: ReplicationCompression::None,
            },
        )
        .await
        .unwrap();
        repo.set_mode("replication1", ReplicationMode::Disabled)
            .await
            .unwrap();

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Ok("bucket1".to_string()));
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_BUCKET"))
            .return_const(Ok("bucket2".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_HOST"))
            .return_const(Ok("http://localhost".to_string()));

        env_with_replications
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let components = CfgParser::from_env(env_with_replications, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let repo = components.replication_repo.read().await.unwrap();
        let replication = repo.get_replication_settings("replication1").await.unwrap();
        let repl_info = repo.get_info("replication1").await.unwrap();
        // The when condition should include the original $and plus migrated $in and $nin
        assert_eq!(
            replication.when,
            Some(serde_json::json!({
                "$and": [true, false],
                "$in": ["KEY", "value"],
                "$nin": ["KEY", "value"]
            }))
        );
        assert_eq!(repl_info.info.mode, ReplicationMode::Disabled);
    }

    #[rstest]
    #[tokio::test]
    async fn test_replications_update_existing_preserves_mode(
        mut env_with_replications: MockEnvGetter,
    ) {
        let cfg = Cfg {
            data_path: env_with_replications.get("RS_DATA_PATH").unwrap().into(),
            ..Default::default()
        };
        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg.clone())
                .build()
                .await,
        );
        storage
            .create_bucket("bucket1", Default::default())
            .await
            .unwrap();
        let mut repo = create_replication_repo(
            storage.clone(),
            Cfg {
                replication_conf: ReplicationConfig {
                    connection_timeout: std::time::Duration::from_secs(10),
                    replication_log_size: 500,
                    verify_ssl: true,
                    ca_path: None,
                    listening_port: 8080,
                },
                ..Default::default()
            },
        )
        .await;
        repo.create_replication(
            "replication1",
            ReplicationSettings {
                src_bucket: "bucket1".to_string(),
                dst_bucket: "bucket2".to_string(),
                dst_host: "http://localhost".to_string(),
                dst_token: None,
                entries: vec![],
                dst_prefix: String::new(),
                each_n: None,
                when: None,
                mode: ReplicationMode::Enabled,
                compression: ReplicationCompression::None,
            },
        )
        .await
        .unwrap();
        repo.set_mode("replication1", ReplicationMode::Disabled)
            .await
            .unwrap();

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Ok("bucket1".to_string()));
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_BUCKET"))
            .return_const(Ok("bucket2".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_HOST"))
            .return_const(Ok("http://localhost".to_string()));

        env_with_replications
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let components = CfgParser::from_env(env_with_replications, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let repo = components.replication_repo.read().await.unwrap();
        let info = repo.get_info("replication1").await.unwrap();
        assert_eq!(info.info.mode, ReplicationMode::Disabled);
    }

    #[rstest]
    #[tokio::test]
    async fn test_replications_update_existing_overrides_mode_when_set(
        mut env_with_replications: MockEnvGetter,
    ) {
        let cfg = Cfg {
            data_path: env_with_replications.get("RS_DATA_PATH").unwrap().into(),
            ..Default::default()
        };
        let storage = Arc::new(
            StorageEngine::builder()
                .with_data_path(cfg.data_path.clone())
                .with_cfg(cfg.clone())
                .build()
                .await,
        );
        storage
            .create_bucket("bucket1", Default::default())
            .await
            .unwrap();
        let mut repo = create_replication_repo(
            storage.clone(),
            Cfg {
                replication_conf: ReplicationConfig {
                    connection_timeout: std::time::Duration::from_secs(10),
                    replication_log_size: 500,
                    verify_ssl: true,
                    ca_path: None,
                    listening_port: 8080,
                },
                ..Default::default()
            },
        )
        .await;
        repo.create_replication(
            "replication1",
            ReplicationSettings {
                src_bucket: "bucket1".to_string(),
                dst_bucket: "bucket2".to_string(),
                dst_host: "http://localhost".to_string(),
                dst_token: None,
                entries: vec![],
                dst_prefix: String::new(),
                each_n: None,
                when: None,
                mode: ReplicationMode::Enabled,
                compression: ReplicationCompression::None,
            },
        )
        .await
        .unwrap();
        repo.set_mode("replication1", ReplicationMode::Disabled)
            .await
            .unwrap();

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Ok("bucket1".to_string()));
        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_BUCKET"))
            .return_const(Ok("bucket2".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_HOST"))
            .return_const(Ok("http://localhost".to_string()));

        env_with_replications
            .expect_get()
            .with(eq("RS_REPLICATION_1_MODE"))
            .return_const(Ok("paused".to_string()));

        env_with_replications
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let components = CfgParser::from_env(env_with_replications, "0.0.0")
            .await
            .build()
            .await
            .unwrap();
        let repo = components.replication_repo.read().await.unwrap();
        let info = repo.get_info("replication1").await.unwrap();
        assert_eq!(info.info.mode, ReplicationMode::Paused);
    }

    #[cfg(test)]
    mod each_s {

        use super::*;

        #[log_test(rstest)]
        #[tokio::test]
        async fn test_each_s_migrated_to_each_t_without_when() {
            test_each_s_migration("2.5", None, serde_json::json!({"$each_t": 2.5})).await;
        }

        #[log_test(rstest)]
        #[tokio::test]
        async fn test_each_s_migrated_to_each_t_with_existing_when() {
            test_each_s_migration(
                "2.0",
                Some(r#"{"&label": {"$eq": 1}}"#),
                serde_json::json!({
                    "&label": {"$eq": 1},
                    "$each_t": 2.0
                }),
            )
            .await;
        }

        async fn test_each_s_migration(
            each_s: &str,
            when: Option<&str>,
            expected: serde_json::Value,
        ) {
            let path = tempfile::tempdir().unwrap().keep();
            let mut env = env_with_each_s(path);

            if let Some(when_condition) = when {
                env.expect_get()
                    .with(eq("RS_REPLICATION_1_WHEN"))
                    .return_const(Ok(when_condition.to_string()));
            }

            env.expect_get()
                .with(eq("RS_REPLICATION_1_EACH_S"))
                .return_const(Ok(each_s.to_string()));

            env.expect_get().return_const(Err(VarError::NotPresent));

            let components = CfgParser::from_env(env, "0.0.0")
                .await
                .build()
                .await
                .unwrap();
            let repo = components.replication_repo.read().await.unwrap();
            let replication = repo.get_replication_settings("replication1").await.unwrap();

            assert_eq!(replication.when, Some(expected));
        }

        /// Creates a base MockEnvGetter for each_s migration tests.
        /// Sets up minimal replication configuration without EACH_S or WHEN.
        /// Caller must add:
        /// - Specific EACH_S and/or WHEN expectations
        /// - Catch-all expectation (last)
        fn env_with_each_s(path: PathBuf) -> MockEnvGetter {
            let mut env = MockEnvGetter::new();
            env.expect_get()
                .with(eq("RS_DATA_PATH"))
                .return_const(Ok(path.to_str().unwrap().to_string()));

            env.expect_get()
                .with(eq("RS_BUCKET_1_NAME"))
                .return_const(Ok("bucket1".to_string()));

            env.expect_all().returning(|| {
                let mut map = BTreeMap::new();
                map.insert("RS_BUCKET_1_NAME".to_string(), "bucket1".to_string());
                map.insert(
                    "RS_REPLICATION_1_NAME".to_string(),
                    "replication1".to_string(),
                );
                map
            });

            env.expect_get()
                .with(eq("RS_REPLICATION_1_NAME"))
                .return_const(Ok("replication1".to_string()));

            env.expect_get()
                .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
                .return_const(Ok("bucket1".to_string()));

            env.expect_get()
                .with(eq("RS_REPLICATION_1_DST_BUCKET"))
                .return_const(Ok("bucket2".to_string()));

            env.expect_get()
                .with(eq("RS_REPLICATION_1_DST_HOST"))
                .return_const(Ok("http://localhost".to_string()));

            env
        }
    }

    #[cfg(test)]
    mod include {

        use super::*;

        #[log_test(rstest)]
        #[tokio::test]
        async fn test_include_migrated_to_in_without_when() {
            test_include_migration(
                "sensor",
                "temp",
                None,
                serde_json::json!({"$in": ["SENSOR", "temp"]}),
            )
            .await;
        }

        #[log_test(rstest)]
        #[tokio::test]
        async fn test_include_migrated_to_in_with_existing_when() {
            test_include_migration(
                "location",
                "warehouse",
                Some(r#"{"&status": {"$eq": "active"}}"#),
                serde_json::json!({
                    "&status": {"$eq": "active"},
                    "$in": ["LOCATION", "warehouse"]
                }),
            )
            .await;
        }

        async fn test_include_migration(
            include_key: &str,
            include_value: &str,
            when: Option<&str>,
            expected: serde_json::Value,
        ) {
            let path = tempfile::tempdir().unwrap().keep();
            let mut env = env_with_include(path, include_key, include_value);

            if let Some(when_condition) = when {
                env.expect_get()
                    .with(eq("RS_REPLICATION_1_WHEN"))
                    .return_const(Ok(when_condition.to_string()));
            }

            env.expect_get().return_const(Err(VarError::NotPresent));

            let components = CfgParser::from_env(env, "0.0.0")
                .await
                .build()
                .await
                .unwrap();
            let repo = components.replication_repo.read().await.unwrap();
            let replication = repo.get_replication_settings("replication1").await.unwrap();

            assert_eq!(replication.when, Some(expected));
        }

        /// Creates a base MockEnvGetter for include migration tests.
        /// Sets up minimal replication configuration with INCLUDE_{key}.
        /// Caller must add:
        /// - Optional WHEN expectation
        /// - Catch-all expectation (last)
        fn env_with_include(
            path: PathBuf,
            include_key: &str,
            include_value: &str,
        ) -> MockEnvGetter {
            let mut env = MockEnvGetter::new();
            env.expect_get()
                .with(eq("RS_DATA_PATH"))
                .return_const(Ok(path.to_str().unwrap().to_string()));

            env.expect_get()
                .with(eq("RS_BUCKET_1_NAME"))
                .return_const(Ok("bucket1".to_string()));

            let include_key_upper = include_key.to_uppercase();
            let include_value_owned = include_value.to_string();
            env.expect_all().returning(move || {
                let mut map = BTreeMap::new();
                map.insert("RS_BUCKET_1_NAME".to_string(), "bucket1".to_string());
                map.insert(
                    "RS_REPLICATION_1_NAME".to_string(),
                    "replication1".to_string(),
                );
                map.insert(
                    format!("RS_REPLICATION_1_INCLUDE_{}", include_key_upper),
                    include_value_owned.clone(),
                );
                map
            });

            env.expect_get()
                .with(eq("RS_REPLICATION_1_NAME"))
                .return_const(Ok("replication1".to_string()));

            env.expect_get()
                .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
                .return_const(Ok("bucket1".to_string()));

            env.expect_get()
                .with(eq("RS_REPLICATION_1_DST_BUCKET"))
                .return_const(Ok("bucket2".to_string()));

            env.expect_get()
                .with(eq("RS_REPLICATION_1_DST_HOST"))
                .return_const(Ok("http://localhost".to_string()));

            // The env.matches() will call env.get() for the matched key
            let include_env_key =
                format!("RS_REPLICATION_1_INCLUDE_{}", include_key.to_uppercase());
            env.expect_get()
                .with(eq(include_env_key))
                .return_const(Ok(include_value.to_string()));

            env
        }
    }

    #[cfg(test)]
    mod exclude {

        use super::*;

        #[log_test(rstest)]
        #[tokio::test]
        async fn test_exclude_migrated_to_nin_without_when() {
            test_exclude_migration(
                "status",
                "inactive",
                None,
                serde_json::json!({"$nin": ["STATUS", "inactive"]}),
            )
            .await;
        }

        #[log_test(rstest)]
        #[tokio::test]
        async fn test_exclude_migrated_to_nin_with_existing_when() {
            test_exclude_migration(
                "region",
                "eu-west",
                Some(r#"{"&type": {"$eq": "production"}}"#),
                serde_json::json!({
                    "&type": {"$eq": "production"},
                    "$nin": ["REGION", "eu-west"]
                }),
            )
            .await;
        }

        async fn test_exclude_migration(
            exclude_key: &str,
            exclude_value: &str,
            when: Option<&str>,
            expected: serde_json::Value,
        ) {
            let path = tempfile::tempdir().unwrap().keep();
            let mut env = env_with_exclude(path, exclude_key, exclude_value);

            if let Some(when_condition) = when {
                env.expect_get()
                    .with(eq("RS_REPLICATION_1_WHEN"))
                    .return_const(Ok(when_condition.to_string()));
            }

            env.expect_get().return_const(Err(VarError::NotPresent));

            let components = CfgParser::from_env(env, "0.0.0")
                .await
                .build()
                .await
                .unwrap();
            let repo = components.replication_repo.read().await.unwrap();
            let replication = repo.get_replication_settings("replication1").await.unwrap();

            assert_eq!(replication.when, Some(expected));
        }

        /// Creates a base MockEnvGetter for exclude migration tests.
        /// Sets up minimal replication configuration with EXCLUDE_{key}.
        /// Caller must add:
        /// - Optional WHEN expectation
        /// - Catch-all expectation (last)
        fn env_with_exclude(
            path: PathBuf,
            exclude_key: &str,
            exclude_value: &str,
        ) -> MockEnvGetter {
            let mut env = MockEnvGetter::new();
            env.expect_get()
                .with(eq("RS_DATA_PATH"))
                .return_const(Ok(path.to_str().unwrap().to_string()));

            env.expect_get()
                .with(eq("RS_BUCKET_1_NAME"))
                .return_const(Ok("bucket1".to_string()));

            let exclude_key_upper = exclude_key.to_uppercase();
            let exclude_value_owned = exclude_value.to_string();
            env.expect_all().returning(move || {
                let mut map = BTreeMap::new();
                map.insert("RS_BUCKET_1_NAME".to_string(), "bucket1".to_string());
                map.insert(
                    "RS_REPLICATION_1_NAME".to_string(),
                    "replication1".to_string(),
                );
                map.insert(
                    format!("RS_REPLICATION_1_EXCLUDE_{}", exclude_key_upper),
                    exclude_value_owned.clone(),
                );
                map
            });

            env.expect_get()
                .with(eq("RS_REPLICATION_1_NAME"))
                .return_const(Ok("replication1".to_string()));

            env.expect_get()
                .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
                .return_const(Ok("bucket1".to_string()));

            env.expect_get()
                .with(eq("RS_REPLICATION_1_DST_BUCKET"))
                .return_const(Ok("bucket2".to_string()));

            env.expect_get()
                .with(eq("RS_REPLICATION_1_DST_HOST"))
                .return_const(Ok("http://localhost".to_string()));

            // The env.matches() will call env.get() for the matched key
            let exclude_env_key =
                format!("RS_REPLICATION_1_EXCLUDE_{}", exclude_key.to_uppercase());
            env.expect_get()
                .with(eq(exclude_env_key))
                .return_const(Ok(exclude_value.to_string()));

            env
        }
    }

    #[fixture]
    fn path() -> PathBuf {
        let tmp = tempfile::tempdir().unwrap();
        tmp.keep()
    }

    #[fixture]
    fn env_with_replications(path: PathBuf) -> MockEnvGetter {
        let mut mock_getter = MockEnvGetter::new();
        mock_getter
            .expect_get()
            .with(eq("RS_DATA_PATH"))
            .return_const(Ok(path.to_str().unwrap().to_string()));

        mock_getter
            .expect_get()
            .with(eq("RS_BUCKET_1_NAME"))
            .return_const(Ok("bucket1".to_string()));

        mock_getter.expect_all().returning(|| {
            let mut map = BTreeMap::new();
            map.insert("RS_BUCKET_1_NAME".to_string(), "bucket1".to_string());

            map.insert(
                "RS_REPLICATION_1_NAME".to_string(),
                "replication1".to_string(),
            );

            map.insert(
                "RS_REPLICATION_1_INCLUDE_KEY".to_string(),
                "bucket1".to_string(),
            );

            map.insert(
                "RS_REPLICATION_1_EXCLUDE_KEY".to_string(),
                "bucket1".to_string(),
            );

            map
        });

        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_NAME"))
            .return_const(Ok("replication1".to_string()));

        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_ENTRIES"))
            .return_const(Ok("entry1,entry2".to_string()));
        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_PREFIX"))
            .return_const(Ok("robot-1".to_string()));

        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_EACH_N"))
            .return_const(Ok("10".to_string()));

        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_WHEN"))
            .return_const(Ok(r#"{"$and":[true, false]}"#.to_string()));

        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_DST_TOKEN"))
            .return_const(Ok("TOKEN".to_string()));

        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_INCLUDE_KEY"))
            .return_const(Ok("value".to_string()));

        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_EXCLUDE_KEY"))
            .return_const(Ok("value".to_string()));

        mock_getter
    }
}
