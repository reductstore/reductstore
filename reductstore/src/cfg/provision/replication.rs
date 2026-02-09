// Copyright 2025-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::CfgParser;
use crate::core::env::{Env, GetEnv};
use crate::replication::{ManageReplications, ReplicationRepoBuilder};
use crate::storage::engine::StorageEngine;
use log::{error, info, warn};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::replication_api::{ReplicationMode, ReplicationSettings};
use reduct_base::Labels;
use std::collections::HashMap;
use std::sync::Arc;

impl<EnvGetter: GetEnv, ExtCfg: Clone + Send + Sync> CfgParser<EnvGetter, ExtCfg> {
    pub(in crate::cfg) async fn provision_replication_repo(
        &self,
        storage: Arc<StorageEngine>,
    ) -> Result<Box<dyn ManageReplications + Send + Sync>, ReductError> {
        let mut repo = ReplicationRepoBuilder::new(self.cfg.clone())
            .build(Arc::clone(&storage))
            .await;
        for (name, settings) in &self.cfg.replications {
            if let Err(e) = repo.create_replication(&name, settings.clone()).await {
                if e.status() == ErrorCode::Conflict {
                    let mut settings = settings.clone();
                    if let Ok(replication) = repo.get_replication(&name) {
                        settings.mode = replication.mode();
                    }
                    repo.update_replication(&name, settings).await?;
                } else {
                    error!("Failed to provision replication '{}': {}", name, e);
                    continue;
                }
            }

            let replication = repo.get_mut_replication(&name)?;
            replication.set_provisioned(true);

            info!(
                "Provisioned replication '{}' with {:?}",
                name,
                replication.masked_settings()
            );
        }
        Ok(repo)
    }

    pub(in crate::cfg) fn parse_replications(
        env: &mut Env<EnvGetter>,
    ) -> HashMap<String, ReplicationSettings> {
        let mut replications = HashMap::<String, (String, ReplicationSettings)>::new();
        for (id, name) in env.matches("RS_REPLICATION_(.*)_NAME") {
            let replication = ReplicationSettings {
                src_bucket: "".to_string(),
                dst_bucket: "".to_string(),
                dst_host: "http://localhost".to_string(),
                dst_token: None,
                entries: vec![],
                include: Labels::default(),
                exclude: Labels::default(),
                each_n: None,
                each_s: None,
                when: None,
                mode: ReplicationMode::Enabled,
            };
            replications.insert(id, (name, replication));
        }

        let mut unfinished_replications = vec![];
        for (id, (name, replication)) in &mut replications {
            if let Some(src_bucket) =
                env.get_optional::<String>(&format!("RS_REPLICATION_{}_SRC_BUCKET", id))
            {
                replication.src_bucket = src_bucket;
            } else {
                error!("Replication '{}' has no source bucket. Drop it.", name);
                unfinished_replications.push(id.clone());
                continue;
            }

            if let Some(remote_bucket) =
                env.get_optional::<String>(&format!("RS_REPLICATION_{}_DST_BUCKET", id))
            {
                replication.dst_bucket = remote_bucket;
            } else {
                error!("Replication '{}' has no destination bucket. Drop it.", name);
                unfinished_replications.push(id.clone());
                continue;
            }

            if let Some(remote_host) =
                env.get_optional::<String>(&format!("RS_REPLICATION_{}_DST_HOST", id))
            {
                match url::Url::parse(&remote_host) {
                    Ok(url) => replication.dst_host = url.to_string(),
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
            replication.dst_token = if token.is_empty() { None } else { Some(token) };

            if let Some(entries) =
                env.get_optional::<String>(&format!("RS_REPLICATION_{}_ENTRIES", id))
            {
                replication.entries = entries.split(",").map(|s| s.to_string()).collect();
            }

            for (key, value) in env.matches(&format!("RS_REPLICATION_{}_INCLUDE_(.*)", id)) {
                warn!(
                    "The include parameter is deprecated. Use 'RS_REPLICATION_{}_WHEN' instead.",
                    id
                );
                replication.include.insert(key, value);
            }

            for (key, value) in env.matches(&format!("RS_REPLICATION_{}_EXCLUDE_(.*)", id)) {
                warn!(
                    "The exclude parameter is deprecated. Use 'RS_REPLICATION_{}_WHEN' instead.",
                    id
                );
                replication.exclude.insert(key, value);
            }

            if let Some(each_n) = env.get_optional::<u64>(&format!("RS_REPLICATION_{}_EACH_N", id))
            {
                replication.each_n = Some(each_n);
            }

            if let Some(each_s) = env.get_optional::<f64>(&format!("RS_REPLICATION_{}_EACH_S", id))
            {
                replication.each_s = Some(each_s);
            }

            if let Some(when) =
                env.get_optional::<serde_json::Value>(&format!("RS_REPLICATION_{}_WHEN", id))
            {
                replication.when = Some(when);
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
        let replication = repo.get_replication("replication1").unwrap();

        assert_eq!(replication.settings().src_bucket, "bucket1");
        assert_eq!(replication.settings().dst_bucket, "bucket2");
        assert_eq!(replication.settings().dst_host, "http://localhost/");
        assert_eq!(replication.settings().dst_token, Some("TOKEN".to_string()));
        assert_eq!(replication.settings().entries, vec!["entry1", "entry2"]);
        assert_eq!(replication.settings().each_n, Some(10));
        assert_eq!(replication.settings().each_s, Some(0.5));
        assert_eq!(
            replication.settings().when,
            Some(serde_json::json!({"$and": [true, false]}))
        );
        assert!(replication.is_provisioned());
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
                include: Labels::default(),
                exclude: Labels::default(),
                each_n: None,
                each_s: None,
                when: None,
                mode: ReplicationMode::Enabled,
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
        let replication = repo.get_replication("replication1").unwrap();
        assert_eq!(
            replication.settings().when,
            Some(serde_json::json!({"$and": [true, false]}))
        );
        assert_eq!(replication.mode(), ReplicationMode::Disabled);
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
                include: Labels::default(),
                exclude: Labels::default(),
                each_n: None,
                each_s: None,
                when: None,
                mode: ReplicationMode::Enabled,
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
        let replication = repo.get_replication("replication1").unwrap();
        assert_eq!(replication.mode(), ReplicationMode::Disabled);
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
            .with(eq("RS_REPLICATION_1_EACH_N"))
            .return_const(Ok("10".to_string()));
        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_EACH_S"))
            .return_const(Ok("0.5".to_string()));
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
