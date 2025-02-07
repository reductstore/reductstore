// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::Cfg;
use crate::core::env::{Env, GetEnv};
use crate::replication::{create_replication_repo, ManageReplications};
use crate::storage::storage::Storage;
use log::{error, info, warn};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::replication_api::ReplicationSettings;
use reduct_base::Labels;
use std::collections::HashMap;
use std::sync::Arc;

impl<EnvGetter: GetEnv> Cfg<EnvGetter> {
    pub(super) fn provision_replication_repo(
        &self,
        storage: Arc<Storage>,
    ) -> Result<Box<dyn ManageReplications + Send + Sync>, ReductError> {
        let mut repo = create_replication_repo(Arc::clone(&storage), self.port);
        for (name, settings) in &self.replications {
            if let Err(e) = repo.create_replication(&name, settings.clone()) {
                if e.status() == ErrorCode::Conflict {
                    repo.update_replication(&name, settings.clone())?;
                } else {
                    error!("Failed to provision replication '{}': {}", name, e);
                    continue;
                }
            }

            let replication = repo.get_mut_replication(&name).unwrap();
            replication.set_provisioned(true);

            info!(
                "Provisioned replication '{}' with {:?}",
                name,
                replication.masked_settings()
            );
        }
        Ok(repo)
    }
    pub(super) fn parse_replications(
        env: &mut Env<EnvGetter>,
    ) -> HashMap<String, ReplicationSettings> {
        let mut replications = HashMap::<String, (String, ReplicationSettings)>::new();
        for (id, name) in env.matches("RS_REPLICATION_(.*)_NAME") {
            let replication = ReplicationSettings {
                src_bucket: "".to_string(),
                dst_bucket: "".to_string(),
                dst_host: "http://localhost".to_string(),
                dst_token: "".to_string(),
                entries: vec![],
                include: Labels::default(),
                exclude: Labels::default(),
                each_n: None,
                each_s: None,
                when: None,
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

            replication.dst_token = env
                .get_masked::<String>(&format!("RS_REPLICATION_{}_DST_TOKEN", id), "".to_string());

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
    use crate::cfg::tests::MockEnvGetter;
    use mockall::predicate::eq;
    use rstest::{fixture, rstest};
    use std::collections::BTreeMap;
    use std::env::VarError;
    use std::path::PathBuf;
    use test_log::test as log_test;

    #[rstest]
    #[tokio::test]
    async fn test_replications(mut env_with_replications: MockEnvGetter) {
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

        let components = Cfg::from_env(env_with_replications).build().unwrap();
        let repo = components.replication_repo.read().await;
        let replication = repo.get_replication("replication1").unwrap();

        assert_eq!(replication.settings().src_bucket, "bucket1");
        assert_eq!(replication.settings().dst_bucket, "bucket2");
        assert_eq!(replication.settings().dst_host, "http://localhost/");
        assert_eq!(replication.settings().dst_token, "TOKEN");
        assert_eq!(replication.settings().entries, vec!["entry1", "entry2"]);
        assert_eq!(replication.settings().each_n, Some(10));
        assert_eq!(replication.settings().each_s, Some(0.5));
        assert_eq!(
            replication.settings().when,
            Some(serde_json::json!({"$and": [true, false]}))
        );
        assert!(replication.is_provisioned());
    }

    #[rstest]
    #[tokio::test]
    async fn test_replications_needs_dst_bucket(mut env_with_replications: MockEnvGetter) {
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

        let components = Cfg::from_env(env_with_replications).build().unwrap();
        let repo = components.replication_repo.read().await;
        assert_eq!(repo.replications().len(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_replications_needs_dst_host(mut env_with_replications: MockEnvGetter) {
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

        let components = Cfg::from_env(env_with_replications).build().unwrap();
        let repo = components.replication_repo.read().await;
        assert_eq!(repo.replications().len(), 0);
    }

    #[rstest]
    #[tokio::test]
    async fn test_replications_update_existing(
        mut env_with_replications: MockEnvGetter,
        path: PathBuf,
    ) {
        let storage = Storage::load(path.clone(), None);
        storage
            .create_bucket("bucket1", Default::default())
            .unwrap();
        let mut repo = create_replication_repo(Arc::new(storage), 8080);
        repo.create_replication(
            "replication1",
            ReplicationSettings {
                src_bucket: "bucket1".to_string(),
                dst_bucket: "bucket2".to_string(),
                dst_host: "http://localhost".to_string(),
                dst_token: "".to_string(),
                entries: vec![],
                include: Labels::default(),
                exclude: Labels::default(),
                each_n: None,
                each_s: None,
                when: None,
            },
        )
        .unwrap();

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

        let components = Cfg::from_env(env_with_replications).build().unwrap();
        let repo = components.replication_repo.read().await;
        let replication = repo.get_replication("replication1").unwrap();
        assert_eq!(
            replication.settings().when,
            Some(serde_json::json!({"$and": [true, false]}))
        );
    }

    #[fixture]
    fn path() -> PathBuf {
        let tmp = tempfile::tempdir().unwrap();
        tmp.into_path()
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

            map
        });

        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_NAME"))
            .return_const(Ok("replication1".to_string()));

        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
            .return_const(Ok("bucket1".to_string()));

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
    }
}
