// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::Components;
use crate::asset::asset_manager::create_asset_manager;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::{create_token_repository, ManageTokens};
use crate::core::env::{Env, GetEnv};
use crate::license::parse_license;
use crate::replication::{create_replication_repo, ManageReplications};
use crate::storage::storage::Storage;
use bytesize::ByteSize;
use log::{error, info, warn};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::bucket_api::BucketSettings;
use reduct_base::msg::replication_api::ReplicationSettings;
use reduct_base::msg::token_api::{Permissions, Token};
use reduct_base::Labels;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Database configuration
pub struct Cfg<EnvGetter: GetEnv> {
    pub log_level: String,
    pub host: String,
    pub port: i32,
    pub api_base_path: String,
    pub data_path: String,
    pub api_token: String,
    pub cert_path: String,
    pub cert_key_path: String,
    pub license_path: Option<String>,
    pub buckets: HashMap<String, BucketSettings>,
    pub tokens: HashMap<String, Token>,
    pub replications: HashMap<String, ReplicationSettings>,

    env: Env<EnvGetter>,
}

impl<EnvGetter: GetEnv> Cfg<EnvGetter> {
    pub fn from_env(env_getter: EnvGetter) -> Self {
        let mut env = Env::new(env_getter);
        let cfg = Cfg {
            log_level: env.get("RS_LOG_LEVEL", "INFO".to_string()),
            host: env.get("RS_HOST", "0.0.0.0".to_string()),
            port: env.get("RS_PORT", 8383),
            api_base_path: env.get("RS_API_BASE_PATH", "/".to_string()),
            data_path: env.get("RS_DATA_PATH", "/data".to_string()),
            api_token: env.get_masked("RS_API_TOKEN", "".to_string()),
            cert_path: env.get_masked("RS_CERT_PATH", "".to_string()),
            cert_key_path: env.get_masked("RS_CERT_KEY_PATH", "".to_string()),
            license_path: env.get_optional("RS_LICENSE_PATH"),
            buckets: Self::parse_buckets(&mut env),
            tokens: Self::parse_tokens(&mut env),
            replications: Self::parse_replications(&mut env),

            env,
        };

        cfg
    }

    pub async fn build(&self) -> Result<Components, ReductError> {
        let storage = Arc::new(RwLock::new(self.provision_storage().await));
        let token_repo = self.provision_tokens();
        let console = create_asset_manager(load_console());
        let replication_engine = self
            .provision_replication_repo(Arc::clone(&storage))
            .await?;

        Ok(Components {
            storage,
            token_repo: RwLock::new(token_repo),
            auth: TokenAuthorization::new(&self.api_token),
            console,
            replication_repo: RwLock::new(replication_engine),
            base_path: self.api_base_path.clone(),
        })
    }

    fn provision_tokens(&self) -> Box<dyn ManageTokens + Send + Sync> {
        let mut token_repo =
            create_token_repository(PathBuf::from(self.data_path.clone()), &self.api_token);

        for (name, token) in &self.tokens {
            let is_generated = match token_repo
                .generate_token(&name, token.permissions.clone().unwrap_or_default())
            {
                Ok(_) => Ok(()),
                Err(e) => {
                    if e.status() == ErrorCode::Conflict {
                        Ok(())
                    } else {
                        Err(e)
                    }
                }
            };

            if let Err(err) = is_generated {
                error!("Failed to provision token '{}': {}", name, err);
            } else {
                let update_token = token_repo.get_mut_token(&name).unwrap();
                update_token.clone_from(token);
                update_token.is_provisioned = true;

                info!(
                    "Provisioned token '{}' with {:?}",
                    update_token.name, update_token.permissions
                );
            }
        }
        token_repo
    }

    async fn provision_storage(&self) -> Storage {
        let license = parse_license(self.license_path.clone());
        let mut storage = Storage::load(PathBuf::from(self.data_path.clone()), license).await;
        for (name, settings) in &self.buckets {
            let settings = match storage.create_bucket(&name, settings.clone()) {
                Ok(bucket) => {
                    bucket.set_provisioned(true);
                    Ok(bucket.settings().clone())
                }
                Err(e) => {
                    if e.status() == ErrorCode::Conflict {
                        let bucket = storage.get_mut_bucket(&name).unwrap();
                        bucket.set_provisioned(false);
                        bucket.set_settings(settings.clone()).unwrap();
                        bucket.set_provisioned(true);

                        Ok(bucket.settings().clone())
                    } else {
                        Err(e)
                    }
                }
            };

            if let Ok(settings) = settings {
                info!("Provisioned bucket '{}' with: {:?}", name, settings);
            } else {
                error!(
                    "Failed to provision bucket '{}': {}",
                    name,
                    settings.err().unwrap()
                );
            }
        }
        storage
    }

    async fn provision_replication_repo(
        &self,
        storage: Arc<RwLock<Storage>>,
    ) -> Result<Box<dyn ManageReplications + Send + Sync>, ReductError> {
        let mut repo = create_replication_repo(Arc::clone(&storage)).await;
        for (name, settings) in &self.replications {
            if let Err(e) = repo.create_replication(&name, settings.clone()).await {
                if e.status() == ErrorCode::Conflict {
                    repo.update_replication(&name, settings.clone()).await?;
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

    fn parse_buckets(env: &mut Env<EnvGetter>) -> HashMap<String, BucketSettings> {
        let mut buckets = HashMap::<String, (String, BucketSettings)>::new();
        for (id, name) in env.matches("RS_BUCKET_(.*)_NAME") {
            buckets.insert(id, (name, BucketSettings::default()));
        }

        for (id, bucket) in &mut buckets {
            let settings = &mut bucket.1;
            settings.quota_type = env.get_optional(&format!("RS_BUCKET_{}_QUOTA_TYPE", id));

            settings.quota_size = env
                .get_optional::<ByteSize>(&format!("RS_BUCKET_{}_QUOTA_SIZE", id))
                .map(|s| s.as_u64());
            settings.max_block_size = env
                .get_optional::<ByteSize>(&format!("RS_BUCKET_{}_MAX_BLOCK_SIZE", id))
                .map(|s| s.as_u64());
            settings.max_block_records =
                env.get_optional(&format!("RS_BUCKET_{}_MAX_BLOCK_RECORDS", id));
        }

        buckets
            .into_iter()
            .map(|(_id, (name, settings))| (name, settings))
            .collect()
    }

    fn parse_tokens(env: &mut Env<EnvGetter>) -> HashMap<String, Token> {
        let mut tokens = HashMap::<String, Token>::new();
        for (id, name) in env.matches("RS_TOKEN_(.*)_NAME") {
            let token = Token {
                name,
                created_at: chrono::Utc::now(),
                ..Token::default()
            };
            tokens.insert(id, token);
        }

        for (id, token) in &mut tokens {
            token.value =
                env.get_masked::<String>(&format!("RS_TOKEN_{}_VALUE", id), "".to_string());
        }

        tokens.retain(|_, token| {
            if token.value.is_empty() {
                warn!("Token '{}' has no value. Drop it.", token.name);
                false
            } else {
                true
            }
        });

        let parse_list_env = |env: &mut Env<EnvGetter>, name: String| -> Vec<String> {
            env.get_optional::<String>(&name)
                .unwrap_or_default()
                .split(",")
                .map(|s| s.to_string())
                .collect()
        };

        // Parse permissions
        for (id, token) in &mut tokens {
            let read = parse_list_env(env, format!("RS_TOKEN_{}_READ", id));
            let write = parse_list_env(env, format!("RS_TOKEN_{}_WRITE", id));
            let permissions = Permissions {
                full_access: env
                    .get_optional(&format!("RS_TOKEN_{}_FULL_ACCESS", id))
                    .unwrap_or_default(),
                read,
                write,
            };

            token.permissions = Some(permissions);
        }

        tokens
            .into_iter()
            .map(|(_, token)| (token.name.clone(), token))
            .collect()
    }

    fn parse_replications(env: &mut Env<EnvGetter>) -> HashMap<String, ReplicationSettings> {
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

            replication.dst_token =
                env.get::<String>(&format!("RS_REPLICATION_{}_DST_TOKEN", id), "".to_string());

            if let Some(entries) =
                env.get_optional::<String>(&format!("RS_REPLICATION_{}_ENTRIES", id))
            {
                replication.entries = entries.split(",").map(|s| s.to_string()).collect();
            }

            for (key, value) in env.matches(&format!("RS_REPLICATION_{}_INCLUDE_(.*)", id)) {
                replication.include.insert(key, value);
            }

            for (key, value) in env.matches(&format!("RS_REPLICATION_{}_EXCLUDE_(.*)", id)) {
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
        }

        replications
            .into_iter()
            .filter(|(id, _)| !unfinished_replications.contains(id))
            .map(|(_, (name, replication))| (name, replication))
            .collect()
    }
}

#[cfg(feature = "web-console")]
fn load_console() -> &'static [u8] {
    info!("Load Web Console");
    include_bytes!(concat!(env!("OUT_DIR"), "/console.zip"))
}

#[cfg(not(feature = "web-console"))]
fn load_console() -> &'static [u8] {
    info!("Web Console is disabled");
    b""
}

impl<EnvGetter: GetEnv> Display for Cfg<EnvGetter> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.env.message())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use mockall::mock;
    use mockall::predicate::eq;
    use rstest::{fixture, rstest};
    use std::collections::BTreeMap;
    use std::env::VarError;

    mock! {
        EnvGetter {}
        impl GetEnv for EnvGetter {
            fn get(&self, key: &str) -> Result<String, VarError>;
            fn all(&self) -> BTreeMap<String,String>;
        }
    }

    #[rstest]
    fn test_default_settings(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.log_level, "INFO");
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 8383);
        assert_eq!(cfg.api_base_path, "/");
        assert_eq!(cfg.data_path, "/data");
        assert_eq!(cfg.api_token, "");
        assert_eq!(cfg.cert_path, "");
        assert_eq!(cfg.cert_key_path, "");

        assert_eq!(cfg.buckets.len(), 0);
        assert_eq!(cfg.tokens.len(), 0);
    }

    #[rstest]
    fn test_log_level(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_LOG_LEVEL"))
            .times(1)
            .return_const(Ok("DEBUG".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.log_level, "DEBUG");
    }

    #[rstest]
    fn test_host(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_HOST"))
            .times(1)
            .return_const(Ok("127.0.0.1".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.host, "127.0.0.1");
    }

    #[rstest]
    fn test_port(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_PORT"))
            .times(1)
            .return_const(Ok("1234".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.port, 1234);
    }

    #[rstest]
    fn test_api_base_path(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_API_BASE_PATH"))
            .times(1)
            .return_const(Ok("/api".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.api_base_path, "/api");
    }

    #[rstest]
    fn test_data_path(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_DATA_PATH"))
            .times(1)
            .return_const(Ok("/tmp".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.data_path, "/tmp");
    }

    #[rstest]
    fn test_api_token(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_API_TOKEN"))
            .times(1)
            .return_const(Ok("XXX".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.api_token, "XXX");
    }

    #[rstest]
    fn test_cert_path(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_CERT_PATH"))
            .times(1)
            .return_const(Ok("/tmp/cert.pem".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.cert_path, "/tmp/cert.pem");
    }

    #[rstest]
    fn test_cert_key_path(mut env_getter: MockEnvGetter) {
        env_getter
            .expect_get()
            .with(eq("RS_CERT_KEY_PATH"))
            .times(1)
            .return_const(Ok("/tmp/cert.key".to_string()));
        env_getter
            .expect_get()
            .return_const(Err(VarError::NotPresent));
        let cfg = Cfg::from_env(env_getter);
        assert_eq!(cfg.cert_key_path, "/tmp/cert.key");
    }

    #[fixture]
    fn env_getter() -> MockEnvGetter {
        let mut mock_getter = MockEnvGetter::new();
        mock_getter.expect_all().returning(|| BTreeMap::new());
        return mock_getter;
    }

    #[fixture]
    fn env_with_buckets() -> MockEnvGetter {
        let tmp = tempfile::tempdir().unwrap();
        let mut mock_getter = MockEnvGetter::new();
        mock_getter
            .expect_get()
            .with(eq("RS_DATA_PATH"))
            .return_const(Ok(tmp.into_path().to_str().unwrap().to_string()));
        mock_getter.expect_all().returning(|| {
            let mut map = BTreeMap::new();
            map.insert("RS_BUCKET_1_NAME".to_string(), "bucket1".to_string());
            map
        });
        mock_getter
            .expect_get()
            .with(eq("RS_BUCKET_1_NAME"))
            .return_const(Ok("bucket1".to_string()));
        mock_getter
    }

    #[fixture]
    fn env_with_tokens() -> MockEnvGetter {
        let tmp = tempfile::tempdir().unwrap();
        let mut mock_getter = MockEnvGetter::new();
        mock_getter
            .expect_get()
            .with(eq("RS_DATA_PATH"))
            .return_const(Ok(tmp.into_path().to_str().unwrap().to_string()));
        mock_getter
            .expect_get()
            .with(eq("RS_API_TOKEN"))
            .return_const(Ok("XXX".to_string()));
        mock_getter.expect_all().returning(|| {
            let mut map = BTreeMap::new();
            map.insert("RS_TOKEN_1_NAME".to_string(), "token1".to_string());
            map
        });
        mock_getter
            .expect_get()
            .with(eq("RS_TOKEN_1_NAME"))
            .return_const(Ok("token1".to_string()));
        mock_getter
    }

    #[fixture]
    fn env_with_replications() -> MockEnvGetter {
        let tmp = tempfile::tempdir().unwrap();
        let mut mock_getter = MockEnvGetter::new();
        mock_getter
            .expect_get()
            .with(eq("RS_DATA_PATH"))
            .return_const(Ok(tmp.into_path().to_str().unwrap().to_string()));
        mock_getter.expect_all().returning(|| {
            let mut map = BTreeMap::new();
            map.insert("RS_BUCKET_1_NAME".to_string(), "bucket1".to_string());

            map.insert(
                "RS_REPLICATION_1_NAME".to_string(),
                "replication1".to_string(),
            );
            map.insert(
                "RS_REPLICATION_1_INCLUDE_key1".to_string(),
                "value1".to_string(),
            );
            map.insert(
                "RS_REPLICATION_1_EXCLUDE_key2".to_string(),
                "value2".to_string(),
            );
            map
        });
        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_NAME"))
            .return_const(Ok("replication1".to_string()));
        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_INCLUDE_key1"))
            .return_const(Ok("value1".to_string()));
        mock_getter
            .expect_get()
            .with(eq("RS_REPLICATION_1_EXCLUDE_key2"))
            .return_const(Ok("value2".to_string()));
        mock_getter
    }

    mod provision {
        use super::*;

        use crate::storage::bucket::Bucket;
        use reduct_base::error::ReductError;
        use reduct_base::msg::bucket_api::QuotaType::FIFO;

        #[rstest]
        #[tokio::test]
        async fn test_buckets(mut env_with_buckets: MockEnvGetter) {
            env_with_buckets
                .expect_get()
                .with(eq("RS_BUCKET_1_QUOTA_TYPE"))
                .return_const(Ok("FIFO".to_string()));
            env_with_buckets
                .expect_get()
                .with(eq("RS_BUCKET_1_QUOTA_SIZE"))
                .return_const(Ok("1GB".to_string()));
            env_with_buckets
                .expect_get()
                .with(eq("RS_BUCKET_1_MAX_BLOCK_SIZE"))
                .return_const(Ok("1MB".to_string()));
            env_with_buckets
                .expect_get()
                .with(eq("RS_BUCKET_1_MAX_BLOCK_RECORDS"))
                .return_const(Ok("1000".to_string()));

            env_with_buckets
                .expect_get()
                .return_const(Err(VarError::NotPresent));

            let cfg = Cfg::from_env(env_with_buckets);
            let components = cfg.build().await.unwrap();

            let storage = components.storage.read().await;
            let bucket1 = storage.get_bucket("bucket1").unwrap();

            assert!(bucket1.is_provisioned());
            assert_eq!(bucket1.settings().quota_type, Some(FIFO));
            assert_eq!(bucket1.settings().quota_size, Some(1_000_000_000));
            assert_eq!(bucket1.settings().max_block_size, Some(1_000_000));
            assert_eq!(bucket1.settings().max_block_records, Some(1000));
        }

        #[rstest]
        #[tokio::test]
        async fn test_buckets_defaults(mut env_with_buckets: MockEnvGetter) {
            env_with_buckets
                .expect_get()
                .return_const(Err(VarError::NotPresent));

            let cfg = Cfg::from_env(env_with_buckets);
            let components = cfg.build().await.unwrap();

            let storage = components.storage.read().await;
            let bucket1 = storage.get_bucket("bucket1").unwrap();

            assert_eq!(
                bucket1.settings(),
                &Bucket::defaults(),
                "use defaults if env vars are not set"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_tokens(mut env_with_tokens: MockEnvGetter) {
            env_with_tokens
                .expect_get()
                .with(eq("RS_TOKEN_1_VALUE"))
                .return_const(Ok("TOKEN".to_string()));
            env_with_tokens
                .expect_get()
                .with(eq("RS_TOKEN_1_FULL_ACCESS"))
                .return_const(Ok("true".to_string()));
            env_with_tokens
                .expect_get()
                .with(eq("RS_TOKEN_1_READ"))
                .return_const(Ok("bucket1,bucket2".to_string()));
            env_with_tokens
                .expect_get()
                .with(eq("RS_TOKEN_1_WRITE"))
                .return_const(Ok("bucket1".to_string()));
            env_with_tokens
                .expect_get()
                .return_const(Err(VarError::NotPresent));

            let cfg = Cfg::from_env(env_with_tokens);
            let components = cfg.build().await.unwrap();

            let repo = components.token_repo.read().await;
            let token1 = repo.get_token("token1").unwrap().clone();
            assert_eq!(token1.value, "TOKEN");
            assert!(token1.is_provisioned);

            let permissions = token1.permissions.unwrap();
            assert_eq!(permissions.full_access, true);
            assert_eq!(permissions.read, vec!["bucket1", "bucket2"]);
            assert_eq!(permissions.write, vec!["bucket1"]);
        }

        #[rstest]
        #[tokio::test]
        async fn test_tokens_no_value(mut env_with_tokens: MockEnvGetter) {
            env_with_tokens
                .expect_get()
                .with(eq("RS_TOKEN_1_VALUE"))
                .return_const(Err(VarError::NotPresent));
            env_with_tokens
                .expect_get()
                .return_const(Err(VarError::NotPresent));

            let cfg = Cfg::from_env(env_with_tokens);
            let components = cfg.build().await.unwrap();

            let repo = components.token_repo.read().await;
            let err = repo.get_token("token1").err().unwrap();
            assert_eq!(err, ReductError::not_found("Token 'token1' doesn't exist"));
        }

        #[rstest]
        #[tokio::test]
        async fn test_replications(mut env_with_replications: MockEnvGetter) {
            env_with_replications
                .expect_get()
                .with(eq("RS_BUCKET_1_NAME"))
                .return_const(Ok("bucket1".to_string()));

            env_with_replications
                .expect_get()
                .with(eq("RS_REPLICATION_1_NAME"))
                .return_const(Ok("replication1".to_string()));
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
                .with(eq("RS_REPLICATION_1_DST_TOKEN"))
                .return_const(Ok("TOKEN".to_string()));
            env_with_replications
                .expect_get()
                .with(eq("RS_REPLICATION_1_ENTRIES"))
                .return_const(Ok("entry1,entry2".to_string()));

            env_with_replications
                .expect_get()
                .with(eq("RS_REPLICATION_1_EACH_N"))
                .return_const(Ok("10".to_string()));
            env_with_replications
                .expect_get()
                .with(eq("RS_REPLICATION_1_EACH_S"))
                .return_const(Ok("0.5".to_string()));

            env_with_replications
                .expect_get()
                .return_const(Err(VarError::NotPresent));

            let cfg = Cfg::from_env(env_with_replications);
            let components = cfg.build().await.unwrap();

            let repo = components.replication_repo.read().await;
            let replication = repo.get_replication("replication1").unwrap();
            assert_eq!(replication.settings().src_bucket, "bucket1");
            assert_eq!(replication.settings().dst_bucket, "bucket2");
            assert_eq!(replication.settings().dst_host, "http://localhost/");
            assert_eq!(replication.settings().dst_token, "TOKEN");
            assert_eq!(replication.settings().entries, vec!["entry1", "entry2"]);
            assert_eq!(
                replication.settings().include,
                Labels::from_iter(vec![("key1".to_string(), "value1".to_string())])
            );
            assert_eq!(
                replication.settings().exclude,
                Labels::from_iter(vec![("key2".to_string(), "value2".to_string())])
            );
            assert_eq!(replication.settings().each_n, Some(10));
            assert_eq!(replication.settings().each_s, Some(0.5));
            assert!(replication.is_provisioned());
        }

        #[rstest]
        #[tokio::test]
        async fn test_override_replication(mut env_with_replications: MockEnvGetter) {
            env_with_replications
                .expect_get()
                .with(eq("RS_BUCKET_1_NAME"))
                .return_const(Ok("bucket1".to_string()));

            env_with_replications
                .expect_get()
                .with(eq("RS_REPLICATION_1_NAME"))
                .return_const(Ok("replication1".to_string()));
            env_with_replications
                .expect_get()
                .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
                .return_const(Ok("bucket1".to_string()));
            env_with_replications
                .expect_get()
                .with(eq("RS_REPLICATION_1_DST_BUCKET"))
                .return_const(Ok("bucket2".to_string()));
        }

        #[rstest]
        #[tokio::test]
        async fn test_replications_needs_dst_bucket(mut env_with_replications: MockEnvGetter) {
            env_with_replications
                .expect_get()
                .with(eq("RS_REPLICATION_1_NAME"))
                .return_const(Ok("replication1".to_string()));

            env_with_replications
                .expect_get()
                .with(eq("RS_REPLICATION_1_SRC_BUCKET"))
                .return_const(Ok("bucket1".to_string()));
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

            let cfg = Cfg::from_env(env_with_replications);
            let components = cfg.build().await.unwrap();

            let repo = components.replication_repo.read().await;
            assert_eq!(repo.replications().await.len(), 0);
        }

        #[rstest]
        #[tokio::test]
        async fn test_replications_needs_dst_host(mut env_with_replications: MockEnvGetter) {
            env_with_replications
                .expect_get()
                .with(eq("RS_REPLICATION_1_NAME"))
                .return_const(Ok("replication1".to_string()));

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
                .return_const(Err(VarError::NotPresent));

            env_with_replications
                .expect_get()
                .return_const(Err(VarError::NotPresent));

            let cfg = Cfg::from_env(env_with_replications);
            let components = cfg.build().await.unwrap();

            let repo = components.replication_repo.read().await;
            assert_eq!(repo.replications().await.len(), 0);
        }
    }
}
