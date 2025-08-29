// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::Cfg;
use crate::core::env::{Env, GetEnv};
use crate::license::parse_license;
use crate::storage::storage::Storage;
use bytesize::ByteSize;
use log::{error, info};
use reduct_base::error::ErrorCode;
use reduct_base::msg::bucket_api::BucketSettings;
use std::collections::HashMap;
use std::path::PathBuf;

impl<EnvGetter: GetEnv> Cfg<EnvGetter> {
    pub(in crate::cfg) fn provision_buckets(&self) -> Storage {
        let license = parse_license(self.license_path.clone());
        let data_path = if self.cs_config.backend_type == backpack_rs::BackendType::Filesystem {
            self.data_path.clone()
        } else {
            self.cs_config
                .cache_path
                .clone()
                .expect("Cache path must be set for remote storage")
        };

        let storage = Storage::load(PathBuf::from(data_path), license);
        for (name, settings) in &self.buckets {
            let settings = match storage.create_bucket(&name, settings.clone()) {
                Ok(bucket) => {
                    let bucket = bucket.upgrade().unwrap();
                    bucket.set_provisioned(true);
                    Ok(bucket.settings().clone())
                }
                Err(e) => {
                    if e.status() == ErrorCode::Conflict {
                        let bucket = storage.get_bucket(&name).unwrap().upgrade().unwrap();
                        bucket.set_provisioned(false);
                        bucket.set_settings(settings.clone()).wait().unwrap();
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

    pub(in crate::cfg) fn parse_buckets(
        env: &mut Env<EnvGetter>,
    ) -> HashMap<String, BucketSettings> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::tests::MockEnvGetter;
    use crate::storage::bucket::Bucket;
    use mockall::predicate::eq;
    use reduct_base::error::ReductError;
    use reduct_base::msg::bucket_api::QuotaType::FIFO;
    use reduct_base::not_found;
    use rstest::{fixture, rstest};
    use std::collections::BTreeMap;
    use std::env::VarError;
    use test_log::test as log_test;

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_buckets(mut env_with_buckets: MockEnvGetter) {
        env_with_buckets
            .expect_get()
            .with(eq("RS_BUCKET_1_NAME"))
            .return_const(Ok("bucket1".to_string()));
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
        let components = cfg.build().unwrap();

        let bucket1 = components
            .storage
            .get_bucket("bucket1")
            .unwrap()
            .upgrade_and_unwrap();

        assert!(bucket1.is_provisioned());
        assert_eq!(bucket1.settings().quota_type, Some(FIFO));
        assert_eq!(bucket1.settings().quota_size, Some(1_000_000_000));
        assert_eq!(bucket1.settings().max_block_size, Some(1_000_000));
        assert_eq!(bucket1.settings().max_block_records, Some(1000));
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_buckets_defaults(mut env_with_buckets: MockEnvGetter) {
        env_with_buckets
            .expect_get()
            .with(eq("RS_BUCKET_1_NAME"))
            .return_const(Ok("bucket1".to_string()));

        env_with_buckets
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = Cfg::from_env(env_with_buckets);
        let components = cfg.build().unwrap();
        let bucket1 = components
            .storage
            .get_bucket("bucket1")
            .unwrap()
            .upgrade_and_unwrap();

        assert_eq!(
            bucket1.settings(),
            Bucket::defaults(),
            "use defaults if env vars are not set"
        );
    }

    #[log_test(rstest)]
    #[tokio::test]
    async fn test_buckets_bad_name(mut env_with_buckets: MockEnvGetter) {
        env_with_buckets
            .expect_get()
            .with(eq("RS_BUCKET_1_NAME"))
            .return_const(Ok("$$$$$".to_string()));

        env_with_buckets
            .expect_get()
            .return_const(Err(VarError::NotPresent));

        let cfg = Cfg::from_env(env_with_buckets);
        let components = cfg.build().unwrap();

        assert_eq!(
            components.storage.get_bucket("$$$$$").err().unwrap(),
            not_found!("Bucket '$$$$$' is not found")
        );
    }

    #[fixture]
    fn env_with_buckets() -> MockEnvGetter {
        let tmp = tempfile::tempdir().unwrap();
        let mut mock_getter = MockEnvGetter::new();
        mock_getter
            .expect_get()
            .with(eq("RS_DATA_PATH"))
            .return_const(Ok(tmp.keep().to_str().unwrap().to_string()));
        mock_getter.expect_all().returning(|| {
            let mut map = BTreeMap::new();
            map.insert("RS_BUCKET_1_NAME".to_string(), "bucket1".to_string());
            map
        });

        mock_getter
    }
}
