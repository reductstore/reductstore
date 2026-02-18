// Copyright 2026 ReductSoftware UG

use crate::backend::BackendType;
use std::path::PathBuf;
use std::time::Duration;

/// Remote storage settings consumed by the backend layer.
#[derive(Clone, Debug, PartialEq, Default)]
pub struct RemoteStorageConfig {
    pub backend_type: BackendType,
    pub bucket: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub cache_path: Option<PathBuf>,
    pub cache_size: u64,
    pub sync_interval: Duration,
    pub default_storage_class: Option<String>,
}
