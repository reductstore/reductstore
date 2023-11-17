// Copyright 2023 ReductStore
// Licensed under the Business Source License 1.1

use reduct_base::Labels;
use url::Url;

pub(crate) struct ReplicationCfg {
    pub name: String,
    pub src_bucket: String,
    pub remote_bucket: String,
    pub remote_host: Url,
    pub remote_token: String,
    pub entries: Vec<String>,
    pub include: Labels,
    pub exclude: Labels,
}
