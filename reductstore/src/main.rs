// Copyright 2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use reductstore::{cfg::ExtCfgParser, launcher::launch_server};

struct EmptyExtCfgParser;

impl ExtCfgParser for EmptyExtCfgParser {
    async fn from_env(&self, env: &mut Env<EnvGetter>, version: &str) -> ExtCfg;
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    launch_server().await;
}
