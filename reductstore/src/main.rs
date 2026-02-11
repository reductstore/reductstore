// Copyright 2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use reductstore::{cfg::CoreExtCfgParser, launcher::launch_server};

#[tokio::main]
async fn main() {
    let ext_cfg_pareser = CoreExtCfgParser;
    launch_server(ext_cfg_pareser).await;
}
