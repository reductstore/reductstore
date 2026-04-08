// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use reductstore::{cfg::CoreExtCfgParser, launcher::launch_server};

#[tokio::main]
async fn main() {
    let ext_cfg_parser = CoreExtCfgParser;
    launch_server(ext_cfg_parser).await;
}
