// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use reductstore::launcher::maybe_print_version_and_exit;
use reductstore::{cfg::CoreExtCfgParser, launcher::launch_server};

#[tokio::main]
async fn main() {
    maybe_print_version_and_exit();
    let ext_cfg_parser = CoreExtCfgParser;
    launch_server(ext_cfg_parser).await;
}
