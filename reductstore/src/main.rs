// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use reduct_base::error::ErrorCode;
use reductstore::launcher::maybe_print_version_and_exit;
use reductstore::{cfg::CoreExtCfgParser, launcher::prepare_server};

#[tokio::main]
async fn main() {
    maybe_print_version_and_exit();
    match prepare_server(CoreExtCfgParser).await {
        Ok(server) => server.launch().await,
        Err(error) if error.status == ErrorCode::Interrupt => {}
        Err(error) => panic!("Failed to prepare server: {error}"),
    }
}
