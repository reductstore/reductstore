// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _build = cxx_build::bridge("src/lib.rs");

    println!("cargo:rerun-if-changed=src/lib.rs");

    println!("Current directory: {:?}", std::env::current_dir());

    prost_build::Config::new()
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".", "#[serde(default)]")
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .compile_protos(&["src/proto/auth.proto"], &["src/protos/"])
        .expect("Failed to compile protos");

    Ok(())
}
