// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
fn main() -> Result<(), Box<dyn std::error::Error>> {

    println!("Current directory: {:?}", std::env::current_dir());

    prost_build::Config::new()
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".reduct.proto.auth", "#[serde(default)]")
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .compile_protos(
            &["src/proto/auth.proto", "src/proto/storage.proto"],
            &["src/protos/"],
        )
        .expect("Failed to compile protos");

    Ok(())
}
