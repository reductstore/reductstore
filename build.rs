use std::fs;

// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let mut writer = vec![];
    let resp = http_req::request::get(
        "https://github.com/reductstore/web-console/releases/download/v1.2.0/web-console.build.zip",
        &mut writer,
    )
    .expect("Failed to download Web Console");
    if resp.status_code() != 200.into() {
        if resp.status_code() == 302.into() {
            http_req::request::get(&resp.headers().get("location").unwrap(), &mut writer)
                .expect("Failed to download Web Console");
        } else {
            panic!("Failed to download Web Console: {}", resp.reason());
        }
    }
    fs::write("src/asset/console.zip", writer).expect("Failed to write console.zip");

    Ok(())
}
