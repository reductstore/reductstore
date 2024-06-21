// Copyright 2023 ReductSoftware UG
// Licensed under the Business Source License 1.1
extern crate core;

use reqwest::blocking::get;
use reqwest::StatusCode;
use std::path::Path;
use std::time::SystemTime;
use std::{env, fs};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // build protos
    prost_build::Config::new()
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .type_attribute(".reduct.proto.auth", "#[serde(default)]")
        .extern_path(".google.protobuf.Timestamp", "::prost_wkt_types::Timestamp")
        .compile_protos(
            &[
                "src/proto/auth.proto",
                "src/proto/storage.proto",
                "src/proto/replication.proto",
            ],
            &["src/protos/"],
        )
        .expect("Failed to compile protos");

    download_web_console();

    // get build time and commit
    let build_time = chrono::DateTime::<chrono::Utc>::from(SystemTime::now())
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let commit = match std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
    {
        Ok(output) => String::from_utf8(output.stdout).expect("Failed to get commit"),
        Err(_) => env::var("GIT_COMMIT").unwrap_or("unknown".to_string()),
    };

    println!("cargo:rustc-env=BUILD_TIME={}", build_time);
    println!("cargo:rustc-env=COMMIT={}", commit);
    Ok(())
}

#[cfg(feature = "web-console")]
fn download_web_console() {
    const WEB_CONSOLE_VERSION: &str = "v1.6.1";
    let out_dir = env::var("OUT_DIR").unwrap();
    let console_path = &format!("{}/console-{}.zip", out_dir, WEB_CONSOLE_VERSION);
    if Path::exists(Path::new(console_path)) {
        return;
    }

    let mut resp = get(format!(
        "https://github.com/reductstore/web-console/releases/download/{}/web-console.build.zip",
        WEB_CONSOLE_VERSION
    ))
    .expect("Failed to download Web Console");
    if resp.status() != StatusCode::OK {
        if resp.status() == StatusCode::FOUND {
            resp = get(resp.headers().get("location").unwrap().to_str().unwrap())
                .expect("Failed to download Web Console");
        } else {
            panic!("Failed to download Web Console: {}", resp.status());
        }
    }
    fs::write(console_path, resp.bytes().unwrap()).expect("Failed to write console.zip");
    fs::copy(console_path, format!("{}/console.zip", out_dir)).expect("Failed to copy console.zip");
}

#[cfg(not(feature = "web-console"))]
fn download_web_console() {}
