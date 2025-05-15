// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1
#[allow(unused_imports)]
use reqwest::{
    blocking::{get, Client},
    StatusCode, Url,
};
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

    #[cfg(feature = "web-console")]
    download_web_console("v1.10.0");

    #[cfg(feature = "select-ext")]
    download_ext("select-ext", "v0.1.0");

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
fn download_web_console(version: &str) {
    let out_dir = env::var("OUT_DIR").unwrap();
    let console_path = &format!("{}/console-{}.zip", out_dir, version);
    if Path::exists(Path::new(console_path)) {
        return;
    }

    let mut resp = get(format!(
        "https://github.com/reductstore/web-console/releases/download/{}/web-console.build.zip",
        version
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

#[cfg(feature = "select-ext")]
fn download_ext(name: &str, version: &str) {
    let sas_url = env::var("ARTIFACT_SAS_URL").unwrap_or_default();
    if sas_url.is_empty() {
        panic!("ARTIFACT_SAS_URL is not set, disable the extensions feature");
    }

    let sas_url = Url::parse(&sas_url).expect("Failed to parse ARTIFACT_SAS_URL");

    let target = env::var("TARGET").unwrap();
    let out_dir = env::var("OUT_DIR").unwrap();

    let ext_path = &format!("{}/{}.zip", out_dir.clone(), name);
    if Path::exists(Path::new(ext_path)) {
        return;
    }

    println!("Downloading {}...", name);
    let mut ext_url = sas_url
        .join(&format!(
            "/artifacts/{}/{}/{}.zip/{}.zip",
            name, version, target, target
        ))
        .expect("Failed to create URL");
    ext_url.set_query(sas_url.query());

    let client = Client::builder().user_agent("ReductStore").build().unwrap();
    let resp = client
        .get(ext_url)
        .send()
        .expect(format!("Failed to download {}.zip", name).as_str());
    if resp.status() != StatusCode::OK {
        panic!("Failed to download {}: {}", name, resp.status());
    }

    println!("Writing {}.zip...", name);

    fs::write(ext_path, resp.bytes().unwrap())
        .expect(format!("Failed to write {}.zip", name).as_str());
    fs::copy(ext_path, format!("{}/{}", out_dir, ext_path)).expect("Failed to copy extension");
}
