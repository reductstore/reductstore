// Copyright 2023-2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use std::{env, fs};
use std::path::Path;
use std::time::SystemTime;
use reqwest::blocking::{get, Client};
use reqwest::header::HeaderMap;
use reqwest::StatusCode;

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
    download_web_console();

    // #[cfg(feature = "extensions")]
    // download_extensions();

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


pub(crate) fn download_web_console() {
    const WEB_CONSOLE_VERSION: &str = "v1.10.0";
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


pub(crate) fn download_extensions() {
    let access_token = env::var("GITHUB_TOKEN").unwrap_or_default();
    if access_token.is_empty() {
        panic!("GITHUB_TOKEN is not set, disable the extensions feature");
    }

    let mut headers = HeaderMap::new();
    headers.insert(
        "Authorization",
        format!("Bearer {}", access_token).parse().unwrap(),
    );
    // headers.insert("Accept", "application/vnd.github.v3+json".parse().unwrap());
    // headers.insert("X-GitHub-Api-Version", "2022-11-28".parse().unwrap());

    println!("{:?}", headers);

    let extensions: Vec<(&str, &str)> = vec![("select-ext", "v0.1.0")];
    let target = env::var("TARGET").unwrap();
    let out_dir = env::var("OUT_DIR").unwrap();

    for (name, version) in extensions {
        let ext_path = &format!("{}/{}.zip", out_dir.clone(), name);
        if Path::exists(Path::new(ext_path)) {
            continue;
        }

        println!("Downloading {}...", name);
        let client = Client::builder().user_agent("ReductStore").build().unwrap();
        let mut resp = client
            .get(format!(
                "https://api.github.com/repos/reductstore/{}/releases/tags/{}",
                name, version
            ))
            .headers(headers.clone())
            .send()
            .expect(&format!("Failed to download: {}", name));

        if resp.status() != StatusCode::OK {
            panic!(
                "Failed to fetch release information {}: {}",
                resp.url(),
                resp.status()
            );
        }

        let download_url = resp
            .json::<serde_json::Value>()
            .expect(&format!("Failed to parse JSON response for {}", name))
            .get("assets")
            .expect(&format!("Failed to get assets for {}", name))
            .as_array()
            .expect(&format!("Failed to parse assets for {}", name))
            .iter()
            .find(|asset| {
                asset
                    .get("name")
                    .expect(&format!("Failed to get name for {}", name))
                    .as_str()
                    .unwrap()
                    == format!("{}.zip", target)
            })
            .expect(&format!("Failed to find asset for {}", name))
            .get("url")
            .expect(&format!("Failed to get URL for {}", name))
            .to_string();

        resp = client
            .get(&download_url)
            .headers(headers.clone())
            .send()
            .expect(&format!("Failed to download: {} ", download_url));
        if resp.status() != StatusCode::OK {
            panic!("Failed to download {}: {}", name, resp.status());
        }

        println!("Writing {}.zip...", name);

        fs::write(ext_path, resp.bytes().unwrap())
            .expect(format!("Failed to write {}.zip", name).as_str());
    }
}
