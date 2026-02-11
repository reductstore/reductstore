// Copyright 2025-2026 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::cfg::io::IoConfig;
use crate::core::sync::AsyncRwLock;
use crate::ext::ext_repository::{ExtRepository, ExtensionApi, IoExtMap};
use dlopen2::wrapper::Container;
use log::{error, info};
use reduct_base::error::ReductError;
use reduct_base::ext::{ExtSettings, IoExtension};
use reduct_base::internal_server_error;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

impl ExtRepository {
    pub(super) fn try_load(
        paths: Vec<PathBuf>,
        static_extensions: Vec<Box<dyn IoExtension + Send + Sync>>,
        settings: ExtSettings,
        io_config: IoConfig,
    ) -> Result<ExtRepository, ReductError> {
        let mut extension_map = IoExtMap::new();

        let query_map = AsyncRwLock::new(HashMap::new());
        let mut ext_wrappers = Vec::new();

        for ext in static_extensions {
            info!("Load static extension: {:?}", ext.info());
            let name = ext.info().name().to_string();
            extension_map.insert(name, Arc::new(AsyncRwLock::new(ext)));
        }

        for path in paths {
            if !path.exists() {
                return Err(internal_server_error!(
                    "Extension directory {:?} does not exist",
                    path
                ));
            }

            for entry in path.read_dir()? {
                let path = entry?.path();
                if path.is_file()
                    && path
                        .extension()
                        .map_or(false, |ext| ext == "so" || ext == "dll" || ext == "dylib")
                {
                    let ext_wrapper = unsafe {
                        match Container::<ExtensionApi>::load(path.clone()) {
                            Ok(wrapper) => wrapper,
                            Err(e) => {
                                error!("Failed to load extension '{:?}': {:?}", path, e);
                                continue;
                            }
                        }
                    };

                    let ext = unsafe { Box::from_raw(ext_wrapper.get_ext(settings.clone())) };

                    info!("Load extension: {:?}", ext.info());

                    let name = ext.info().name().to_string();
                    extension_map.insert(name, Arc::new(AsyncRwLock::new(ext)));
                    ext_wrappers.push(ext_wrapper);
                }
            }
        }

        Ok(ExtRepository {
            extension_map,
            query_map,
            ext_wrappers,
            io_config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reduct_base::ext::IoExtensionInfo;
    use reduct_base::msg::server_api::ServerInfo;
    use reqwest::blocking::get;
    use reqwest::StatusCode;
    use rstest::{fixture, rstest};
    use std::fs;
    use tempfile::tempdir;
    use test_log::test as log_test;

    const EXTENSION_VERSION: &str = "0.2.3";

    #[log_test(rstest)]
    #[tokio::test]
    #[ignore] // requires network and external binary
    async fn test_load_extension(ext_repo: ExtRepository) {
        assert_eq!(ext_repo.extension_map.len(), 1);
        let ext = ext_repo
            .extension_map
            .get("test-ext")
            .unwrap()
            .read()
            .await
            .unwrap();
        let info = ext.info().clone();
        assert_eq!(
            info,
            IoExtensionInfo::builder()
                .name("test-ext")
                .version(EXTENSION_VERSION)
                .build()
        );
    }

    #[log_test(rstest)]
    fn test_failed_load(ext_settings: ExtSettings) {
        let path = tempdir().unwrap().keep();
        fs::create_dir_all(&path).unwrap();
        fs::write(&path.join("libtest.so"), b"test").unwrap();
        let ext_repo =
            ExtRepository::try_load(vec![path], vec![], ext_settings, IoConfig::default()).unwrap();
        assert_eq!(ext_repo.extension_map.len(), 0);
    }

    #[log_test(rstest)]
    fn test_failed_open_dir(ext_settings: ExtSettings) {
        let path = PathBuf::from("non_existing_dir");
        let ext_repo =
            ExtRepository::try_load(vec![path], vec![], ext_settings, IoConfig::default());
        assert_eq!(
            ext_repo.err().unwrap(),
            internal_server_error!("Extension directory \"non_existing_dir\" does not exist")
        );
    }

    #[fixture]
    fn ext_settings() -> ExtSettings {
        ExtSettings::builder()
            .server_info(ServerInfo::default())
            .build()
    }

    #[fixture]
    fn ext_repo(ext_settings: ExtSettings) -> ExtRepository {
        // This is the path to the build directory of the extension from ext_stub crate

        let file_name = if cfg!(target_os = "linux") {
            // This is the path to the build directory of the extension from ext_stub crate
            if cfg!(target_arch = "aarch64") {
                "libtest_ext-aarch64-unknown-linux-gnu.so"
            } else if cfg!(target_arch = "x86_64") {
                "libtest_ext-x86_64-unknown-linux-gnu.so"
            } else {
                panic!("Unsupported architecture")
            }
        } else if cfg!(target_os = "macos") {
            if cfg!(target_arch = "aarch64") {
                "libtest_ext-aarch64-apple-darwin.dylib"
            } else if cfg!(target_arch = "x86_64") {
                "libtest_ext-x86_64-apple-darwin.dylib"
            } else {
                panic!("Unsupported architecture")
            }
        } else if cfg!(target_os = "windows") {
            if cfg!(target_arch = "x86_64") {
                "libtest_ext-x86_64-pc-windows-gnu.dll"
            } else {
                panic!("Unsupported architecture")
            }
        } else {
            panic!("Unsupported platform")
        };

        let ext_path = PathBuf::from(tempdir().unwrap().keep()).join("ext");
        fs::create_dir_all(ext_path.clone()).unwrap();

        let link = format!(
            "https://github.com/reductstore/test-ext/releases/download/v{}/{}",
            EXTENSION_VERSION, file_name
        );

        let mut resp = get(link).expect("Failed to download extension");
        if resp.status() != StatusCode::OK {
            if resp.status() == StatusCode::FOUND {
                resp = get(resp.headers().get("location").unwrap().to_str().unwrap())
                    .expect("Failed to download extension");
            } else {
                panic!("Failed to download extension: {}", resp.status());
            }
        }

        fs::write(ext_path.join(file_name), resp.bytes().unwrap())
            .expect("Failed to write extension");

        let empty_ext_path = tempdir().unwrap().keep();
        ExtRepository::try_load(
            vec![ext_path, empty_ext_path],
            vec![],
            ext_settings,
            IoConfig::default(),
        )
        .unwrap()
    }
}
