// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::asset::asset_manager::ManageStaticAsset;
use crate::ext::ext_repository::{ExtRepository, ExtensionApi, IoExtMap};
use dlopen2::wrapper::Container;
use log::{error, info};
use reduct_base::error::ReductError;
use reduct_base::ext::ExtSettings;
use reduct_base::internal_server_error;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock as AsyncRwLock;

impl ExtRepository {
    pub(super) fn try_load(
        paths: Vec<PathBuf>,
        embedded_extensions: Vec<Box<dyn ManageStaticAsset + Sync + Send>>,
        settings: ExtSettings,
    ) -> Result<ExtRepository, ReductError> {
        let mut extension_map = IoExtMap::new();

        let query_map = AsyncRwLock::new(HashMap::new());
        let mut ext_wrappers = Vec::new();

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
            embedded_extensions,
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

    #[log_test(rstest)]
    fn test_load_extension(ext_repo: ExtRepository) {
        assert_eq!(ext_repo.extension_map.len(), 1);
        let ext = ext_repo
            .extension_map
            .get("test-ext")
            .unwrap()
            .blocking_read();
        let info = ext.info().clone();
        assert_eq!(
            info,
            IoExtensionInfo::builder()
                .name("test-ext")
                .version("0.2.2")
                .build()
        );
    }

    #[log_test(rstest)]
    fn test_failed_load(ext_settings: ExtSettings) {
        let path = tempdir().unwrap().keep();
        fs::create_dir_all(&path).unwrap();
        fs::write(&path.join("libtest.so"), b"test").unwrap();
        let ext_repo = ExtRepository::try_load(vec![path], vec![], ext_settings).unwrap();
        assert_eq!(ext_repo.extension_map.len(), 0);
    }

    #[log_test(rstest)]
    fn test_failed_open_dir(ext_settings: ExtSettings) {
        let path = PathBuf::from("non_existing_dir");
        let ext_repo = ExtRepository::try_load(vec![path], vec![], ext_settings);
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
        const EXTENSION_VERSION: &str = "0.2.2";

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
        ExtRepository::try_load(vec![ext_path, empty_ext_path], vec![], ext_settings).unwrap()
    }
}
