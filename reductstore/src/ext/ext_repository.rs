// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use dlopen2::wrapper::{Container, WrapperApi};
use log::info;
use reduct_base::error::ReductError;
use reduct_base::ext::{IoExtension, IoExtensionInfo};
use reduct_base::{internal_server_error, not_found};
use std::collections::hash_map::Values;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

type IoExtRef = Arc<RwLock<Box<dyn IoExtension + Send + Sync>>>;
type IoExtMap = HashMap<String, IoExtRef>;

#[derive(WrapperApi)]
struct PluginApi {
    get_plugin: extern "C" fn() -> *mut (dyn IoExtension + Send + Sync),
}

pub struct ExtRepository {
    extension_map: IoExtMap,
}

impl ExtRepository {
    pub(crate) fn try_load(path: &PathBuf) -> Result<ExtRepository, ReductError> {
        let mut extension_map = IoExtMap::new();
        for entry in path.read_dir()? {
            let path = entry?.path();
            if path.is_file()
                && path
                    .extension()
                    .map_or(false, |ext| ext == "so" || ext == "dll")
            {
                let plugin_api_wrapper = unsafe {
                    Container::<PluginApi>::load(path)
                        .map_err(|e| internal_server_error!("Failed to load plugin: {}", e))?
                };
                let plugin = unsafe { Box::from_raw(plugin_api_wrapper.get_plugin()) };
                info!("Load extension: {:?}", plugin.info());

                let name = plugin.info().name().to_string();
                extension_map.insert(name, Arc::new(RwLock::new(plugin)));
            }
        }

        Ok(ExtRepository { extension_map })
    }

    pub fn get_extension(&self, name: &str) -> Result<IoExtRef, ReductError> {
        self.extension_map
            .get(name)
            .cloned()
            .ok_or(not_found!("Extension '{}' not found", name))
    }

    pub fn extensions(&self) -> Values<'_, String, IoExtRef> {
        self.extension_map.values()
    }
}
