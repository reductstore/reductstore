// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use dlopen2::wrapper::{Container, WrapperApi};
use log::info;
use reduct_base::error::ReductError;
use reduct_base::ext::{IoExtension, IoExtensionInfo};
use reduct_base::{internal_server_error, not_found};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

type IoExtRef = Arc<RwLock<Box<dyn IoExtension>>>;
type IoExtMap = HashMap<String, IoExtRef>;

#[derive(WrapperApi)]
struct PluginApi {
    get_plugin: extern "C" fn() -> *mut dyn IoExtension,
}

pub struct ExtRepository {
    extension_map: IoExtRef,
}

impl ExtRepository {
    fn load(path: &PathBuf) -> ExtRepository {
        // let plugin_api_wrapper = unsafe {
        //     Container::<PluginApi>::load(
        //         "/home/atimin/Projects/reductstore/test_plugin/target/release/libtest_plugin.so",
        //     )
        //     .expect("Failed to load plugin")
        // };
        // let plugin = unsafe { Box::from_raw(plugin_api_wrapper.get_plugin()) };
        //
        // info!("Plugin: {:?}", plugin.info())
        //
        todo!()
    }

    fn get_extension(&self, name: &str) -> Result<IoExtRef, ReductError> {
        // self.extension_map
        //     .read()
        //     .map_err(|_| internal_server_error!("Failed to read extension map"))?
        //     .get(name)
        //     .cloned()
        //     .ok_or(not_found!("Extension '{}' not found", name))
        todo!()
    }
}
