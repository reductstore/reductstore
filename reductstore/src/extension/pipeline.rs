// Copyright 2025 ReductSoftware UG
// Licensed under the Business Source License 1.1

use reduct_base::error::ReductError;
use reduct_base::extension::{
    ExtensionConfig, ExtensionInfo, IoExtension, IoReadExtension, IoWriteExtension,
};
use std::path::PathBuf;

trait ExtPipeline: IoReadExtension + IoWriteExtension {
    fn init(
        &self,
        extension_name: &str,
        position: u32,
        config: ExtensionConfig,
    ) -> Result<(), ReductError>;

    fn enable(&self, extension_name: &str) -> Result<(), ReductError>;

    fn disable(&self, extension_name: &str) -> Result<(), ReductError>;
}

pub fn load_pipeline(path: PathBuf) -> Result<Box<dyn ExtPipeline>, ReductError> {
    todo!("load_pipeline")
}
