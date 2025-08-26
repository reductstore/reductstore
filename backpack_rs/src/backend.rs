// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
mod fs;

pub trait Backend {
    fn path(&self) -> &PathBuf;
}

pub type BoxedBackend = Box<dyn Backend + Send + Sync>;

pub(crate) use fs::FileSystemBackend;
use std::path::PathBuf;
