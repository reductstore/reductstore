// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::path::PathBuf;

pub(crate) struct FileSystemBackend {
    path: PathBuf,
}

impl FileSystemBackend {
    pub fn new(path: PathBuf) -> Self {
        FileSystemBackend { path }
    }
}

impl super::Backend for FileSystemBackend {
    fn path(&self) -> &PathBuf {
        &self.path
    }
}
