// Copyright 2023 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::collections::HashMap;

pub mod batch;
pub mod error;
pub mod msg;

#[cfg(feature = "io")]
pub mod io;

#[cfg(feature = "ext")]
pub mod ext;
pub mod logger;

pub type Labels = HashMap<String, String>;
