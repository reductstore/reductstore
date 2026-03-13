// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

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
