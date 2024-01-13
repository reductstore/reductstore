use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Copyright 2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct DiagnosticsError {
    pub count: u64,
    pub last_message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct DiagnosticsItem {
    pub ok: u64,
    pub errored: u64,
    pub errors: HashMap<i16, DiagnosticsError>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Diagnostics {
    pub hourly: DiagnosticsItem,
}
