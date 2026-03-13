use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0
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
