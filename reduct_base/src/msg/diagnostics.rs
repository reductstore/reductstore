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
    /// Replicated payload bytes before wire compression
    #[serde(default)]
    pub data_size: u64,
    /// Payload bytes sent over the wire after compression
    #[serde(default)]
    pub compressed_data_size: u64,
    /// Compression ratio (data_size / compressed_data_size, 1.0 when uncompressed)
    #[serde(default)]
    pub compression_ratio: f64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Diagnostics {
    pub hourly: DiagnosticsItem,
}
