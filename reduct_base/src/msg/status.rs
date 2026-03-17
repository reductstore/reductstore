// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

use serde::{Deserialize, Serialize};

/// Status of a resource such as bucket or entry.
#[derive(Serialize, Deserialize, Default, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceStatus {
    /// Ready for regular operations
    #[default]
    #[serde(rename = "READY")]
    Ready = 0,
    /// Currently being deleted
    #[serde(rename = "DELETING")]
    Deleting = 1,
}
