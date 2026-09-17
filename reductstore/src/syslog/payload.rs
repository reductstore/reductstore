// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0

//! Event-specific payload structs for each `$system` event family. Each builds
//! the family's JSON object that [`SystemEvent::to_flat_json`](crate::syslog::SystemEvent::to_flat_json)
//! flattens into the persisted record.

pub(crate) mod audit;
pub(crate) mod lifecycle;
pub(crate) mod log;
pub(crate) mod replication;
pub(crate) mod usage;
