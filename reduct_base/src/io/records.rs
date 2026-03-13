// Copyright 2021-2026 ReductSoftware UG
// Licensed under the Apache License, Version 2.0
mod cursor;
mod errored;
mod iter;
mod one_shot;

pub use cursor::CursorRecord;
pub use iter::IterRecord;
pub use one_shot::OneShotRecord;

// only for tests
pub use errored::{ErroredReadRecord, ErroredSeekRecord};
