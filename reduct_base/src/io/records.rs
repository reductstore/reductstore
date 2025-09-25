// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
mod cursor;
mod errored;
mod iter;
mod one_shot;

pub use cursor::CursorRecord;
pub use iter::IterRecord;
pub use one_shot::OneShotRecord;

// only for tests
pub use errored::{ErroredReadRecord, ErroredSeekRecord};
