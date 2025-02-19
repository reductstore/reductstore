// Copyright 2025 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use derive_builder::Builder;

#[derive(Debug, Default, Builder)]
#[builder(setter(into))]
pub struct PluginInfo {
    name: String,
    version: String,
    description: String,
}

pub trait Plugin {
    fn info(&self) -> PluginInfo;
}
