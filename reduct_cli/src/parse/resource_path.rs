// Copyright 2023-2024 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::builder::TypedValueParser;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Arg, Command, Error};
use std::ffi::OsStr;

#[derive(Clone)]
pub(crate) struct ResourcePathParser {}

impl TypedValueParser for ResourcePathParser {
    type Value = (String, String);

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        let value = value.to_string_lossy().to_string();
        let is_folder = [".", "/", ".."].iter().any(|s| value.starts_with(s));
        if is_folder {
            return Ok((value, "".to_string()));
        }

        if let Some((alias_or_url, resource_name)) = value.rsplit_once('/') {
            Ok((alias_or_url.to_string(), resource_name.to_string()))
        } else {
            let mut err = Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
            err.insert(
                ContextKind::InvalidArg,
                ContextValue::String(arg.unwrap().to_string()),
            );
            err.insert(ContextKind::InvalidValue, ContextValue::String(value));
            Err(err)
        }
    }
}

impl ResourcePathParser {
    pub fn new() -> Self {
        Self {}
    }
}
