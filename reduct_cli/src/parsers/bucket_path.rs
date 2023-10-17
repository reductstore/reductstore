// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use clap::builder::TypedValueParser;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Arg, Command, Error};
use std::ffi::OsStr;

#[derive(Clone)]
pub(crate) struct BucketPathParser {}

impl TypedValueParser for BucketPathParser {
    type Value = (String, String);

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        if let Some((alias_or_url, bucket_name)) = value.to_string_lossy().rsplit_once('/') {
            Ok((alias_or_url.to_string(), bucket_name.to_string()))
        } else {
            let mut err = Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
            err.insert(
                ContextKind::InvalidArg,
                ContextValue::String(arg.unwrap().to_string()),
            );
            err.insert(
                ContextKind::InvalidValue,
                ContextValue::String(value.to_string_lossy().to_string()),
            );
            Err(err)
        }
    }
}

impl BucketPathParser {
    pub fn new() -> Self {
        Self {}
    }
}
