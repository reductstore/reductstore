// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use bytesize::ByteSize;
use clap::builder::TypedValueParser;
use clap::error::{ContextKind, ContextValue, ErrorKind};
use clap::{Arg, Command, Error};
use reduct_rs::QuotaType;
use std::ffi::OsStr;

#[derive(Clone)]
pub(crate) struct ByteSizeParser;

impl TypedValueParser for ByteSizeParser {
    type Value = ByteSize;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        let value = value.to_string_lossy();
        let value = value.parse::<ByteSize>().map_err(|_| {
            let mut err = Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
            err.insert(
                ContextKind::InvalidArg,
                ContextValue::String(arg.unwrap().to_string()),
            );
            err.insert(
                ContextKind::InvalidValue,
                ContextValue::String(value.to_string()),
            );
            err
        })?;
        Ok(value)
    }
}

impl ByteSizeParser {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Clone)]
pub(crate) struct QuotaTypeParser;

impl TypedValueParser for QuotaTypeParser {
    type Value = QuotaType;

    fn parse_ref(
        &self,
        cmd: &Command,
        arg: Option<&Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, Error> {
        let value = value.to_string_lossy();
        let value = value.parse::<QuotaType>().map_err(|_| {
            let mut err = Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
            err.insert(
                ContextKind::InvalidArg,
                ContextValue::String(arg.unwrap().to_string()),
            );
            err.insert(
                ContextKind::InvalidValue,
                ContextValue::String(value.to_string()),
            );
            err
        })?;
        Ok(value)
    }
}

impl QuotaTypeParser {
    pub fn new() -> Self {
        Self {}
    }
}

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
