// Copyright 2023 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::convert::Infallible;
use url::{ParseError, Url};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    details: String,
}

impl Error {
    pub fn new(msg: &str) -> Error {
        Error {
            details: msg.to_string(),
        }
    }
}

impl From<ParseError> for Error {
    fn from(err: ParseError) -> Self {
        Error::new(&err.to_string())
    }
}

impl From<Infallible> for Error {
    fn from(err: Infallible) -> Self {
        Error::new(&err.to_string())
    }
}
