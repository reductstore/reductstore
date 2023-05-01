// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

use crate::core::status::HttpError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use prost::DecodeError;

// mod server_api;
// mod token_api;
// mod http_server;
pub mod http_server;
pub mod server_api;
pub mod token_api;

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        let body = format!("{{\"detail\": \"{}\"}}", self.message.to_string());

        // its often easiest to implement `IntoResponse` by calling other implementations
        let mut resp = (StatusCode::from_u16(self.status as u16).unwrap(), body).into_response();
        resp.headers_mut()
            .insert("content-type", "application/json".parse().unwrap());
        resp.headers_mut()
            .insert("x-reduct-error", self.message.parse().unwrap());
        resp
    }
}

impl From<DecodeError> for HttpError {
    fn from(err: DecodeError) -> Self {
        HttpError::bad_request(&format!("Invalid request: {}", err))
    }
}
