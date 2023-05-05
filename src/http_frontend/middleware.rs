// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use axum::headers::{ContentType, HeaderMapExt, Server};
use axum::http::{response::Response, Request, StatusCode};
use std::fmt::format;
use std::task::{Context, Poll};

use crate::core::status::HttpError;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::TypedHeader;
use futures_util::future::BoxFuture;
use hyper::Body;
use tower::Service;

pub async fn default_headers<B>(
    request: Request<B>,
    next: Next<B>,
) -> Result<impl IntoResponse, HttpError> {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "Server",
        format!("ReductStore {}", env!("CARGO_PKG_VERSION"))
            .parse()
            .unwrap(),
    );
    Ok(response)
}
