// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use axum::http::{HeaderMap, Request};
use std::sync::{Arc, RwLock};

use crate::auth::policy::Policy;
use crate::core::status::HttpError;
use crate::http_frontend::HttpServerComponents;
use axum::middleware::Next;
use axum::response::IntoResponse;
use log::{debug, error};

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

pub async fn print_statuses<B>(
    request: Request<B>,
    next: Next<B>,
) -> Result<impl IntoResponse, HttpError> {
    let msg = format!(
        "{} {}",
        request.method(),
        request.uri().path_and_query().unwrap()
    );

    let response = next.run(request).await;
    if response.status().as_u16() >= 500 {
        error!("{} [{}]", msg, response.status());
    } else {
        debug!("{} [{}]", msg, response.status());
    }

    Ok(response)
}

pub fn check_permissions<P>(
    components: Arc<RwLock<HttpServerComponents>>,
    headers: HeaderMap,
    policy: P,
) -> Result<(), HttpError>
where
    P: Policy,
{
    let components = components.read().unwrap();
    components.auth.check(
        headers
            .get("Authorization")
            .map(|header| header.to_str().unwrap()),
        &components.token_repo,
        policy,
    )?;
    Ok(())
}
