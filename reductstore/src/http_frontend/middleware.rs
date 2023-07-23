// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use axum::http::{HeaderMap, Request};

use axum::middleware::Next;
use axum::response::IntoResponse;
use log::{debug, error};

use crate::auth::policy::Policy;
use crate::http_frontend::{HttpError, HttpServerState};

pub async fn default_headers<B>(
    request: Request<B>,
    next: Next<B>,
) -> Result<impl IntoResponse, HttpError> {
    let mut response = next.run(request).await;
    let version: &str = env!("CARGO_PKG_VERSION");
    response.headers_mut().insert(
        "Server",
        format!("ReductStore {}", version).parse().unwrap(),
    );

    let tokens: Vec<&str> = version.splitn(3, ".").collect();
    response.headers_mut().insert(
        "x-reduct-api",
        format!("{}.{}", tokens[0], tokens[1]).parse().unwrap(),
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
    let err_msg = match response.headers().get("x-reduct-error") {
        Some(msg) => msg.to_str().unwrap(),
        None => "",
    };

    if response.status().as_u16() >= 500 {
        error!("{} [{}] {}", msg, response.status(), err_msg);
    } else {
        debug!("{} [{}] {}", msg, response.status(), err_msg);
    }

    Ok(response)
}

pub async fn check_permissions<P>(
    components: &HttpServerState,
    headers: HeaderMap,
    policy: P,
) -> Result<(), HttpError>
where
    P: Policy,
{
    components.auth.check(
        headers
            .get("Authorization")
            .map(|header| header.to_str().unwrap()),
        components.token_repo.read().await.as_ref(),
        policy,
    )?;
    Ok(())
}
