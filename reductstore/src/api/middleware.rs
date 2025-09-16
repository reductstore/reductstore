// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use axum::body::Body;
use axum::http::{HeaderMap, Request};

use axum::middleware::Next;
use axum::response::IntoResponse;
use log::{debug, error};
use reduct_base::error::ErrorCode;

use crate::api::{Components, HttpError};
use crate::auth::policy::Policy;

pub(super) async fn default_headers(
    request: Request<Body>,
    next: Next,
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

pub async fn print_statuses(
    request: Request<Body>,
    next: Next,
) -> Result<impl IntoResponse, HttpError> {
    if request.uri().path_and_query().is_none() {
        return Err(HttpError::new(
            ErrorCode::BadRequest,
            "Failed to get path and query",
        ));
    }

    let path_and_query = request.uri().path_and_query().unwrap();

    let msg = format!("{} {}", request.method(), path_and_query);

    let response = next.run(request).await;
    let err_msg = match response.headers().get("x-reduct-error") {
        Some(msg) => msg.to_str().unwrap_or("Failed to get error message"),
        None => "",
    };

    if response.status().as_u16() >= 500 {
        error!("{} [{}] {}", msg, response.status(), err_msg);
    } else {
        debug!("{} [{}] {}", msg, response.status(), err_msg);
    }

    Ok(response)
}

pub(crate) async fn check_permissions<P>(
    components: &Components,
    headers: &HeaderMap,
    policy: P,
) -> Result<(), HttpError>
where
    P: Policy,
{
    components.auth.check(
        headers
            .get("Authorization")
            .map(|header| header.to_str().unwrap_or("")),
        components.token_repo.read().await.as_ref(),
        policy,
    )?;
    Ok(())
}
