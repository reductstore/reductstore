// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1
//

use crate::api::HttpError;
use crate::api::{Components, StateKeeper};
use axum::extract::State;

use axum::body::Body;
use axum::http::header::{CONTENT_TYPE, LOCATION};
use axum::http::{HeaderValue, Request, StatusCode};
use axum::response::IntoResponse;
use axum_extra::headers::HeaderMap;
use bytes::Bytes;
use log::debug;
use mime_guess::mime;
use reduct_base::error::{ErrorCode, IntEnum};
use std::sync::Arc;

pub(super) async fn redirect_to_index(
    State(components): State<Arc<StateKeeper>>,
) -> impl IntoResponse {
    let components = match components.get_anonymous().await {
        Ok(c) => c,
        Err(err) => {
            return (
                StatusCode::from_u16(err.0.status().int_value() as u16)
                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                HeaderMap::new(),
                err.0.message,
            )
                .into_response();
        }
    };

    let base_path = components.cfg.api_base_path.clone();
    let mut headers = HeaderMap::new();
    headers.insert(LOCATION, format!("{}ui/", base_path).parse().unwrap());
    (StatusCode::FOUND, headers, "".to_string()).into_response()
}

pub(super) async fn show_ui(
    State(keeper): State<Arc<StateKeeper>>,
    request: Request<Body>,
) -> Result<impl IntoResponse, HttpError> {
    let components = keeper.get_anonymous().await?;
    let base_path = components.cfg.api_base_path.clone();

    let path = request.uri().path();
    if !path.starts_with(&format!("{}ui/", base_path)) {
        return Err(HttpError::new(ErrorCode::NotFound, "Not found"));
    }

    let path = path[base_path.len() + 3..].to_string();
    let path = if path.is_empty() {
        "index.html".to_string()
    } else {
        path
    };

    let content = match components.console.read(&path) {
        Ok(content) => Ok(content),
        Err(err) => {
            debug!("Failed to read {}: {}", path, err);
            components.console.read("index.html")
        }
    };

    let mime = mime_guess::from_path(&path)
        .first()
        .unwrap_or(mime::TEXT_HTML)
        .to_string();
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_str(&mime).unwrap());

    let content = if mime == mime::TEXT_HTML.to_string() {
        let content = String::from_utf8(content?.to_vec()).unwrap();
        let content = content.replace("/ui/", &format!("{}ui/", base_path));
        Bytes::from(content)
    } else {
        content?
    };
    Ok((headers, content))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::HttpBody;

    use crate::api::tests::components;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_img_decoding(#[future] components: Arc<Components>) {
        let request = Request::get("/ui/favicon.png").body(Body::empty()).unwrap();
        let response = show_ui(State(components.await), request)
            .await
            .unwrap()
            .into_response();
        assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "image/png");
        assert_eq!(response.body().size_hint().lower(), 592);
    }
}
