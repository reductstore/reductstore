// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.
//

use crate::core::status::HttpError;
use crate::http_frontend::HttpServerState;
use axum::extract::State;

use axum::headers::HeaderMap;
use axum::http::header::{CONTENT_TYPE, LOCATION};
use axum::http::{HeaderValue, Request, StatusCode};
use axum::response::IntoResponse;
use bytes::Bytes;
use hyper::Body;
use log::debug;
use mime_guess::mime;
use std::sync::{Arc, RwLock};

pub async fn redirect_to_index(
    State(components): State<Arc<HttpServerState>>,
) -> impl IntoResponse {
    let base_path = components.base_path.clone();
    let mut headers = HeaderMap::new();
    headers.insert(LOCATION, format!("{}ui/", base_path).parse().unwrap());
    (StatusCode::FOUND, headers, Bytes::new()).into_response()
}

pub async fn show_ui(
    State(components): State<Arc<HttpServerState>>,
    request: Request<Body>,
) -> Result<impl IntoResponse, HttpError> {
    let base_path = components.base_path.clone();

    let path = request.uri().path();
    if !path.starts_with(&format!("{}ui/", base_path)) {
        return Err(HttpError::not_found("Not found"));
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

    use crate::http_frontend::tests::components;
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_img_decoding(components: Arc<RwLock<HttpServerState>>) {
        let request = Request::get("/ui/favicon.png").body(Body::empty()).unwrap();
        let response = show_ui(State(components), request)
            .await
            .unwrap()
            .into_response();
        assert_eq!(response.headers().get(CONTENT_TYPE).unwrap(), "image/png");
        assert_eq!(response.body().size_hint().lower(), 7037);
    }
}
