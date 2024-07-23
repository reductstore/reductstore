// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::HttpError;
use axum::http::HeaderMap;
use reduct_base::error::ErrorCode;
use std::collections::HashMap;

pub(super) fn parse_content_length_from_header(headers: &HeaderMap) -> Result<usize, HttpError> {
    let content_size = headers
        .get("content-length")
        .ok_or(HttpError::new(
            ErrorCode::UnprocessableEntity,
            "content-length header is required",
        ))?
        .to_str()
        .map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "content-length header must be a string",
            )
        })?
        .parse::<usize>()
        .map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "content-length header must be a number",
            )
        })?;
    Ok(content_size)
}

pub(super) fn parse_timestamp_from_query(
    params: &HashMap<String, String>,
) -> Result<u64, HttpError> {
    match params
        .get("ts")
        .ok_or(HttpError::new(
            ErrorCode::UnprocessableEntity,
            "'ts' parameter is required",
        ))?
        .parse::<u64>()
    {
        Ok(ts) => Ok(ts),
        Err(_) => {
            return Err(HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'ts' must be an unix timestamp in microseconds",
            ));
        }
    }
}
