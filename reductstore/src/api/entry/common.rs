// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::HttpError;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use reduct_base::error::ReductError;
use reduct_base::unprocessable_entity;
use std::collections::HashMap;
use std::str::FromStr;

pub(super) fn parse_content_length_from_header(headers: &HeaderMap) -> Result<usize, HttpError> {
    let content_size = headers
        .get("content-length")
        .ok_or(unprocessable_entity!("content-length header is required"))?
        .to_str()
        .map_err(|_| unprocessable_entity!("content-length header must be a string",))?
        .parse::<usize>()
        .map_err(|_| unprocessable_entity!("content-length header must be a number"))?;
    Ok(content_size)
}

pub(super) fn parse_timestamp_from_query(
    params: &HashMap<String, String>,
) -> Result<u64, HttpError> {
    match params
        .get("ts")
        .ok_or(unprocessable_entity!("'ts' parameter is required",))?
        .parse::<u64>()
    {
        Ok(ts) => Ok(ts),
        Err(_) => {
            Err(unprocessable_entity!("'ts' must be an unix timestamp in microseconds",).into())
        }
    }
}

pub(super) fn err_to_batched_header(headers: &mut HeaderMap, time: u64, err: &ReductError) {
    let name = format!("x-reduct-error-{}", time);
    let value = format!("{},{}", err.status(), err.message());
    headers.insert(
        HeaderName::from_str(&name).unwrap(),
        HeaderValue::from_str(&value).unwrap(),
    );
}
