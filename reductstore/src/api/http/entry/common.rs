// Copyright 2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::http::HttpError;
use axum::http::{HeaderMap, HeaderName, HeaderValue};
use reduct_base::error::{ErrorCode, ReductError};
use reduct_base::msg::entry_api::{QueryEntry, QueryType};
use reduct_base::unprocessable_entity;
use std::collections::HashMap;
use std::str::FromStr;

pub(crate) fn parse_content_length_from_header(headers: &HeaderMap) -> Result<u64, HttpError> {
    let content_size = headers
        .get("content-length")
        .ok_or(unprocessable_entity!("content-length header is required"))?
        .to_str()
        .map_err(|_| unprocessable_entity!("content-length header must be a string",))?
        .parse::<u64>()
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

pub(super) fn parse_query_params(
    params: HashMap<String, String>,
    only_metadata: bool,
) -> Result<QueryEntry, HttpError> {
    let continuous = parse_continuous_flag(&params)?;
    let ttl = parse_ttl(&params)?;

    // filter parameters in order of priority
    let (start, stop) = parse_time_range(&params)?;
    let (include, exclude) = parse_include_exclude_filters(&params);
    let each_s = parse_each_s(&params)?;
    let each_n = parse_each_n(&params)?;
    let limit = parse_limit(params)?;

    Ok(QueryEntry {
        query_type: QueryType::Query,
        entries: None,
        start,
        stop,
        include: Some(include),
        exclude: Some(exclude),
        each_s,
        each_n,
        limit,
        continuous,
        ttl,
        only_metadata: Some(only_metadata),
        when: None,
        strict: None,
        ext: None,
    })
}

pub(super) fn parse_time_range(
    params: &HashMap<String, String>,
) -> Result<(Option<u64>, Option<u64>), HttpError> {
    let start = match params.get("start") {
        Some(start) => {
            let val = start.parse::<u64>().map_err(|_| {
                unprocessable_entity!("'start' must be an unix timestamp in microseconds",)
            })?;
            Some(val)
        }
        None => None,
    };

    let stop = match params.get("stop") {
        Some(stop) => {
            let val = stop.parse::<u64>().map_err(|_| {
                unprocessable_entity!("'stop' must be an unix timestamp in microseconds",)
            })?;
            Some(val)
        }
        None => None,
    };

    Ok((start, stop))
}

fn parse_ttl(params: &HashMap<String, String>) -> Result<Option<u64>, HttpError> {
    let ttl = match params.get("ttl") {
        Some(ttl) => {
            let val = ttl.parse::<u64>().map_err(|_| {
                unprocessable_entity!("'ttl' must be in seconds as an unsigned integer",)
            })?;
            Some(val)
        }
        None => None,
    };
    Ok(ttl)
}

fn parse_continuous_flag(params: &HashMap<String, String>) -> Result<Option<bool>, HttpError> {
    let continuous = match params.get("continuous") {
        Some(continue_) => {
            let val = continue_
                .parse::<bool>()
                .map_err(|_| unprocessable_entity!("'continue' must be a bool value"))?;
            Some(val)
        }
        None => None,
    };
    Ok(continuous)
}

fn parse_limit(params: HashMap<String, String>) -> Result<Option<u64>, HttpError> {
    let limit = match params.get("limit") {
        Some(limit) => Some(
            limit
                .parse::<u64>()
                .map_err(|_| unprocessable_entity!("'limit' must unsigned integer",))?,
        ),
        None => None,
    };
    Ok(limit)
}

fn parse_each_n(params: &HashMap<String, String>) -> Result<Option<u64>, HttpError> {
    let each_n = match params.get("each_n") {
        Some(each_n) => {
            let value = each_n
                .parse::<u64>()
                .map_err(|_| unprocessable_entity!("'each_n' must unsigned integer",))?;
            if value == 0 {
                return Err(unprocessable_entity!("'each_n' must be greater than 0",).into());
            }
            Some(value)
        }
        None => None,
    };
    Ok(each_n)
}

fn parse_each_s(params: &HashMap<String, String>) -> Result<Option<f64>, HttpError> {
    let each_s = match params.get("each_s") {
        Some(each_s) => {
            let value = each_s
                .parse::<f64>()
                .map_err(|_| unprocessable_entity!("'each_s' must be a float value",))?;

            if value <= 0.0 {
                return Err(unprocessable_entity!("Time must be greater than 0 seconds",).into());
            }
            Some(value)
        }
        None => None,
    };
    Ok(each_s)
}

fn parse_include_exclude_filters(
    params: &HashMap<String, String>,
) -> (HashMap<String, String>, HashMap<String, String>) {
    let mut include = HashMap::new();
    let mut exclude = HashMap::new();

    for (k, v) in params.iter() {
        if k.starts_with("include-") {
            include.insert(k[8..].to_string(), v.to_string());
        } else if k.starts_with("exclude-") {
            exclude.insert(k[8..].to_string(), v.to_string());
        }
    }
    (include, exclude)
}

pub(super) fn check_and_extract_ts_or_query_id(
    params: HashMap<String, String>,
    last_record: u64,
) -> Result<(Option<u64>, Option<u64>), HttpError> {
    let ts = match params.get("ts") {
        Some(ts) => Some(ts.parse::<u64>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'ts' must be an unix timestamp in microseconds",
            )
        })?),
        None => None,
    };

    let query_id = match params.get("q") {
        Some(query) => Some(query.parse::<u64>().map_err(|_| {
            HttpError::new(ErrorCode::UnprocessableEntity, "'query' must be a number")
        })?),
        None => None,
    };

    let ts = if ts.is_none() && query_id.is_none() {
        Some(last_record)
    } else {
        ts
    };
    Ok((query_id, ts))
}

#[cfg(test)]
mod tests {
    use super::*;

    use rstest::rstest;
    use std::collections::HashMap;

    mod parse_ttl {
        use super::*;

        #[rstest]
        fn test_ok() {
            let params = HashMap::from_iter(vec![("ttl".to_string(), "10".to_string())]);
            let ttl = parse_ttl(&params).unwrap();
            assert_eq!(ttl, Some(10));
        }

        #[rstest]
        fn test_default() {
            let params = HashMap::new();
            let ttl = parse_ttl(&params).unwrap();
            assert_eq!(ttl, None);
        }

        #[rstest]
        fn test_bad() {
            let params = HashMap::from_iter(vec![("ttl".to_string(), "a".to_string())]);
            let result = parse_ttl(&params);
            assert_eq!(
                result.err().unwrap().into_inner().to_string(),
                "[UnprocessableEntity] 'ttl' must be in seconds as an unsigned integer"
            );
        }
    }

    mod parse_continuous_flag {
        use super::*;

        #[rstest]
        fn test_ok() {
            let params = HashMap::from_iter(vec![("continuous".to_string(), "true".to_string())]);
            let continuous = parse_continuous_flag(&params).unwrap();
            assert_eq!(continuous, Some(true));
        }

        #[rstest]
        fn test_default() {
            let params = HashMap::new();
            let continuous = parse_continuous_flag(&params).unwrap();
            assert_eq!(continuous, None);
        }

        #[rstest]
        fn test_bad() {
            let params = HashMap::from_iter(vec![("continuous".to_string(), "a".to_string())]);
            let result = parse_continuous_flag(&params);
            assert_eq!(
                result.err().unwrap().into_inner().to_string(),
                "[UnprocessableEntity] 'continue' must be a bool value"
            );
        }
    }

    mod parse_time_range {
        use super::*;

        #[rstest]
        fn test_ok() {
            let params = HashMap::from_iter(vec![
                ("start".to_string(), "0".to_string()),
                ("stop".to_string(), "10".to_string()),
            ]);
            let (start, stop) = parse_time_range(&params).unwrap();
            assert_eq!(start, Some(0));
            assert_eq!(stop, Some(10));
        }

        #[rstest]
        fn test_default() {
            let params = HashMap::new();
            let (start, stop) = parse_time_range(&params).unwrap();
            assert_eq!(start, None);
            assert_eq!(stop, None);
        }

        #[rstest]
        fn test_start_bad() {
            let params = HashMap::from_iter(vec![("start".to_string(), "a".to_string())]);
            let result = parse_time_range(&params);
            assert_eq!(
                result.err().unwrap().into_inner().to_string(),
                "[UnprocessableEntity] 'start' must be an unix timestamp in microseconds"
            );
        }

        #[rstest]
        fn test_stop_bad() {
            let params = HashMap::from_iter(vec![("stop".to_string(), "a".to_string())]);
            let result = parse_time_range(&params);
            assert_eq!(
                result.err().unwrap().into_inner().to_string(),
                "[UnprocessableEntity] 'stop' must be an unix timestamp in microseconds"
            );
        }
    }

    mod parse_include_exclude_filters {
        use super::*;

        #[rstest]
        fn test_ok() {
            let params = HashMap::from_iter(vec![
                ("include-key".to_string(), "value".to_string()),
                ("exclude-key".to_string(), "value".to_string()),
            ]);
            let (include, exclude) = parse_include_exclude_filters(&params);
            assert_eq!(
                include,
                HashMap::from_iter(vec![("key".to_string(), "value".to_string())])
            );
            assert_eq!(
                exclude,
                HashMap::from_iter(vec![("key".to_string(), "value".to_string())])
            );
        }

        #[rstest]
        fn test_no_filters() {
            let params = HashMap::new();
            let (include, exclude) = parse_include_exclude_filters(&params);
            assert_eq!(include, HashMap::new());
            assert_eq!(exclude, HashMap::new());
        }
    }

    mod parse_each_s {
        use super::*;

        #[rstest]
        fn test_ok() {
            let params = HashMap::from_iter(vec![("each_s".to_string(), "1.0".to_string())]);
            let each_s = parse_each_s(&params).unwrap();
            assert_eq!(each_s, Some(1.0));
        }

        #[rstest]
        fn test_default() {
            let params = HashMap::new();
            let each_s = parse_each_s(&params).unwrap();
            assert_eq!(each_s, None);
        }

        #[rstest]
        fn test_bad() {
            let params = HashMap::from_iter(vec![("each_s".to_string(), "a".to_string())]);
            let result = parse_each_s(&params);
            assert_eq!(
                result.err().unwrap().into_inner().to_string(),
                "[UnprocessableEntity] 'each_s' must be a float value"
            );
        }

        #[rstest]
        fn test_zero() {
            let params = HashMap::from_iter(vec![("each_s".to_string(), "0".to_string())]);
            let result = parse_each_s(&params);
            assert_eq!(
                result.err().unwrap().into_inner().to_string(),
                "[UnprocessableEntity] Time must be greater than 0 seconds"
            );
        }
    }

    mod parse_each_n {
        use super::*;

        #[rstest]
        fn test_ok() {
            let params = HashMap::from_iter(vec![("each_n".to_string(), "1".to_string())]);
            let each_n = parse_each_n(&params).unwrap();
            assert_eq!(each_n, Some(1));
        }

        #[rstest]
        fn test_default() {
            let params = HashMap::new();
            let each_n = parse_each_n(&params).unwrap();
            assert_eq!(each_n, None);
        }

        #[rstest]
        fn test_bad() {
            let params = HashMap::from_iter(vec![("each_n".to_string(), "a".to_string())]);
            let result = parse_each_n(&params);
            assert_eq!(
                result.err().unwrap().into_inner().to_string(),
                "[UnprocessableEntity] 'each_n' must unsigned integer"
            );
        }

        #[rstest]
        fn test_zero() {
            let params = HashMap::from_iter(vec![("each_n".to_string(), "0".to_string())]);
            let result = parse_each_n(&params);
            assert_eq!(
                result.err().unwrap().into_inner().to_string(),
                "[UnprocessableEntity] 'each_n' must be greater than 0"
            );
        }
    }

    mod parse_limit {
        use super::*;

        #[rstest]
        fn test_ok() {
            let params = HashMap::from_iter(vec![("limit".to_string(), "1".to_string())]);
            let limit = parse_limit(params).unwrap();
            assert_eq!(limit, Some(1));
        }

        #[rstest]
        fn test_default() {
            let params = HashMap::new();
            let limit = parse_limit(params).unwrap();
            assert_eq!(limit, None);
        }

        #[rstest]
        fn test_bad() {
            let params = HashMap::from_iter(vec![("limit".to_string(), "a".to_string())]);
            let result = parse_limit(params);
            assert_eq!(
                result.err().unwrap().into_inner().to_string(),
                "[UnprocessableEntity] 'limit' must unsigned integer"
            );
        }
    }
}
