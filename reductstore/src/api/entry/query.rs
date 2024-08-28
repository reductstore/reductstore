// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

use crate::api::entry::QueryInfoAxum;
use crate::api::middleware::check_permissions;
use crate::api::{Components, HttpError};
use crate::auth::policy::ReadAccessPolicy;
use crate::storage::query::base::QueryOptions;

use axum::extract::{Path, Query, State};
use axum_extra::headers::HeaderMap;
use reduct_base::error::ErrorCode;
use reduct_base::msg::entry_api::{EntryInfo, QueryInfo};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

// GET /:bucket/:entry/q?start=<number>&stop=<number>&continue=<number>&exclude-<label>=<value>&include-<label>=<value>&ttl=<number>
pub(crate) async fn query(
    State(components): State<Arc<Components>>,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<QueryInfoAxum, HttpError> {
    let bucket_name = path.get("bucket_name").unwrap();
    let entry_name = path.get("entry_name").unwrap();

    check_permissions(
        &components,
        headers,
        ReadAccessPolicy {
            bucket: bucket_name.clone(),
        },
    )
    .await?;

    let entry_info = {
        let storage = components.storage.read().await;
        let bucket = storage.get_bucket(bucket_name)?;
        bucket.get_entry(entry_name)?.info().await?
    };

    // system parameters
    let continuous = parse_continuous_flag(&params)?;
    let ttl = parse_ttl(&params)?;

    // filter parameters in order of priority
    let (start, stop) = parse_time_range(&params, entry_info)?;
    let (include, exclude) = parse_include_exclude_filters(&params);
    let each_s = parse_each_s(&params)?;
    let each_n = parse_each_n(&params)?;
    let limit = parse_limit(params)?;

    let mut storage = components.storage.write().await;
    let bucket = storage.get_bucket_mut(bucket_name)?;
    let entry = bucket.get_or_create_entry(entry_name)?;
    let id = entry.query(
        start,
        stop,
        QueryOptions {
            continuous,
            include,
            exclude,
            ttl,
            each_s,
            each_n,
            limit,
        },
    )?;

    Ok(QueryInfoAxum::from(QueryInfo { id }))
}

fn parse_ttl(params: &HashMap<String, String>) -> Result<Duration, HttpError> {
    let ttl = match params.get("ttl") {
        Some(ttl) => ttl.parse::<u64>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'ttl' must be in seconds as an unsigned integer",
            )
        })?,
        None => QueryOptions::default().ttl.as_secs(),
    };
    Ok(Duration::from_secs(ttl))
}

fn parse_continuous_flag(params: &HashMap<String, String>) -> Result<bool, HttpError> {
    let continuous = match params.get("continuous") {
        Some(continue_) => continue_.parse::<bool>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'continue' must be a bool value",
            )
        })?,
        None => false,
    };
    Ok(continuous)
}

fn parse_limit(params: HashMap<String, String>) -> Result<Option<u64>, HttpError> {
    let limit = match params.get("limit") {
        Some(limit) => Some(limit.parse::<u64>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'limit' must unsigned integer",
            )
        })?),
        None => None,
    };
    Ok(limit)
}

fn parse_each_n(params: &HashMap<String, String>) -> Result<Option<u64>, HttpError> {
    let each_n = match params.get("each_n") {
        Some(each_n) => {
            let value = each_n.parse::<u64>().map_err(|_| {
                HttpError::new(
                    ErrorCode::UnprocessableEntity,
                    "'each_n' must unsigned integer",
                )
            })?;
            if value == 0 {
                return Err(HttpError::new(
                    ErrorCode::UnprocessableEntity,
                    "'each_n' must be greater than 0",
                ));
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
            let value = each_s.parse::<f64>().map_err(|_| {
                HttpError::new(
                    ErrorCode::UnprocessableEntity,
                    "'each_s' must be a float value",
                )
            })?;
            if value <= 0.0 {
                return Err(HttpError::new(
                    ErrorCode::UnprocessableEntity,
                    "Time must be greater than 0 seconds",
                ));
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

fn parse_time_range(
    params: &HashMap<String, String>,
    entry_info: EntryInfo,
) -> Result<(u64, u64), HttpError> {
    let start = match params.get("start") {
        Some(start) => start.parse::<u64>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'start' must be an unix timestamp in microseconds",
            )
        })?,
        None => entry_info.oldest_record,
    };

    let stop = match params.get("stop") {
        Some(stop) => stop.parse::<u64>().map_err(|_| {
            HttpError::new(
                ErrorCode::UnprocessableEntity,
                "'stop' must be an unix timestamp in microseconds",
            )
        })?,
        None => entry_info.latest_record + 1,
    };

    Ok((start, stop))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{components, headers, path_to_entry_1};
    use rstest::*;

    mod parse_ttl {
        use super::*;

        #[rstest]
        fn test_ok() {
            let params = HashMap::from_iter(vec![("ttl".to_string(), "10".to_string())]);
            let ttl = parse_ttl(&params).unwrap();
            assert_eq!(ttl, Duration::from_secs(10));
        }

        #[rstest]
        fn test_default() {
            let params = HashMap::new();
            let ttl = parse_ttl(&params).unwrap();
            assert_eq!(ttl, Duration::from_secs(60));
        }

        #[rstest]
        fn test_bad() {
            let params = HashMap::from_iter(vec![("ttl".to_string(), "a".to_string())]);
            let result = parse_ttl(&params);
            assert_eq!(
                result.err().unwrap().0.to_string(),
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
            assert_eq!(continuous, true);
        }

        #[rstest]
        fn test_default() {
            let params = HashMap::new();
            let continuous = parse_continuous_flag(&params).unwrap();
            assert_eq!(continuous, false);
        }

        #[rstest]
        fn test_bad() {
            let params = HashMap::from_iter(vec![("continuous".to_string(), "a".to_string())]);
            let result = parse_continuous_flag(&params);
            assert_eq!(
                result.err().unwrap().0.to_string(),
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
            let entry_info = EntryInfo {
                oldest_record: 0,
                latest_record: 10,
                ..EntryInfo::default()
            };
            let (start, stop) = parse_time_range(&params, entry_info).unwrap();
            assert_eq!(start, 0);
            assert_eq!(stop, 10);
        }

        #[rstest]
        fn test_default() {
            let params = HashMap::new();
            let entry_info = EntryInfo {
                oldest_record: 0,
                latest_record: 10,
                ..EntryInfo::default()
            };
            let (start, stop) = parse_time_range(&params, entry_info).unwrap();
            assert_eq!(start, 0);
            assert_eq!(stop, 11);
        }

        #[rstest]
        fn test_start_bad() {
            let params = HashMap::from_iter(vec![("start".to_string(), "a".to_string())]);
            let entry_info = EntryInfo::default();
            let result = parse_time_range(&params, entry_info);
            assert_eq!(
                result.err().unwrap().0.to_string(),
                "[UnprocessableEntity] 'start' must be an unix timestamp in microseconds"
            );
        }

        #[rstest]
        fn test_stop_bad() {
            let params = HashMap::from_iter(vec![("stop".to_string(), "a".to_string())]);
            let entry_info = EntryInfo::default();
            let result = parse_time_range(&params, entry_info);
            assert_eq!(
                result.err().unwrap().0.to_string(),
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
                result.err().unwrap().0.to_string(),
                "[UnprocessableEntity] 'each_s' must be a float value"
            );
        }

        #[rstest]
        fn test_zero() {
            let params = HashMap::from_iter(vec![("each_s".to_string(), "0".to_string())]);
            let result = parse_each_s(&params);
            assert_eq!(
                result.err().unwrap().0.to_string(),
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
                result.err().unwrap().0.to_string(),
                "[UnprocessableEntity] 'each_n' must unsigned integer"
            );
        }

        #[rstest]
        fn test_zero() {
            let params = HashMap::from_iter(vec![("each_n".to_string(), "0".to_string())]);
            let result = parse_each_n(&params);
            assert_eq!(
                result.err().unwrap().0.to_string(),
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
                result.err().unwrap().0.to_string(),
                "[UnprocessableEntity] 'limit' must unsigned integer"
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_limited_query(
        #[future] components: Arc<Components>,
        path_to_entry_1: Path<HashMap<String, String>>,
        headers: HeaderMap,
    ) {
        let components = components.await;
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "1".to_string());

        let result = query(
            State(Arc::clone(&components)),
            path_to_entry_1,
            Query(params),
            headers,
        )
        .await;

        let query: QueryInfo = result.unwrap().into();

        let mut storage = components.storage.write().await;
        let entry = storage
            .get_bucket_mut("bucket-1")
            .unwrap()
            .get_entry_mut("entry-1")
            .unwrap();

        let rx = entry.get_query_receiver(query.id).await.unwrap();
        assert!(rx.recv().await.unwrap().unwrap().last());

        assert_eq!(
            rx.recv().await.unwrap().err().unwrap().status,
            ErrorCode::NoContent
        );
    }
}
