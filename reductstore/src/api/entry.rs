// Copyright 2023-2024 ReductSoftware UG
// Licensed under the Business Source License 1.1

pub(crate) mod common;
mod read_batched;
mod read_query;
mod read_query_post;
mod read_single;
mod remove_batched;
mod remove_entry;
mod remove_query;
mod remove_query_post;
mod remove_single;
mod rename_entry;
mod update_batched;
mod update_single;
mod write_batched;
mod write_single;

use crate::api::entry::read_batched::read_batched_records;
use crate::api::entry::read_single::read_record;
use crate::api::entry::remove_entry::remove_entry;

use crate::api::entry::write_batched::write_batched_records;
use crate::api::entry::write_single::write_record;
use crate::api::HttpError;
use crate::api::StateKeeper;
use axum::extract::{FromRequest, Path, Query, State};

use axum_extra::headers::HeaderMapExt;

use crate::api::entry::remove_batched::remove_batched_records;
use crate::api::entry::remove_query::remove_query;
use crate::api::entry::remove_single::remove_record;
use crate::api::entry::rename_entry::rename_entry;
use crate::api::entry::update_batched::update_batched_records;
use crate::api::entry::update_single::update_record;

use crate::api::entry::read_query_post::read_query_json;
use crate::api::entry::remove_query_post::remove_query_json;
use crate::api::ErrorCode;
use axum::body::{to_bytes, Body};
use axum::http::{HeaderMap, Request};
use axum::response::IntoResponse;
use axum::routing::{delete, get, head, patch, post, put};
use bytes::Bytes;
use hyper::Response;
use reduct_base::msg::entry_api::{QueryEntry, QueryInfo, QueryType, RemoveQueryInfo, RenameEntry};
use reduct_macros::{IntoResponse, Twin};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) struct MethodExtractor {
    name: String,
}

impl MethodExtractor {
    pub fn name(&self) -> &str {
        &self.name
    }

    #[cfg(test)]
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

impl<S> FromRequest<S> for MethodExtractor
where
    S: Send + Sync + 'static,
{
    type Rejection = HttpError;

    async fn from_request(req: Request<Body>, _: &S) -> Result<Self, Self::Rejection> {
        let method = req.method().to_string();
        Ok(MethodExtractor { name: method })
    }
}

#[derive(IntoResponse, Twin)]
pub(super) struct QueryInfoAxum(QueryInfo);

#[derive(IntoResponse, Twin)]
pub(super) struct RemoveQueryInfoAxum(RemoveQueryInfo);

#[derive(IntoResponse, Twin)]
pub(super) struct QueryEntryAxum(pub QueryEntry);

impl<S> FromRequest<S> for QueryEntryAxum
where
    Bytes: FromRequest<S>,
    S: Send + Sync,
{
    type Rejection = Response<Body>;

    async fn from_request(req: Request<Body>, state: &S) -> Result<Self, Self::Rejection> {
        let body = Bytes::from_request(req, state)
            .await
            .map_err(IntoResponse::into_response)?;

        let query: QueryEntry =
            serde_json::from_slice(&body).map_err(|e| HttpError::from(e).into_response())?;
        Ok(QueryEntryAxum(query))
    }
}

// Workaround for DELETE /:bucket/:entry and DELETE /:bucket/:entry?ts=<number>
async fn remove_entry_router(
    keeper: State<Arc<StateKeeper>>,
    headers: HeaderMap,
    path: Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<(), HttpError> {
    if params.is_empty() {
        remove_entry(keeper, path, headers).await
    } else {
        remove_record(keeper, headers, path, Query(params)).await
    }
}

async fn query_entry_router(
    keeper: State<Arc<StateKeeper>>,
    headers: HeaderMap,
    path: Path<HashMap<String, String>>,
    request: QueryEntryAxum,
) -> Response<Body> {
    let request = request.0;
    match request.query_type {
        QueryType::Query => read_query_json(keeper, path, request, headers)
            .await
            .into_response(),
        QueryType::Remove => remove_query_json(keeper, path, request, headers)
            .await
            .into_response(),
    }
}

fn patch_entry_path(
    path: &HashMap<String, String>,
    entry_name: String,
) -> Path<HashMap<String, String>> {
    let mut path = path.clone();
    path.insert("entry_name".to_string(), entry_name);
    Path(path)
}

fn strip_route_suffix(
    path: &HashMap<String, String>,
    suffix: &str,
) -> Option<Path<HashMap<String, String>>> {
    let entry_name = path.get("entry_name")?;
    let entry_name = entry_name.strip_suffix(suffix)?;
    if entry_name.is_empty() {
        return None;
    }

    Some(patch_entry_path(path, entry_name.to_string()))
}

async fn read_entry_router(
    keeper: State<Arc<StateKeeper>>,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    headers: HeaderMap,
    method: MethodExtractor,
) -> Response<Body> {
    if let Some(path) = strip_route_suffix(&path, "/batch") {
        return read_batched_records(keeper, path, Query(params), headers, method)
            .await
            .into_response();
    }

    if let Some(path) = strip_route_suffix(&path, "/q") {
        return read_query::read_query(keeper, path, Query(params), headers)
            .await
            .into_response();
    }

    read_record(keeper, Path(path), Query(params), headers, method)
        .await
        .into_response()
}

async fn write_entry_router(
    keeper: State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Response<Body> {
    if let Some(path) = strip_route_suffix(&path, "/batch") {
        return write_batched_records(keeper, headers, path, body)
            .await
            .into_response();
    }

    if let Some(path) = strip_route_suffix(&path, "/q") {
        let body = match to_bytes(body, usize::MAX).await {
            Ok(body) => body,
            Err(e) => return HttpError::from(e).into_response(),
        };

        let request: QueryEntry = match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(e) => return HttpError::from(e).into_response(),
        };
        return query_entry_router(keeper, headers, path, QueryEntryAxum(request)).await;
    }

    write_record(keeper, headers, Path(path), Query(params), body)
        .await
        .into_response()
}

async fn update_entry_router(
    keeper: State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Response<Body> {
    if let Some(path) = strip_route_suffix(&path, "/batch") {
        return update_batched_records(keeper, headers, path, body)
            .await
            .into_response();
    }

    update_record(keeper, headers, Path(path), Query(params), body)
        .await
        .into_response()
}

async fn remove_entry_dispatcher(
    keeper: State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    Query(params): Query<HashMap<String, String>>,
    body: Body,
) -> Response<Body> {
    if let Some(path) = strip_route_suffix(&path, "/batch") {
        return remove_batched_records(keeper, headers, path, body)
            .await
            .into_response();
    }

    if let Some(path) = strip_route_suffix(&path, "/q") {
        return remove_query(keeper, path, Query(params), headers)
            .await
            .into_response();
    }

    remove_entry_router(keeper, headers, Path(path), Query(params))
        .await
        .into_response()
}

async fn rename_entry_dispatcher(
    keeper: State<Arc<StateKeeper>>,
    headers: HeaderMap,
    Path(path): Path<HashMap<String, String>>,
    body: Body,
) -> Response<Body> {
    if let Some(path) = strip_route_suffix(&path, "/rename") {
        let body = match to_bytes(body, usize::MAX).await {
            Ok(body) => body,
            Err(e) => return HttpError::from(e).into_response(),
        };

        let request: RenameEntry = match serde_json::from_slice(&body) {
            Ok(request) => request,
            Err(e) => return HttpError::from(e).into_response(),
        };

        return rename_entry(keeper, path, headers, axum::Json(request))
            .await
            .into_response();
    }

    HttpError::new(ErrorCode::NotFound, "Not found").into_response()
}

pub(super) fn create_entry_api_routes() -> axum::Router<Arc<StateKeeper>> {
    axum::Router::new()
        .route("/{bucket_name}/{*entry_name}", post(write_entry_router))
        .route("/{bucket_name}/{*entry_name}", patch(update_entry_router))
        .route("/{bucket_name}/{*entry_name}", get(read_entry_router))
        .route("/{bucket_name}/{*entry_name}", head(read_entry_router))
        .route(
            "/{bucket_name}/{*entry_name}",
            delete(remove_entry_dispatcher),
        )
        .route("/{bucket_name}/{*entry_name}", put(rename_entry_dispatcher))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::tests::{headers, keeper, path_to_entry_1};
    use reduct_base::error::ErrorCode;
    use reduct_base::msg::entry_api::QueryEntry;
    use rstest::rstest;

    mod method_extractor {
        use super::*;

        #[tokio::test]
        async fn test_from_request() {
            let req = Request::builder()
                .method("GET")
                .uri("http://localhost")
                .body(Body::empty())
                .unwrap();
            let extractor = MethodExtractor::from_request(req, &()).await.unwrap();
            assert_eq!(extractor.name(), "GET");
        }

        #[tokio::test]
        async fn test_from_request_post() {
            let req = Request::builder()
                .method("HEAD")
                .uri("http://localhost")
                .body(Body::empty())
                .unwrap();
            let extractor = MethodExtractor::from_request(req, &()).await.unwrap();
            assert_eq!(extractor.name(), "HEAD");
        }
    }

    mod strip_route_suffix {
        use super::*;

        #[test]
        fn test_strip_route_suffix_nested_entry() {
            let path = HashMap::from_iter(vec![
                ("bucket_name".to_string(), "bucket-1".to_string()),
                ("entry_name".to_string(), "x/y/z/batch".to_string()),
            ]);

            let Path(path) = strip_route_suffix(&path, "/batch").unwrap();
            assert_eq!(path.get("entry_name").unwrap(), "x/y/z");
        }

        #[test]
        fn test_strip_route_suffix_returns_none() {
            let path = HashMap::from_iter(vec![
                ("bucket_name".to_string(), "bucket-1".to_string()),
                ("entry_name".to_string(), "x/y/z".to_string()),
            ]);

            assert!(strip_route_suffix(&path, "/batch").is_none());
        }
    }

    mod write_entry_router {
        use super::*;

        #[rstest]
        #[tokio::test]
        async fn accepts_hierarchical_name(
            #[future] keeper: Arc<StateKeeper>,
            mut headers: HeaderMap,
        ) {
            headers.insert("content-length", "0".parse().unwrap());

            let response = write_entry_router(
                State(keeper.await),
                headers,
                Path(HashMap::from_iter(vec![
                    ("bucket_name".to_string(), "bucket-1".to_string()),
                    ("entry_name".to_string(), "entry/a".to_string()),
                ])),
                Query(HashMap::from_iter(vec![(
                    "ts".to_string(),
                    "1".to_string(),
                )])),
                Body::empty(),
            )
            .await;

            assert_eq!(response.status(), 200);
        }
    }

    mod query_entry_axum {
        use super::*;

        #[tokio::test]
        async fn test_from_request() {
            let query = QueryEntry {
                query_type: QueryType::Query,
                ..Default::default()
            };
            let body = serde_json::to_vec(&query).unwrap();
            let req = Request::builder()
                .method("POST")
                .uri("http://localhost")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            let axum_query = QueryEntryAxum::from_request(req, &()).await.unwrap();
            assert_eq!(axum_query.0, query);
        }

        #[tokio::test]
        async fn test_from_request_unprocessable_entity() {
            let req = Request::builder()
                .method("POST")
                .uri("http://localhost")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap();
            let resp = QueryEntryAxum::from_request(req, &()).await.err().unwrap();
            assert_eq!(resp.status(), 422);
        }
    }

    mod remove_entry_router {
        use super::*;
        use crate::api::tests::keeper;

        #[rstest]
        #[tokio::test]
        async fn test_remove_entry_router(
            #[future] keeper: Arc<StateKeeper>,
            path_to_entry_1: Path<HashMap<String, String>>,
            headers: HeaderMap,
        ) {
            let query = Query(HashMap::new());
            let keeper = keeper.await;
            let components = keeper.get_anonymous().await.unwrap();
            let result =
                remove_entry_router(State(keeper.clone()), headers, path_to_entry_1, query)
                    .await
                    .unwrap();
            assert_eq!(result, ());

            let err = components
                .storage
                .get_bucket("bucket-1")
                .await
                .unwrap()
                .upgrade()
                .unwrap()
                .get_entry("entry-1")
                .await
                .err()
                .unwrap();

            assert!(
                matches!(err.status(), ErrorCode::NotFound | ErrorCode::Conflict),
                "Entry should be gone or marked deleting"
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_remove_record(
            #[future] keeper: Arc<StateKeeper>,
            path_to_entry_1: Path<HashMap<String, String>>,
            headers: HeaderMap,
        ) {
            let query = Query(HashMap::from_iter(vec![(
                "ts".to_string(),
                "0".to_string(),
            )]));
            let keeper = keeper.await;
            let components = keeper.get_anonymous().await.unwrap();
            let result =
                remove_entry_router(State(keeper.clone()), headers, path_to_entry_1, query)
                    .await
                    .unwrap();
            assert_eq!(result, ());

            let entry = components
                .storage
                .get_bucket("bucket-1")
                .await
                .unwrap()
                .upgrade()
                .unwrap()
                .get_entry("entry-1")
                .await
                .unwrap()
                .upgrade_and_unwrap();

            let err = entry.begin_read(0).await.err().unwrap();
            assert_eq!(err.status(), ErrorCode::NotFound);
        }
    }

    mod query_entry_router {
        use super::*;
        use crate::api::tests::keeper;

        #[rstest]
        #[tokio::test]
        async fn test_query_entry_router_read(
            #[future] keeper: Arc<StateKeeper>,
            path_to_entry_1: Path<HashMap<String, String>>,
            headers: HeaderMap,
        ) {
            let query = QueryEntry {
                query_type: QueryType::Query,
                ..Default::default()
            };
            let result = query_entry_router(
                State(keeper.await),
                headers,
                path_to_entry_1,
                QueryEntryAxum(query),
            )
            .await;
            assert_eq!(result.status(), 200);
        }

        #[rstest]
        #[tokio::test]
        async fn test_query_entry_router_remove(
            #[future] keeper: Arc<StateKeeper>,
            path_to_entry_1: Path<HashMap<String, String>>,
            headers: HeaderMap,
        ) {
            let query = QueryEntry {
                query_type: QueryType::Remove,
                start: Some(0),
                ..Default::default()
            };
            let result = query_entry_router(
                State(keeper.await),
                headers,
                path_to_entry_1,
                QueryEntryAxum(query),
            )
            .await;
            assert_eq!(result.status(), 200);
        }
    }

    pub async fn query(
        path_to_entry_1: &Path<HashMap<String, String>>,
        keeper: Arc<StateKeeper>,
        ttl: Option<u64>,
    ) -> u64 {
        let options = QueryEntry {
            start: Some(0),
            stop: Some(u64::MAX),
            ttl,
            ..Default::default()
        };

        let components = keeper.get_anonymous().await.unwrap();
        let query_id = {
            components
                .storage
                .get_bucket(path_to_entry_1.get("bucket_name").unwrap())
                .await
                .unwrap()
                .upgrade()
                .unwrap()
                .get_entry(path_to_entry_1.get("entry_name").unwrap())
                .await
                .unwrap()
                .upgrade()
                .unwrap()
                .query(options)
                .await
                .unwrap()
        };
        query_id
    }
}
