// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::policy::FullAccessPolicy;
use crate::http_frontend::middleware::check_permissions;
use crate::http_frontend::HttpServerState;
use axum::extract::{Path, State};
use axum::headers::HeaderMap;
use reduct_base::error::HttpError;
use std::sync::{Arc, RwLock};

// DELETE /tokens/:name
pub async fn remove_token(
    State(components): State<Arc<RwLock<HttpServerState>>>,
    Path(token_name): Path<String>,
    headers: HeaderMap,
) -> Result<(), HttpError> {
    check_permissions(Arc::clone(&components), headers, FullAccessPolicy {})?;

    let mut components = components.write().unwrap();
    components.token_repo.remove_token(&token_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_frontend::tests::{components, headers};

    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_remove_token(components: Arc<RwLock<HttpServerState>>, headers: HeaderMap) {
        let token = remove_token(State(components), Path("test".to_string()), headers).await;
        assert!(token.is_ok());
    }
}
