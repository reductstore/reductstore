// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::find_alias;
use crate::context::CliContext;
use anyhow::anyhow;
use reduct_rs::ReductClient;
use url::Url;

/// Build a ReductStore client from an alias or URL
pub(crate) fn build_client(ctx: &CliContext, alias_or_url: &str) -> anyhow::Result<ReductClient> {
    let (url, token) = match find_alias(ctx, alias_or_url) {
        Ok(alias) => (alias.url, alias.token),
        Err(_) => {
            if let Ok(mut url) = Url::parse(alias_or_url) {
                let token = url.username().to_string();
                url.set_username("").unwrap();
                (url, token)
            } else {
                return Err(anyhow!("'{}' isn't an alias or a valid URL", alias_or_url));
            }
        }
    };

    let client = ReductClient::builder()
        .url(url.as_str())
        .api_token(token.as_str())
        .try_build()?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{context, current_token};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_build_client(context: CliContext, current_token: String) {
        let client = build_client(&context, "local").unwrap();
        assert_eq!(client.url(), "http://localhost:8383");
        assert_eq!(client.api_token(), current_token);
    }

    #[rstest]
    #[tokio::test]
    async fn test_build_client_with_url(context: CliContext) {
        let client = build_client(&context, "http://localhost:8000").unwrap();
        assert_eq!(client.url(), "http://localhost:8000");
        assert_eq!(client.api_token(), "");
    }

    #[rstest]
    #[tokio::test]
    async fn test_build_client_with_url_and_token(context: CliContext) {
        let client = build_client(&context, "http://sometoken@localhost:8000").unwrap();
        assert_eq!(client.url(), "http://localhost:8000");
        assert_eq!(client.api_token(), "sometoken");
    }

    #[rstest]
    #[tokio::test]
    async fn test_build_client_with_invalid_url(context: CliContext) {
        assert!(build_client(&context, "xxx://localhost:8000/").is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn test_build_client_with_invalid_alias(context: CliContext) {
        assert!(build_client(&context, "invalid").is_err());
    }
}
