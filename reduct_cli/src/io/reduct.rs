// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::find_alias;
use crate::context::CliContext;
use anyhow::anyhow;
use colored::Colorize;
use reduct_rs::ReductClient;
use url::Url;

/// Build a ReductStore client from an alias or URL
pub(crate) async fn build_client(
    ctx: &CliContext,
    alias_or_url: &str,
) -> anyhow::Result<ReductClient> {
    let (url, token) = match parse_url_and_token(ctx, alias_or_url) {
        Ok(value) => value,
        Err(err) => return Err(err),
    };

    let client = ReductClient::builder()
        .url(url.as_str())
        .api_token(token.as_str())
        .verify_ssl(!ctx.ignore_ssl())
        .http1_only() // can't use http2 to stream data between two servers
        .timeout(ctx.timeout())
        .try_build()?;

    let status = client.server_info().await?;
    if let Some(license) = status.license {
        if license.expiry_date < chrono::Utc::now() {
            eprintln!(
                "{}",
                format!(
                    "Warning: License for {} at expired at {}",
                    url.as_str(),
                    license.expiry_date
                )
                .yellow()
                .bold()
            );
        }

        const TB: u64 = 1000_000_000_000u64;
        if license.disk_quota > 0 && status.usage > (license.disk_quota as u64 * TB) {
            eprintln!(
                "{}",
                format!(
                    "Warning: Disk usage of {} exceeds licensed quota of {} TB, currently at {} TB",
                    url.as_str(),
                    license.disk_quota,
                    status.usage / TB
                )
                .yellow()
                .bold()
            );
        }
    }

    Ok(client)
}

/// Parse an alias or URL into a URL and a token
///
/// If the input is an alias, the URL and token are retrieved from the alias.
/// If the input is a URL, the token is extracted from the username part of the URL.
pub(crate) fn parse_url_and_token(
    ctx: &CliContext,
    alias_or_url: &str,
) -> anyhow::Result<(Url, String)> {
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
    Ok((url, token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::tests::{context, current_token};
    use rstest::rstest;

    #[rstest]
    #[tokio::test]
    async fn test_build_client(context: CliContext, current_token: String) {
        let client = build_client(&context, "local").await.unwrap();
        assert_eq!(client.url(), "http://localhost:8383");
        assert_eq!(client.api_token(), current_token);
    }

    #[rstest]
    #[tokio::test]
    async fn test_build_client_with_url(context: CliContext) {
        let err = build_client(&context, "http://localhost:8383")
            .await
            .err()
            .unwrap();
        assert_eq!(
            err.to_string(),
            "[Unauthorized] No bearer token in request header"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_build_client_with_url_and_token(context: CliContext, current_token: String) {
        let client = build_client(
            &context,
            &format!("http://{}@localhost:8383", current_token),
        )
        .await
        .unwrap();
        assert_eq!(client.url(), "http://localhost:8383");
        assert_eq!(client.api_token(), current_token);
    }

    #[rstest]
    #[tokio::test]
    async fn test_build_client_with_invalid_url(context: CliContext) {
        assert!(build_client(&context, "xxx://localhost:8000/")
            .await
            .is_err());
    }

    #[rstest]
    #[tokio::test]
    async fn test_build_client_with_invalid_alias(context: CliContext) {
        assert!(build_client(&context, "invalid").await.is_err());
    }
}
