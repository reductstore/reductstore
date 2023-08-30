// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::config::find_alias;
use crate::context::CliContext;
use reduct_rs::ReductClient;

pub(crate) fn build_client(ctx: &CliContext, alias: &str) -> anyhow::Result<ReductClient> {
    let alias = find_alias(ctx, alias)?;

    let client = ReductClient::builder()
        .url(alias.url.as_str())
        .api_token(alias.token.as_str())
        .build();
    Ok(client)
}
