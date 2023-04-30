// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use crate::auth::proto::token::Permissions;
use http_body_util::BodyExt;
use hyper::body::Body;
use hyper::{body::Incoming as IncomingBody, Request};
use prost::Message;

use crate::auth::proto::{TokenCreateResponse, TokenRepo};
use crate::core::status::HttpError;
use crate::http_frontend::http_server::HttpServerComponents;

pub struct TokenApi {}

impl TokenApi {
    // GET /tokens
    pub async fn token_list<Body>(
        components: &mut HttpServerComponents,
        _: &Request<Body>,
    ) -> Result<TokenRepo, HttpError> {
        let mut list = TokenRepo::default();
        for x in components.token_repo.get_token_list()?.iter() {
            list.tokens.push(x.clone());
        }
        list.tokens.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(list)
    }

    // POST /tokens/:name
    pub async fn create_token(
        components: &mut HttpServerComponents,
        req: Request<IncomingBody>,
    ) -> Result<TokenCreateResponse, HttpError> {
        let name = req.uri().path().split("/").last().unwrap().to_string();

        let data = match req.into_body().collect().await {
            Ok(body) => body.to_bytes(),
            Err(e) => return Err(HttpError::bad_request(&e.to_string())),
        };

        let permissions = match Permissions::decode(data) {
            Ok(permissions) => permissions,
            Err(e) => return Err(HttpError::bad_request(&e.to_string())),
        };

        components.token_repo.create_token(&name, permissions)
    }
}
