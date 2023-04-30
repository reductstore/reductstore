// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::arch::x86_64::_mm256_rcp_ps;
use std::cell::RefCell;
use std::collections::HashMap;

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use http_body_util::Full;
use hyper::service::Service;
use hyper::{body::Incoming as IncomingBody, Method, Request, Response};
use log::{debug, error};
use serde::Serialize;

use crate::asset::asset_manager::ZipAssetManager;
use crate::auth::policy::*;
use crate::auth::proto::TokenRepo;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::TokenRepository;

use crate::core::status::{HTTPStatus, HttpError};
use crate::http_frontend::server_api::ServerApi;
use crate::http_frontend::token_api::TokenApi;
use crate::storage::storage::Storage;

type GenericError = Box<dyn std::error::Error + Send + Sync>;

pub struct HttpServerComponents {
    pub storage: Storage,
    pub auth: TokenAuthorization,
    pub token_repo: TokenRepository,
    pub console: ZipAssetManager,
}

#[derive(Clone)]
pub struct HttpServer {
    components: Arc<RwLock<HttpServerComponents>>,
    //todo: very straight forward, should be grained
    api_base_path: String,
    cert_path: String,
    cert_key_path: String,
}

impl HttpServer {
    pub fn new(
        components: Arc<RwLock<HttpServerComponents>>,
        api_base_path: String,
        cert_path: String,
        cert_key_path: String,
    ) -> Self {
        Self {
            components,
            api_base_path,
            cert_path,
            cert_key_path,
        }
    }

    async fn process_msg<Msg: Serialize, Plc: Policy, Handle, Fut>(
        comp: Arc<RwLock<HttpServerComponents>>,
        policy: Plc,
        req: Request<IncomingBody>,
        msg: Handle,
    ) -> Result<Response<Full<Bytes>>, GenericError>
    where
        Handle: FnOnce(Arc<RwLock<HttpServerComponents>>, Request<IncomingBody>) -> Fut,
        Fut: Future<Output = Result<Msg, HttpError>> + Send,
    {
        // Check errors and access, then create response

        fn mk_response(
            method: Method,
            path: String,
            content: String,
            status: HTTPStatus,
            headers: HashMap<String, String>,
        ) -> Result<Response<Full<Bytes>>, GenericError> {
            let mut builder = Response::builder();
            let mut content = content;
            builder = builder.header("ReductStore", env!("CARGO_PKG_VERSION"));
            if !headers.contains_key("Content-Type") {
                builder = builder.header("Content-Type", "application/json");
            }

            for (k, v) in headers {
                builder = builder.header(k, v);
            }

            if status >= HTTPStatus::InternalServerError {
                error!("{} {} [{}]", method, path, status as u16);
            } else {
                debug!("{} {} [{}]", method, path, status as u16);
            }

            if status >= HTTPStatus::BadRequest {
                builder = builder.header("x-reduct-error", content.clone());
                content = format!("{{\"detail\": \"{}\"}}", content);
            }

            let content = if method == &Method::HEAD {
                String::new()
            } else {
                content
            };

            Ok(builder
                .status(status as u16)
                .body(Full::new(Bytes::from(content)))
                .unwrap())
        }

        let header = match req.headers().get("Authorization") {
            Some(header) => header.to_str().ok(),
            None => None,
        };

        let auth = {
            let mut comp = comp.read().unwrap();
            comp.auth.check(header, &comp.token_repo, policy)
        };

        let method = req.method().clone();
        let path = req.uri().path().to_string();
        let msg = {
            match auth {
                Ok(_) => msg(Arc::clone(&comp), req).await,
                Err(e) => Err(e),
            }
        };

        match msg {
            Ok(msg) => {
                let body = serde_json::to_string(&msg).unwrap();
                mk_response(method, path, body, HTTPStatus::OK, HashMap::new())
            }
            Err(e) => mk_response(method, path, e.message.clone(), e.status, HashMap::new()),
        }
    }
}

impl Service<Request<IncomingBody>> for HttpServer {
    type Response = Response<Full<Bytes>>;
    type Error = GenericError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, GenericError>> + Send>>;

    fn call(&mut self, req: Request<IncomingBody>) -> Self::Future {
        let _base = if self.api_base_path.chars().last().unwrap() == '/' {
            self.api_base_path.clone()
        } else {
            format!("{}/", self.api_base_path)
        };

        let route = (req.method(), req.uri().path());

        // Server API

        let server_info_path: String = format!("{}api/v1/info", self.api_base_path);
        let server_list_path: String = format!("{}api/v1/list", self.api_base_path);
        let server_alive_path: String = format!("{}api/v1/alive", self.api_base_path);
        let servre_me_path: String = format!("{}api/v1/me", self.api_base_path);

        if route == (&Method::GET, &server_info_path) {
            // GET /info
            let resp = HttpServer::process_msg(
                Arc::clone(&self.components),
                AuthenticatedPolicy {},
                req,
                ServerApi::info,
            );
            return Box::pin(resp);
        }

        if route == (&Method::GET, &server_list_path) {
            // GET /list
            let resp = HttpServer::process_msg(
                Arc::clone(&self.components),
                AuthenticatedPolicy {},
                req,
                ServerApi::list,
            );
            return Box::pin(resp);
        }

        if route == (&Method::HEAD, &server_alive_path) {
            // HEAD /alive
            let resp = HttpServer::process_msg(
                Arc::clone(&self.components),
                AnonymousPolicy {},
                req,
                ServerApi::info,
            );
            return Box::pin(resp);
        }
        if route == (&Method::GET, &servre_me_path) {
            // GET /me
            let resp = HttpServer::process_msg(
                Arc::clone(&self.components),
                AnonymousPolicy {},
                req,
                ServerApi::me,
            );
            return Box::pin(resp);
        }

        // Token API
        let token_path: String = format!("{}api/v1/tokens/", self.api_base_path);
        if route == (&Method::GET, &token_path[0..token_path.len() - 2]) {
            // GET /tokens
            let resp = HttpServer::process_msg(
                Arc::clone(&self.components),
                AuthenticatedPolicy {},
                req,
                TokenApi::token_list,
            );
            return Box::pin(resp);
        }

        if route.0 == &Method::GET && route.1.starts_with(&token_path) {
            // GET /tokens/:name
            let resp = HttpServer::process_msg(
                Arc::clone(&self.components),
                AuthenticatedPolicy {},
                req,
                TokenApi::get_token,
            );
            return Box::pin(resp);
        }

        if route.0 == &Method::POST && route.1.starts_with(&token_path) {
            // POST /tokens/:name
            let resp = HttpServer::process_msg(
                Arc::clone(&self.components),
                AuthenticatedPolicy {},
                req,
                TokenApi::create_token,
            );
            return Box::pin(resp);
        }

        if route.0 == &Method::DELETE && route.1.starts_with(&token_path) {
            // DELETE /tokens/:name
            let resp = HttpServer::process_msg(
                Arc::clone(&self.components),
                AuthenticatedPolicy {},
                req,
                TokenApi::remove_token,
            );
            return Box::pin(resp);
        }

        let resp = HttpServer::process_msg(
            Arc::clone(&self.components),
            AnonymousPolicy {},
            req,
            |_, _| async { Err::<TokenRepo, HttpError>(HttpError::not_found("Not found.")) },
        );
        Box::pin(resp)
    }
}
