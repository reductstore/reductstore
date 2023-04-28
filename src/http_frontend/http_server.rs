// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::cell::RefCell;
use std::collections::HashMap;

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::service::Service;
use hyper::{body::Incoming as IncomingBody, Method, Request, Response};
use log::{debug, error};
use serde::Serialize;

use crate::asset::asset_manager::ZipAssetManager;
use crate::auth::policy::*;
use crate::auth::proto::TokenRepo;
use crate::auth::token_auth::TokenAuthorization;
use crate::auth::token_repository::TokenRepository;

use crate::core::status::{HTTPError, HTTPStatus};
use crate::storage::storage::Storage;

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;

pub struct HttpServerComponents {
    pub storage: Storage,
    pub auth: TokenAuthorization,
    pub token_repo: TokenRepository,
    pub console: ZipAssetManager,
}

#[derive(Clone)]
pub struct HttpServer {
    components: Rc<RefCell<HttpServerComponents>>,
    api_base_path: String,
    cert_path: String,
    cert_key_path: String,
}

fn full<T: Into<Bytes>>(chunk: T) -> BoxBody {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

impl HttpServer {
    pub fn new(
        components: Rc<RefCell<HttpServerComponents>>,
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

    fn process_msg<Msg: Serialize, Plc: Policy>(
        comp: &HttpServerComponents,
        policy: Plc,
        req: Request<IncomingBody>,
        msg: Result<Msg, HTTPError>,
    ) -> Result<Response<Full<Bytes>>, GenericError> {
        fn mk_response(
            req: &Request<IncomingBody>,
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
                error!("{} {} [{}]", req.method(), req.uri().path(), status as u16);
            } else {
                debug!("{} {} [{}]", req.method(), req.uri().path(), status as u16);
            }

            if status >= HTTPStatus::BadRequest {
                builder = builder.header("x-reduct-error", content.clone());
                content = format!("{{\"detail\": \"{}\"}}", content);
            }

            let content = if req.method() == &Method::HEAD {
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

        let msg = match comp.auth.check(header, &comp.token_repo, &policy) {
            Ok(()) => msg,
            Err(e) => Err(e),
        };

        match msg {
            Ok(msg) => {
                let body = serde_json::to_string(&msg).unwrap();
                mk_response(&req, body, HTTPStatus::OK, HashMap::new())
            }
            Err(e) => mk_response(&req, e.message.clone(), e.status, HashMap::new()),
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

        let info_path: String = format!("{}api/v1/info", self.api_base_path);
        let list_path: String = format!("{}api/v1/list", self.api_base_path);
        let alive_path: String = format!("{}api/v1/alive", self.api_base_path);
        let me_path: String = format!("{}api/v1/me", self.api_base_path);

        let comp = self.components.borrow_mut();
        let route = (req.method(), req.uri().path());
        let resp = if route == (&Method::GET, &info_path) {
            // GET /info
            HttpServer::process_msg(&comp, AuthenticatedPolicy {}, req, comp.storage.info())
        } else if route == (&Method::GET, &list_path) {
            // GET /list
            HttpServer::process_msg(
                &comp,
                AuthenticatedPolicy {},
                req,
                comp.storage.get_bucket_list(),
            )
        } else if route == (&Method::HEAD, &alive_path) {
            // GET /alive
            HttpServer::process_msg(&comp, AnonymousPolicy {}, req, comp.storage.info())
        } else if route == (&Method::GET, &me_path) {
            let headers = req.headers().clone();
            let header = match headers.get("Authorization") {
                Some(header) => header.to_str().ok(),
                None => None,
            };
            HttpServer::process_msg(
                &comp,
                AnonymousPolicy {},
                req,
                comp.token_repo.validate_token(header),
            )
        } else {
            HttpServer::process_msg::<TokenRepo, AnonymousPolicy>(
                &comp,
                AnonymousPolicy {},
                req,
                Err(HTTPError::not_found("Not found")),
            )
        };

        Box::pin(async move { resp })
    }
}
