pub mod asset;
pub mod auth;
pub mod core;
pub mod storage;

use crate::core::env::Env;
use crate::core::logger::Logger;
use crate::storage::storage::Storage;
use asset::asset_manager::ZipAssetManager;
use auth::token_auth::TokenAuthorization;
use auth::token_repository::TokenRepository;
use std::cell::RefCell;
use std::future::Future;

use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
use std::str::FromStr;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::service::Service;
use hyper::{body::Incoming as IncomingBody, Method, Request, Response};

use hyper::server::conn::http1;
use log::info;
use tokio::net::TcpListener;

type GenericError = Box<dyn std::error::Error + Send + Sync>;
type BoxBody = http_body_util::combinators::BoxBody<Bytes, hyper::Error>;
type Result<T> = std::result::Result<T, GenericError>;

struct HttpServerComponents {
    storage: Storage,
    auth: TokenAuthorization,
    token_repo: TokenRepository,
    console: ZipAssetManager,
}

#[derive(Clone)]
struct HttpServer {
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

impl HttpServer {}

impl Service<Request<IncomingBody>> for HttpServer {
    type Response = Response<Full<Bytes>>;
    type Error = GenericError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response>> + Send>>;

    fn call(&mut self, req: Request<IncomingBody>) -> Self::Future {
        fn mk_response(s: String, status: u16) -> Result<Response<Full<Bytes>>> {
            Ok(Response::builder()
                .status(status)
                .body(Full::new(Bytes::from(s)))
                .unwrap())
        }

        let _INFO_PATH = format!("{}/info", self.api_base_path);
        let resp = match (req.method(), req.uri().path()) {
            (&Method::GET, _INFO_PATH) => {
                let comp = self.components.borrow_mut();
                let info = comp.storage.info().unwrap();

                let body = serde_json::to_string(&info).unwrap();
                mk_response(body, 200)
            }
            _ => mk_response("not found".to_string(), 404),
        };

        info!(
            "{} {} [{}]",
            req.method(),
            req.uri().path(),
            resp.as_ref().unwrap().status().as_str()
        );
        Box::pin(async move { resp })
    }
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime");

    // Combine it with a `LocalSet,  which means it can spawn !Send futures...
    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, run())
}

async fn run() {
    // todo: check graceful shutdown
    let version: &str = env!("CARGO_PKG_VERSION");

    info!("ReductStore {}", version);

    let mut env = Env::new();

    let log_level = env.get::<String>("LOG_LEVEL", "INFO".to_string(), false);
    let host = env.get::<String>("RS_HOST", "0.0.0.0".to_string(), false);
    let port = env.get::<i32>("RS_PORT", 8383, false);
    let api_base_path = env.get::<String>("RS_API_BASE_PATH", "/".to_string(), false);
    let data_path = env.get::<String>("RS_DATA_PATH", "/data".to_string(), false);
    let api_token = env.get::<String>("RS_API_TOKEN", "".to_string(), true);
    let cert_path = env.get::<String>("RS_CERT_PATH", "".to_string(), true);
    let cert_key_path = env.get::<String>("RS_CERT_KEY_PATH", "".to_string(), true);

    Logger::init(&log_level);

    info!("Configuration: \n {}", env.message());

    let components = HttpServerComponents {
        storage: Storage::new(PathBuf::from(data_path.clone())),
        auth: TokenAuthorization::new(&api_token),
        token_repo: TokenRepository::new(PathBuf::from(data_path), Some(api_token)),
        console: ZipAssetManager::new(""),
    };

    let server = HttpServer {
        components: Rc::new(RefCell::new(components)),
        api_base_path,
        cert_path,
        cert_key_path,
    };

    let addr = SocketAddr::new(
        IpAddr::from_str(&host).expect("Invalid host address"),
        port as u16,
    );

    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind to address");
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .expect("Failed to accept connection");
        let server = server.clone();
        tokio::task::spawn_local(async move {
            if let Err(err) = http1::Builder::new().serve_connection(stream, server).await {
                println!("Failed to serve connection: {:?}", err);
            }
        });
    }
}
