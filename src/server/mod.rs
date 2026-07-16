pub mod result;
pub mod socket;

use crate::metrics::metrics_handler;
use anyhow::Result;
use axum::{
    error_handling::HandleErrorLayer,
    http::{Method, StatusCode, Uri},
    response::IntoResponse,
    routing::get,
    BoxError, Router,
};

use std::{
    env,
    net::{IpAddr, SocketAddr},
    str::FromStr,
    time::Duration,
};
use tower::ServiceBuilder;
use tracing::info;

pub async fn main() -> Result<()> {
    let ip = env::var("IP").unwrap_or_else(|_| "127.0.0.1".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "8001".to_string());

    // WebSocket 라우터 (타임아웃 제외)
    let ws_router = socket::router();

    // 일반 HTTP 라우터 (타임아웃 적용)
    let http_router = Router::new()
        .route("/", get(|| async { "Hello,RPC Server !" }))
        .route("/health", get(health_check))
        .route("/metrics", get(metrics_handler))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(handle_timeout_error))
                .timeout(Duration::from_secs(1)),
        );

    let app = http_router.merge(ws_router).fallback(handler_404);

    let addr = SocketAddr::from((
        IpAddr::from_str(ip.as_str()).unwrap(),
        port.parse::<u16>().unwrap(),
    ));
    info!("Listening on {} Server port{}", addr, port);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}

async fn handler_404() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "nothing to see here")
}

async fn handle_timeout_error(
    // `Method` and `Uri` are extractors so they can be used here
    method: Method,
    uri: Uri,
    // the last argument must be the error itself
    err: BoxError,
) -> (StatusCode, String) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("`{method} {uri}` failed with {err}"),
    )
}

async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}
