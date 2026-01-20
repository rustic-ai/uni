// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use axum::{
    Router,
    extract::{Path, Request, State},
    http::{HeaderName, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
};
use metrics_exporter_prometheus::PrometheusHandle;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use uni_common::config::ServerConfig;
use uni_db::Uni;
use uni_db::Value;

// Embedded static files
#[derive(rust_embed::RustEmbed)]
#[folder = "src/server/static/"]
struct Asset;

/// Shared application state passed to handlers.
struct AppState {
    db: Uni,
    config: ServerConfig,
    metrics_handle: Option<PrometheusHandle>,
}

/// Starts the HTTP server with the given database and configuration.
///
/// # Security
///
/// See [`ServerConfig`] for CORS and authentication options. Production deployments
/// should configure explicit `allowed_origins` and enable API key authentication.
///
/// # Errors
///
/// Returns an error if the TCP listener fails to bind or the server encounters
/// a fatal error.
pub async fn start_server(db: Uni, port: u16, config: ServerConfig) -> anyhow::Result<()> {
    // Log security warnings
    if let Some(warning) = config.security_warning() {
        warn!("SECURITY: {}", warning);
    }

    // Initialize Prometheus recorder
    // We attempt to install it. If it fails, it might be already installed.
    // Ideally we should get the handle if installed, but the crate doesn't easily expose "get handle if installed".
    // For this prototype, we store the handle if we successfully installed it.
    let metrics_handle = metrics_exporter_prometheus::PrometheusBuilder::new()
        .install_recorder()
        .ok();

    let app_state = Arc::new(AppState {
        db,
        config,
        metrics_handle,
    });

    // Build CORS layer based on config
    let cors = build_cors_layer(&app_state.config);

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/assets/*file", get(static_handler))
        .route("/api/v1/query", post(query_handler))
        .route("/api/v1/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .layer(middleware::from_fn_with_state(
            app_state.clone(),
            api_key_middleware,
        ))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(app_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    info!("Server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Builds the CORS layer based on server configuration.
fn build_cors_layer(config: &ServerConfig) -> CorsLayer {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            header::CONTENT_TYPE,
            header::AUTHORIZATION,
            HeaderName::from_static("x-api-key"),
        ]);

    if config.allowed_origins.is_empty() {
        // No origins allowed - most restrictive
        cors
    } else if config.allowed_origins.len() == 1 && config.allowed_origins[0] == "*" {
        // Allow any origin (development mode)
        cors.allow_origin(AllowOrigin::any())
    } else {
        // Allow specific origins
        let origins: Vec<HeaderValue> = config
            .allowed_origins
            .iter()
            .filter_map(|o| o.parse().ok())
            .collect();
        cors.allow_origin(origins)
    }
}

/// API key authentication middleware.
///
/// Checks for `X-API-Key` header when authentication is configured.
/// Skips authentication for health endpoint.
async fn api_key_middleware(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path();

    // Health endpoint is always public
    if path == "/health" {
        return next.run(request).await;
    }

    // Metrics endpoint authentication based on config
    if path == "/api/v1/metrics" && !state.config.require_auth_for_metrics {
        return next.run(request).await;
    }

    // Static assets are always public
    if path == "/" || path.starts_with("/assets/") {
        return next.run(request).await;
    }

    // Check API key if configured
    if let Some(expected_key) = &state.config.api_key {
        let provided_key = request
            .headers()
            .get("x-api-key")
            .and_then(|v| v.to_str().ok());

        match provided_key {
            Some(key) if key == expected_key => {
                // Key matches, continue
                next.run(request).await
            }
            Some(_) => {
                // Key provided but doesn't match
                (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse {
                        error: "Invalid API key".to_string(),
                    }),
                )
                    .into_response()
            }
            None => {
                // No key provided
                (
                    StatusCode::UNAUTHORIZED,
                    Json(ErrorResponse {
                        error: "API key required. Set X-API-Key header.".to_string(),
                    }),
                )
                    .into_response()
            }
        }
    } else {
        // No authentication configured
        next.run(request).await
    }
}

/// Health check endpoint for load balancers and monitoring.
async fn health_handler() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

async fn index_handler() -> impl IntoResponse {
    static_handler(Path("index.html".to_string())).await
}

async fn static_handler(Path(path): Path<String>) -> impl IntoResponse {
    let path = if path.is_empty() {
        "index.html".to_string()
    } else {
        path
    };

    match Asset::get(&path) {
        Some(content) => {
            let mime = mime_guess::from_path(&path).first_or_octet_stream();
            (
                [(axum::http::header::CONTENT_TYPE, mime.as_ref())],
                content.data,
            )
                .into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

#[derive(Deserialize)]
struct QueryRequest {
    query: String,
    params: Option<HashMap<String, Value>>,
}

#[derive(Serialize)]
struct QueryResponse {
    columns: Vec<String>,
    rows: Vec<Vec<Value>>,
    execution_time_ms: u128,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

async fn query_handler(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<QueryRequest>,
) -> impl IntoResponse {
    let start = std::time::Instant::now();
    let params = payload.params.unwrap_or_default();

    let builder = state.db.query_with(&payload.query);

    let mut builder = builder;
    for (k, v) in params {
        builder = builder.param(&k, v);
    }

    match builder.fetch_all().await {
        Ok(result) => {
            let duration = start.elapsed();
            let columns = result.columns().to_vec();
            let rows = result.rows().iter().map(|r| r.values.clone()).collect();

            let response = QueryResponse {
                columns,
                rows,
                execution_time_ms: duration.as_millis(),
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            let response = ErrorResponse {
                error: e.to_string(),
            };
            (StatusCode::BAD_REQUEST, Json(response)).into_response()
        }
    }
}

async fn metrics_handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Some(handle) = &state.metrics_handle {
        handle.render()
    } else {
        "Metrics not initialized (recorder install failed)".to_string()
    }
}
