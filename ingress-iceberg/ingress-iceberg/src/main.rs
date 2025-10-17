use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tracing::{info, error};

mod iceberg_client;
mod arrow_handler;

use iceberg_client::IcebergClient;
use arrow_handler::ArrowStreamHandler;

#[derive(Clone)]
pub struct AppState {
    iceberg_client: IcebergClient,
    arrow_handler: ArrowStreamHandler,
}

#[derive(Deserialize)]
struct IngestRequest {
    table_name: String,
    namespace: Option<String>,
    data: String, // Base64 encoded Arrow data
}

#[derive(Serialize)]
struct IngestResponse {
    success: bool,
    message: String,
    records_ingested: Option<u64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting ingress-iceberg server...");

    // Initialize Iceberg client
    let iceberg_client = IcebergClient::new(
        "http://localhost:8181".to_string(), // REST catalog URL
    ).await?;

    // Initialize Arrow handler
    let arrow_handler = ArrowStreamHandler::new();

    let app_state = AppState {
        iceberg_client,
        arrow_handler,
    };

    // Build our application with routes
    let app = Router::new()
        .route("/health", post(health_check))
        .route("/ingest", post(ingest_data))
        .layer(CorsLayer::permissive())
        .with_state(app_state);

    // Run the server
    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    info!("Server listening on {}", addr);
    
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "ingress-iceberg"
    }))
}

async fn ingest_data(
    State(state): State<AppState>,
    Json(payload): Json<IngestRequest>,
) -> Result<Json<IngestResponse>, StatusCode> {
    info!("Received ingest request for table: {}", payload.table_name);

    match state.arrow_handler.process_arrow_data(&payload.data).await {
        Ok(record_batch) => {
            match state.iceberg_client.write_to_table(
                &payload.namespace.unwrap_or_else(|| "default".to_string()),
                &payload.table_name,
                record_batch,
            ).await {
                Ok(records_written) => {
                    info!("Successfully wrote {} records to table {}", records_written, payload.table_name);
                    Ok(Json(IngestResponse {
                        success: true,
                        message: format!("Successfully ingested {} records", records_written),
                        records_ingested: Some(records_written),
                    }))
                }
                Err(e) => {
                    error!("Failed to write to Iceberg table: {}", e);
                    Err(StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }
        Err(e) => {
            error!("Failed to process Arrow data: {}", e);
            Err(StatusCode::BAD_REQUEST)
        }
    }
}
