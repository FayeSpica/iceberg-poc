use serde::{Deserialize, Serialize};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    body::Bytes,
};
use tracing::{info, error};

use crate::iceberg_client::IcebergClient;
use crate::arrow_handler::ArrowStreamHandler;

#[derive(Clone)]
pub struct AppState {
    pub iceberg_client: IcebergClient,
    pub arrow_handler: ArrowStreamHandler,
}

#[derive(Deserialize)]
pub struct IngestQuery {
    pub table_name: String,
    pub namespace: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct IngestResponse {
    pub success: bool,
    pub message: String,
    pub records_ingested: Option<u64>,
}

pub async fn health_check() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "healthy",
        "service": "ingress-iceberg"
    }))
}

pub async fn ingest_data(
    State(state): State<AppState>,
    Query(query): Query<IngestQuery>,
    body: Bytes,
) -> Result<Json<IngestResponse>, (StatusCode, String)> {
    info!("Received ingest request for table: {}", query.table_name);

    match state.arrow_handler.process_arrow_bytes(&body).await {
        Ok(record_batch) => {
            match state.iceberg_client.write_to_table(
                &query.namespace.unwrap_or_else(|| "default".to_string()),
                &query.table_name,
                record_batch,
            ).await {
                Ok(records_written) => {
                    info!("Successfully wrote {} records to table {}", records_written, query.table_name);
                    Ok(Json(IngestResponse {
                        success: true,
                        message: format!("Successfully ingested {} records", records_written),
                        records_ingested: Some(records_written),
                    }))
                }
                Err(e) => {
                    error!("Failed to write to Iceberg table: {}", e);
                    Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
                }
            }
        }
        Err(e) => {
            error!("Failed to process Arrow data: {}", e);
            Err((StatusCode::BAD_REQUEST, e.to_string()))
        }
    }
}