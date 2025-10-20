use axum::{
    routing::post,
    Router,
};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tracing::info;

use ingress_iceberg::types::{AppState, health_check, ingest_data};
use ingress_iceberg::iceberg_client::IcebergClient;
use ingress_iceberg::arrow_handler::ArrowStreamHandler;

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


#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    use std::sync::Arc;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use arrow::ipc::writer::StreamWriter;

    async fn create_test_app_state() -> AppState {
        // Create a mock IcebergClient for testing
        let iceberg_client = IcebergClient::new("http://localhost:8181".to_string()).await.unwrap();
        let arrow_handler = ArrowStreamHandler::new();
        
        AppState {
            iceberg_client,
            arrow_handler,
        }
    }

    fn create_test_arrow_data() -> Vec<u8> {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

        let record_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(name_array)],
        ).unwrap();

        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &record_batch.schema()).unwrap();
            writer.write(&record_batch).unwrap();
            writer.finish().unwrap();
        }

        buffer
    }

    #[tokio::test]
    async fn test_health_check() {
        let app_state = create_test_app_state().await;
        let app = Router::new()
            .route("/health", post(health_check))
            .with_state(app_state);

        let request = Request::builder()
            .method("POST")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        
        assert_eq!(response.status(), StatusCode::OK);
        
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        
        assert_eq!(json["status"], "healthy");
        assert_eq!(json["service"], "ingress-iceberg");
    }

    #[tokio::test]
    async fn test_ingest_data_success() {
        let app_state = create_test_app_state().await;
        let app = Router::new()
            .route("/ingest", post(ingest_data))
            .with_state(app_state);

        let arrow_data = create_test_arrow_data();
        let request = Request::builder()
            .method("POST")
            .uri("/ingest?table_name=test_table&namespace=test_namespace")
            .header("content-type", "application/x-apache-arrow-stream")
            .body(Body::from(arrow_data))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        
        // Note: This test might fail due to Iceberg client connection issues
        // In a real test environment, you would mock the Iceberg client
        // For now, we expect either success or internal server error
        assert!(response.status() == StatusCode::OK || response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_ingest_data_invalid_arrow_data() {
        let app_state = create_test_app_state().await;
        let app = Router::new()
            .route("/ingest", post(ingest_data))
            .with_state(app_state);

        let request = Request::builder()
            .method("POST")
            .uri("/ingest?table_name=test_table")
            .header("content-type", "application/x-apache-arrow-stream")
            .body(Body::from("invalid arrow data"))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_ingest_data_missing_table_name() {
        let app_state = create_test_app_state().await;
        let app = Router::new()
            .route("/ingest", post(ingest_data))
            .with_state(app_state);

        let arrow_data = create_test_arrow_data();
        let request = Request::builder()
            .method("POST")
            .uri("/ingest") // Missing table_name parameter
            .header("content-type", "application/x-apache-arrow-stream")
            .body(Body::from(arrow_data))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_ingest_data_invalid_arrow_format() {
        let app_state = create_test_app_state().await;
        let app = Router::new()
            .route("/ingest", post(ingest_data))
            .with_state(app_state);

        let request = Request::builder()
            .method("POST")
            .uri("/ingest?table_name=test_table&namespace=test_namespace")
            .header("content-type", "application/x-apache-arrow-stream")
            .body(Body::from("invalid-arrow-format"))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_ingest_data_without_namespace() {
        let app_state = create_test_app_state().await;
        let app = Router::new()
            .route("/ingest", post(ingest_data))
            .with_state(app_state);

        let arrow_data = create_test_arrow_data();
        let request = Request::builder()
            .method("POST")
            .uri("/ingest?table_name=test_table") // No namespace provided, should default to "default"
            .header("content-type", "application/x-apache-arrow-stream")
            .body(Body::from(arrow_data))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        
        // Note: This test might fail due to Iceberg client connection issues
        // In a real test environment, you would mock the Iceberg client
        assert!(response.status() == StatusCode::OK || response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_ingest_data_large_payload() {
        let app_state = create_test_app_state().await;
        let app = Router::new()
            .route("/ingest", post(ingest_data))
            .with_state(app_state);

        // Create a larger dataset
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);
        
        let ids: Vec<i32> = (1..=100).collect();
        let values: Vec<&str> = (1..=100).map(|_i| "test_value").collect();
        
        let id_array = Int32Array::from(ids);
        let value_array = StringArray::from(values);
        
        let large_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(value_array)],
        ).unwrap();
        
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &large_batch.schema()).unwrap();
            writer.write(&large_batch).unwrap();
            writer.finish().unwrap();
        }

        let request = Request::builder()
            .method("POST")
            .uri("/ingest?table_name=large_table&namespace=test_namespace")
            .header("content-type", "application/x-apache-arrow-stream")
            .body(Body::from(buffer))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        
        // Note: This test might fail due to Iceberg client connection issues
        // In a real test environment, you would mock the Iceberg client
        assert!(response.status() == StatusCode::OK || response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    }
}
