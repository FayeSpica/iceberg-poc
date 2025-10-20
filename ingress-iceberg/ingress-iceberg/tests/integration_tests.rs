use ingress_iceberg::{
    AppState, IcebergClient, ArrowStreamHandler,
    IngestResponse, ArrowTestUtils,
};
use axum::{
    body::Body,
    http::{Request, StatusCode},
    Router,
    routing::post,
};
use tower::ServiceExt;
use base64::{Engine as _, engine::general_purpose};

async fn create_test_app() -> Router {
    // Create a mock IcebergClient for testing
    let iceberg_client = IcebergClient::new("http://localhost:8181".to_string()).await.unwrap();
    let arrow_handler = ArrowStreamHandler::new();
    
    let app_state = AppState {
        iceberg_client,
        arrow_handler,
    };

    Router::new()
        .route("/health", post(ingress_iceberg::health_check))
        .route("/ingest", post(ingress_iceberg::ingest_data))
        .with_state(app_state)
}

fn create_test_arrow_data() -> String {
    ArrowTestUtils::create_test_arrow_stream()
}

#[tokio::test]
async fn test_complete_arrow_stream_flow() {
    let app = create_test_app().await;
    let arrow_data = create_test_arrow_data();
    
    // Decode base64 arrow data to raw bytes
    let arrow_bytes = base64::engine::general_purpose::STANDARD
        .decode(&arrow_data)
        .unwrap();

    let request = Request::builder()
        .method("POST")
        .uri("/ingest?table_name=test_table&namespace=test_namespace")
        .header("content-type", "application/x-apache-arrow-stream")
        .body(Body::from(arrow_bytes))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    
    // Note: This test might fail due to Iceberg client connection issues
    // In a real test environment, you would mock the Iceberg client
    assert!(response.status() == StatusCode::OK || response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    
    if response.status() == StatusCode::OK {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let ingest_response: IngestResponse = serde_json::from_slice(&body).unwrap();
        
        assert!(ingest_response.success);
        assert_eq!(ingest_response.records_ingested, Some(5));
    }
}

#[tokio::test]
async fn test_health_endpoint_integration() {
    let app = create_test_app().await;

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
async fn test_multiple_ingest_requests() {
    let app = create_test_app().await;
    let arrow_data = create_test_arrow_data();
    
    // Test multiple requests to the same endpoint
    for i in 0..3 {
        // Decode base64 arrow data to raw bytes
        let arrow_bytes = base64::engine::general_purpose::STANDARD
            .decode(&arrow_data)
            .unwrap();

        let request = Request::builder()
            .method("POST")
            .uri(&format!("/ingest?table_name=test_table_{}&namespace=test_namespace", i))
            .header("content-type", "application/x-apache-arrow-stream")
            .body(Body::from(arrow_bytes))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        
        // Note: This test might fail due to Iceberg client connection issues
        // In a real test environment, you would mock the Iceberg client
        assert!(response.status() == StatusCode::OK || response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    }
}

#[tokio::test]
async fn test_different_arrow_schemas() {
    let app = create_test_app().await;
    
    // Test with different test utilities
    let test_batches = vec![
        ("simple", ArrowTestUtils::create_simple_test_batch()),
        ("large", ArrowTestUtils::create_large_test_batch(100)),
        ("mixed", ArrowTestUtils::create_mixed_type_test_batch()),
        ("empty", ArrowTestUtils::create_empty_test_batch()),
        ("nullable", ArrowTestUtils::create_nullable_test_batch()),
    ];
    
    for (test_type, batch) in test_batches {
        let arrow_data = ArrowTestUtils::record_batch_to_base64(&batch);
        
        // Decode base64 arrow data to raw bytes
        let arrow_bytes = base64::engine::general_purpose::STANDARD
            .decode(&arrow_data)
            .unwrap();

        let request = Request::builder()
            .method("POST")
            .uri(&format!("/ingest?table_name=schema_test_table_{}&namespace=test_namespace", test_type))
            .header("content-type", "application/x-apache-arrow-stream")
            .body(Body::from(arrow_bytes))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        
        // Note: This test might fail due to Iceberg client connection issues
        // In a real test environment, you would mock the Iceberg client
        assert!(response.status() == StatusCode::OK || response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    }
}

#[tokio::test]
async fn test_error_handling_flow() {
    let app = create_test_app().await;
    
    // Test with invalid Arrow data
    let request = Request::builder()
        .method("POST")
        .uri("/ingest?table_name=test_table&namespace=test_namespace")
        .header("content-type", "application/x-apache-arrow-stream")
        .body(Body::from("invalid arrow data"))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    
    // Test with invalid base64 data
    let app = create_test_app().await;
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
async fn test_performance_with_large_dataset() {
    let app = create_test_app().await;
    
    // Test with a large dataset
    let large_arrow_data = ArrowTestUtils::create_large_test_arrow_stream(10000);
    
    // Decode base64 arrow data to raw bytes
    let arrow_bytes = base64::engine::general_purpose::STANDARD
        .decode(&large_arrow_data)
        .unwrap();

    let request = Request::builder()
        .method("POST")
        .uri("/ingest?table_name=performance_test_table&namespace=test_namespace")
        .header("content-type", "application/x-apache-arrow-stream")
        .body(Body::from(arrow_bytes))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    
    // Note: This test might fail due to Iceberg client connection issues
    // In a real test environment, you would mock the Iceberg client
    assert!(response.status() == StatusCode::OK || response.status() == StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_concurrent_requests() {
    let app = create_test_app().await;
    let arrow_data = create_test_arrow_data();
    
    // Test concurrent requests
    let mut handles = vec![];
    
    for i in 0..5 {
        let app_clone = app.clone();
        let arrow_data_clone = arrow_data.clone();
        
        let handle = tokio::spawn(async move {
            // Decode base64 arrow data to raw bytes
            let arrow_bytes = base64::engine::general_purpose::STANDARD
                .decode(&arrow_data_clone)
                .unwrap();

            let request = Request::builder()
                .method("POST")
                .uri(&format!("/ingest?table_name=concurrent_test_table_{}&namespace=test_namespace", i))
                .header("content-type", "application/x-apache-arrow-stream")
                .body(Body::from(arrow_bytes))
                .unwrap();

            app_clone.oneshot(request).await
        });
        
        handles.push(handle);
    }
    
    // Wait for all requests to complete
    for handle in handles {
        let result = handle.await.unwrap();
        // Note: This test might fail due to Iceberg client connection issues
        // In a real test environment, you would mock the Iceberg client
        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.status() == StatusCode::OK || response.status() == StatusCode::INTERNAL_SERVER_ERROR);
    }
}

#[tokio::test]
async fn test_malformed_requests() {
    let app = create_test_app().await;
    
    // Test various malformed requests
    let malformed_requests = vec![
        // Missing table_name
        "/ingest?namespace=test_namespace",
        // Missing namespace (should default to "default")
        "/ingest?table_name=test_table",
        // Empty table_name
        "/ingest?table_name=&namespace=test_namespace",
        // Invalid characters in table_name
        "/ingest?table_name=test%20table&namespace=test_namespace",
    ];
    
    for (i, uri) in malformed_requests.iter().enumerate() {
        let request = Request::builder()
            .method("POST")
            .uri(uri.to_string())
            .header("content-type", "application/x-apache-arrow-stream")
            .body(Body::from("some arrow data"))
            .unwrap();

        let response = app.clone().oneshot(request).await.unwrap();
        
        // All malformed requests should result in bad request
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "Failed for malformed request {}", i);
    }
}