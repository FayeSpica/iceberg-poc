# Ingress Iceberg

A Rust HTTP server that receives ArrowStream format data and writes it to Apache Iceberg tables using a REST catalog.

## Features

- HTTP server for receiving Arrow data
- ArrowStream format support
- Integration with Iceberg REST catalog
- Docker Compose setup for local development
- Automatic table and namespace creation

## Prerequisites

- Rust 1.70+
- Docker and Docker Compose
- curl (for testing)

## Quick Start

1. **Start the Iceberg REST catalog and MinIO:**
   ```bash
   docker-compose up -d
   ```

2. **Build and run the ingress server:**
   ```bash
   cargo run
   ```

3. **Test the service:**
   ```bash
   # Health check
   curl -X POST http://localhost:3000/health

   # Ingest data (example with base64 encoded Arrow data)
   curl -X POST http://localhost:3000/ingest \
     -H "Content-Type: application/json" \
     -d '{
       "table_name": "test_table",
       "namespace": "default",
       "data": "base64_encoded_arrow_data_here"
     }'
   ```

## API Endpoints

### POST /health
Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "service": "ingress-iceberg"
}
```

### POST /ingest
Ingest Arrow data into an Iceberg table.

**Request Body:**
```json
{
  "table_name": "string",
  "namespace": "string (optional, defaults to 'default')",
  "data": "string (base64 encoded Arrow data)"
}
```

**Response:**
```json
{
  "success": true,
  "message": "Successfully ingested 1000 records",
  "records_ingested": 1000
}
```

## Configuration

The server connects to the Iceberg REST catalog at `http://localhost:8181` by default. You can modify this in the `main.rs` file.

## Development

### Project Structure

```
src/
├── main.rs              # HTTP server and main application
├── arrow_handler.rs     # Arrow data processing
└── iceberg_client.rs    # Iceberg REST catalog integration
```

### Dependencies

- **axum**: HTTP server framework
- **arrow**: Apache Arrow data processing
- **iceberg**/**iceberg-rest-catalog**: Iceberg table API and REST catalog client
- **serde**: Serialization/deserialization
- **tokio**: Async runtime

## Docker Services

- **rest-catalog**: Iceberg REST catalog server
- **minio**: S3-compatible object storage
- **mc**: MinIO client for bucket management

## Notes

- The current implementation is a foundation that demonstrates the architecture
- Full Iceberg integration requires additional work for:
  - Parquet file generation and upload
  - Snapshot management
  - Metadata updates
  - Partitioning support
- The REST catalog is configured to use MinIO as the underlying storage