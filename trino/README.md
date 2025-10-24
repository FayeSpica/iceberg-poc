# Trino with Iceberg REST Catalog

This directory contains the Docker Compose configuration for running Trino with Apache Iceberg REST catalog integration.

## Architecture

- **Trino**: Distributed SQL query engine
- **Iceberg REST Catalog**: REST API for Iceberg table metadata
- **MinIO**: S3-compatible object storage for data files

## Services

- `trino`: Main Trino coordinator and worker
- `iceberg-rest`: Iceberg REST catalog service
- `minio`: S3-compatible object storage
- `create-bucket`: Initializes the warehouse bucket

## Quick Start

1. **Start the services:**
   ```bash
   docker-compose up -d
   ```

2. **Wait for services to be ready:**
   - Trino UI: http://localhost:18080
   - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)
   - Iceberg REST API: http://192.168.31.179:8181

3. **Connect to Trino:**
   ```bash
   docker exec -it trino-iceberg trino
   ```

4. **Create the default schema (if it doesn't exist):**
   ```sql
   CREATE SCHEMA IF NOT EXISTS iceberg.default;
   ```

## Configuration

### Trino Configuration
- **Coordinator**: Single node setup with coordinator included
- **Memory**: 2GB JVM heap
- **Port**: 18080 (HTTP server)

### Iceberg Catalog Configuration
- **Type**: REST catalog
- **URI**: http://192.168.31.179:8181
- **Warehouse**: s3://warehouse/
- **S3 Endpoint**: http://192.168.31.179:9000
- **Credentials**: minioadmin/minioadmin

## Usage Examples

### Create the default schema (required first step)
```sql
CREATE SCHEMA IF NOT EXISTS iceberg.default;
```

### Create a table
```sql
CREATE TABLE iceberg.default.test_table (
    id BIGINT,
    name VARCHAR,
    created_at TIMESTAMP
) WITH (
    location = 's3://warehouse/default/test_table'
);
```

### Insert data
```sql
INSERT INTO iceberg.default.test_table VALUES 
(1, 'Alice', TIMESTAMP '2024-01-01 10:00:00'),
(2, 'Bob', TIMESTAMP '2024-01-01 11:00:00');
```

### Query data
```sql
SELECT * FROM iceberg.default.test_table;
```

## Troubleshooting

### Schema Not Found Error
If you get `SchemaNotFoundException: Schema default not found`, follow these steps:

1. **Connect to Trino:**
   ```bash
   docker exec -it trino-iceberg trino
   ```

2. **Create the default schema:**
   ```sql
   CREATE SCHEMA IF NOT EXISTS iceberg.default;
   ```

3. **Verify the schema exists:**
   ```sql
   SHOW SCHEMAS FROM iceberg;
   ```

### General Troubleshooting

1. **Check service status:**
   ```bash
   docker-compose ps
   ```

2. **View logs:**
   ```bash
   docker-compose logs trino
   ```

3. **Restart services:**
   ```bash
   docker-compose restart
   ```

4. **Check Iceberg REST catalog connectivity:**
   ```bash
   curl http://192.168.31.179:8181/v1/config
   ```

## Cleanup

To stop and remove all containers and volumes:
```bash
docker-compose down -v
```
