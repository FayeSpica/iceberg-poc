# Spark Iceberg Integration

This directory contains the Spark Iceberg integration setup that connects to the REST catalog running in the parent directory.

## Files

- `docker-compose.yml` - Docker Compose configuration for Spark Iceberg
- `spark-defaults.conf` - Spark configuration for Iceberg integration

## Usage

1. First, start the main Iceberg services from the parent directory:
   ```bash
   cd ..
   docker-compose up -d
   ```

2. Then start the Spark Iceberg service:
   ```bash
   cd spark
   docker-compose up -d
   ```

3. Access Spark UI at http://localhost:8080

4. To run Spark SQL commands, exec into the container:
   ```bash
   docker exec -it spark-iceberg spark-sql
   ```

## Configuration

The Spark service is configured to:
- Connect to the REST catalog at `http://iceberg-rest:8181`
- Use S3-compatible storage (MinIO) for data storage
- Use the `default` catalog name
- Store data in the `s3://warehouse/` location

## Network

The Spark service connects to the same Docker network as the main Iceberg services to access the REST catalog and MinIO storage.
