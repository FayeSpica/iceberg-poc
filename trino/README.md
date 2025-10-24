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

### Create partitioned table with hour() partition
```sql
-- 创建带小时分区的表
CREATE TABLE iceberg.default.p1 (
    id BIGINT,
    name VARCHAR,
    ts TIMESTAMP,
    value DOUBLE
) WITH (
    partitioning = ARRAY['hour(ts)'],
    location = 's3://warehouse/default/p1'
);

-- 插入测试数据
INSERT INTO iceberg.default.p1 VALUES 
(1, 'Alice', TIMESTAMP '2024-01-01 10:15:30', 100.5),
(2, 'Bob', TIMESTAMP '2024-01-01 11:20:15', 200.3),
(3, 'Charlie', TIMESTAMP '2024-01-01 12:30:45', 150.7);

-- 测试分区裁剪 - 只查询特定小时的数据
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 10;

-- 基于ts字段的时间范围过滤
SELECT * FROM iceberg.default.p1 
WHERE ts >= TIMESTAMP '2024-01-01 10:00:00' 
  AND ts < TIMESTAMP '2024-01-01 11:00:00';

-- 精确时间点查询
SELECT * FROM iceberg.default.p1 
WHERE ts = TIMESTAMP '2024-01-01 10:15:30';

-- 跨小时时间范围查询
SELECT * FROM iceberg.default.p1 
WHERE ts >= TIMESTAMP '2024-01-01 10:30:00' 
  AND ts <= TIMESTAMP '2024-01-01 12:30:00';
```

### Run the complete test script
```bash
# 执行完整的分区表测试脚本
docker exec -i trino-iceberg trino < test_partitioned_table.sql
```

## Query Analysis and Performance

Trino provides several tools for query analysis and optimization:

### EXPLAIN - Query Plan Analysis
```sql
-- 查看查询计划
EXPLAIN (FORMAT TEXT) 
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 10;

-- 查看JSON格式的查询计划
EXPLAIN (FORMAT JSON) 
SELECT * FROM iceberg.default.p1;
```

### EXPLAIN ANALYZE - Execution Statistics
```sql
-- 查看实际执行统计（类似PostgreSQL的EXPLAIN ANALYZE）
EXPLAIN ANALYZE 
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 10;

-- 查看详细执行信息
EXPLAIN ANALYZE VERBOSE 
SELECT * FROM iceberg.default.p1;
```

### Performance Monitoring
```sql
-- 查看表结构（包括分区信息）
SHOW CREATE TABLE iceberg.default.p1;

-- 查看表统计信息
SHOW STATS FOR iceberg.default.p1;
```

### Key Metrics in EXPLAIN ANALYZE:
- **CPU Time**: 实际CPU使用时间
- **Wall Time**: 总执行时间
- **Input Rows**: 输入行数
- **Output Rows**: 输出行数
- **Input Size**: 输入数据大小
- **Output Size**: 输出数据大小
- **Partition Pruning**: 分区裁剪效果

## Understanding Output Partitioning

### Why "Output partitioning: SINGLE []" appears:

The `Output partitioning: SINGLE []` in Trino query plans is **normal and expected** for most queries. Here's why:

#### 1. **Query Type Matters**
```sql
-- Simple SELECT - Usually SINGLE partition
EXPLAIN SELECT * FROM iceberg.default.p1;

-- Aggregation - Usually SINGLE partition  
EXPLAIN SELECT COUNT(*) FROM iceberg.default.p1;

-- ORDER BY - Usually SINGLE partition
EXPLAIN SELECT * FROM iceberg.default.p1 ORDER BY ts;
```

#### 2. **Partition Pruning vs Output Partitioning**
- **Input Partitioning**: How data is read from partitions (this is where pruning happens)
- **Output Partitioning**: How results are distributed to workers

#### 3. **When You Might See Multiple Output Partitions**
```sql
-- Window functions with PARTITION BY
EXPLAIN SELECT 
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(HOUR FROM ts) ORDER BY ts) as rn
FROM iceberg.default.p1;

-- Large result sets that need distribution
EXPLAIN SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) BETWEEN 10 AND 12;
```

#### 4. **What This Means for Performance**
- `SINGLE []` doesn't mean poor performance
- Partition pruning still works at the **input level**
- The query engine optimizes based on data size and complexity

### Analyze Partitioning Behavior
```bash
# Run the partitioning analysis script
docker exec -i trino-iceberg trino < analyze_partitioning.sql
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
