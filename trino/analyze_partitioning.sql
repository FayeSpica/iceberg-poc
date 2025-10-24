-- 分析Trino查询Iceberg分区表的输出分区情况
-- 解释为什么会出现 Output partitioning: SINGLE []

-- 1. 首先创建测试数据
CREATE SCHEMA IF NOT EXISTS iceberg.default;

-- 创建分区表
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
(2, 'Bob', TIMESTAMP '2024-01-01 10:45:20', 200.3),
(3, 'Charlie', TIMESTAMP '2024-01-01 11:20:15', 300.1),
(4, 'David', TIMESTAMP '2024-01-01 11:35:40', 250.9),
(5, 'Eve', TIMESTAMP '2024-01-01 12:10:30', 400.8),
(6, 'Frank', TIMESTAMP '2024-01-01 12:25:15', 350.4);

-- 2. 分析不同查询的输出分区情况

-- 情况1: 简单SELECT查询 - 通常输出SINGLE分区
EXPLAIN (FORMAT TEXT)
SELECT * FROM iceberg.default.p1;

-- 情况2: 带WHERE条件的查询 - 可能输出SINGLE分区
EXPLAIN (FORMAT TEXT)
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 10;

-- 情况3: 聚合查询 - 通常输出SINGLE分区
EXPLAIN (FORMAT TEXT)
SELECT 
    EXTRACT(HOUR FROM ts) as hour,
    COUNT(*) as cnt
FROM iceberg.default.p1 
GROUP BY EXTRACT(HOUR FROM ts);

-- 情况4: 排序查询 - 可能输出SINGLE分区
EXPLAIN (FORMAT TEXT)
SELECT * FROM iceberg.default.p1 
ORDER BY ts;

-- 情况5: 限制行数的查询 - 通常输出SINGLE分区
EXPLAIN (FORMAT TEXT)
SELECT * FROM iceberg.default.p1 
LIMIT 10;

-- 3. 分析为什么会出现SINGLE分区

-- 查看表的元数据信息
SHOW CREATE TABLE iceberg.default.p1;

-- 查看表统计信息
SHOW STATS FOR iceberg.default.p1;

-- 4. 测试可能导致多分区输出的查询

-- 情况6: 窗口函数查询 - 可能保持分区
EXPLAIN (FORMAT TEXT)
SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(HOUR FROM ts) ORDER BY ts) as rn
FROM iceberg.default.p1;

-- 情况7: 分组查询 - 可能保持分区
EXPLAIN (FORMAT TEXT)
SELECT 
    EXTRACT(HOUR FROM ts) as hour,
    COUNT(*) as cnt
FROM iceberg.default.p1 
GROUP BY EXTRACT(HOUR FROM ts);

-- 5. 使用EXPLAIN ANALYZE查看实际执行情况
EXPLAIN ANALYZE
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 10;

-- 6. 查看查询计划中的分区信息
EXPLAIN (FORMAT JSON)
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 10;

-- 7. 测试复杂查询的分区情况
EXPLAIN (FORMAT TEXT)
SELECT 
    EXTRACT(HOUR FROM ts) as hour,
    AVG(value) as avg_value,
    COUNT(*) as record_count
FROM iceberg.default.p1 
WHERE ts >= TIMESTAMP '2024-01-01 10:00:00'
  AND ts < TIMESTAMP '2024-01-01 13:00:00'
GROUP BY EXTRACT(HOUR FROM ts)
ORDER BY hour;
