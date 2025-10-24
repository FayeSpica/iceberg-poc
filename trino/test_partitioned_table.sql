-- 创建带分区的Iceberg表p1
-- 包含ts字段(timestamp)和隐藏分区hour()

-- 1. 首先确保默认schema存在
CREATE SCHEMA IF NOT EXISTS iceberg.default;

-- 2. 创建分区表p1
CREATE TABLE iceberg.default.p1 (
    id BIGINT,
    name VARCHAR,
    ts TIMESTAMP,
    value DOUBLE
) WITH (
    partitioning = ARRAY['hour(ts)'],
    location = 's3://warehouse/default/p1'
);

-- 3. 插入测试数据 - 不同小时的数据
-- 插入2024-01-01 10:00:00的数据
INSERT INTO iceberg.default.p1 VALUES 
(1, 'Alice', TIMESTAMP '2024-01-01 10:15:30', 100.5),
(2, 'Bob', TIMESTAMP '2024-01-01 10:45:20', 200.3),
(3, 'Charlie', TIMESTAMP '2024-01-01 10:30:45', 150.7);

-- 插入2024-01-01 11:00:00的数据
INSERT INTO iceberg.default.p1 VALUES 
(4, 'David', TIMESTAMP '2024-01-01 11:20:15', 300.1),
(5, 'Eve', TIMESTAMP '2024-01-01 11:35:40', 250.9),
(6, 'Frank', TIMESTAMP '2024-01-01 11:50:25', 180.2);

-- 插入2024-01-01 12:00:00的数据
INSERT INTO iceberg.default.p1 VALUES 
(7, 'Grace', TIMESTAMP '2024-01-01 12:10:30', 400.8),
(8, 'Henry', TIMESTAMP '2024-01-01 12:25:15', 350.4),
(9, 'Ivy', TIMESTAMP '2024-01-01 12:40:50', 220.6);

-- 插入2024-01-01 13:00:00的数据
INSERT INTO iceberg.default.p1 VALUES 
(10, 'Jack', TIMESTAMP '2024-01-01 13:05:20', 500.2),
(11, 'Kate', TIMESTAMP '2024-01-01 13:30:35', 450.7),
(12, 'Leo', TIMESTAMP '2024-01-01 13:55:10', 320.3);

-- 4. 查看表结构和分区信息
DESCRIBE iceberg.default.p1;

-- 5. 查看所有数据
SELECT * FROM iceberg.default.p1 ORDER BY ts;

-- 6. 测试分区裁剪 - 查询特定小时的数据
-- 查询10点的数据（应该只扫描10点的分区）
SELECT 
    id, 
    name, 
    ts, 
    value,
    EXTRACT(HOUR FROM ts) as hour
FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 10;

-- 查询11点的数据（应该只扫描11点的分区）
SELECT 
    id, 
    name, 
    ts, 
    value,
    EXTRACT(HOUR FROM ts) as hour
FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 11;

-- 查询12点的数据（应该只扫描12点的分区）
SELECT 
    id, 
    name, 
    ts, 
    value,
    EXTRACT(HOUR FROM ts) as hour
FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 12;

-- 7. 测试范围查询的分区裁剪
-- 查询10点到12点的数据（应该扫描10、11、12点的分区）
SELECT 
    id, 
    name, 
    ts, 
    value,
    EXTRACT(HOUR FROM ts) as hour
FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) BETWEEN 10 AND 12
ORDER BY ts;

-- 8. 查看分区统计信息
SELECT 
    EXTRACT(HOUR FROM ts) as hour,
    COUNT(*) as record_count,
    MIN(ts) as min_timestamp,
    MAX(ts) as max_timestamp
FROM iceberg.default.p1 
GROUP BY EXTRACT(HOUR FROM ts)
ORDER BY hour;

-- 9. 测试更复杂的分区裁剪查询
-- 查询特定时间范围的数据
SELECT 
    id, 
    name, 
    ts, 
    value
FROM iceberg.default.p1 
WHERE ts >= TIMESTAMP '2024-01-01 11:00:00' 
  AND ts < TIMESTAMP '2024-01-01 13:00:00'
ORDER BY ts;

-- 10. 查询分析和性能测试
-- 使用EXPLAIN查看查询计划
EXPLAIN (FORMAT TEXT) 
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 10;

-- 使用EXPLAIN ANALYZE查看实际执行统计
EXPLAIN ANALYZE 
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 10;

-- 使用EXPLAIN ANALYZE查看分区裁剪效果
EXPLAIN ANALYZE 
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) BETWEEN 10 AND 12;

-- 使用EXPLAIN ANALYZE查看全表扫描
EXPLAIN ANALYZE 
SELECT * FROM iceberg.default.p1;

-- 11. 查看表的分区信息
-- 显示表的分区结构
SHOW CREATE TABLE iceberg.default.p1;

-- 12. 查看查询统计信息
-- 使用EXPLAIN ANALYZE VERBOSE获取详细信息
EXPLAIN ANALYZE VERBOSE 
SELECT 
    EXTRACT(HOUR FROM ts) as hour,
    COUNT(*) as record_count
FROM iceberg.default.p1 
GROUP BY EXTRACT(HOUR FROM ts)
ORDER BY hour;

-- 13. 基于ts字段的各种过滤查询测试
-- 测试精确时间点查询
SELECT * FROM iceberg.default.p1 
WHERE ts = TIMESTAMP '2024-01-01 10:15:30';

-- 测试时间范围查询（应该只扫描相关分区）
SELECT * FROM iceberg.default.p1 
WHERE ts >= TIMESTAMP '2024-01-01 10:00:00' 
  AND ts < TIMESTAMP '2024-01-01 11:00:00'
ORDER BY ts;

-- 测试跨小时的时间范围查询
SELECT * FROM iceberg.default.p1 
WHERE ts >= TIMESTAMP '2024-01-01 10:30:00' 
  AND ts <= TIMESTAMP '2024-01-01 12:30:00'
ORDER BY ts;

-- 测试特定分钟范围查询
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(MINUTE FROM ts) BETWEEN 15 AND 45
ORDER BY ts;

-- 测试特定秒范围查询
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(SECOND FROM ts) >= 30
ORDER BY ts;

-- 14. 使用EXPLAIN ANALYZE测试不同过滤条件的分区裁剪效果
-- 单小时查询 - 应该只扫描一个分区
EXPLAIN ANALYZE 
SELECT * FROM iceberg.default.p1 
WHERE ts >= TIMESTAMP '2024-01-01 10:00:00' 
  AND ts < TIMESTAMP '2024-01-01 11:00:00';

-- 跨小时查询 - 应该扫描多个分区
EXPLAIN ANALYZE 
SELECT * FROM iceberg.default.p1 
WHERE ts >= TIMESTAMP '2024-01-01 10:00:00' 
  AND ts < TIMESTAMP '2024-01-01 13:00:00';

-- 全表扫描 - 应该扫描所有分区
EXPLAIN ANALYZE 
SELECT * FROM iceberg.default.p1;

-- 15. 测试复杂的时间过滤条件
-- 查询特定小时和分钟组合
SELECT * FROM iceberg.default.p1 
WHERE EXTRACT(HOUR FROM ts) = 11 
  AND EXTRACT(MINUTE FROM ts) >= 20
ORDER BY ts;

-- 查询特定时间模式
SELECT * FROM iceberg.default.p1 
WHERE ts::TIME BETWEEN TIME '10:00:00' AND TIME '12:59:59'
ORDER BY ts;

-- 查询最近N分钟的数据（相对于某个时间点）
SELECT * FROM iceberg.default.p1 
WHERE ts >= TIMESTAMP '2024-01-01 12:00:00' - INTERVAL '30' MINUTE
  AND ts <= TIMESTAMP '2024-01-01 12:00:00' + INTERVAL '30' MINUTE
ORDER BY ts;
