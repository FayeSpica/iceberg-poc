# Iceberg REST Catalog (Java via Docker)

使用官方镜像 `tabulario/iceberg-rest:1.6.0` 通过 docker-compose 启动 Iceberg REST Catalog。

## 启动（HadoopCatalog + 本地卷）

```bash
docker compose up -d
curl http://localhost:8181/v1/config
```

docker-compose.yml 中默认配置：

- `CATALOG_CATALOG__IMPL=org.apache.iceberg.hadoop.HadoopCatalog`
- `CATALOG_WAREHOUSE=file:///warehouse`

数据会持久化到本地卷 `warehouse_data` 并挂载到容器 `/warehouse`。

## 可选：使用 MinIO/S3 作为仓库

1) 启用 `minio` 服务（取消 docker-compose 中注释），并设置以下环境变量：

```
CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
CATALOG_WAREHOUSE=s3://warehouse/
CATALOG_S3_ENDPOINT=http://minio:9000
CATALOG_S3_PATH_STYLE_ACCESS=true
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
```

2) 访问 REST Catalog：

```bash
curl http://localhost:8181/v1/config
```

## 与 Trino 或 Spark 集成（示例）

Trino `etc/catalog/iceberg.properties`：

```
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
```

Spark 配置：

```
spark.sql.catalog.my=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.my.catalog-impl=org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.my.uri=http://localhost:8181
```