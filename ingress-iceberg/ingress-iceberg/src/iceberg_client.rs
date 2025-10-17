use arrow::record_batch::RecordBatch;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
struct TableMetadata {
    #[serde(rename = "table-name")]
    table_name: String,
    namespace: Vec<String>,
    #[serde(rename = "table-type")]
    table_type: String,
    schema: serde_json::Value,
    #[serde(rename = "current-schema-id")]
    current_schema_id: i32,
    #[serde(rename = "schemas")]
    schemas: HashMap<String, serde_json::Value>,
    #[serde(rename = "current-spec-id")]
    current_spec_id: i32,
    #[serde(rename = "specs")]
    specs: HashMap<String, serde_json::Value>,
    #[serde(rename = "last-partition-id")]
    last_partition_id: i32,
    #[serde(rename = "default-spec-id")]
    default_spec_id: i32,
    #[serde(rename = "last-assigned-field-id")]
    last_assigned_field_id: i32,
    properties: HashMap<String, String>,
    #[serde(rename = "current-snapshot-id")]
    current_snapshot_id: Option<i64>,
    #[serde(rename = "refs")]
    refs: HashMap<String, serde_json::Value>,
    #[serde(rename = "snapshot-log")]
    snapshot_log: Vec<serde_json::Value>,
    #[serde(rename = "metadata-log")]
    metadata_log: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NamespaceList {
    namespaces: Vec<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableList {
    #[serde(rename = "identifiers")]
    identifiers: Vec<serde_json::Value>,
}

#[derive(Clone)]
pub struct IcebergClient {
    client: Client,
    base_url: String,
}

impl IcebergClient {
    pub async fn new(base_url: String) -> anyhow::Result<Self> {
        let client = Client::new();
        
        // Test connection to REST catalog
        let response = client
            .get(&format!("{}/v1/config", base_url))
            .send()
            .await?;
        
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to connect to Iceberg REST catalog at {}: {}",
                base_url,
                response.status()
            ));
        }

        Ok(Self { client, base_url })
    }

    pub async fn ensure_namespace_exists(&self, namespace: &str) -> anyhow::Result<()> {
        let namespaces: NamespaceList = self
            .client
            .get(&format!("{}/v1/namespaces", self.base_url))
            .send()
            .await?
            .json()
            .await?;

        let namespace_parts: Vec<String> = namespace.split('.').map(|s| s.to_string()).collect();
        
        if !namespaces.namespaces.contains(&namespace_parts) {
            // Create namespace
            let create_request = serde_json::json!({
                "namespace": namespace_parts,
                "properties": {}
            });

            self.client
                .post(&format!("{}/v1/namespaces", self.base_url))
                .json(&create_request)
                .send()
                .await?;
        }

        Ok(())
    }

    pub async fn ensure_table_exists(
        &self,
        namespace: &str,
        table_name: &str,
        schema: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.ensure_namespace_exists(namespace).await?;

        let _table_identifier = format!("{}.{}", namespace, table_name);
        
        // Check if table exists
        let response = self
            .client
            .head(&format!("{}/v1/namespaces/{}/tables/{}", self.base_url, namespace, table_name))
            .send()
            .await?;

        if response.status().is_success() {
            return Ok(());
        }

        // Create table if it doesn't exist
        let create_request = serde_json::json!({
            "name": table_name,
            "location": format!("s3://iceberg-data/{}/{}", namespace, table_name),
            "schema": schema,
            "partition-spec": [],
            "write-order": {
                "order-id": 0,
                "fields": []
            },
            "stage-create": true,
            "properties": {
                "write.format.default": "parquet",
                "write.metadata.metrics.default": "truncate(16)"
            }
        });

        self.client
            .post(&format!("{}/v1/namespaces/{}/tables", self.base_url, namespace))
            .json(&create_request)
            .send()
            .await?;

        Ok(())
    }

    pub async fn write_to_table(
        &self,
        namespace: &str,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> anyhow::Result<u64> {
        // Convert Arrow schema to Iceberg schema
        let iceberg_schema = self.convert_arrow_schema_to_iceberg(&record_batch.schema())?;
        
        // Ensure table exists
        self.ensure_table_exists(namespace, table_name, &iceberg_schema).await?;

        // For now, we'll just return the number of rows
        // In a real implementation, you would:
        // 1. Convert the RecordBatch to Parquet format
        // 2. Upload the Parquet file to S3
        // 3. Create a new snapshot with the new data file
        // 4. Update the table metadata

        let row_count = record_batch.num_rows() as u64;
        
        // TODO: Implement actual data writing to Iceberg
        // This is a placeholder implementation
        tracing::info!(
            "Would write {} rows to table {}.{}",
            row_count,
            namespace,
            table_name
        );

        Ok(row_count)
    }

    fn convert_arrow_schema_to_iceberg(&self, arrow_schema: &arrow::datatypes::Schema) -> anyhow::Result<serde_json::Value> {
        let mut fields = Vec::new();
        let mut field_id = 1;

        for field in arrow_schema.fields() {
            let iceberg_type = match field.data_type() {
                arrow::datatypes::DataType::Int8 => "int",
                arrow::datatypes::DataType::Int16 => "int",
                arrow::datatypes::DataType::Int32 => "int",
                arrow::datatypes::DataType::Int64 => "long",
                arrow::datatypes::DataType::UInt8 => "int",
                arrow::datatypes::DataType::UInt16 => "int",
                arrow::datatypes::DataType::UInt32 => "int",
                arrow::datatypes::DataType::UInt64 => "long",
                arrow::datatypes::DataType::Float32 => "float",
                arrow::datatypes::DataType::Float64 => "double",
                arrow::datatypes::DataType::Utf8 => "string",
                arrow::datatypes::DataType::LargeUtf8 => "string",
                arrow::datatypes::DataType::Boolean => "boolean",
                arrow::datatypes::DataType::Date32 => "date",
                arrow::datatypes::DataType::Date64 => "timestamp",
                arrow::datatypes::DataType::Timestamp(_, _) => "timestamp",
                _ => "string", // Default fallback
            };

            fields.push(serde_json::json!({
                "id": field_id,
                "name": field.name(),
                "required": !field.is_nullable(),
                "type": iceberg_type
            }));

            field_id += 1;
        }

        Ok(serde_json::json!({
            "type": "struct",
            "fields": fields
        }))
    }
}