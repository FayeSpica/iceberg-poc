use arrow::record_batch::RecordBatch;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use anyhow::Result;

// Iceberg 0.7.0 compatible types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub namespace: Vec<String>,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    #[serde(rename = "type")]
    pub schema_type: String,
    pub fields: Vec<SchemaField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    #[serde(rename = "type")]
    pub field_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpec {
    #[serde(rename = "spec-id")]
    pub spec_id: i32,
    pub fields: Vec<PartitionField>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionField {
    #[serde(rename = "field-id")]
    pub field_id: i32,
    #[serde(rename = "source-id")]
    pub source_id: i32,
    pub name: String,
    #[serde(rename = "transform")]
    pub transform: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    #[serde(rename = "table-name")]
    pub table_name: String,
    pub namespace: Vec<String>,
    #[serde(rename = "table-type")]
    pub table_type: String,
    pub schema: Schema,
    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i32,
    #[serde(rename = "schemas")]
    pub schemas: HashMap<String, Schema>,
    #[serde(rename = "current-spec-id")]
    pub current_spec_id: i32,
    #[serde(rename = "specs")]
    pub specs: HashMap<String, PartitionSpec>,
    #[serde(rename = "last-partition-id")]
    pub last_partition_id: i32,
    #[serde(rename = "default-spec-id")]
    pub default_spec_id: i32,
    #[serde(rename = "last-assigned-field-id")]
    pub last_assigned_field_id: i32,
    pub properties: HashMap<String, String>,
    #[serde(rename = "current-snapshot-id")]
    pub current_snapshot_id: Option<i64>,
    #[serde(rename = "refs")]
    pub refs: HashMap<String, serde_json::Value>,
    #[serde(rename = "snapshot-log")]
    pub snapshot_log: Vec<serde_json::Value>,
    #[serde(rename = "metadata-log")]
    pub metadata_log: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct NamespaceList {
    pub namespaces: Vec<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TableList {
    #[serde(rename = "identifiers")]
    pub identifiers: Vec<TableIdentifier>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateTableRequest {
    pub name: String,
    pub location: String,
    pub schema: Schema,
    #[serde(rename = "partition-spec")]
    pub partition_spec: Vec<PartitionField>,
    #[serde(rename = "write-order")]
    pub write_order: WriteOrder,
    #[serde(rename = "stage-create")]
    pub stage_create: bool,
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WriteOrder {
    #[serde(rename = "order-id")]
    pub order_id: i32,
    pub fields: Vec<serde_json::Value>,
}

#[derive(Clone)]
pub struct IcebergClient {
    client: Client,
    base_url: String,
}

impl IcebergClient {
    /// Create a new Iceberg client with REST catalog support
    pub async fn new(base_url: String) -> Result<Self> {
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

    /// Ensure namespace exists, create if it doesn't
    pub async fn ensure_namespace_exists(&self, namespace: &str) -> Result<()> {
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
            let create_request = Namespace {
                namespace: namespace_parts,
                properties: HashMap::new(),
            };

            self.client
                .post(&format!("{}/v1/namespaces", self.base_url))
                .json(&create_request)
                .send()
                .await?;
        }

        Ok(())
    }

    /// Ensure table exists, create if it doesn't
    pub async fn ensure_table_exists(
        &self,
        namespace: &str,
        table_name: &str,
        schema: &Schema,
    ) -> Result<()> {
        self.ensure_namespace_exists(namespace).await?;

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
        let create_request = CreateTableRequest {
            name: table_name.to_string(),
            location: format!("s3://iceberg-data/{}/{}", namespace, table_name),
            schema: schema.clone(),
            partition_spec: vec![],
            write_order: WriteOrder {
                order_id: 0,
                fields: vec![],
            },
            stage_create: true,
            properties: HashMap::from([
                ("write.format.default".to_string(), "parquet".to_string()),
                ("write.metadata.metrics.default".to_string(), "truncate(16)".to_string()),
            ]),
        };

        self.client
            .post(&format!("{}/v1/namespaces/{}/tables", self.base_url, namespace))
            .json(&create_request)
            .send()
            .await?;

        Ok(())
    }

    /// Write data to an Iceberg table
    pub async fn write_to_table(
        &self,
        namespace: &str,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> Result<u64> {
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

    /// Convert Arrow schema to Iceberg schema
    fn convert_arrow_schema_to_iceberg(&self, arrow_schema: &arrow::datatypes::Schema) -> Result<Schema> {
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

            fields.push(SchemaField {
                id: field_id,
                name: field.name().clone(),
                required: !field.is_nullable(),
                field_type: iceberg_type.to_string(),
            });

            field_id += 1;
        }

        Ok(Schema {
            schema_type: "struct".to_string(),
            fields,
        })
    }

    /// Get table metadata
    pub async fn get_table_metadata(&self, namespace: &str, table_name: &str) -> Result<TableMetadata> {
        let response = self
            .client
            .get(&format!("{}/v1/namespaces/{}/tables/{}", self.base_url, namespace, table_name))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to get table metadata: {}",
                response.status()
            ));
        }

        let metadata: TableMetadata = response.json().await?;
        Ok(metadata)
    }

    /// List tables in a namespace
    pub async fn list_tables(&self, namespace: &str) -> Result<Vec<TableIdentifier>> {
        let response = self
            .client
            .get(&format!("{}/v1/namespaces/{}/tables", self.base_url, namespace))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to list tables: {}",
                response.status()
            ));
        }

        let table_list: TableList = response.json().await?;
        Ok(table_list.identifiers)
    }

    /// List namespaces
    pub async fn list_namespaces(&self) -> Result<Vec<Vec<String>>> {
        let response = self
            .client
            .get(&format!("{}/v1/namespaces", self.base_url))
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to list namespaces: {}",
                response.status()
            ));
        }

        let namespace_list: NamespaceList = response.json().await?;
        Ok(namespace_list.namespaces)
    }
}