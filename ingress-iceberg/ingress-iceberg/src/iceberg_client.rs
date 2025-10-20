use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use arrow::record_batch::RecordBatch;
use iceberg::arrow::writer::ArrowWriter;
use iceberg::catalog::{Catalog, CreateTableRequest, NamespaceIdent, TableIdentifier};
use iceberg::spec::{NestedField, PrimitiveType, Schema, StructType, Type};
use iceberg::table::Table;
use iceberg_rest_catalog::RestCatalog;
use url::Url;

#[derive(Clone)]
pub struct IcebergClient {
    catalog: Arc<RestCatalog>,
    warehouse_root: String,
}

impl IcebergClient {
    pub async fn new(base_url: String) -> anyhow::Result<Self> {
        let url = Url::parse(&base_url)
            .with_context(|| format!("Invalid REST catalog URL: {}", base_url))?;

        let catalog = RestCatalog::builder()
            .base_uri(url)
            .build()
            .await
            .context("Failed to create Iceberg REST catalog client")?;

        Ok(Self {
            catalog: Arc::new(catalog),
            warehouse_root: "s3://iceberg-data".to_string(),
        })
    }

    pub async fn ensure_namespace_exists(&self, namespace: &str) -> anyhow::Result<()> {
        let namespace_ident = NamespaceIdent::from_str(namespace)
            .with_context(|| format!("Invalid namespace identifier: {}", namespace))?;

        if !self
            .catalog
            .namespace_exists(&namespace_ident)
            .await
            .context("Failed to check namespace existence")?
        {
            self.catalog
                .create_namespace(&namespace_ident, HashMap::new())
                .await
                .context("Failed to create namespace")?;
        }

        Ok(())
    }

    pub async fn ensure_table_exists(
        &self,
        namespace: &str,
        table_name: &str,
        schema: &Schema,
    ) -> anyhow::Result<()> {
        self.ensure_namespace_exists(namespace).await?;

        let namespace_ident = NamespaceIdent::from_str(namespace)
            .with_context(|| format!("Invalid namespace identifier: {}", namespace))?;
        let table_ident = TableIdentifier::new(namespace_ident, table_name.to_string());

        if self
            .catalog
            .table_exists(&table_ident)
            .await
            .context("Failed to check table existence")?
        {
            return Ok(());
        }

        let mut properties = HashMap::new();
        properties.insert(
            "write.format.default".to_string(),
            "parquet".to_string(),
        );
        properties.insert(
            "write.metadata.metrics.default".to_string(),
            "truncate(16)".to_string(),
        );

        let request = CreateTableRequest::builder()
            .identifier(table_ident.clone())
            .schema(schema.clone())
            .location(self.default_table_location(namespace, table_name))
            .properties(properties)
            .build();

        self.catalog
            .create_table(request)
            .await
            .context("Failed to create Iceberg table")?;

        Ok(())
    }

    pub async fn write_to_table(
        &self,
        namespace: &str,
        table_name: &str,
        record_batch: RecordBatch,
    ) -> anyhow::Result<u64> {
        let iceberg_schema =
            self.convert_arrow_schema_to_iceberg(&record_batch.schema())?;

        self
            .ensure_table_exists(namespace, table_name, &iceberg_schema)
            .await?;

        let table_ident = TableIdentifier::from_str(&format!("{}.{}", namespace, table_name))
            .with_context(|| format!("Invalid table identifier for {}.{}", namespace, table_name))?;

        let table = self
            .catalog
            .load_table(&table_ident)
            .await
            .context("Failed to load Iceberg table")?;

        let mut writer = self.create_arrow_writer(&table, record_batch.schema().as_ref())?;
        writer.write(&record_batch)?;
        let summary = writer.close().await?;

        Ok(summary.rows_written())
    }

    fn convert_arrow_schema_to_iceberg(
        &self,
        arrow_schema: &arrow::datatypes::Schema,
    ) -> anyhow::Result<Schema> {
        let mut fields = Vec::new();
        let mut field_id = 1;

        for field in arrow_schema.fields() {
            let iceberg_type = self.map_arrow_type(field.data_type())?;

            let nested_field = if field.is_nullable() {
                NestedField::optional(field_id, field.name(), iceberg_type, None)
            } else {
                NestedField::required(field_id, field.name(), iceberg_type, None)
            };

            fields.push(nested_field);
            field_id += 1;
        }

        let struct_type = StructType::new(fields);
        Ok(Schema::builder().with_struct_type(struct_type).build())
    }

    fn map_arrow_type(&self, data_type: &arrow::datatypes::DataType) -> anyhow::Result<Type> {
        use arrow::datatypes::DataType;

        let primitive = match data_type {
            DataType::Int8 | DataType::Int16 | DataType::Int32 => PrimitiveType::Int,
            DataType::Int64 => PrimitiveType::Long,
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => PrimitiveType::Int,
            DataType::UInt64 => PrimitiveType::Long,
            DataType::Float32 => PrimitiveType::Float,
            DataType::Float64 => PrimitiveType::Double,
            DataType::Boolean => PrimitiveType::Boolean,
            DataType::Utf8 | DataType::LargeUtf8 => PrimitiveType::String,
            DataType::Binary | DataType::LargeBinary => PrimitiveType::Binary,
            DataType::Timestamp(_, _) => PrimitiveType::Timestamp,
            DataType::Date32 | DataType::Date64 => PrimitiveType::Date,
            _ => PrimitiveType::String,
        };

        Ok(Type::Primitive(primitive))
    }

    fn create_arrow_writer(
        &self,
        table: &Table,
        schema: &arrow::datatypes::Schema,
    ) -> anyhow::Result<ArrowWriter> {
        let io = table.io().clone();
        let location_generator = table.location_generator().clone();

        ArrowWriter::try_new(
            table.new_append(),
            schema,
            io,
            location_generator,
        )
        .context("Failed to create Iceberg Arrow writer")
    }

    fn default_table_location(&self, namespace: &str, table_name: &str) -> String {
        format!(
            "{}/{}/{}",
            self.warehouse_root,
            namespace.replace('.', "/"),
            table_name
        )
    }
}
