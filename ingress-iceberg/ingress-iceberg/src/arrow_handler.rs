use arrow::record_batch::RecordBatch;
use arrow::ipc::reader::StreamReader;
use std::io::Cursor;
use std::sync::Arc;
use base64::{Engine as _, engine::general_purpose};

#[derive(Clone)]
pub struct ArrowStreamHandler;

impl ArrowStreamHandler {
    pub fn new() -> Self {
        Self
    }

    pub async fn process_arrow_data(&self, base64_data: &str) -> anyhow::Result<RecordBatch> {
        // Decode base64 data
        let arrow_data = general_purpose::STANDARD
            .decode(base64_data)
            .map_err(|e| anyhow::anyhow!("Failed to decode base64 data: {}", e))?;

        // Create a cursor to read the Arrow data
        let cursor = Cursor::new(arrow_data);
        
        // Create a stream reader
        let mut reader = StreamReader::try_new(cursor, None)
            .map_err(|e| anyhow::anyhow!("Failed to create Arrow stream reader: {}", e))?;

        // Read the first (and typically only) record batch
        match reader.next() {
            Some(Ok(batch)) => Ok(batch),
            Some(Err(e)) => Err(anyhow::anyhow!("Failed to read Arrow record batch: {}", e)),
            None => Err(anyhow::anyhow!("No record batch found in Arrow stream")),
        }
    }

    pub async fn process_arrow_bytes(&self, arrow_bytes: &[u8]) -> anyhow::Result<RecordBatch> {
        // Create a cursor to read the Arrow data directly from bytes
        let cursor = Cursor::new(arrow_bytes);
        
        // Create a stream reader
        let mut reader = StreamReader::try_new(cursor, None)
            .map_err(|e| anyhow::anyhow!("Failed to create Arrow stream reader: {}", e))?;

        // Read the first (and typically only) record batch
        match reader.next() {
            Some(Ok(batch)) => Ok(batch),
            Some(Err(e)) => Err(anyhow::anyhow!("Failed to read Arrow record batch: {}", e)),
            None => Err(anyhow::anyhow!("No record batch found in Arrow stream")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray, BooleanArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::writer::StreamWriter;

    fn create_test_record_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]);

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);
        let active_array = BooleanArray::from(vec![true, false, true, true, false]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(active_array),
            ],
        ).unwrap()
    }

    fn create_arrow_stream_bytes(record_batch: &RecordBatch) -> Vec<u8> {
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &record_batch.schema()).unwrap();
            writer.write(record_batch).unwrap();
            writer.finish().unwrap();
        }
        buffer
    }

    #[tokio::test]
    async fn test_process_arrow_data_success() {
        let handler = ArrowStreamHandler::new();
        let test_batch = create_test_record_batch();
        let arrow_bytes = create_arrow_stream_bytes(&test_batch);
        let base64_data = general_purpose::STANDARD.encode(&arrow_bytes);

        let result = handler.process_arrow_data(&base64_data).await;
        
        assert!(result.is_ok());
        let processed_batch = result.unwrap();
        assert_eq!(processed_batch.num_rows(), 5);
        assert_eq!(processed_batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_process_arrow_data_invalid_base64() {
        let handler = ArrowStreamHandler::new();
        let invalid_base64 = "invalid-base64-data!@#";

        let result = handler.process_arrow_data(invalid_base64).await;
        
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("Failed to decode base64 data"));
    }

    #[tokio::test]
    async fn test_process_arrow_data_invalid_arrow_data() {
        let handler = ArrowStreamHandler::new();
        let invalid_arrow_data = "SGVsbG8gV29ybGQ="; // "Hello World" in base64

        let result = handler.process_arrow_data(invalid_arrow_data).await;
        
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("Failed to create Arrow stream reader"));
    }

    #[tokio::test]
    async fn test_process_arrow_data_empty_stream() {
        let handler = ArrowStreamHandler::new();
        // Create an empty arrow stream
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let empty_batch = RecordBatch::new_empty(Arc::new(schema));
        let arrow_bytes = create_arrow_stream_bytes(&empty_batch);
        let base64_data = general_purpose::STANDARD.encode(&arrow_bytes);

        let result = handler.process_arrow_data(&base64_data).await;
        
        assert!(result.is_ok());
        let processed_batch = result.unwrap();
        assert_eq!(processed_batch.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_process_arrow_data_large_dataset() {
        let handler = ArrowStreamHandler::new();
        
        // Create a larger dataset
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]);
        
        let ids: Vec<i32> = (1..=1000).collect();
        let values: Vec<&str> = (1..=1000).map(|_i| "test_value").collect();
        
        let id_array = Int32Array::from(ids);
        let value_array = StringArray::from(values);
        
        let large_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(id_array), Arc::new(value_array)],
        ).unwrap();
        
        let arrow_bytes = create_arrow_stream_bytes(&large_batch);
        let base64_data = general_purpose::STANDARD.encode(&arrow_bytes);

        let result = handler.process_arrow_data(&base64_data).await;
        
        assert!(result.is_ok());
        let processed_batch = result.unwrap();
        assert_eq!(processed_batch.num_rows(), 1000);
        assert_eq!(processed_batch.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_process_arrow_bytes_success() {
        let handler = ArrowStreamHandler::new();
        let test_batch = create_test_record_batch();
        let arrow_bytes = create_arrow_stream_bytes(&test_batch);

        let result = handler.process_arrow_bytes(&arrow_bytes).await;
        
        assert!(result.is_ok());
        let processed_batch = result.unwrap();
        assert_eq!(processed_batch.num_rows(), 5);
        assert_eq!(processed_batch.num_columns(), 3);
    }

    #[tokio::test]
    async fn test_process_arrow_bytes_invalid_data() {
        let handler = ArrowStreamHandler::new();
        let invalid_data = b"invalid arrow data";

        let result = handler.process_arrow_bytes(invalid_data).await;
        
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("Failed to create Arrow stream reader"));
    }

    #[tokio::test]
    async fn test_process_arrow_bytes_empty_data() {
        let handler = ArrowStreamHandler::new();
        let empty_data = b"";

        let result = handler.process_arrow_bytes(empty_data).await;
        
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("Failed to create Arrow stream reader"));
    }
}