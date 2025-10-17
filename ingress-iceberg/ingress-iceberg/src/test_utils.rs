use arrow::{
    array::{Array, Int32Array, StringArray, BooleanArray, Float64Array, Date32Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
    ipc::writer::StreamWriter,
};
use base64::{Engine as _, engine::general_purpose};
use std::sync::Arc;

/// Test utilities for creating mock Arrow data
pub struct ArrowTestUtils;

impl ArrowTestUtils {
    /// Create a simple test record batch with id, name, and active columns
    pub fn create_simple_test_batch() -> RecordBatch {
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

    /// Create a large test record batch for performance testing
    pub fn create_large_test_batch(row_count: usize) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("score", DataType::Float64, false),
        ]);

        let ids: Vec<i32> = (1..=row_count as i32).collect();
        let values: Vec<&str> = (1..=row_count).map(|_i| "test_value").collect();
        let scores: Vec<f64> = (1..=row_count).map(|i| i as f64 * 0.1).collect();

        let id_array = Int32Array::from(ids);
        let value_array = StringArray::from(values);
        let score_array = Float64Array::from(scores);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(value_array),
                Arc::new(score_array),
            ],
        ).unwrap()
    }

    /// Create a test record batch with different data types
    pub fn create_mixed_type_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
            Field::new("score", DataType::Float64, false),
            Field::new("date", DataType::Date32, false),
        ]);

        let id_array = Int32Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let active_array = BooleanArray::from(vec![true, false, true]);
        let score_array = Float64Array::from(vec![95.5, 87.2, 91.8]);
        let date_array = Date32Array::from(vec![19000, 19001, 19002]); // Days since epoch

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(active_array),
                Arc::new(score_array),
                Arc::new(date_array),
            ],
        ).unwrap()
    }

    /// Create an empty record batch for testing edge cases
    pub fn create_empty_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let id_array = Int32Array::from(Vec::<i32>::new());
        let name_array = StringArray::from(Vec::<&str>::new());

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
            ],
        ).unwrap()
    }

    /// Create a record batch with nullable fields
    pub fn create_nullable_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),  // nullable
            Field::new("name", DataType::Utf8, true), // nullable
        ]);

        let id_array = Int32Array::from(vec![Some(1), None, Some(3)]);
        let name_array = StringArray::from(vec![Some("Alice"), None, Some("Charlie")]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
            ],
        ).unwrap()
    }

    /// Convert a record batch to base64 encoded Arrow stream
    pub fn record_batch_to_base64(record_batch: &RecordBatch) -> String {
        let mut buffer = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buffer, &record_batch.schema()).unwrap();
            writer.write(record_batch).unwrap();
            writer.finish().unwrap();
        }
        general_purpose::STANDARD.encode(&buffer)
    }

    /// Create a test record batch and return it as base64 encoded Arrow stream
    pub fn create_test_arrow_stream() -> String {
        let batch = Self::create_simple_test_batch();
        Self::record_batch_to_base64(&batch)
    }

    /// Create a large test arrow stream for performance testing
    pub fn create_large_test_arrow_stream(row_count: usize) -> String {
        let batch = Self::create_large_test_batch(row_count);
        Self::record_batch_to_base64(&batch)
    }

    /// Create test data with specific schema
    pub fn create_custom_test_batch(schema: Schema, data: Vec<Arc<dyn Array>>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(schema), data).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_simple_test_batch() {
        let batch = ArrowTestUtils::create_simple_test_batch();
        assert_eq!(batch.num_rows(), 5);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_create_large_test_batch() {
        let batch = ArrowTestUtils::create_large_test_batch(1000);
        assert_eq!(batch.num_rows(), 1000);
        assert_eq!(batch.num_columns(), 3);
    }

    #[test]
    fn test_create_empty_test_batch() {
        let batch = ArrowTestUtils::create_empty_test_batch();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_record_batch_to_base64() {
        let batch = ArrowTestUtils::create_simple_test_batch();
        let base64 = ArrowTestUtils::record_batch_to_base64(&batch);
        assert!(!base64.is_empty());
        // Verify it's valid base64
        let decoded = general_purpose::STANDARD.decode(&base64);
        assert!(decoded.is_ok());
    }

    #[test]
    fn test_create_test_arrow_stream() {
        let stream = ArrowTestUtils::create_test_arrow_stream();
        assert!(!stream.is_empty());
    }
}