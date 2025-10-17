use arrow::record_batch::RecordBatch;
use arrow::ipc::reader::StreamReader;
use std::io::Cursor;
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
}