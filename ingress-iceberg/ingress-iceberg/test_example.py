#!/usr/bin/env python3
"""
Example script to test the ingress-iceberg service.
This script creates sample Arrow data and sends it to the service.
"""

import pyarrow as pa
import pyarrow.ipc as ipc
import base64
import requests
import json

def create_sample_data():
    """Create sample Arrow data for testing."""
    # Create sample data
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'age': [25, 30, 35, 28, 32],
        'city': ['New York', 'London', 'Tokyo', 'Paris', 'Sydney']
    }
    
    # Create Arrow table
    table = pa.table(data)
    
    # Convert to Arrow IPC format
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    
    # Get the Arrow data as bytes
    arrow_data = sink.getvalue().to_pybytes()
    
    # Encode as base64
    base64_data = base64.b64encode(arrow_data).decode('utf-8')
    
    return base64_data

def test_health_check():
    """Test the health check endpoint."""
    print("Testing health check...")
    response = requests.post("http://localhost:3000/health")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    print()

def test_ingest_data():
    """Test the data ingestion endpoint."""
    print("Testing data ingestion...")
    
    # Create sample Arrow data
    arrow_data = create_sample_data()
    
    # Prepare the request
    payload = {
        "table_name": "test_table",
        "namespace": "default",
        "data": arrow_data
    }
    
    # Send the request
    response = requests.post(
        "http://localhost:3000/ingest",
        json=payload,
        headers={"Content-Type": "application/json"}
    )
    
    print(f"Status: {response.status_code}")
    print(f"Response: {response.json()}")
    print()

if __name__ == "__main__":
    print("Testing ingress-iceberg service...")
    print("=" * 50)
    
    try:
        test_health_check()
        test_ingest_data()
        print("Tests completed successfully!")
    except requests.exceptions.ConnectionError:
        print("Error: Could not connect to the service.")
        print("Make sure the service is running on http://localhost:3000")
    except Exception as e:
        print(f"Error: {e}")