# Fluss Python Bindings

Python bindings for the Fluss streaming data platform.

## Features

- Connect to Fluss servers
- Create and manage tables
- Write and read streaming data
- Support for various data types (int, string, float, etc.)

## Installation

```bash
pip install fluss-python
```

## Quick Start

```python
import fluss_python

# Create connection
config = fluss_python.PyConnectionConfig("127.0.0.1:9123")
connection = fluss_python.PyFlussConnection.new(config)

# Create table
admin = connection.get_admin()
table_path = fluss_python.PyTablePath("fluss", "test_table")

# Define schema
schema = fluss_python.PySchema()
schema.add_column("id", "int")
schema.add_column("name", "string")

# Create table descriptor and table
table_desc = fluss_python.PyTableDescriptor(schema)
admin.create_table(table_path, table_desc, True)

# Get table info
table_info = admin.get_table(table_path)
print(f"Table created: {table_info}")
```

## Supported Data Types

- `int` - 32-bit integer
- `bigint` - 64-bit integer  
- `float` - 32-bit floating point
- `double` - 64-bit floating point
- `string` - Variable length string
- `boolean` - Boolean values
- `bytes` - Binary data
- `decimal(precision, scale)` - Fixed precision decimal
- `char(length)` - Fixed length string
- `binary(length)` - Fixed length binary

## Requirements

- Python 3.7+
- Fluss server running on accessible network

## Development

This package is built using PyO3 and maturin. To build from source:

```bash
git clone <repository>
cd fluss-rust/python-bindings
maturin develop
```
- maturin

### Build and Install

```bash
cd python-bindings

# Install maturin
pip install maturin

# Build and install in development mode
maturin develop

# Or build and install normally
pip install .
```

For development:
```bash
maturin develop
```

## Usage

```python
import asyncio
from fluss_python import (
    PyConnectionConfig, 
    PyFlussConnection, 
    PyTablePath, 
    PySchema, 
    PyTableDescriptor
)

async def main():
    # Create connection configuration
    config = PyConnectionConfig("127.0.0.1:9123", 30)
    
    # Create connection
    conn = await PyFlussConnection.new(config)
    
    # Get admin interface
    admin = conn.get_admin()
    
    # Create schema
    schema = PySchema()
    schema.add_column("c1", "int")
    schema.add_column("c2", "string")
    
    # Create table descriptor
    table_descriptor = PyTableDescriptor(schema)
    
    # Create table path
    table_path = PyTablePath("fluss", "python_test")
    
    # Create table
    await admin.create_table(table_path, table_descriptor, True)
    
    # Get table info
    table_info = await admin.get_table(table_path)
    print(f"Created table: {table_info}")
    
    # Get table for operations
    table = await conn.get_table(table_path)
    
    # Create append writer
    writer = table.new_append()
    
    # Create log scanner
    scanner = table.new_scan()
    await scanner.subscribe(0, 0)
    
    # Poll for records
    records = await scanner.poll(10)
    for record in records:
        print(record)

if __name__ == "__main__":
    asyncio.run(main())
```

## Features

- **Connection Management**: Configure and manage connections to Fluss servers
- **Table Operations**: Create, query, and manage tables
- **Data Writing**: Append data to tables
- **Data Reading**: Scan and poll for records
- **Schema Management**: Define and work with table schemas
- **Async Support**: Full async/await support using asyncio

## API Reference

### PyConnectionConfig

Configuration for connecting to a Fluss server.

```python
config = PyConnectionConfig(bootstrap_server="127.0.0.1:9123", rw_timeout_secs=30)
```

### PyFlussConnection

Main connection class for interacting with Fluss.

```python
conn = await PyFlussConnection.new(config)
admin = conn.get_admin()
table = await conn.get_table(table_path)
```

### PyTablePath

Represents a table path (database.table).

```python
table_path = PyTablePath("database_name", "table_name")
```

### PySchema

Schema builder for defining table structure.

```python
schema = PySchema()
schema.add_column("column_name", "int")  # Supported types: int, string, bigint, float, double, boolean
```

### PyTableDescriptor

Table descriptor containing schema and other metadata.

```python
descriptor = PyTableDescriptor(schema)
```

### PyFlussAdmin

Administrative operations interface.

```python
admin = conn.get_admin()
await admin.create_table(table_path, descriptor, ignore_if_exists=True)
table_info = await admin.get_table(table_path)
```

### PyTable

Table interface for data operations.

```python
table = await conn.get_table(table_path)
writer = table.new_append()
scanner = table.new_scan()
```

### PyAppendWriter

Writer for appending data to tables.

```python
writer = table.new_append()
# Note: Data format needs to be implemented based on your specific needs
```

### PyLogScanner

Scanner for reading log records from tables.

```python
scanner = table.new_scan()
await scanner.subscribe(bucket=0, offset=0)
records = await scanner.poll(timeout_secs=10)
```