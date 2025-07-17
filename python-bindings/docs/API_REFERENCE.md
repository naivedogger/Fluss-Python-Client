# Fluss Python Bindings API Reference

## Overview

The Fluss Python bindings provide a comprehensive interface to interact with Fluss streaming data processing capabilities from Python. This document describes all available classes, methods, and functions based on the actual implementation.

## Installation

```bash
cd python-bindings
maturin develop
```

## Core Classes

### PyConnectionConfig

Configuration for connecting to Fluss server.

```python
config = fluss.PyConnectionConfig("127.0.0.1:9123", 30)
```

**Methods:**
- `__init__(bootstrap_server: str, rw_timeout_secs: Optional[int] = None)`
- `bootstrap_server` (property): Get bootstrap server address

### PyTablePath

Represents a table path with database and table name.

```python
table_path = fluss.PyTablePath("fluss", "my_table")
```

**Methods:**
- `__init__(database: str, table: str)`
- `database` (property): Get database name
- `table` (property): Get table name
- `__str__()`: String representation (database.table)

### PySchema

Schema definition for tables.

```python
schema = fluss.PySchema()
schema.add_column("id", "bigint")
schema.add_column("name", "string")
```

**Methods:**
- `__init__()`
- `add_column(name: str, data_type: str) -> None`
- `get_columns() -> List[Tuple[str, str]]`
- `get_column_names() -> List[str]`
- `column_count() -> int`

**Supported Data Types:**
- `int`, `bigint`, `tinyint`, `smallint`
- `float`, `double`
- `string`, `char`, `binary`, `bytes`
- `boolean`
- `decimal`, `timestamp`, `date`, `time`, `timestamp_ltz`
- `array`, `map`, `row`

### PyTableDescriptor

Table descriptor for table creation.

```python
descriptor = fluss.PyTableDescriptor(schema)
```

**Methods:**
- `__init__(schema: PySchema)`

## Connection Management

### PyFlussConnection

Main connection class for interacting with Fluss.

```python
conn = fluss.PyFlussConnection.new(config)
```

**Methods:**
- `new(config: PyConnectionConfig) -> PyFlussConnection`
- `get_admin() -> PyFlussAdmin`
- `get_table(table_path: PyTablePath) -> PyTable`
- `is_connected() -> bool`
- `close() -> None`

## Administrative Operations

### PyFlussAdmin

Administrative client for table management.

```python
admin = conn.get_admin()
```

**Methods:**
- `create_table(table_path: PyTablePath, descriptor: PyTableDescriptor, ignore_if_exists: bool = False) -> None`
- `drop_table(table_path: PyTablePath, ignore_if_not_exists: bool = False) -> None`
- `table_exists(table_path: PyTablePath) -> bool`
- `get_table(table_path: PyTablePath) -> PyTableInfo`

### PyTableInfo

Information about a table.

```python
table_info = admin.get_table(table_path)
```

**Properties:**
- `table_path`: Table path
- `created_at`: Creation timestamp
- `row_count`: Number of rows

## Table Operations

### PyTable

Table representation with schema access and data operations.

```python
table = conn.get_table(table_path)
```

**Methods:**
- `get_schema() -> PySchema`: Get real schema from server
- `new_append() -> PyAppendWriter`: Create writer for appending data
- `new_scan() -> PyLogScanner`: Create scanner for reading data
- `new_batch_scan() -> PyBatchScanner`: Create batch scanner

### PyAppendWriter

Writer for appending data to tables.

```python
writer = table.new_append()
```

**Methods:**
- `append_row(row_data: Dict[str, Any]) -> PyWriteResult`
- `append_batch(batch_data: List[Dict[str, Any]]) -> PyWriteResult`
- `flush() -> None`
- `close() -> None`

### PyLogScanner

Scanner for reading log data.

```python
scanner = table.new_scan()
```

**Methods:**
- `subscribe(bucket: int, offset: int) -> None`
- `poll(timeout_secs: int) -> List[PyLogRecord]`
- `seek(bucket: int, offset: int) -> None`
- `close() -> None`

### PyBatchScanner

Scanner for reading batch data.

```python
scanner = table.new_batch_scan()
```

**Methods:**
- `scan(limit: int) -> List[PyBatchRecord]`
- `close() -> None`

## Data Types

### PyWriteResult

Result of write operations.

```python
result = writer.append_row(data)
```

**Properties:**
- `rows_written`: Number of rows written

### PyLogRecord

Log record representation.

```python
records = scanner.poll(timeout_secs)
for record in records:
    print(record.data)
```

**Properties:**
- `id`: Record ID
- `data`: Record data
- `get_field(field_name: str) -> str`

### PyBatchRecord

Batch record representation.

```python
records = scanner.scan(limit)
for record in records:
    print(record.data)
```

**Properties:**
- `id`: Record ID
- `data`: Record data
- `get_field(field_name: str) -> str`

## Constants

The module provides several constants:

- `__version__`: "0.1.0"
- `DEFAULT_TIMEOUT`: 30
- `DEFAULT_BATCH_SIZE`: 1000
- `MAX_RETRY_COUNT`: 3

## Error Handling

All methods that can fail raise appropriate exceptions. Use standard Python exception handling:

```python
try:
    admin.create_table(table_path, descriptor)
except Exception as e:
    print(f"Failed to create table: {e}")
```

## Complete Example

```python
import fluss_python as fluss

# 1. Create connection
config = fluss.PyConnectionConfig("127.0.0.1:9123", 30)
conn = fluss.PyFlussConnection.new(config)

# 2. Create table
admin = conn.get_admin()
table_path = fluss.PyTablePath("fluss", "users")
schema = fluss.PySchema()
schema.add_column("id", "bigint")
schema.add_column("name", "string")
schema.add_column("score", "float")
schema.add_column("active", "boolean")

descriptor = fluss.PyTableDescriptor(schema)
admin.create_table(table_path, descriptor, True)

# 3. Verify table and schema
table = conn.get_table(table_path)
actual_schema = table.get_schema()
print(f"Table schema: {actual_schema.get_columns()}")

# 4. Write data
writer = table.new_append()
data = [
    {"id": 1, "name": "Alice", "score": 95.5, "active": True},
    {"id": 2, "name": "Bob", "score": 87.2, "active": False}
]
result = writer.append_batch(data)
print(f"Wrote {result.rows_written} rows")
writer.flush()
writer.close()

# 5. Read data
scanner = table.new_scan()
scanner.subscribe(0, 0)  # bucket=0, offset=0
records = scanner.poll(1)  # timeout=1 second
for record in records:
    print(f"Record: {record.data}")
scanner.close()

# 6. Cleanup
conn.close()
```

## Advanced Features

### Working with Different Data Types

```python
schema = fluss.PySchema()
schema.add_column("id", "bigint")
schema.add_column("name", "string")
schema.add_column("age", "int")
schema.add_column("salary", "double")
schema.add_column("is_active", "boolean")
schema.add_column("created_at", "timestamp")
schema.add_column("metadata", "bytes")

# Write data with different types
data = {
    "id": 12345,
    "name": "John Doe",
    "age": 30,
    "salary": 75000.50,
    "is_active": True,
    "created_at": "2023-01-01T00:00:00Z",
    "metadata": b"some binary data"
}
writer.append_row(data)
```

### Batch Operations

```python
# Write multiple rows at once
batch_data = [
    {"id": 1, "name": "Alice", "score": 95.5},
    {"id": 2, "name": "Bob", "score": 87.2},
    {"id": 3, "name": "Carol", "score": 92.1}
]
result = writer.append_batch(batch_data)
print(f"Batch wrote {result.rows_written} rows")
```

### Reading with Timeouts

```python
scanner = table.new_scan()
scanner.subscribe(0, 0)

# Poll with different timeouts
quick_records = scanner.poll(1)  # 1 second timeout
longer_records = scanner.poll(5)  # 5 second timeout
```

### Error Handling Patterns

```python
try:
    # Connection operations
    conn = fluss.PyFlussConnection.new(config)
    if not conn.is_connected():
        raise Exception("Failed to connect")
    
    # Table operations
    admin = conn.get_admin()
    admin.create_table(table_path, descriptor, True)
    
    # Data operations
    table = conn.get_table(table_path)
    writer = table.new_append()
    writer.append_row(data)
    writer.flush()
    
except Exception as e:
    print(f"Error: {e}")
finally:
    # Cleanup
    if 'writer' in locals():
        writer.close()
    if 'conn' in locals():
        conn.close()
```

For more detailed examples and usage patterns, see the test files in the project repository.

```python
conn = fluss.PyFlussConnection.new(config)
```

**Methods:**
- `new(config: PyConnectionConfig) -> PyFlussConnection`
- `get_admin() -> PyFlussAdmin`
- `get_table(table_path: PyTablePath) -> PyTable`
- `close() -> None`
- `is_connected() -> bool`

### PyConnectionPool

Connection pool for high-performance applications.

```python
pool = fluss.PyConnectionPool(config, 5, 10)
```

**Methods:**
- `__init__(config: PyConnectionConfig, min_size: int, max_size: int)`
- `get_connection() -> PyFlussConnection`
- `return_connection(connection: PyFlussConnection) -> None`
- `get_pool_size() -> Tuple[int, int]`
- `close() -> None`

## Administrative Operations

### PyFlussAdmin

Administrative client for table management.

```python
admin = conn.get_admin()
```

**Methods:**
- `create_table(table_path: PyTablePath, descriptor: PyTableDescriptor, ignore_if_exists: bool = False) -> None`
- `drop_table(table_path: PyTablePath, ignore_if_not_exists: bool = False) -> None`
- `table_exists(table_path: PyTablePath) -> bool`
- `list_tables(database: str) -> List[str]`
- `get_table_info(table_path: PyTablePath) -> TableInfo`

## Table Operations

### PyTable

Table representation with schema access.

```python
table = conn.get_table(table_path)
```

**Methods:**
- `get_path() -> PyTablePath`
- `get_schema() -> PySchema`
- `get_writer() -> PyAppendWriter`
- `get_log_scanner() -> PyLogScanner`
- `get_batch_scanner() -> PyBatchScanner`

### PyAppendWriter

Writer for appending data to tables.

```python
writer = table.get_writer()
```

**Methods:**
- `append_row(row: List[Any]) -> PyWriteResult`
- `append_batch(batch: List[List[Any]]) -> PyWriteResult`
- `flush() -> None`
- `close() -> None`

### PyLogScanner

Scanner for reading log data.

```python
scanner = table.get_log_scanner()
```

**Methods:**
- `subscribe_bucket(bucket_id: int, start_offset: int) -> None`
- `poll(timeout_seconds: int) -> List[PyLogRecord]`
- `seek(bucket_id: int, offset: int) -> None`
- `close() -> None`

### PyBatchScanner

Scanner for reading batch data.

```python
scanner = table.get_batch_scanner()
```

**Methods:**
- `scan(limit: int) -> List[PyBatchRecord]`
- `scan_with_filter(limit: int, filter_expr: str) -> List[PyBatchRecord]`
- `close() -> None`

## Advanced Features

### PyTransaction

Transaction management with commit/rollback support.

```python
transaction = fluss.PyTransaction("txn_123")
```

**Methods:**
- `__init__(transaction_id: str)`
- `begin() -> None`
- `commit() -> None`
- `rollback() -> None`
- `is_committed() -> bool`

### PyQueryBuilder

SQL-like query building with conditions, sorting, and pagination.

```python
builder = fluss.PyQueryBuilder()
builder.add_condition("status", "active")
builder.add_order_by("created_at", "DESC")
builder.set_limit(50)
```

**Methods:**
- `__init__()`
- `add_condition(column: str, value: str) -> None`
- `add_in_condition(column: str, values: List[str]) -> None`
- `add_order_by(column: str, direction: str = "ASC") -> None`
- `set_limit(limit: int) -> None`
- `set_offset(offset: int) -> None`
- `build() -> str`
- `clear() -> None`

### PySerializer

Data serialization support for multiple formats.

```python
serializer = fluss.PySerializer("json")
```

**Methods:**
- `__init__(format: str)` - Supported formats: "json", "avro", "protobuf", "messagepack"
- `serialize(data: Dict[str, Any]) -> bytes`
- `deserialize(data: bytes) -> Dict[str, Any]`
- `get_format() -> str`

### PyMetrics

Performance monitoring and metrics collection.

```python
metrics = fluss.PyMetrics()
```

**Methods:**
- `__init__()`
- `connection_count` (property): Get connection count
- `write_count` (property): Get write count
- `read_count` (property): Get read count
- `error_count` (property): Get error count
- `increment_connection_count() -> None`
- `increment_write_count() -> None`
- `increment_read_count() -> None`
- `increment_error_count() -> None`
- `reset() -> None`
- `to_dict() -> Dict[str, int]`

### PyConfigManager

Configuration management with default values.

```python
config_manager = fluss.PyConfigManager()
```

**Methods:**
- `__init__()`
- `set_config(key: str, value: str) -> None`
- `get_config(key: str) -> Optional[str]`
- `get_config_or_default(key: str, default: str) -> str`
- `remove_config(key: str) -> bool`
- `list_configs() -> List[Tuple[str, str]]`
- `get_default_config() -> Dict[str, str]`
- `load_from_file(file_path: str) -> None`
- `save_to_file(file_path: str) -> None`

## Data Types

### PyWriteResult

Result of write operations.

**Properties:**
- `rows_written`: Number of rows written
- `offset`: Offset of written data

### PyLogRecord

Log record representation.

**Properties:**
- `offset`: Record offset
- `data`: Record data
- `get_field(field_name: str) -> str`

### PyBatchRecord

Batch record representation.

**Properties:**
- `id`: Record ID
- `data`: Record data
- `get_field(field_name: str) -> str`

## Utility Functions

### Data Type Creation

```python
# Create data types
int_type = fluss.create_int_datatype()
string_type = fluss.create_string_datatype()
decimal_type = fluss.create_decimal_datatype(10, 2)
timestamp_type = fluss.create_timestamp_datatype()
array_type = fluss.create_array_datatype("string")
map_type = fluss.create_map_datatype("string", "int")
```

### Validation

```python
# Validate names
is_valid_table = fluss.validate_table_name("my_table")
is_valid_column = fluss.validate_column_name("my_column")
```

### String Utilities

```python
# Format and parse table paths
formatted = fluss.format_table_path("db", "table")
database, table = fluss.parse_table_path("db.table")
```

### Configuration Utilities

```python
# Get default configurations
config = fluss.get_default_config()
merged = fluss.merge_configs(config1, config2)
```

### Timestamp Utilities

```python
# Work with timestamps
current = fluss.current_timestamp()
formatted = fluss.format_timestamp(current)
```

### System Information

```python
# Get system information
version = fluss.get_version()
timeout = fluss.get_default_timeout()
batch_size = fluss.get_default_batch_size()
retry_count = fluss.get_max_retry_count()
```

## Error Handling

All methods that can fail return appropriate exceptions. Use standard Python exception handling:

```python
try:
    admin.create_table(table_path, descriptor)
except Exception as e:
    print(f"Failed to create table: {e}")
```

## Examples

### Basic Usage

```python
import fluss_python as fluss

# Create configuration
config = fluss.PyConnectionConfig("localhost:9123", 60)

# Create connection
conn = fluss.PyFlussConnection.new(config)

# Create table
table_path = fluss.PyTablePath("my_db", "users")
schema = fluss.PySchema()
schema.add_column("id", "int")
schema.add_column("name", "string")

descriptor = fluss.PyTableDescriptor(schema)
admin = conn.get_admin()
admin.create_table(table_path, descriptor, True)

# Write data
table = conn.get_table(table_path)
writer = table.get_writer()
writer.append_row([1, "Alice"])
writer.append_row([2, "Bob"])
writer.flush()
writer.close()

# Read data
scanner = table.get_log_scanner()
scanner.subscribe_bucket(0, 0)
records = scanner.poll(5)
for record in records:
    print(f"Record: {record.data}")
scanner.close()

# Cleanup
conn.close()
```

### Advanced Usage with Transactions

```python
import fluss_python as fluss

config = fluss.PyConnectionConfig("localhost:9123", 60)
conn = fluss.PyFlussConnection.new(config)

# Use transaction
transaction = fluss.PyTransaction("txn_123")
transaction.begin()

try:
    # Perform operations
    table_path = fluss.PyTablePath("my_db", "orders")
    table = conn.get_table(table_path)
    writer = table.get_writer()
    writer.append_row([101, "Order 1", 100.50])
    writer.flush()
    
    # Commit transaction
    transaction.commit()
    print("Transaction committed successfully")
except Exception as e:
    transaction.rollback()
    print(f"Transaction rolled back: {e}")

writer.close()
conn.close()
```

For more examples, see the `examples/` directory in the project.
