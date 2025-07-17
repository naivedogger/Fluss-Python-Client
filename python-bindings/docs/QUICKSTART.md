# Fluss Python Bindings - Quick Start Guide

This guide will help you get started with the Fluss Python bindings quickly.

## Prerequisites

- Python 3.8+
- Rust toolchain (for building from source)
- Fluss server running (default: `127.0.0.1:9123`)

## Installation

### From Source

1. Clone the repository and navigate to the Python bindings directory:
```bash
git clone <repository-url>
cd fluss-rust/python-bindings
```

2. Install using maturin:
```bash
# Install maturin if you haven't already
pip install maturin

# Build and install the package
maturin develop
```

3. Verify installation:
```bash
python -c "import fluss_python as fluss; print('Fluss bindings installed successfully!')"
```

## Basic Usage

### 1. Connect to Fluss Server

```python
import fluss_python as fluss

# Create connection configuration
config = fluss.PyConnectionConfig("127.0.0.1:9123", 30)

# Create connection
conn = fluss.PyFlussConnection.new(config)

# Check connection
if conn.is_connected():
    print("✅ Connected to Fluss server")
else:
    print("❌ Connection failed")
```

### 2. Create a Table

```python
# Get admin client
admin = conn.get_admin()

# Define table path
table_path = fluss.PyTablePath("my_database", "users")

# Create schema
schema = fluss.PySchema()
schema.add_column("id", "bigint")
schema.add_column("name", "string")
schema.add_column("email", "string")
schema.add_column("age", "int")
schema.add_column("is_active", "boolean")

# Create table descriptor
descriptor = fluss.PyTableDescriptor(schema)

# Create table (ignore if exists)
admin.create_table(table_path, descriptor, True)
print("✅ Table created successfully")
```

### 3. Verify Table Schema

```python
# Get table object
table = conn.get_table(table_path)

# Get schema from server
server_schema = table.get_schema()

print(f"Table has {server_schema.column_count()} columns:")
for column_name, column_type in server_schema.get_columns():
    print(f"  - {column_name}: {column_type}")
```

### 4. Write Data

```python
# Create writer
writer = table.new_append()

# Write single row
single_row = {
    "id": 1,
    "name": "Alice Johnson",
    "email": "alice@example.com",
    "age": 28,
    "is_active": True
}
result = writer.append_row(single_row)
print(f"Wrote {result.rows_written} row")

# Write multiple rows
batch_data = [
    {"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 32, "is_active": True},
    {"id": 3, "name": "Carol Brown", "email": "carol@example.com", "age": 25, "is_active": False},
    {"id": 4, "name": "David Wilson", "email": "david@example.com", "age": 35, "is_active": True}
]
result = writer.append_batch(batch_data)
print(f"Wrote {result.rows_written} rows in batch")

# Flush and close
writer.flush()
writer.close()
print("✅ Data written successfully")
```

### 5. Read Data

```python
# Create scanner
scanner = table.new_scan()

# Subscribe to read from beginning
scanner.subscribe(0, 0)  # bucket=0, offset=0

# Poll for records
records = scanner.poll(5)  # timeout=5 seconds
print(f"Read {len(records)} records:")

for record in records:
    print(f"  Record ID: {record.id}, Data: {record.data}")

# Close scanner
scanner.close()
```

### 6. Cleanup

```python
# Close connection
conn.close()
print("✅ Connection closed")
```

## Data Types Reference

### Supported Types

| Python Type | Fluss Type | Example |
|-------------|------------|---------|
| `int` | `int`, `bigint` | `42`, `123456789` |
| `float` | `float`, `double` | `3.14`, `99.99` |
| `str` | `string` | `"Hello World"` |
| `bool` | `boolean` | `True`, `False` |
| `bytes` | `bytes` | `b"binary data"` |

### Schema Definition Examples

```python
# Basic types
schema = fluss.PySchema()
schema.add_column("id", "bigint")
schema.add_column("name", "string")
schema.add_column("age", "int")
schema.add_column("salary", "double")
schema.add_column("is_active", "boolean")

# Date/time types
schema.add_column("created_at", "timestamp")
schema.add_column("birth_date", "date")
schema.add_column("login_time", "time")

# Binary data
schema.add_column("profile_picture", "bytes")
schema.add_column("document", "binary")
```