# Fluss Python Client

![Experimental](https://img.shields.io/badge/status-experimental-orange)

An unofficial experimental Python client for [Fluss](https://alibaba.github.io/fluss-docs/docs/intro/), a streaming storage built for real-time analytics.

## About Fluss

Fluss is a streaming storage that serves as the real-time data layer for Lakehouse architectures. It enables low-latency, high-throughput data ingestion and processing while seamlessly integrating with popular compute engines.

## Features

This Python client provides foundational capabilities for:
- Table management (create, query schema)
- Real-time data ingestion (append operations)
- Log streaming operations (subscribe, scan, poll)
- Integration with popular Python data tools (PyArrow, Pandas, Polars)

## Quick Start

### Step 1: Start Fluss Cluster

#### Requirements

Fluss runs on all UNIX-like environments (Linux, macOS). Before you start, make sure you have the following software installed:

- Java 17 or higher (Java 8 and Java 11 are not recommended)
- JAVA_HOME environment variable set correctly

#### Fluss Setup

Go to the [downloads](https://alibaba.github.io/fluss-docs/downloads/#fluss-060) page and download Fluss-0.6.0. After downloading the latest release, extract it:

```shell
tar -xzf fluss-0.7-SNAPSHOT-bin.tgz
cd fluss-0.7-SNAPSHOT/
```

Start the Fluss local cluster:

```shell
./bin/local-cluster.sh start
```

After that, the Fluss local cluster is started and ready to use.

### Step 2: Setup Python Environment

This client supports Linux and macOS. You will need to [install Rust](https://www.rust-lang.org/tools/install) first.

Create and activate a Python virtual environment:

```shell
python3 -m venv fluss-env
source fluss-env/bin/activate
```

Install the required Python dependencies:

```shell
pip install --upgrade pip
pip install maturin pyarrow pandas polars
```

### Step 3: Build and Install the Client

Navigate to the python-bindings directory and build the client:

```shell
cd python-bindings
maturin develop --release
```

This command compiles the Rust code and installs the `fluss_python` module in your Python environment.

### Step 4: Run Your First Example

Now you can run Python scripts that use Fluss:

```shell
cd ..
python ./test.py
```

Here's what the example does:

```python
import fluss_python as fluss
import time

# 1. Connect to Fluss server
config = fluss.PyConnectionConfig("127.0.0.1:9123", 30)
conn = fluss.PyFlussConnection.new(config)

# 2. Create a table
admin = conn.get_admin()
table_name = f"test_table_{int(time.time())}"
table_path = fluss.PyTablePath("fluss", table_name)

# Define table schema
schema = fluss.PySchema()
schema.add_column("id", "int")
schema.add_column("name", "string")
schema.add_column("score", "float")

# Create the table
descriptor = fluss.PyTableDescriptor(schema)
admin.create_table(table_path, descriptor, True)

# 3. Write data
table = conn.get_table(table_path)
writer = table.new_append()

sample_data = [
    {"id": 1, "name": "Alice", "score": 95.5},
    {"id": 2, "name": "Bob", "score": 87.2},
    {"id": 3, "name": "Charlie", "score": 92.1}
]

writer.append_batch(sample_data)
writer.flush()
writer.close()

# 4. Read data
scanner = table.new_scan()
scanner.subscribe(0, 0)
records = scanner.poll(2)

for record in records:
    print(f"Record: {record.get_all_values()}")
scanner.close()
```

### Step 5: Data Conversion Examples

You can easily convert Fluss records to popular Python data formats:

```python
from fluss_converters import records_to_arrow, records_to_pandas, records_to_polars

# Convert to PyArrow Table
arrow_table = records_to_arrow(records)
print(f"Arrow Table: {arrow_table.num_rows} rows, {arrow_table.num_columns} columns")

# Convert to Pandas DataFrame
pandas_df = records_to_pandas(records)
print(f"Pandas DataFrame: {pandas_df.shape}")

# Convert to Polars DataFrame
polars_df = records_to_polars(records)
print(f"Polars DataFrame: {polars_df.shape}")
```

### Step 6: Clean Up

When you're done, stop the Fluss cluster:

```shell
cd fluss-0.7-SNAPSHOT/
./bin/local-cluster.sh stop
```

## Troubleshooting

### Common Issues

1. **Connection failed**: Make sure the Fluss server is running on `127.0.0.1:9123`
2. **Build errors**: Ensure you have Rust installed and updated
3. **Import errors**: Make sure you're in the correct Python virtual environment
4. **Java issues**: Verify Java 17+ is installed and JAVA_HOME is set

### Getting Help

If you encounter issues:

1. Check that all prerequisites are installed
2. Verify the Fluss server is running
3. Make sure you're using the correct Python virtual environment
4. Try rebuilding with `maturin develop --release`

## What's Next

This is an experimental client. Feel free to explore the API, contribute improvements, or report issues. The client provides a foundation for building Python applications that interact with Fluss streaming storage.