import fluss_python as fluss
import pyarrow as pa

# TODO: Keep consistent with the Java client
config = fluss.ConnectionConfig("127.0.0.1:9123", 30)
conn = fluss.FlussConnection(config)

schema = fluss.Schema()
schema.add_column("id", fluss.DataType.int())
schema.add_column("name", fluss.DataType.string())
schema.add_column("score", fluss.DataType.float())
print(schema)

# Can also be created this way
schema_1 = fluss.Schema([
    ("id", fluss.DataType.int()),
    ("name", fluss.DataType.string()),
    ("score", fluss.DataType.float())
])
print(schema_1)

# Create TableDescriptor, schema must be provided
table_descriptor = fluss.TableDescriptor(schema)
print(table_descriptor)

admin = conn.get_admin()
table_path = fluss.TablePath("fluss", "test_table_12")
admin.create_table(table_path, table_descriptor, True)

table = conn.get_table(table_path)

# First create a new PyArrow RecordBatch
pa_schema = pa.schema([
    ("id", pa.int32()),
    ("name", pa.string()),
    ("score", pa.float32())
])
batch = pa.RecordBatch.from_arrays([
    pa.array([1, 2, 3]),
    pa.array(["Alice", "Bob", "Charlie"]),
    pa.array([95.2, 87.2, 92.1])
], schema=pa_schema)

writer = table.new_append()
# Directly write data using PyArrow RecordBatch
writer.append_batch(batch)
# Append another batch of dictionaries.
writer.append_batch([
    {"id": 4, "name": "David", "score": 78.9},
    {"id": 5, "name": "Eve", "score": 88.3}
])

scanner = table.new_scan()
log_scanner = scanner.create_log_scanner()

# Subscribe from the beginning
bucket = 0
offset = 0
log_scanner.subscribe(bucket, offset)
# Read data and form PyArrow RecordBatches
record_batches = log_scanner.poll(10)  # Add timeout parameter
print(f"Read {len(record_batches)} batches")
i = 0
for batch in record_batches:
    print(f"Batch {i}: {batch.num_rows} rows, {batch.num_columns} columns")
    i += 1
    # Convert to pandas to view data
    print(batch.to_pandas())

    # Now batch is a PyArrow RecordBatch
    # So various other conversions should be easily supported
    # For example, convert to Polars DataFrame
    import polars as pl
    polars_df = pl.from_arrow(batch)
    print(f"Polars DataFrame: {polars_df.shape}")
    print(polars_df)

    import pandas
    pandas_df = batch.to_pandas()
    print(f"Pandas DataFrame: {pandas_df.shape}")
    print(pandas_df)

    import duckdb
    duck_conn = duckdb.connect(':memory:')
    duck_conn.register('duck_table', batch)
    duck_result = duck_conn.execute('SELECT * FROM duck_table').fetchdf()
    print(f"DuckDB DataFrame: {duck_result.shape}")
    print(duck_result)