import fluss_python as fluss
import pyarrow as pa

# 尽可能还是和 java client 保持一致
config = fluss.ConnectionConfig("127.0.0.1:9123", 30)
conn = fluss.FlussConnection(config)

schema = fluss.Schema()
schema.add_column("id", fluss.DataType.int())
schema.add_column("name", fluss.DataType.string())
schema.add_column("score", fluss.DataType.float())
print(schema)

# 也可以这么创建
schema_1 = fluss.Schema([
    ("id", fluss.DataType.int()),
    ("name", fluss.DataType.string()),
    ("score", fluss.DataType.float())
])
print(schema_1)

# 创建 TableDescriptor，需要传入 schema，默认只有 1 个 bucket
table_descriptor = fluss.TableDescriptor(schema)
print(table_descriptor)


admin = conn.get_admin()
table_path = fluss.TablePath("fluss", "test_table_11")
admin.create_table(table_path, table_descriptor, True)


table = conn.get_table(table_path)

# 需要支持 LogRecord 的写入吗？

# 先创建一个新的 PyArrow RecordBatch
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
# 直接使用 PyArrow RecordBatch 写入数据
writer.append_batch(batch)
# 再写入一批 dict。这里需要确保写入的数据和定义的 schema 匹配
writer.append_batch([
    {"id": 4, "name": "David", "score": 78.9},
    {"id": 5, "name": "Eve", "score": 88.3}
])

scanner = table.new_scan()
log_scanner = scanner.create_log_scanner()
# snapshot：还没支持

# 订阅所有数据
bucket = 0
offset = 0
log_scanner.subscribe(bucket, offset)
# 读取数据 形成 pyarrow record batch
record_batches = log_scanner.poll(10)  # 添加 timeout 参数
print(f"读取到 {len(record_batches)} 个批次")
i = 0
for batch in record_batches:
    print(f"Batch {i}: {batch.num_rows} rows, {batch.num_columns} columns")
    i += 1
    # 转换为 pandas 查看数据
    print(batch.to_pandas())

    # 现在 batch 就是一个 PyArrow RecordBatch
    # 所以应该可以很方便地支持各种其他转换了
    # 例如转换为 Polars DataFrame
    import polars as pl
    polars_df = pl.from_arrow(batch)
    print(f"Polars DataFrame: {polars_df.shape}")
    print(polars_df)

    import pandas as pd
    pandas_df = batch.to_pandas()
    print(f"Pandas DataFrame: {pandas_df.shape}")
    print(pandas_df)

    import duckdb
    duck_conn = duckdb.connect(':memory:')
    duck_conn.register('duck_table', batch)
    duck_result = duck_conn.execute('SELECT * FROM duck_table').fetchdf()
    print(f"DuckDB DataFrame: {duck_result.shape}")
    print(duck_result)

# 现在不支持删除表 =.= 