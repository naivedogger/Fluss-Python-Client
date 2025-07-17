use pyo3::prelude::*;
use std::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

use fluss_rust::{
    args::Args,
    connection::{ConnectionConfig, FlussConnection},
    metadata::{TablePath, DataTypes, Schema, TableDescriptor, TableInfo, DataType},
    admin::admin::FlussAdmin,
    table::table::{Table, TableAppend},
    table::scanner::{TableScan, log::LogScanner},
    record::{ScanRecord, row::InternalRow},
};

// Import Arrow types for data conversion
use arrow::array::{ArrayRef, Int64Array, StringArray, RecordBatch, BooleanArray, Float64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};

// Helper function to convert Rust DataType to Python string
fn datatype_to_string(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean(_) => "boolean".to_string(),
        DataType::TinyInt(_) => "tinyint".to_string(),
        DataType::SmallInt(_) => "smallint".to_string(),
        DataType::Int(_) => "int".to_string(),
        DataType::BigInt(_) => "bigint".to_string(),
        DataType::Float(_) => "float".to_string(),
        DataType::Double(_) => "double".to_string(),
        DataType::Char(_) => "string".to_string(), // Char is treated as string in Python
        DataType::String(_) => "string".to_string(),
        DataType::Decimal(_) => "decimal".to_string(), // Could be improved to include precision/scale
        DataType::Date(_) => "date".to_string(),
        DataType::Time(_) => "time".to_string(),
        DataType::Timestamp(_) => "timestamp".to_string(),
        DataType::TimestampLTz(_) => "timestamp_ltz".to_string(),
        DataType::Bytes(_) => "bytes".to_string(),
        DataType::Binary(_) => "binary".to_string(),
        DataType::Array(_) => "array".to_string(), // Could be improved to include element type
        DataType::Map(_) => "map".to_string(), // Could be improved to include key/value types
        DataType::Row(_) => "row".to_string(), // Row types are complex, simplified for now
    }
}

// Python wrapper for ConnectionConfig
#[pyclass]
pub struct PyConnectionConfig {
    inner: ConnectionConfig,
}

#[pymethods]
impl PyConnectionConfig {
    #[new]
    #[pyo3(signature = (bootstrap_server, rw_timeout_secs = None))]
    fn new(bootstrap_server: String, rw_timeout_secs: Option<u64>) -> Self {
        let timeout = Duration::from_secs(rw_timeout_secs.unwrap_or(30));
        let mut args = Args::default();
        args.bootstrap_server = bootstrap_server;
        args.rw_timeout = timeout;
        
        PyConnectionConfig {
            inner: ConnectionConfig::from_args(args),
        }
    }
    
    #[getter]
    fn bootstrap_server(&self) -> String {
        self.inner.bootstrap_server.clone()
    }
    
    #[getter]
    fn rw_timeout_secs(&self) -> u64 {
        self.inner.rw_timeout.as_secs()
    }
}

// Python wrapper for TablePath
#[pyclass]
pub struct PyTablePath {
    inner: TablePath,
}

#[pymethods]
impl PyTablePath {
    #[new]
    fn new(database: String, table: String) -> Self {
        PyTablePath {
            inner: TablePath::new(database, table),
        }
    }
    
    #[getter]
    fn database(&self) -> String {
        self.inner.database().to_string()
    }
    
    #[getter]
    fn table(&self) -> String {
        self.inner.table().to_string()
    }
    
    fn __str__(&self) -> String {
        format!("{}.{}", self.inner.database(), self.inner.table())
    }
    
    fn __repr__(&self) -> String {
        format!("PyTablePath('{}', '{}')", self.inner.database(), self.inner.table())
    }
}

// Python wrapper for Schema
#[pyclass]
pub struct PySchema {
    inner: Schema,
    // Store column information for easy access
    columns: Vec<(String, String)>,
}

#[pymethods]
impl PySchema {
    #[new]
    fn new() -> Self {
        PySchema {
            inner: Schema::builder().build(),
            columns: Vec::new(),
        }
    }
    
    fn add_column(&mut self, name: String, data_type: String) -> PyResult<()> {
        let _dt = match data_type.as_str() {
            "int" => DataTypes::int(),
            "string" => DataTypes::string(),
            "bigint" => DataTypes::bigint(),
            "float" => DataTypes::float(),
            "double" => DataTypes::double(),
            "boolean" => DataTypes::boolean(),
            "tinyint" => DataTypes::tinyint(),
            "smallint" => DataTypes::smallint(),
            "bytes" => DataTypes::bytes(),
            "date" => DataTypes::date(),
            "time" => DataTypes::time(),
            "timestamp" => DataTypes::timestamp(),
            "timestamp_ltz" => DataTypes::timestamp_ltz(),
            _ => {
                // Try to parse complex types like decimal(10,2), char(255), etc.
                if data_type.starts_with("decimal(") && data_type.ends_with(")") {
                    let params = &data_type[8..data_type.len()-1];
                    let parts: Vec<&str> = params.split(',').collect();
                    if parts.len() == 2 {
                        let precision = parts[0].trim().parse::<u32>().map_err(|_| 
                            PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid decimal precision"))?;
                        let scale = parts[1].trim().parse::<u32>().map_err(|_| 
                            PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid decimal scale"))?;
                        DataTypes::decimal(precision, scale)
                    } else {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            "Invalid decimal format. Use decimal(precision,scale)"
                        ));
                    }
                } else if data_type.starts_with("char(") && data_type.ends_with(")") {
                    let length_str = &data_type[5..data_type.len()-1];
                    let length = length_str.parse::<u32>().map_err(|_| 
                        PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid char length"))?;
                    DataTypes::char(length)
                } else if data_type.starts_with("binary(") && data_type.ends_with(")") {
                    let length_str = &data_type[7..data_type.len()-1];
                    let length = length_str.parse::<usize>().map_err(|_| 
                        PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid binary length"))?;
                    DataTypes::binary(length)
                } else {
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        format!("Unsupported data type: {}", data_type)
                    ));
                }
            }
        };
        
        // Add to columns list
        self.columns.push((name.clone(), data_type));
        
        // Rebuild the schema with all columns
        self.rebuild_schema()?;
        Ok(())
    }
    
    fn get_columns(&self) -> Vec<(String, String)> {
        self.columns.clone()
    }
    
    fn get_column_names(&self) -> Vec<String> {
        self.columns.iter().map(|(name, _)| name.clone()).collect()
    }
    
    fn get_column_type(&self, column_name: &str) -> Option<String> {
        self.columns.iter()
            .find(|(name, _)| name == column_name)
            .map(|(_, dtype)| dtype.clone())
    }
    
    fn column_count(&self) -> usize {
        self.columns.len()
    }
    
    // Helper method to rebuild schema from all columns
    fn rebuild_schema(&mut self) -> PyResult<()> {
        let mut builder = Schema::builder();
        
        for (name, data_type) in &self.columns {
            let dt = match data_type.as_str() {
                "int" => DataTypes::int(),
                "string" => DataTypes::string(),
                "bigint" => DataTypes::bigint(),
                "float" => DataTypes::float(),
                "double" => DataTypes::double(),
                "boolean" => DataTypes::boolean(),
                "tinyint" => DataTypes::tinyint(),
                "smallint" => DataTypes::smallint(),
                "bytes" => DataTypes::bytes(),
                "date" => DataTypes::date(),
                "time" => DataTypes::time(),
                "timestamp" => DataTypes::timestamp(),
                "timestamp_ltz" => DataTypes::timestamp_ltz(),
                _ => {
                    // Handle complex types
                    if data_type.starts_with("decimal(") && data_type.ends_with(")") {
                        let params = &data_type[8..data_type.len()-1];
                        let parts: Vec<&str> = params.split(',').collect();
                        if parts.len() == 2 {
                            let precision = parts[0].trim().parse::<u32>().map_err(|_| 
                                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid decimal precision"))?;
                            let scale = parts[1].trim().parse::<u32>().map_err(|_| 
                                PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid decimal scale"))?;
                            DataTypes::decimal(precision, scale)
                        } else {
                            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                "Invalid decimal format. Use decimal(precision,scale)"
                            ));
                        }
                    } else if data_type.starts_with("char(") && data_type.ends_with(")") {
                        let length_str = &data_type[5..data_type.len()-1];
                        let length = length_str.parse::<u32>().map_err(|_| 
                            PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid char length"))?;
                        DataTypes::char(length)
                    } else if data_type.starts_with("binary(") && data_type.ends_with(")") {
                        let length_str = &data_type[7..data_type.len()-1];
                        let length = length_str.parse::<usize>().map_err(|_| 
                            PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid binary length"))?;
                        DataTypes::binary(length)
                    } else {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                            format!("Unsupported data type: {}", data_type)
                        ));
                    }
                }
            };
            
            builder = builder.column(name, dt);
        }
        
        self.inner = builder.build();
        Ok(())
    }
    
    fn __str__(&self) -> String {
        format!("PySchema(columns={})", self.columns.len())
    }
}

// Python wrapper for TableDescriptor
#[pyclass]
pub struct PyTableDescriptor {
    inner: TableDescriptor,
}

#[pymethods]
impl PyTableDescriptor {
    #[new]
    fn new(schema: &PySchema) -> Self {
        PyTableDescriptor {
            inner: TableDescriptor::builder()
                .schema(schema.inner.clone())
                .build(),
        }
    }
    
    fn __str__(&self) -> String {
        format!("PyTableDescriptor")
    }
}

// Python wrapper for FlussConnection
#[pyclass]
pub struct PyFlussConnection {
    runtime: Runtime,
    connection: Option<FlussConnection>,
    config: ConnectionConfig,
}

#[pymethods]
impl PyFlussConnection {
    #[staticmethod]
    fn new(config: &PyConnectionConfig) -> PyResult<Self> {
        let runtime = Runtime::new()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to create tokio runtime: {}", e)
            ))?;
        
        let connection_config = config.inner.clone();
        
        // 尝试创建连接，添加超时机制
        let connection = runtime.block_on(async {
            // 添加超时机制，避免无限等待
            let timeout_duration = std::time::Duration::from_secs(10);
            match tokio::time::timeout(timeout_duration, FlussConnection::new(connection_config.clone())).await {
                Ok(conn) => Ok(conn),
                Err(_) => Err("Failed to create FlussConnection: timeout after 10 seconds".to_string()),
            }
        });
        
        match connection {
            Ok(conn) => Ok(PyFlussConnection {
                runtime,
                connection: Some(conn),
                config: connection_config,
            }),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
        }
    }
    
    fn get_admin(&self) -> PyResult<PyFlussAdmin> {
        if let Some(ref conn) = self.connection {
            let admin = conn.get_admin();
            Ok(PyFlussAdmin::new_with_real_admin(admin))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No connection available - connection may have failed during initialization"
            ))
        }
    }
    
    fn get_table(&self, table_path: &PyTablePath) -> PyResult<PyTable> {
        if let Some(ref conn) = self.connection {
            let table_path_rust = table_path.inner.clone();
            let table = self.runtime.block_on(async {
                conn.get_table(&table_path_rust).await
            });
            
            Ok(PyTable::new_with_real_table(table, table_path))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No connection available - connection may have failed during initialization"
            ))
        }
    }
    
    fn is_connected(&self) -> bool {
        self.connection.is_some()
    }
    
    fn close(&self) -> PyResult<()> {
        // 在真实实现中，这里会关闭连接
        // 现在只是占位符
        Ok(())
    }
}

// Python wrapper for FlussAdmin
#[pyclass]
pub struct PyFlussAdmin {
    runtime: Option<Runtime>,
    admin: Option<FlussAdmin>,
}

#[pymethods]
impl PyFlussAdmin {
    #[new]
    fn new() -> Self {
        PyFlussAdmin {
            runtime: None,
            admin: None,
        }
    }
    
    fn create_table(&self, table_path: &PyTablePath, descriptor: &PyTableDescriptor, ignore_if_exists: bool) -> PyResult<()> {
        if let (Some(runtime), Some(admin)) = (&self.runtime, &self.admin) {
            let table_path_rust = table_path.inner.clone();
            let descriptor_rust = descriptor.inner.clone();
            
            runtime.block_on(async {
                admin.create_table(&table_path_rust, &descriptor_rust, ignore_if_exists).await
            }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to create table: {}", e)
            ))?;
            
            println!("Table {} created successfully", table_path.__str__());
        } else {
            // 占位符实现
            println!("Creating table: {} (ignore_if_exists: {})", table_path.__str__(), ignore_if_exists);
        }
        Ok(())
    }
    
    // 异步版本的 create_table（可选）
    fn create_table_async(&self, table_path: &PyTablePath, descriptor: &PyTableDescriptor, ignore_if_exists: bool) -> PyResult<()> {
        if let (Some(runtime), Some(admin)) = (&self.runtime, &self.admin) {
            let table_path_rust = table_path.inner.clone();
            let descriptor_rust = descriptor.inner.clone();
            
            // 使用 spawn 在后台执行，不阻塞当前线程
            runtime.spawn(async move {
                match admin.create_table(&table_path_rust, &descriptor_rust, ignore_if_exists).await {
                    Ok(_) => println!("Table created successfully (async)"),
                    Err(e) => println!("Failed to create table (async): {}", e),
                }
            });
            
            println!("Table creation initiated asynchronously");
        }
        Ok(())
    }
    
    fn drop_table(&self, table_path: &PyTablePath, ignore_if_not_exists: bool) -> PyResult<()> {
        // todo: 目前暂未实现
        if let (Some(runtime), Some(admin)) = (&self.runtime, &self.admin) {
            let table_path_rust = table_path.inner.clone();
            
            // Check if table exists before attempting to drop
            let table_exists = runtime.block_on(async {
                match admin.get_table(&table_path_rust).await {
                    Ok(_) => true,
                    Err(_) => false,
                }
            });
            
            if !table_exists && !ignore_if_not_exists {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Table {} does not exist", table_path.__str__())
                ));
            }
            
            if table_exists {
                println!("WARNING: drop_table is not implemented in Fluss server yet.");
                println!("Table {} cannot be dropped. Consider using a different table name.", table_path.__str__());
                return Err(PyErr::new::<pyo3::exceptions::PyNotImplementedError, _>(
                    "drop_table is not implemented in Fluss server yet"
                ));
            }
        } else {
            // 占位符实现
            println!("WARNING: drop_table is not implemented in Fluss server yet.");
            println!("Table {} cannot be dropped. Consider using a different table name.", table_path.__str__());
        }
        Ok(())
    }
    
    fn get_table(&self, table_path: &PyTablePath) -> PyResult<PyTableInfo> {
        if let (Some(runtime), Some(admin)) = (&self.runtime, &self.admin) {
            let table_path_rust = table_path.inner.clone();
            
            let table_info = runtime.block_on(async {
                admin.get_table(&table_path_rust).await
            }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to get table: {}", e)
            ))?;
            
            Ok(PyTableInfo::new_from_real_table_info(table_info))
        } else {
            // 占位符实现
            Ok(PyTableInfo::new(table_path))
        }
    }
    
    fn list_tables(&self, database: Option<String>) -> PyResult<Vec<String>> {
        // 目前Fluss还没有list_tables的API，所以这里仍然是占位符
        let db = database.unwrap_or_else(|| "default".to_string());
        Ok(vec![format!("{}.table1", db), format!("{}.table2", db)])
    }
    
    fn table_exists(&self, table_path: &PyTablePath) -> PyResult<bool> {
        if let (Some(runtime), Some(admin)) = (&self.runtime, &self.admin) {
            let table_path_rust = table_path.inner.clone();
            
            let exists = runtime.block_on(async {
                match admin.get_table(&table_path_rust).await {
                    Ok(_) => true,
                    Err(_) => false,
                }
            });
            
            Ok(exists)
        } else {
            // 占位符实现
            Ok(true)
        }
    }
}

impl PyFlussAdmin {
    pub fn new_with_real_admin(admin: FlussAdmin) -> Self {
        let runtime = Runtime::new().ok();
        PyFlussAdmin {
            runtime,
            admin: Some(admin),
        }
    }
}

// Python wrapper for Table
#[pyclass]
pub struct PyTable {
    table_path: TablePath,
    runtime: Option<Runtime>,
    table: Option<Table>,
}

#[pymethods]
impl PyTable {
    #[new]
    fn new(table_path: &PyTablePath) -> Self {
        PyTable { 
            table_path: table_path.inner.clone(),
            runtime: None,
            table: None,
        }
    }
    
    fn get_path(&self) -> PyTablePath {
        PyTablePath { inner: self.table_path.clone() }
    }
    
    fn new_append(&self) -> PyAppendWriter {
        if let (Some(runtime), Some(table)) = (&self.runtime, &self.table) {
            let table_append = table.new_append();
            let table_info = table.get_table_info();
            let schema = table_info.schema.clone();
            PyAppendWriter::new_with_real_append(table_append, runtime, Some(schema))
        } else {
            PyAppendWriter::new(&PyTablePath { inner: self.table_path.clone() })
        }
    }
    
    fn new_scan(&self) -> PyLogScanner {
        if let (Some(runtime), Some(table)) = (&self.runtime, &self.table) {
            let table_scan = table.new_scan();
            let mut scanner = PyLogScanner::new_with_real_scan(table_scan, runtime);
            
            // Set schema information from table
            if let Ok(schema) = self.get_schema() {
                scanner.set_schema_info(schema.get_columns());
            }
            
            scanner
        } else {
            PyLogScanner::new(&PyTablePath { inner: self.table_path.clone() })
        }
    }
    
    fn new_batch_scan(&self) -> PyBatchScanner {
        // 目前Fluss可能还没有batch scan的API，所以这里仍然是占位符
        PyBatchScanner::new(&PyTablePath { inner: self.table_path.clone() })
    }
    
    fn get_schema(&self) -> PyResult<PySchema> {
        if let (Some(_runtime), Some(table)) = (&self.runtime, &self.table) {
            let table_info = table.get_table_info();
            
            // 从真实的TableInfo.schema中提取列信息
            let mut schema = PySchema::new();
            for column in table_info.schema.columns() {
                let column_name = column.name().to_string();
                let column_type = datatype_to_string(column.data_type());
                schema.add_column(column_name, column_type)?;
            }
            
            Ok(schema)
        } else {
            // 占位符实现
            let mut schema = PySchema::new();
            schema.add_column("id".to_string(), "bigint".to_string())?;
            schema.add_column("name".to_string(), "string".to_string())?;
            schema.add_column("age".to_string(), "int".to_string())?;
            Ok(schema)
        }
    }
}

impl PyTable {
    pub fn new_with_real_table(table: Table, table_path: &PyTablePath) -> Self {
        let runtime = Runtime::new().ok();
        PyTable {
            table_path: table_path.inner.clone(),
            runtime,
            table: Some(table),
        }
    }
}

// Python wrapper for TableInfo
#[pyclass]
pub struct PyTableInfo {
    table_path: TablePath,
    created_at: String,
    row_count: u64,
    // 添加对真实TableInfo的引用
    real_table_info: Option<TableInfo>,
}

#[pymethods]
impl PyTableInfo {
    #[new]
    fn new(table_path: &PyTablePath) -> Self {
        PyTableInfo {
            table_path: table_path.inner.clone(),
            created_at: "2025-01-01T00:00:00Z".to_string(),
            row_count: 0,
            real_table_info: None,
        }
    }
    
    fn get_path(&self) -> PyTablePath {
        PyTablePath { inner: self.table_path.clone() }
    }
    
    #[getter]
    fn created_at(&self) -> String {
        self.created_at.clone()
    }
    
    #[getter]
    fn row_count(&self) -> u64 {
        self.row_count
    }
    
    fn __str__(&self) -> String {
        format!("TableInfo(path={}, created_at={}, row_count={})", 
                self.table_path.database(), self.created_at, self.row_count)
    }
}

impl PyTableInfo {
    pub fn new_from_real_table_info(table_info: TableInfo) -> Self {
        PyTableInfo {
            table_path: table_info.table_path.clone(),
            created_at: "2025-01-01T00:00:00Z".to_string(), // 需要从real_table_info中获取
            row_count: 0, // 需要从real_table_info中获取
            real_table_info: Some(table_info),
        }
    }
}

// Python wrapper for AppendWriter
#[pyclass]
pub struct PyAppendWriter {
    table_path: TablePath,
    runtime: Option<Runtime>,
    table_append: Option<TableAppend>,
    schema: Option<Schema>,
}

#[pymethods]
impl PyAppendWriter {
    #[new]
    fn new(table_path: &PyTablePath) -> Self {
        PyAppendWriter { 
            table_path: table_path.inner.clone(),
            runtime: None,
            table_append: None,
            schema: None,
        }
    }
    
    fn append_row(&self, row_data: HashMap<String, PyObject>) -> PyResult<PyWriteResult> {
        if let (Some(runtime), Some(table_append)) = (&self.runtime, &self.table_append) {
            // Convert single row to batch format
            let batch_data = vec![row_data];
            
            // Convert to RecordBatch
            let record_batch = self.convert_to_record_batch(&batch_data)?;
            
            // Create writer and append
            let writer = table_append.create_writer();
            let result = runtime.block_on(async {
                writer.append(record_batch).await
            });
            
            match result {
                Ok(_) => Ok(PyWriteResult::new(1)),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to append row: {}", e)
                ))
            }
        } else {
            // 占位符实现
            println!("Appending row to {}: {:?}", self.table_path.database(), row_data.keys().collect::<Vec<_>>());
            Ok(PyWriteResult::new(1))
        }
    }
    
    fn append_batch(&self, batch_data: Vec<HashMap<String, PyObject>>) -> PyResult<PyWriteResult> {
        if let (Some(runtime), Some(table_append)) = (&self.runtime, &self.table_append) {
            if batch_data.is_empty() {
                return Ok(PyWriteResult::new(0));
            }
            
            // Convert to RecordBatch
            let record_batch = self.convert_to_record_batch(&batch_data)?;
            
            // Create writer and append
            let writer = table_append.create_writer();
            let result = runtime.block_on(async {
                writer.append(record_batch).await
            });
            
            match result {
                Ok(_) => Ok(PyWriteResult::new(batch_data.len() as u64)),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to append batch: {}", e)
                ))
            }
        } else {
            // 占位符实现
            println!("Appending batch of {} rows to {}", batch_data.len(), self.table_path.database());
            Ok(PyWriteResult::new(batch_data.len() as u64))
        }
    }
    
    fn flush(&self) -> PyResult<()> {
        // 在 Rust 端，append 操作通常会自动处理 flush
        // 这里只是打印日志表示 flush 完成
        println!("Flushing writer for {}", self.table_path.database());
        Ok(())
    }
    
    fn close(&self) -> PyResult<()> {
        if let (Some(_runtime), Some(_table_append)) = (&self.runtime, &self.table_append) {
            // 真实实现的尝试
            println!("Attempting real close for {}", self.table_path.database());
            // TODO: 实现真正的close
        } else {
            // 占位符实现
            println!("Closing writer for {}", self.table_path.database());
        }
        Ok(())
    }
    
    fn create_writer(&self) -> PyWriterInstance {
        PyWriterInstance::new(self)
    }
}

impl PyAppendWriter {
    pub fn new_with_real_append(table_append: TableAppend, runtime: &Runtime, schema: Option<Schema>) -> Self {
        // Since we can't clone Runtime, we need to create a new one
        // This is a limitation of the current design
        let new_runtime = Runtime::new().unwrap_or_else(|_| panic!("Failed to create runtime"));
        PyAppendWriter {
            table_path: TablePath::new("real_table".to_string(), "initialized".to_string()),
            runtime: Some(new_runtime),
            table_append: Some(table_append),
            schema,
        }
    }
    
    // Helper function to convert Python data to Arrow RecordBatch
    fn convert_to_record_batch(&self, data: &[HashMap<String, PyObject>]) -> PyResult<RecordBatch> {
        if data.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>("Cannot convert empty data to RecordBatch"));
        }
        
        // Create fields and arrays based on schema if available
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        
        if let Some(schema) = &self.schema {
            // Use real schema information
            for column in schema.columns() {
                let column_name = column.name();
                let column_type = column.data_type();
                
                // Convert fluss DataType to Arrow DataType
                let arrow_type = match column_type {
                    DataType::Boolean(_) => ArrowDataType::Boolean,
                    DataType::TinyInt(_) => ArrowDataType::Int8,
                    DataType::SmallInt(_) => ArrowDataType::Int16,
                    DataType::Int(_) => ArrowDataType::Int32,
                    DataType::BigInt(_) => ArrowDataType::Int64,
                    DataType::Float(_) => ArrowDataType::Float32,
                    DataType::Double(_) => ArrowDataType::Float64,
                    DataType::String(_) | DataType::Char(_) => ArrowDataType::Utf8,
                    _ => ArrowDataType::Utf8, // Default to string for complex types
                };
                
                fields.push(Field::new(column_name, arrow_type.clone(), true));
                
                // Create array based on data type
                match arrow_type {
                    ArrowDataType::Boolean => {
                        let values: Vec<Option<bool>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<bool>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(BooleanArray::from(values)) as ArrayRef);
                    }
                    ArrowDataType::Int8 => {
                        let values: Vec<Option<i8>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<i8>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(arrow::array::Int8Array::from(values)) as ArrayRef);
                    }
                    ArrowDataType::Int16 => {
                        let values: Vec<Option<i16>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<i16>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(arrow::array::Int16Array::from(values)) as ArrayRef);
                    }
                    ArrowDataType::Int32 => {
                        let values: Vec<Option<i32>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<i32>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(arrow::array::Int32Array::from(values)) as ArrayRef);
                    }
                    ArrowDataType::Int64 => {
                        let values: Vec<Option<i64>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<i64>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(Int64Array::from(values)) as ArrayRef);
                    }
                    ArrowDataType::Float32 => {
                        let values: Vec<Option<f32>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<f32>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(arrow::array::Float32Array::from(values)) as ArrayRef);
                    }
                    ArrowDataType::Float64 => {
                        let values: Vec<Option<f64>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<f64>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(Float64Array::from(values)) as ArrayRef);
                    }
                    _ => {
                        // Default to string
                        let values: Vec<Option<String>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<String>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(StringArray::from(values)) as ArrayRef);
                    }
                }
            }
        } else {
            // Fallback to type inference if no schema available
            let first_row = &data[0];
            let column_names: Vec<String> = first_row.keys().cloned().collect();
            
            for column_name in &column_names {
                // Determine data type by examining first non-None value
                let mut data_type = ArrowDataType::Utf8; // Default to string
                for row in data {
                    if let Some(value) = row.get(column_name) {
                        Python::with_gil(|py| {
                            if let Ok(_) = value.extract::<i64>(py) {
                                data_type = ArrowDataType::Int64;
                            } else if let Ok(_) = value.extract::<f64>(py) {
                                data_type = ArrowDataType::Float64;
                            } else if let Ok(_) = value.extract::<bool>(py) {
                                data_type = ArrowDataType::Boolean;
                            }
                            // Default to string for other types
                        });
                        break;
                    }
                }
                
                fields.push(Field::new(column_name, data_type.clone(), true));
                
                // Create array based on data type
                match data_type {
                    ArrowDataType::Int64 => {
                        let values: Vec<Option<i64>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<i64>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(Int64Array::from(values)) as ArrayRef);
                    }
                    ArrowDataType::Float64 => {
                        let values: Vec<Option<f64>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<f64>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(Float64Array::from(values)) as ArrayRef);
                    }
                    ArrowDataType::Boolean => {
                        let values: Vec<Option<bool>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<bool>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(BooleanArray::from(values)) as ArrayRef);
                    }
                    _ => {
                        // Default to string
                        let values: Vec<Option<String>> = data.iter().map(|row| {
                            row.get(column_name).and_then(|v| {
                                Python::with_gil(|py| {
                                    v.extract::<String>(py).ok()
                                })
                            })
                        }).collect();
                        
                        arrays.push(Arc::new(StringArray::from(values)) as ArrayRef);
                    }
                }
            }
        }
        
        let schema = ArrowSchema::new(fields);
        RecordBatch::try_new(Arc::new(schema), arrays)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to create RecordBatch: {}", e)
            ))
    }
}

// Python wrapper for WriteResult
#[pyclass]
pub struct PyWriteResult {
    rows_written: u64,
    offset: u64,
}

#[pymethods]
impl PyWriteResult {
    #[new]
    fn new(rows_written: u64) -> Self {
        PyWriteResult {
            rows_written,
            offset: 12345, // 占位符
        }
    }
    
    #[getter]
    fn rows_written(&self) -> u64 {
        self.rows_written
    }
    
    #[getter]
    fn offset(&self) -> u64 {
        self.offset
    }
    
    fn __str__(&self) -> String {
        format!("WriteResult(rows_written={}, offset={})", self.rows_written, self.offset)
    }
}

// Python wrapper for LogScanner
#[pyclass]
pub struct PyLogScanner {
    table_path: TablePath,
    runtime: Option<Runtime>,
    table_scan: Option<TableScan>,
    // Store schema information for data conversion
    schema_info: Vec<(String, String)>,
    // Store subscription state
    subscribed_bucket: Option<i32>,
    subscribed_offset: Option<i64>,
}

#[pymethods]
impl PyLogScanner {
    #[new]
    fn new(table_path: &PyTablePath) -> Self {
        PyLogScanner { 
            table_path: table_path.inner.clone(),
            runtime: None,
            table_scan: None,
            schema_info: vec![
                ("id".to_string(), "bigint".to_string()),
                ("name".to_string(), "string".to_string()),
                ("age".to_string(), "int".to_string()),
            ],
            subscribed_bucket: None,
            subscribed_offset: None,
        }
    }
    
    fn subscribe(&mut self, bucket: i32, offset: i64) -> PyResult<()> {
        if let (Some(_runtime), Some(_table_scan)) = (&self.runtime, &self.table_scan) {
            // Store subscription state for later use
            self.subscribed_bucket = Some(bucket);
            self.subscribed_offset = Some(offset);
            
            println!("Subscribed to bucket {} at offset {} for {}", bucket, offset, self.table_path.database());
        } else {
            // 占位符实现
            println!("Subscribing to bucket {} at offset {} for {}", bucket, offset, self.table_path.database());
        }
        Ok(())
    }
    
    fn poll(&self, timeout_secs: u64) -> PyResult<Vec<PyLogRecord>> {
        if let (Some(runtime), Some(table_scan)) = (&self.runtime, &self.table_scan) {
            let timeout = Duration::from_secs(timeout_secs);
            
            // Check if we have subscription state
            if let (Some(bucket), Some(offset)) = (self.subscribed_bucket, self.subscribed_offset) {
                let scan_records = runtime.block_on(async {
                    // Create a new scanner for this poll operation
                    let log_scanner = table_scan.create_log_scanner();
                    
                    // Use the stored subscription state
                    log_scanner.subscribe(bucket, offset).await;
                    
                    // Poll for records
                    log_scanner.poll(timeout).await
                }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to poll records: {}", e)
                ))?;
                
                println!("Polled {} records using stored subscription state (bucket: {}, offset: {})", 
                         scan_records.count(), bucket, offset);
                
                // Convert ScanRecords to PyLogRecord
                let mut py_records = Vec::new();
                for record in scan_records.into_iter() {
                    // Convert ScanRecord to PyLogRecord with real data
                    let py_record = PyLogRecord::new_from_scan_record(&record, &self.schema_info);
                    py_records.push(py_record);
                }
                
                Ok(py_records)
            } else {
                // No subscription state, return empty
                println!("No subscription state available. Please call subscribe() first.");
                Ok(vec![])
            }
        } else {
            // 占位符实现
            println!("Polling for {} seconds from {}", timeout_secs, self.table_path.database());
            Ok(vec![
                PyLogRecord::new(1, "test_data".to_string()),
                PyLogRecord::new(2, "test_data2".to_string()),
            ])
        }
    }
    
    fn seek(&mut self, bucket: i32, offset: i64) -> PyResult<()> {
        if let (Some(_runtime), Some(_table_scan)) = (&self.runtime, &self.table_scan) {
            // Update subscription state for seeking
            self.subscribed_bucket = Some(bucket);
            self.subscribed_offset = Some(offset);
            
            println!("Seeked to bucket {} offset {} for {}", bucket, offset, self.table_path.database());
        } else {
            // 占位符实现
            println!("Seeking to bucket {} offset {} for {}", bucket, offset, self.table_path.database());
        }
        Ok(())
    }
    
    fn close(&mut self) -> PyResult<()> {
        if let (Some(_runtime), Some(_table_scan)) = (&self.runtime, &self.table_scan) {
            // Clear subscription state
            self.subscribed_bucket = None;
            self.subscribed_offset = None;
            
            println!("Closed log scanner for {}", self.table_path.database());
        } else {
            // 占位符实现
            println!("Closing log scanner for {}", self.table_path.database());
        }
        Ok(())
    }
    
    fn create_log_scanner(&self) -> PyLogScannerInstance {
        PyLogScannerInstance::new(self)
    }
    
    fn get_schema_info(&self) -> Vec<(String, String)> {
        self.schema_info.clone()
    }
    
    fn set_schema_info(&mut self, schema_info: Vec<(String, String)>) {
        self.schema_info = schema_info;
    }
}

impl PyLogScanner {
    pub fn new_with_real_scan(table_scan: TableScan, runtime: &Runtime) -> Self {
        // Since we can't clone Runtime, we need to create a new one
        // This is a limitation of the current design
        let new_runtime = Runtime::new().unwrap_or_else(|_| panic!("Failed to create runtime"));
        PyLogScanner {
            table_path: TablePath::new("real_table".to_string(), "initialized".to_string()),
            runtime: Some(new_runtime),
            table_scan: Some(table_scan),
            schema_info: vec![
                ("id".to_string(), "bigint".to_string()),
                ("name".to_string(), "string".to_string()),
                ("age".to_string(), "int".to_string()),
            ],
            subscribed_bucket: None,
            subscribed_offset: None,
        }
    }
}

// Python wrapper for BatchScanner
#[pyclass]
pub struct PyBatchScanner {
    table_path: TablePath,
}

#[pymethods]
impl PyBatchScanner {
    #[new]
    fn new(table_path: &PyTablePath) -> Self {
        PyBatchScanner { table_path: table_path.inner.clone() }
    }
    
    fn scan(&self, limit: Option<u64>) -> PyResult<Vec<PyBatchRecord>> {
        // 占位符实现
        let limit = limit.unwrap_or(100);
        println!("Scanning {} records from {}", limit, self.table_path.database());
        Ok(vec![
            PyBatchRecord::new(1, "batch_data1".to_string()),
            PyBatchRecord::new(2, "batch_data2".to_string()),
        ])
    }
    
    fn scan_with_filter(&self, filter: HashMap<String, PyObject>, limit: Option<u64>) -> PyResult<Vec<PyBatchRecord>> {
        // 占位符实现
        let limit = limit.unwrap_or(100);
        println!("Scanning {} records with filter from {}", limit, self.table_path.database());
        Ok(vec![])
    }
    
    fn close(&self) -> PyResult<()> {
        // 占位符实现
        println!("Closing batch scanner for {}", self.table_path.database());
        Ok(())
    }
}

// Python wrapper for LogRecord
#[pyclass]
pub struct PyLogRecord {
    offset: i64,
    timestamp: i64,
    change_type: String,
    // Store column values as a map for easy access
    column_values: HashMap<String, String>, // Use String instead of PyObject to avoid Clone issues
}

#[pymethods]
impl PyLogRecord {
    #[new]
    fn new(offset: i64, data: String) -> Self {
        let mut column_values = HashMap::new();
        column_values.insert("data".to_string(), data);
        
        PyLogRecord { 
            offset,
            timestamp: 0,
            change_type: "AppendOnly".to_string(),
            column_values,
        }
    }
    
    #[getter]
    fn offset(&self) -> i64 {
        self.offset
    }
    
    #[getter]
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
    
    #[getter]
    fn change_type(&self) -> String {
        self.change_type.clone()
    }
    
    fn get_value(&self, column: &str) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            match self.column_values.get(column) {
                Some(value) => {
                    // Try to parse as different types
                    if let Ok(int_val) = value.parse::<i64>() {
                        Ok(int_val.into_py(py))
                    } else if let Ok(float_val) = value.extract::<f64>() {
                        Ok(float_val.into_py(py))
                    } else if let Ok(bool_val) = value.extract::<bool>() {
                        Ok(bool_val.into_py(py))
                    } else {
                        Ok(value.clone().into_py(py))
                    }
                }
                None => Ok(py.None()),
            }
        })
    }
    
    fn get_column_names(&self) -> Vec<String> {
        self.column_values.keys().cloned().collect()
    }
    
    fn get_all_values(&self) -> HashMap<String, String> {
        self.column_values.clone()
    }
    
    fn __str__(&self) -> String {
        format!("LogRecord(offset={}, timestamp={}, change_type={}, columns={})", 
                self.offset, self.timestamp, self.change_type, self.column_values.len())
    }
}

impl PyLogRecord {
    // Create from real ScanRecord
    pub fn new_from_scan_record(scan_record: &ScanRecord, schema_info: &[(String, String)]) -> Self {
        let mut column_values = HashMap::new();
        let row = scan_record.row();
        
        for (i, (column_name, data_type)) in schema_info.iter().enumerate() {
            if i >= row.get_field_count() {
                break;
            }
            
            let value = if row.is_null_at(i) {
                "null".to_string()
            } else {
                match data_type.as_str() {
                    "int" => row.get_int(i).to_string(),
                    "bigint" => row.get_long(i).to_string(),
                    "float" => row.get_float(i).to_string(),
                    "double" => row.get_double(i).to_string(),
                    "boolean" => row.get_boolean(i).to_string(),
                    "tinyint" => row.get_byte(i).to_string(),
                    "smallint" => row.get_short(i).to_string(),
                    "string" => row.get_string(i),
                    "bytes" => format!("bytes[{}]", row.get_bytes(i).len()),
                    data_type if data_type.starts_with("char(") => {
                        // Extract length from char(N)
                        let length = data_type[5..data_type.len()-1].parse::<usize>().unwrap_or(1);
                        row.get_char(i, length)
                    },
                    data_type if data_type.starts_with("binary(") => {
                        // Extract length from binary(N)
                        let length = data_type[7..data_type.len()-1].parse::<usize>().unwrap_or(1);
                        format!("binary[{}]", row.get_binary(i, length).len())
                    },
                    _ => row.get_string(i), // Default to string
                }
            };
            
            column_values.insert(column_name.clone(), value);
        }
        
        PyLogRecord {
            offset: scan_record.offset(),
            timestamp: scan_record.timestamp(),
            change_type: scan_record.change_type().to_string(),
            column_values,
        }
    }
}

// Python wrapper for BatchRecord
#[pyclass]
pub struct PyBatchRecord {
    id: i64,
    data: String,
}

#[pymethods]
impl PyBatchRecord {
    #[new]
    fn new(id: i64, data: String) -> Self {
        PyBatchRecord { id, data }
    }
    
    #[getter]
    fn id(&self) -> i64 {
        self.id
    }
    
    #[getter]
    fn data(&self) -> String {
        self.data.clone()
    }
    
    fn get_value(&self, column: &str) -> PyResult<PyObject> {
        // 占位符实现
        Python::with_gil(|py| {
            match column {
                "id" => Ok(self.id.into_py(py)),
                "name" => Ok(self.data.clone().into_py(py)),
                _ => Ok(py.None()),
            }
        })
    }
    
    fn __str__(&self) -> String {
        format!("BatchRecord(id={}, data={})", self.id, self.data)
    }
}

// Python module
#[pymodule]
fn fluss_python(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Core classes
    m.add_class::<PyConnectionConfig>()?;
    m.add_class::<PyFlussConnection>()?;
    m.add_class::<PyFlussAdmin>()?;
    m.add_class::<PyTablePath>()?;
    m.add_class::<PySchema>()?;
    m.add_class::<PyTableDescriptor>()?;
    m.add_class::<PyTable>()?;
    m.add_class::<PyTableInfo>()?;
    
    // Data access classes
    m.add_class::<PyAppendWriter>()?;
    m.add_class::<PyWriterInstance>()?;
    m.add_class::<PyWriteResult>()?;
    m.add_class::<PyLogScanner>()?;
    m.add_class::<PyLogScannerInstance>()?;
    m.add_class::<PyBatchScanner>()?;
    m.add_class::<PyLogRecord>()?;
    m.add_class::<PyBatchRecord>()?;
    
    // Add some constants
    m.add("__version__", "0.1.0")?;
    m.add("DEFAULT_TIMEOUT", 30)?;
    m.add("DEFAULT_BATCH_SIZE", 1000)?;
    m.add("MAX_RETRY_COUNT", 3)?;
    
    Ok(())
}

// Python wrapper for Writer Instance
#[pyclass]
pub struct PyWriterInstance {
    table_path: TablePath,
}

#[pymethods]
impl PyWriterInstance {
    #[new]
    fn new(append_writer: &PyAppendWriter) -> Self {
        PyWriterInstance {
            table_path: append_writer.table_path.clone(),
        }
    }
    
    fn append(&self, batch_data: Vec<HashMap<String, PyObject>>) -> PyResult<()> {
        // 占位符实现 - 在真实实现中这里会调用底层的append方法
        println!("Appending batch of {} records to {}", batch_data.len(), self.table_path.database());
        Ok(())
    }
    
    fn flush(&self) -> PyResult<()> {
        println!("Flushing writer for {}", self.table_path.database());
        Ok(())
    }
    
    fn close(&self) -> PyResult<()> {
        println!("Closing writer for {}", self.table_path.database());
        Ok(())
    }
}

// Python wrapper for LogScanner Instance
#[pyclass]
pub struct PyLogScannerInstance {
    table_path: TablePath,
}

#[pymethods]
impl PyLogScannerInstance {
    #[new]
    fn new(log_scanner: &PyLogScanner) -> Self {
        PyLogScannerInstance {
            table_path: log_scanner.table_path.clone(),
        }
    }
    
    fn subscribe(&self, bucket: i32, offset: i64) -> PyResult<()> {
        // 占位符实现 - 在真实实现中这里会调用底层的subscribe方法
        println!("Subscribing to bucket {} at offset {} for {}", bucket, offset, self.table_path.database());
        Ok(())
    }
    
    fn poll(&self, timeout_secs: u64) -> PyResult<Vec<PyLogRecord>> {
        // 占位符实现 - 在真实实现中这里会调用底层的poll方法
        println!("Polling for {} seconds from {}", timeout_secs, self.table_path.database());
        Ok(vec![
            PyLogRecord::new(1, "test_record_1".to_string()),
            PyLogRecord::new(2, "test_record_2".to_string()),
        ])
    }
    
    fn seek(&self, bucket: i32, offset: i64) -> PyResult<()> {
        println!("Seeking to bucket {} offset {} for {}", bucket, offset, self.table_path.database());
        Ok(())
    }
}
