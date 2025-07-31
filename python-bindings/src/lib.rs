use pyo3::prelude::*;
use pyo3::types::PyList;
use std::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

// Global runtime instance
static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

// Helper function to get or create the runtime
fn get_runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("Failed to create tokio runtime")
    })
}

use fluss_rust::{
    args::Args,
    connection::{FlussConnection as RustFlussConnection, ConnectionConfig as RustConnectionConfig},
    metadata::{TablePath as RustTablePath, Schema as RustSchema, SchemaBuilder as RustSchemaBuilder, TableDescriptor as RustTableDescriptor, TableInfo as RustTableInfo, DataType as RustDataType, Column as RustColumn, TableBucket as RustTableBucket},
    admin::admin::FlussAdmin as RustFlussAdmin,
    table::table::{Table as RustTable, TableAppend as RustTableAppend},
    table::scanner::{TableScan as RustTableScan}, 
    table::scanner::log::{LogScanner as RustLogScanner},
    record::{ScanRecord, row::InternalRow},
};

// Import Arrow types for data conversion
use arrow::array::{ArrayRef, Int64Array, StringArray, RecordBatch, BooleanArray, Float64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use arrow_pyarrow::{FromPyArrow, ToPyArrow};

// Import our datatype module
mod datatype;
use datatype::{DataType, datatype_to_string};

// Python wrapper for ConnectionConfig
#[pyclass]
pub struct ConnectionConfig {
    inner: RustConnectionConfig,
}

#[pymethods]
impl ConnectionConfig {
    #[new]
    #[pyo3(signature = (bootstrap_server, rw_timeout_secs = None))]
    fn new(bootstrap_server: String, rw_timeout_secs: Option<u64>) -> Self {
        let timeout = Duration::from_secs(rw_timeout_secs.unwrap_or(30));
        let mut args = Args::default();
        args.bootstrap_server = bootstrap_server;
        args.rw_timeout = timeout;
        
        ConnectionConfig {
            inner: RustConnectionConfig::from_args(args),
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
pub struct TablePath {
    inner: RustTablePath,
}

#[pymethods]
impl TablePath {
    #[new]
    fn new(database: String, table: String) -> Self {
        TablePath {
            inner: RustTablePath::new(database, table),
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
        format!("TablePath('{}', '{}')", self.inner.database(), self.inner.table())
    }
}

impl TablePath {
    /// Create a TablePath from a Rust TablePath
    pub fn from_rust_table_path(rust_table_path: RustTablePath) -> Self {
        TablePath {
            inner: rust_table_path,
        }
    }
}

#[pyclass]
pub struct Column {
    inner: RustColumn,
}

#[pymethods]
impl Column {
    #[new]
    fn new(name: String, data_type: &DataType) -> Self {
        Column {
            inner: RustColumn::new(&name, data_type.inner.clone()),
        }
    }

    fn with_comment(&self, comment: String) -> Self {
        Column {
            inner: self.inner.clone().with_comment(&comment),
        }
    }
    
    fn with_data_type(&self, data_type: &DataType) -> Self {
        Column {
            inner: self.inner.with_data_type(data_type.inner.clone()),
        }
    }
    
    #[getter]
    fn name(&self) -> String {
        self.inner.name().to_string()
    }
    
    #[getter]
    fn data_type(&self) -> DataType {
        DataType::from_inner(self.inner.data_type().clone())
    }
    
    fn __str__(&self) -> String {
        format!("Column(name='{}', type='{}')", self.name(), datatype_to_string(&self.data_type().inner))
    }
}

// Python wrapper for Schema
#[pyclass]
pub struct Schema {
    inner: RustSchema,
    // Store column information for easy access
    columns: Vec<(String, DataType)>,
}

#[pymethods]
impl Schema {
    #[new]
    #[pyo3(signature = (columns = None))]
    fn new(columns: Option<Vec<(String, Bound<'_, PyAny>)>>) -> PyResult<Self> {
        let mut column_data = Vec::new();
        let mut builder = RustSchemaBuilder::new();
        
        if let Some(cols) = columns {
            for (name, dtype_obj) in cols {
                // Extract DataType from PyAny
                let dtype: DataType = dtype_obj.extract()?;
                column_data.push((name.clone(), dtype.clone()));
                builder = builder.column(&name, dtype.inner.clone());
            }
        }
        
        Ok(Schema {
            inner: builder.build(),
            columns: column_data,
        })
    }
    
    fn add_column(&mut self, name: String, data_type: &DataType) -> PyResult<()> {
        // Add to columns list
        self.columns.push((name.clone(), data_type.clone()));
        
        // Rebuild the schema with all columns
        self.rebuild_schema()?;
        Ok(())
    }
    
    fn get_columns(&self) -> Vec<(String, String)> {
        self.columns.iter().map(|(name, dtype)| (name.clone(), datatype_to_string(&dtype.inner))).collect()
    }
    
    fn get_column_names(&self) -> Vec<String> {
        self.columns.iter().map(|(name, _)| name.clone()).collect()
    }
    
    fn get_column_type(&self, column_name: &str) -> Option<String> {
        self.columns.iter()
            .find(|(name, _)| name == column_name)
            .map(|(_, dtype)| datatype_to_string(&dtype.inner))
    }
    
    fn column_count(&self) -> usize {
        self.columns.len()
    }
    
    // Helper method to rebuild schema from all columns
    fn rebuild_schema(&mut self) -> PyResult<()> {
        let mut builder = RustSchemaBuilder::new();
        for (name, data_type) in &self.columns {
            builder = builder.column(name, data_type.inner.clone());
        }
        self.inner = builder.build();
        Ok(())
    }
    
    fn __str__(&self) -> String {
        format!("Schema: columns={:?}", self.get_columns())
    }
}

impl Schema {
    /// Internal method to create a Schema from a Rust Schema
    pub fn from_inner(rust_schema: RustSchema) -> Self {
        let mut columns = Vec::new();
        for column in rust_schema.columns() {
            let column_name = column.name().to_string();
            let column_type = DataType::from_inner(column.data_type().clone());
            columns.push((column_name, column_type));
        }
        
        Schema {
            inner: rust_schema,
            columns,
        }
    }
}

// Python wrapper for TableDescriptor
#[pyclass]
pub struct TableDescriptor {
    inner: RustTableDescriptor,
    schema: RustSchema,
    comment: Option<String>,
    partition_keys: Vec<String>,
    bucket_count: Option<i32>,
    bucket_keys: Vec<String>,
    properties: HashMap<String, String>,
    custom_properties: HashMap<String, String>,
}

#[pymethods]
impl TableDescriptor {
    #[new]
    fn new(schema: &Schema) -> Self {
        let builder = RustTableDescriptor::builder().schema(schema.inner.clone());
        let inner = builder.build();
        TableDescriptor {
            inner,
            schema: schema.inner.clone(),
            comment: None,
            partition_keys: Vec::new(),
            bucket_count: None,
            bucket_keys: Vec::new(),
            properties: HashMap::new(),
            custom_properties: HashMap::new(),
        }
    }

    fn comment(&mut self, comment: String) {
        self.comment = Some(comment);
        self.rebuild_inner();
    }

    fn partition_by(&mut self, partition_keys: Vec<String>) {
        self.partition_keys = partition_keys;
        self.rebuild_inner();
    }

    fn distributed_by(&mut self, bucket_count: Option<i32>, bucket_keys: Vec<String>) {
        self.bucket_count = bucket_count;
        self.bucket_keys = bucket_keys;
        self.rebuild_inner();
    }

    fn log_format(&mut self, _log_format: String) {
        // TODO: need to convert String into LogFormat enum
        // For now, this is a no-op until LogFormat is properly implemented
    }

    fn kv_format(&mut self, _kv_format: String) {
        // TODO: need to convert String into KVFormat enum
        // For now, this is a no-op until KVFormat is properly implemented
    }

    fn property(&mut self, key: &str, value: String) {
        self.properties.insert(key.to_string(), value);
        self.rebuild_inner();
    }

    fn properties(&mut self, properties: HashMap<String, String>) {
        self.properties.extend(properties);
        self.rebuild_inner();
    }

    fn custom_property(&mut self, key: &str, value: &str) {
        self.custom_properties.insert(key.to_string(), value.to_string());
        self.rebuild_inner();
    }

    fn custom_properties(&mut self, properties: HashMap<String, String>) {
        self.custom_properties.extend(properties);
        self.rebuild_inner();
    }
    
    fn __str__(&self) -> String {
        // Convert RustSchema to our Python Schema for display
        let py_schema = Schema::from_inner(self.schema.clone());
        
        format!("TableDescriptor: {}, partition_keys={:?}, bucket_count={:?}, bucket_keys={:?}, properties={:?}, custom_properties={:?}",
            py_schema.__str__(), self.partition_keys, self.bucket_count, self.bucket_keys, self.properties, self.custom_properties)
    }
}

impl TableDescriptor {
    fn rebuild_inner(&mut self) {
        let mut builder = RustTableDescriptor::builder()
            .schema(self.schema.clone())
            .partitioned_by(self.partition_keys.clone())
            .properties(self.properties.clone())
            .custom_properties(self.custom_properties.clone());

        if let Some(ref comment) = self.comment {
            builder = builder.comment(comment);
        }

        if !self.bucket_keys.is_empty() {
            builder = builder.distributed_by(self.bucket_count, self.bucket_keys.clone());
        }

        self.inner = builder.build();
    }
}

// Python wrapper for FlussConnection
#[pyclass]
pub struct FlussConnection {
    connection: Option<RustFlussConnection>,
    config: RustConnectionConfig,
}

#[pymethods]
impl FlussConnection {
    #[new]
    fn new(config: &ConnectionConfig) -> PyResult<Self> {
        let connection_config = config.inner.clone();
        
        // Use global runtime to execute async operation
        let connection = get_runtime().block_on(async {
            let timeout_duration = std::time::Duration::from_secs(10);
            match tokio::time::timeout(timeout_duration, RustFlussConnection::new(connection_config.clone())).await {
                Ok(conn) => Ok(conn),
                Err(_) => Err("Failed to create FlussConnection: timeout after 10 seconds".to_string()),
            }
        });
        
        match connection {
            Ok(conn) => Ok(FlussConnection {
                connection: Some(conn),
                config: connection_config,
            }),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e)),
        }
    }
    
    fn get_admin(&self) -> PyResult<FlussAdmin> {
        if let Some(ref conn) = self.connection {
            let admin = conn.get_admin();
            Ok(FlussAdmin::from_rust_admin(admin))
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No connection available - connection may have failed during initialization"
            ))
        }
    }
    
    fn get_table(&self, table_path: &TablePath) -> PyResult<Table> {
        if let Some(ref conn) = self.connection {
            let table_path_rust = table_path.inner.clone();
            let table = get_runtime().block_on(async {
                conn.get_table(&table_path_rust).await
            });
            
            Ok(Table::from_rust_table(table, table_path))
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
        // TODO: Implement connection close
        Ok(())
    }
}

// Python wrapper for FlussAdmin
#[pyclass]
pub struct FlussAdmin {
    admin: Option<RustFlussAdmin>,
}

#[pymethods]
impl FlussAdmin {
    #[new]
    fn new() -> Self {
        FlussAdmin {
            admin: None,
        }
    }
    
    fn create_table(&self, table_path: &TablePath, descriptor: &TableDescriptor, ignore_if_exists: bool) -> PyResult<()> {
        if let Some(admin) = &self.admin {
            let table_path_rust = table_path.inner.clone();
            let descriptor_rust = descriptor.inner.clone();
            
            get_runtime().block_on(async {
                admin.create_table(&table_path_rust, &descriptor_rust, ignore_if_exists).await
            }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to create table: {}", e)
            ))?;
            
            println!("Table {} created successfully", table_path.__str__());
        } else {
            // TODO: Implement create_table without real admin connection
        }
        Ok(())
    }
    
    fn drop_table(&self, table_path: &TablePath, ignore_if_not_exists: bool) -> PyResult<()> {
        // TODO: Implement drop_table when available in Fluss server
        Ok(())
    }
    
    fn get_table(&self, table_path: &TablePath) -> PyResult<TableInfo> {
        if let Some(admin) = &self.admin {
            let table_path_rust = table_path.inner.clone();
            
            let table_info = get_runtime().block_on(async {
                admin.get_table(&table_path_rust).await
            }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Failed to get table: {}", e)
            ))?;
            
            Ok(TableInfo::from_rust_table_info(table_info))
        } else {
            // TODO: exception handling for no admin connection
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No admin connection available - cannot get table info"
            ))
        }
    }
    
    fn list_tables(&self, database: Option<String>) -> PyResult<Vec<String>> {
        // TODO: Implement list_tables API when available in Fluss server
        let db = database.unwrap_or_else(|| "default".to_string());
        Ok(vec![format!("{}.table1", db), format!("{}.table2", db)])
    }
    
    fn table_exists(&self, table_path: &TablePath) -> PyResult<bool> {
        if let Some(admin) = &self.admin {
            let table_path_rust = table_path.inner.clone();
            
            let exists = get_runtime().block_on(async {
                match admin.get_table(&table_path_rust).await {
                    Ok(_) => true,
                    Err(_) => false,
                }
            });
            
            Ok(exists)
        } else {
            // TODO: handle exception
            Ok(true)
        }
    }
}

impl FlussAdmin {
    pub fn from_rust_admin(admin: RustFlussAdmin) -> Self {
        FlussAdmin {
            admin: Some(admin),
        }
    }
}

// Python wrapper for Table
#[pyclass]
pub struct Table {
    table_path: RustTablePath,
    table: Option<RustTable>,
}

#[pymethods]
impl Table {
    #[new]
    fn new(table_path: &TablePath) -> Self {
        Table { 
            table_path: table_path.inner.clone(),
            table: None,
        }
    }
    
    fn get_path(&self) -> TablePath {
        TablePath { inner: self.table_path.clone() }
    }
    
    // java client 那边，newAppend 之后还要 createWriter
    fn new_append(&self) -> AppendWriter {
        if let Some(table) = &self.table {
            let table_append = table.new_append();
            let table_info = table.get_table_info();
            let schema = table_info.schema.clone();
            AppendWriter::from_rust_append(table_append, Some(schema))
        } else {
            AppendWriter::new(&TablePath { inner: self.table_path.clone() })
        }
    }
    
    fn new_scan(&self) -> PyResult<TableScan> {
        if let Some(table) = &self.table {
            let table_scan = table.new_scan();
            let schema = table.get_table_info().schema.clone();
            Ok(TableScan::from_rust_table_scan(table_scan, schema))
        } else {
            // handle error
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No active table connection - please ensure connection is established before scanning"
            ))
        }
    }
    
    fn get_schema(&self) -> PyResult<Schema> {
        if let Some(table) = &self.table {
            let table_info = table.get_table_info();
            
            // Clone the schema and create Python wrapper directly
            let schema = Schema::from_inner(table_info.schema.clone());
            
            Ok(schema)
        } else {
            // TODO: handle exception
            let schema = Schema::new(None)?;
            Ok(schema)
        }
    }
}

impl Table {
    pub fn from_rust_table(table: RustTable, table_path: &TablePath) -> Self {
        Table {
            table_path: table_path.inner.clone(),
            table: Some(table),
        }
    }
}

// Python wrapper for TableInfo
#[pyclass]
pub struct TableInfo {
    inner: RustTableInfo,
}

#[pymethods]
impl TableInfo {
    
    fn get_table_path(&self) -> TablePath {
        TablePath::from_rust_table_path(self.inner.get_table_path().clone())
    }
    
    fn __str__(&self) -> String {
        format!("TableInfo(path={})", 
                self.get_table_path().database())
    }
}

impl TableInfo {
    pub fn from_rust_table_info(table_info: RustTableInfo) -> Self {
        TableInfo {
            inner: table_info,
        }
    }
}

// Python wrapper for AppendWriter
#[pyclass]
pub struct AppendWriter {
    table_path: RustTablePath,
    table_append: Option<RustTableAppend>,
    schema: Option<RustSchema>,
}

#[pymethods]
impl AppendWriter {
    #[new]
    fn new(table_path: &TablePath) -> Self {
        AppendWriter { 
            table_path: table_path.inner.clone(),
            table_append: None,
            schema: None,
        }
    }
    
    fn append(&self, row_data: HashMap<String, PyObject>) -> PyResult<()> {
        if let Some(table_append) = &self.table_append {
            // Convert single row to batch format
            let batch_data = vec![row_data];
            
            // Convert to RecordBatch
            let record_batch = self.convert_to_record_batch(&batch_data)?;
            
            // Create writer and append
            let writer = table_append.create_writer();
            let result = get_runtime().block_on(async {
                writer.append(record_batch).await
            });
            
            match result {
                Ok(_) => Ok(()),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to append row: {}", e)
                ))
            }
        } else {
            // exception
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No active table connection - please ensure connection is established before writing"
            ))
        }
    }
    
    // support both Pyarrow RecordBatch and list of dictionaries
    fn append_batch(&self, batch_data: &Bound<'_, PyAny>) -> PyResult<()> {
        if let Some(table_append) = &self.table_append {
            // Try to use Arrow FFI first (zero-copy for PyArrow RecordBatch)
            let record_batch = if self.is_arrow_record_batch(batch_data)? {
                // Use Arrow FFI to directly convert PyArrow RecordBatch to Rust RecordBatch
                RecordBatch::from_pyarrow_bound(batch_data).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to convert PyArrow RecordBatch via FFI: {}", e)
                    )
                })?
            } else {
                // Fallback: assume it's a list of dictionaries
                let dict_data: Vec<HashMap<String, PyObject>> = batch_data.extract()?;
                if dict_data.is_empty() {
                    return Ok(());
                }
                self.convert_to_record_batch(&dict_data)?
            };
            
            if record_batch.num_rows() == 0 {
                return Ok(());
            }
            
            // Create writer and append
            let writer = table_append.create_writer();
            let result = get_runtime().block_on(async {
                writer.append(record_batch).await
            });
            
            match result {
                Ok(_) => Ok(()),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    format!("Failed to append batch: {}", e)
                ))
            }
        } else {
            Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "No active table connection - please ensure connection is established before writing"
            ))
        }
    }
    
    fn flush(&self) -> PyResult<()> {
        // TODO: Implement flush for append writer
        Ok(())
    }
    
    fn close(&self) -> PyResult<()> {
        if let Some(_table_append) = &self.table_append {
            // TODO: Implement close with real table append
        } else {
            // TODO: Implement close without real table append
        }
        Ok(())
    }
}

impl AppendWriter {
    pub fn from_rust_append(table_append: RustTableAppend, schema: Option<RustSchema>) -> Self {
        AppendWriter {
            table_path: RustTablePath::new("real_table".to_string(), "initialized".to_string()),
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
                    RustDataType::Boolean(_) => ArrowDataType::Boolean,
                    RustDataType::TinyInt(_) => ArrowDataType::Int8,
                    RustDataType::SmallInt(_) => ArrowDataType::Int16,
                    RustDataType::Int(_) => ArrowDataType::Int32,
                    RustDataType::BigInt(_) => ArrowDataType::Int64,
                    RustDataType::Float(_) => ArrowDataType::Float32,
                    RustDataType::Double(_) => ArrowDataType::Float64,
                    RustDataType::String(_) | RustDataType::Char(_) => ArrowDataType::Utf8,
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
    
    // Helper function to check if object is a PyArrow RecordBatch
    fn is_arrow_record_batch(&self, py_object: &Bound<'_, PyAny>) -> PyResult<bool> {
        Python::with_gil(|py| {
            // Try to import pyarrow and check if object is a RecordBatch
            match py.import("pyarrow") {
                Ok(pyarrow) => {
                    let record_batch_type = pyarrow.getattr("RecordBatch")?;
                    Ok(py_object.is_instance(&record_batch_type)?)
                }
                Err(_) => Ok(false), // PyArrow not available
            }
        })
    }
}

#[pyclass(unsendable)]
pub struct TableScan {
    inner: RustTableScan,
    schema: RustSchema,
}

#[pymethods]
impl TableScan {
    fn create_log_scanner(&self) -> LogScanner {
        let rust_log_scanner = self.inner.create_log_scanner();
        // Extract schema info from table_info for the LogScanner
        LogScanner::from_rust_log_scan(rust_log_scanner, self.schema.clone())
    }
}

impl TableScan {
    pub fn from_rust_table_scan(rust_scan: RustTableScan, schema: RustSchema) -> Self {
        TableScan {
            inner: rust_scan,
            schema: schema,
        }
    }
}

#[pyclass]
pub struct TableBucket {
    inner: RustTableBucket,
}

#[pymethods]
impl TableBucket {
    #[new]
    fn new(table_id: i64, bucket_id: i32) -> Self {
        TableBucket {
            inner: RustTableBucket::new(table_id, bucket_id),
        }
    }

    #[getter]
    fn table_id(&self) -> i64 {
        self.inner.table_id()
    } 
    
    #[getter]
    fn bucket_id(&self) -> i32 {
        self.inner.bucket_id()
    }
    
    #[getter]
    fn partition_id(&self) -> Option<i64> {
        self.inner.partition_id()
    }
    
    fn __str__(&self) -> String {
        format!("TableBucket(table_id={}, bucket_id={})", 
                self.table_id(), self.bucket_id())
    }
}

// Python wrapper for LogScanner
#[pyclass(unsendable)]
pub struct LogScanner {
    inner: RustLogScanner,
    schema: RustSchema,
}

#[pymethods]
impl LogScanner {
    fn subscribe(&mut self, bucket: i32, offset: i64) -> PyResult<()> {
        get_runtime().block_on(async {
            self.inner.subscribe(bucket, offset).await;
            Ok(())
        })
    }
    
    // 这里返回的内部每个元素是一个 Arrow RecordBatch
    fn poll(&self, timeout_secs: u64) -> PyResult<Vec<PyObject>> {
        let timeout = Duration::from_secs(timeout_secs);
        let records = get_runtime().block_on(async {
            self.inner.poll(timeout).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            format!("Failed to poll log records: {}", e)
        ))?;
        
        let mut py_batches = Vec::new();
        
        for record in records {
            let columnar_row = record.row();
            // 问题：现在貌似每个 ColumnarRow 就只有一行
            let record_batch = columnar_row.get_record_batch();
            let row_id = columnar_row.row_id();

            // 这里有个问题：ColumnarRow 实际上是一个 row
            // 但是 recordBatch 是 arrow 格式的几条记录
            // ColumnarRow 是根据 row_id 获取的
            // 所以就重复了。

            Python::with_gil(|py| {
                match record_batch.to_pyarrow(py) {
                    Ok(py_batch) => {
                        if row_id == 0 {
                            // workaround for duplicate RecordBatch
                            py_batches.push(py_batch);
                        }
                        Ok(())
                    }
                    Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                        format!("Failed to convert RecordBatch to PyArrow: {}", e)
                    ))
                }
            })?;
        }
        
        Ok(py_batches)
    }
    
    fn seek(&mut self, bucket: i32, offset: i64) -> PyResult<()> {
        // TODO
        Ok(())
    }
    
    fn close(&mut self) -> PyResult<()> {
        // TODO
        Ok(())
    }
}

impl LogScanner {
    pub fn from_rust_log_scan(log_scanner: RustLogScanner, schema: RustSchema) -> Self {
        LogScanner {
            inner: log_scanner,
            schema: schema,
        }
    }
}

// Python wrapper for BatchScanner
#[pyclass]
pub struct PyBatchScanner {
    table_path: RustTablePath,
}

#[pymethods]
impl PyBatchScanner {
    #[new]
    fn new(table_path: &TablePath) -> Self {
        PyBatchScanner { table_path: table_path.inner.clone() }
    }
    
    fn scan(&self, limit: Option<u64>) -> PyResult<Vec<PyBatchRecord>> {
        // TODO: Implement scan for batch scanner
        Ok(vec![])
    }
    
    fn scan_with_filter(&self, filter: HashMap<String, PyObject>, limit: Option<u64>) -> PyResult<Vec<PyBatchRecord>> {
        // TODO: Implement scan_with_filter
        let limit = limit.unwrap_or(100);
        Ok(vec![])
    }
    
    fn close(&self) -> PyResult<()> {
        // TODO: Implement close for batch scanner
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
                    } else if let Ok(float_val) = value.parse::<f64>() {
                        Ok(float_val.into_py(py))
                    } else if let Ok(bool_val) = value.parse::<bool>() {
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
        // TODO: Implement get_value for batch record
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
    m.add_class::<ConnectionConfig>()?;
    m.add_class::<FlussConnection>()?;
    m.add_class::<FlussAdmin>()?;
    m.add_class::<TablePath>()?;
    m.add_class::<Schema>()?;
    m.add_class::<Column>()?;
    m.add_class::<DataType>()?;
    m.add_class::<TableDescriptor>()?;
    m.add_class::<Table>()?;
    m.add_class::<TableInfo>()?;
    m.add_class::<TableScan>()?;
    
    // Data access classes
    m.add_class::<AppendWriter>()?;
    m.add_class::<LogScanner>()?;
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