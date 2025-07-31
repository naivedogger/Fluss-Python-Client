use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use fluss_rust::metadata::{
    DataType as RustDataType, DataTypes, BooleanType, TinyIntType, SmallIntType, IntType, BigIntType,
    FloatType, DoubleType, CharType, StringType, DecimalType, DateType, TimeType,
    TimestampType, TimestampLTzType, BytesType, BinaryType, ArrayType, MapType, RowType
};

/// Python wrapper for Fluss DataType
#[pyclass(name = "DataType")]
#[derive(Clone)]
pub struct DataType {
    pub(crate) inner: RustDataType,
}

#[pymethods]
impl DataType {
    // 基础类型的静态构造方法
    #[staticmethod]
    fn boolean() -> Self {
        DataType { inner: DataTypes::boolean() }
    }

    #[staticmethod]
    fn tinyint() -> Self {
        DataType { inner: DataTypes::tinyint() }
    }

    #[staticmethod]
    fn smallint() -> Self {
        DataType { inner: DataTypes::smallint() }
    }

    #[staticmethod]
    fn int() -> Self {
        DataType { inner: DataTypes::int() }
    }

    #[staticmethod]
    fn bigint() -> Self {
        DataType { inner: DataTypes::bigint() }
    }

    #[staticmethod]
    fn float() -> Self {
        DataType { inner: DataTypes::float() }
    }

    #[staticmethod]
    fn double() -> Self {
        DataType { inner: DataTypes::double() }
    }

    #[staticmethod]
    fn string() -> Self {
        DataType { inner: DataTypes::string() }
    }

    #[staticmethod]
    fn bytes() -> Self {
        DataType { inner: DataTypes::bytes() }
    }

    #[staticmethod]
    fn date() -> Self {
        DataType { inner: DataTypes::date() }
    }

    #[staticmethod]
    fn time() -> Self {
        DataType { inner: DataTypes::time() }
    }

    #[staticmethod]
    fn timestamp() -> Self {
        DataType { inner: DataTypes::timestamp() }
    }

    #[staticmethod]
    fn timestamp_ltz() -> Self {
        DataType { inner: DataTypes::timestamp_ltz() }
    }

    // 参数化类型的构造方法
    #[staticmethod]
    fn decimal(precision: u32, scale: u32) -> PyResult<Self> {
        if precision < 1 || precision > 38 {
            return Err(PyValueError::new_err(
                "Decimal precision must be between 1 and 38"
            ));
        }
        if scale > precision {
            return Err(PyValueError::new_err(
                "Decimal scale cannot be greater than precision"
            ));
        }
        Ok(DataType { inner: DataTypes::decimal(precision, scale) })
    }

    #[staticmethod]
    fn char(length: u32) -> PyResult<Self> {
        if length == 0 {
            return Err(PyValueError::new_err("Char length must be greater than 0"));
        }
        Ok(DataType { inner: DataTypes::char(length) })
    }

    #[staticmethod]
    fn binary(length: usize) -> PyResult<Self> {
        if length == 0 {
            return Err(PyValueError::new_err("Binary length must be greater than 0"));
        }
        Ok(DataType { inner: DataTypes::binary(length) })
    }

    #[staticmethod]
    fn time_with_precision(precision: u32) -> PyResult<Self> {
        if precision > 9 {
            return Err(PyValueError::new_err(
                "Time precision must be between 0 and 9"
            ));
        }
        Ok(DataType { inner: DataTypes::time_with_precision(precision) })
    }

    #[staticmethod]
    fn timestamp_with_precision(precision: u32) -> PyResult<Self> {
        if precision > 9 {
            return Err(PyValueError::new_err(
                "Timestamp precision must be between 0 and 9"
            ));
        }
        Ok(DataType { inner: DataTypes::timestamp_with_precision(precision) })
    }

    #[staticmethod]
    fn timestamp_ltz_with_precision(precision: u32) -> PyResult<Self> {
        if precision > 9 {
            return Err(PyValueError::new_err(
                "Timestamp LTZ precision must be between 0 and 9"
            ));
        }
        Ok(DataType { inner: DataTypes::timestamp_ltz_with_precision(precision) })
    }

    #[staticmethod]
    fn array(element_type: &DataType) -> Self {
        DataType { 
            inner: DataTypes::array(element_type.inner.clone()) 
        }
    }

    #[staticmethod]
    fn map(key_type: &DataType, value_type: &DataType) -> Self {
        DataType { 
            inner: DataTypes::map(key_type.inner.clone(), value_type.inner.clone()) 
        }
    }

    // 获取类型信息
    fn type_name(&self) -> String {
        match &self.inner {
            RustDataType::Boolean(_) => "Boolean".to_string(),
            RustDataType::TinyInt(_) => "TinyInt".to_string(),
            RustDataType::SmallInt(_) => "SmallInt".to_string(),
            RustDataType::Int(_) => "Int".to_string(),
            RustDataType::BigInt(_) => "BigInt".to_string(),
            RustDataType::Float(_) => "Float".to_string(),
            RustDataType::Double(_) => "Double".to_string(),
            RustDataType::String(_) => "String".to_string(),
            RustDataType::Bytes(_) => "Bytes".to_string(),
            RustDataType::Date(_) => "Date".to_string(),
            RustDataType::Time(t) => format!("Time({})", t.precision()),
            RustDataType::Timestamp(t) => format!("Timestamp({})", t.precision()),
            RustDataType::TimestampLTz(t) => format!("TimestampLTZ({})", t.precision()),
            RustDataType::Char(c) => format!("Char({})", c.length()),
            RustDataType::Decimal(d) => format!("Decimal({}, {})", d.precision(), d.scale()),
            RustDataType::Binary(b) => format!("Binary({})", b.length()),
            RustDataType::Array(_) => "Array".to_string(),
            RustDataType::Map(_) => "Map".to_string(),
            RustDataType::Row(_) => "Row".to_string(),
        }
    }

    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }

    fn as_non_nullable(&self) -> Self {
        DataType { 
            inner: self.inner.as_non_nullable() 
        }
    }

    // 类型检查方法
    fn is_boolean(&self) -> bool {
        matches!(self.inner, RustDataType::Boolean(_))
    }

    fn is_integer(&self) -> bool {
        matches!(self.inner, RustDataType::TinyInt(_) | RustDataType::SmallInt(_) | RustDataType::Int(_) | RustDataType::BigInt(_))
    }

    fn is_floating_point(&self) -> bool {
        matches!(self.inner, RustDataType::Float(_) | RustDataType::Double(_))
    }

    fn is_numeric(&self) -> bool {
        self.is_integer() || self.is_floating_point() || matches!(self.inner, RustDataType::Decimal(_))
    }

    fn is_string_like(&self) -> bool {
        matches!(self.inner, RustDataType::String(_) | RustDataType::Char(_))
    }

    fn is_temporal(&self) -> bool {
        matches!(self.inner, RustDataType::Date(_) | RustDataType::Time(_) | RustDataType::Timestamp(_) | RustDataType::TimestampLTz(_))
    }

    fn is_complex(&self) -> bool {
        matches!(self.inner, RustDataType::Array(_) | RustDataType::Map(_) | RustDataType::Row(_))
    }

    // 获取参数信息（如果适用）
    fn get_precision(&self) -> Option<u32> {
        match &self.inner {
            RustDataType::Decimal(d) => Some(d.precision()),
            RustDataType::Time(t) => Some(t.precision()),
            RustDataType::Timestamp(t) => Some(t.precision()),
            RustDataType::TimestampLTz(t) => Some(t.precision()),
            _ => None,
        }
    }

    fn get_scale(&self) -> Option<u32> {
        match &self.inner {
            RustDataType::Decimal(d) => Some(d.scale()),
            _ => None,
        }
    }

    fn get_length(&self) -> Option<u32> {
        match &self.inner {
            RustDataType::Char(c) => Some(c.length()),
            RustDataType::Binary(b) => Some(b.length() as u32),
            _ => None,
        }
    }

    // 获取复合类型的元素类型
    fn get_element_type(&self) -> Option<DataType> {
        match &self.inner {
            RustDataType::Array(arr) => Some(DataType { inner: arr.element_type().clone() }),
            _ => None,
        }
    }

    fn get_key_type(&self) -> Option<DataType> {
        match &self.inner {
            RustDataType::Map(map) => Some(DataType { inner: map.key_type().clone() }),
            _ => None,
        }
    }

    fn get_value_type(&self) -> Option<DataType> {
        match &self.inner {
            RustDataType::Map(map) => Some(DataType { inner: map.value_type().clone() }),
            _ => None,
        }
    }

    // Python 特殊方法
    fn __str__(&self) -> String {
        datatype_to_string(&self.inner)
    }

    fn __repr__(&self) -> String {
        format!("DataType.{}", self.type_name())
    }

    fn __eq__(&self, other: &DataType) -> bool {
        self.inner == other.inner
    }

    fn __hash__(&self) -> isize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        self.inner.hash(&mut hasher);
        hasher.finish() as isize
    }
}

impl DataType {
    /// 内部使用：从 Rust DataType 创建
    pub fn from_inner(inner: RustDataType) -> Self {
        DataType { inner }
    }

    /// 内部使用：获取内部的 Rust DataType
    pub fn into_inner(self) -> RustDataType {
        self.inner
    }

    /// 内部使用：获取内部 DataType 的引用
    pub fn inner(&self) -> &RustDataType {
        &self.inner
    }
}

// 辅助函数：将 DataType 转换为字符串
pub fn datatype_to_string(data_type: &RustDataType) -> String {
    match data_type {
        RustDataType::Boolean(_) => "boolean".to_string(),
        RustDataType::TinyInt(_) => "tinyint".to_string(),
        RustDataType::SmallInt(_) => "smallint".to_string(),
        RustDataType::Int(_) => "int".to_string(),
        RustDataType::BigInt(_) => "bigint".to_string(),
        RustDataType::Float(_) => "float".to_string(),
        RustDataType::Double(_) => "double".to_string(),
        RustDataType::String(_) => "string".to_string(),
        RustDataType::Bytes(_) => "bytes".to_string(),
        RustDataType::Date(_) => "date".to_string(),
        RustDataType::Time(t) => {
            if t.precision() == 0 {
                "time".to_string()
            } else {
                format!("time({})", t.precision())
            }
        },
        RustDataType::Timestamp(t) => {
            if t.precision() == 6 {
                "timestamp".to_string()
            } else {
                format!("timestamp({})", t.precision())
            }
        },
        RustDataType::TimestampLTz(t) => {
            if t.precision() == 6 {
                "timestamp_ltz".to_string()
            } else {
                format!("timestamp_ltz({})", t.precision())
            }
        },
        RustDataType::Char(c) => format!("char({})", c.length()),
        RustDataType::Decimal(d) => format!("decimal({},{})", d.precision(), d.scale()),
        RustDataType::Binary(b) => format!("binary({})", b.length()),
        RustDataType::Array(arr) => format!("array<{}>", datatype_to_string(arr.element_type())),
        RustDataType::Map(map) => format!("map<{},{}>", 
                                     datatype_to_string(map.key_type()), 
                                     datatype_to_string(map.value_type())),
        RustDataType::Row(row) => {
            let fields: Vec<String> = row.fields().iter()
                .map(|field| format!("{}: {}", field.name(), datatype_to_string(field.data_type())))
                .collect();
            format!("row<{}>", fields.join(", "))
        },
    }
}