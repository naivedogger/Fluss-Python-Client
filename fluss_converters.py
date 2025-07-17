#!/usr/bin/env python3
"""Fluss 数据转换工具"""

import pyarrow as pa
import pandas as pd
import polars as pl
from typing import List, Dict, Any, Optional

def convert_string_to_type(value: str) -> Any:
    """尝试将字符串转换为适当的类型"""
    if value == "null":
        return None
    
    # 尝试转换为 boolean
    if value.lower() in ["true", "false"]:
        return value.lower() == "true"
    
    # 尝试转换为整数
    try:
        return int(value)
    except ValueError:
        pass
    
    # 尝试转换为浮点数
    try:
        return float(value)
    except ValueError:
        pass
    
    # 保持字符串
    return value

def records_to_dict(records) -> Dict[str, List[Any]]:
    """将 PyLogRecord 列表转换为字典格式"""
    if not records:
        return {}
    
    # 获取所有列名
    column_names = records[0].get_column_names()
    
    # 初始化结果字典
    result = {col: [] for col in column_names}
    
    # 提取每行数据
    for record in records:
        values = record.get_all_values()
        for col in column_names:
            # 获取原始值并进行类型转换
            raw_value = values.get(col, None)
            if raw_value == "null" or raw_value is None:
                result[col].append(None)
            else:
                # 尝试转换为适当的类型
                converted_value = convert_string_to_type(raw_value)
                result[col].append(converted_value)
    
    return result

def to_arrow(records) -> pa.Table:
    """将 PyLogRecord 列表转换为 PyArrow Table"""
    data_dict = records_to_dict(records)
    return pa.table(data_dict)

def to_pandas(records) -> pd.DataFrame:
    """将 PyLogRecord 列表转换为 Pandas DataFrame"""
    data_dict = records_to_dict(records)
    return pd.DataFrame(data_dict)

def to_polars(records) -> pl.DataFrame:
    """将 PyLogRecord 列表转换为 Polars DataFrame"""
    data_dict = records_to_dict(records)
    return pl.DataFrame(data_dict)

# 为了方便使用，创建全局函数
def records_to_arrow(records) -> pa.Table:
    """将 PyLogRecord 列表转换为 PyArrow Table"""
    return to_arrow(records)

def records_to_pandas(records) -> pd.DataFrame:
    """将 PyLogRecord 列表转换为 Pandas DataFrame"""
    return to_pandas(records)

def records_to_polars(records) -> pl.DataFrame:
    """将 PyLogRecord 列表转换为 Polars DataFrame"""
    return to_polars(records)
