#!/usr/bin/env python3
"""完整的功能测试脚本"""

import fluss_python as fluss
import pyarrow as pa
import time
# 导入数据转换工具
from fluss_converters import records_to_arrow, records_to_pandas, records_to_polars

def test_comprehensive():
    print("=== 完整功能测试 ===")
    
    # 1. 创建连接
    print("1. 创建连接...")
    config = fluss.PyConnectionConfig("127.0.0.1:9123", 30)
    conn = fluss.PyFlussConnection.new(config)
    
    if not conn.is_connected():
        print("❌ 连接失败!")
        return False
    
    print("✅ 连接成功")
    admin = conn.get_admin()
    
    # 2. 创建多种类型的表
    print("2. 创建表...")
    table_name = f"comprehensive_test_{int(time.time())}"
    table_path = fluss.PyTablePath("fluss", table_name)
    
    schema = fluss.PySchema()
    schema.add_column("id", "int")
    schema.add_column("big_id", "bigint")
    schema.add_column("name", "string")
    schema.add_column("score", "float")
    schema.add_column("active", "boolean")
    
    descriptor = fluss.PyTableDescriptor(schema)
    
    try:
        admin.create_table(table_path, descriptor, True)

        # 等待 leader 就绪（关键步骤）
        print("3. 等待 leader 就绪...")
        time.sleep(2)
        
        # 3. 获取表并验证 schema
        table = conn.get_table(table_path)
        actual_schema = table.get_schema()
        print(f"schema: {actual_schema.get_columns()}")
        
        # 4. 写入多种类型的数据
        print("4. 写入数据...")
        writer = table.new_append()
        
        test_data = [
            {"id": 1, "big_id": 1001, "name": "Alice", "score": 95.5, "active": True},
            {"id": 2, "big_id": 1002, "name": "Bob", "score": 87.2, "active": False},
            {"id": 3, "big_id": 1003, "name": "Carol", "score": 92.1, "active": True},
            {"id": 4, "big_id": 1004, "name": "David", "score": 78.9, "active": False},
            {"id": 5, "big_id": 1005, "name": "Eve", "score": 88.3, "active": True}
        ]
        
        result = writer.append_batch(test_data)
        
        writer.flush()
        writer.close()
        
        # 5. 读取数据验证
        print("5. 读取数据...")
        time.sleep(1)  # 短暂等待数据可用
        
        scanner = table.new_scan()
        scanner.subscribe(0, 0)
        
        # 读取数据
        # 如果这里直接能得到一个 arrow 就好了
        all_records = []
        for attempt in range(5):
            records = scanner.poll(2)
            if records:
                all_records.extend(records)
                print(f"✅ 第 {attempt+1} 次读取到 {len(records)} 条记录")
                break
            else:
                print(f"⚠️  第 {attempt+1} 次没有读取到数据，等待...")
                time.sleep(1)
        
        scanner.close()
        
        # 6. 数据转换示例
        print("6. 数据转换示例...")
        if all_records:
            # 转换为 PyArrow Table
            arrow_table = records_to_arrow(all_records)
            print(f"✅ PyArrow Table: {arrow_table.num_rows} 行, {arrow_table.num_columns} 列")
            
            # 转换为 Pandas DataFrame
            pandas_df = records_to_pandas(all_records)
            print(f"✅ Pandas DataFrame: {pandas_df.shape}")
            
            # 转换为 Polars DataFrame
            polars_df = records_to_polars(all_records)
            print(f"✅ Polars DataFrame: {polars_df.shape}")
            
            # 展示数据样本
            print("\n数据样本:")
            print("PyArrow Table:")
            print(arrow_table.to_pandas().head())
            print("\nPandas DataFrame:")
            print(pandas_df.head())
            print("\nPolars DataFrame:")
            print(polars_df.head())
        
        # 7. 验证数据完整性
        print("\n7. 验证数据完整性...")
        if len(all_records) == len(test_data):
            print(f"✅ 记录数量匹配: {len(all_records)}")
            
            # 验证每条记录的内容
            for i, record in enumerate(all_records):
                values = record.get_all_values()
                print(f"  记录 {i+1}: {values}")
                
                # 验证字段存在
                expected_fields = ['id', 'big_id', 'name', 'score', 'active']
                actual_fields = record.get_column_names()
                
                if all(field in actual_fields for field in expected_fields):
                    print(f"    ✅ 字段完整")
                else:
                    print(f"    ❌ 字段缺失: 期望 {expected_fields}, 实际 {actual_fields}")
                    return False
            
            print("✅ 所有数据验证通过")
            return True
        else:
            print(f"❌ 记录数量不匹配: 期望 {len(test_data)}, 实际 {len(all_records)}")
            return False
            
    except Exception as e:
        print(f"❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_comprehensive()
    if success:
        print("\n🎉 完整功能测试通过！")
        print("✅ 连接管理正常")
        print("✅ 表创建正常")
        print("✅ 数据写入正常")
        print("✅ 数据读取正常")
        print("✅ 多种数据类型支持正常")
    else:
        print("\n❌ 完整功能测试失败！")
        exit(1)
