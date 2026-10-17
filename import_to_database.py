"""
淘宝用户购物行为数据集 - 数据库导入脚本
将 UserBehavior.csv 导入 SQLite 数据库
数据量: ~1亿行, 3.42GB
"""

import sqlite3
import pandas as pd
import time
import os

# === 配置 ===
CSV_PATH = r"f:\课程\5488big data\group project\UserBehavior.csv"
DB_PATH = r"f:\课程\5488big data\group project\UserBehavior.db"
CHUNK_SIZE = 500_000  # 每次读取50万行

# === 列名定义（CSV无表头）===
COLUMNS = ['user_id', 'item_id', 'category_id', 'behavior_type', 'timestamp']

def create_database():
    """创建SQLite数据库并导入数据"""
    
    # 如果数据库已存在，先删除
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print("已删除旧数据库文件")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 创建表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_behavior (
            user_id INTEGER NOT NULL,
            item_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            behavior_type TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        )
    ''')
    conn.commit()
    print("数据表 user_behavior 已创建")
    
    # 分块读取CSV并导入
    print(f"\n开始导入数据 (每块 {CHUNK_SIZE:,} 行)...")
    total_rows = 0
    start_time = time.time()
    
    for i, chunk in enumerate(pd.read_csv(
        CSV_PATH,
        names=COLUMNS,
        chunksize=CHUNK_SIZE,
        dtype={
            'user_id': 'int32',
            'item_id': 'int32',
            'category_id': 'int32',
            'behavior_type': 'category',
            'timestamp': 'int64'
        }
    )):
        chunk_start = time.time()
        chunk.to_sql('user_behavior', conn, if_exists='append', index=False)
        total_rows += len(chunk)
        chunk_time = time.time() - chunk_start
        elapsed = time.time() - start_time
        print(f"  块 {i+1}: 导入 {len(chunk):,} 行 | 累计 {total_rows:,} 行 | "
              f"本块 {chunk_time:.1f}s | 总耗时 {elapsed:.1f}s")
    
    # 创建索引以加速查询
    print("\n创建索引...")
    
    print("  创建 behavior_type 索引...")
    cursor.execute('CREATE INDEX idx_behavior ON user_behavior(behavior_type)')
    
    print("  创建 timestamp 索引...")
    cursor.execute('CREATE INDEX idx_timestamp ON user_behavior(timestamp)')
    
    print("  创建 user_id 索引...")
    cursor.execute('CREATE INDEX idx_user ON user_behavior(user_id)')
    
    print("  创建 category_id 索引...")
    cursor.execute('CREATE INDEX idx_category ON user_behavior(category_id)')
    
    conn.commit()
    
    # 验证导入结果
    result = cursor.execute('SELECT COUNT(*) FROM user_behavior').fetchone()
    print(f"\n导入完成！总行数: {result[0]:,}")
    
    # 显示各行为类型统计
    print("\n各行为类型统计:")
    stats = cursor.execute('''
        SELECT behavior_type, COUNT(*) as cnt 
        FROM user_behavior 
        GROUP BY behavior_type 
        ORDER BY cnt DESC
    ''').fetchall()
    for bt, cnt in stats:
        print(f"  {bt}: {cnt:,}")
    
    total_time = time.time() - start_time
    print(f"\n总耗时: {total_time:.1f}s ({total_time/60:.1f}分钟)")
    
    # 显示数据库文件大小
    db_size = os.path.getsize(DB_PATH) / (1024**3)
    print(f"数据库文件大小: {db_size:.2f} GB")
    
    conn.close()
    print("\n数据库已保存至:", DB_PATH)

if __name__ == '__main__':
    create_database()
