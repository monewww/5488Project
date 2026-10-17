"""
淘宝用户购物行为数据集 - 数据可视化
从 SQLite 数据库读取数据，生成多张可视化图表
"""

import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
from datetime import datetime
import os

# === 配置 ===
DB_PATH = r"f:\课程\5488big data\group project\UserBehavior.db"
OUTPUT_DIR = r"f:\课程\5488big data\group project\visualizations"

# 时间戳过滤：只保留2017-11-25到2017-12-03的数据
TIMESTAMP_MIN = 1511539200  # 2017-11-25 00:00:00 UTC+8
TIMESTAMP_MAX = 1512316800  # 2017-12-04 00:00:00 UTC+8
WHERE_CLAUSE = f"WHERE timestamp >= {TIMESTAMP_MIN} AND timestamp < {TIMESTAMP_MAX}"

# 设置中文字体
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
matplotlib.rcParams['axes.unicode_minus'] = False

def get_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    return conn

def ensure_output_dir():
    """确保输出目录存在"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"创建输出目录: {OUTPUT_DIR}")

# ============================================================
# 图表1: 用户行为类型分布
# ============================================================
def plot_behavior_distribution():
    """各行为类型数量分布（柱状图 + 饼图）"""
    print("\n[图表1] 生成用户行为类型分布...")
    conn = get_connection()
    
    df = pd.read_sql_query('''
        SELECT behavior_type, COUNT(*) as count
        FROM user_behavior
        GROUP BY behavior_type
        ORDER BY count DESC
    ''', conn)
    conn.close()
    
    # 行为类型中文映射
    type_map = {'pv': '点击(PV)', 'buy': '购买(Buy)', 'cart': '加购(Cart)', 'fav': '收藏(Fav)'}
    df['behavior_cn'] = df['behavior_type'].map(type_map)
    
    # 颜色
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4']
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # 柱状图
    bars = ax1.bar(df['behavior_cn'], df['count'], color=colors, edgecolor='white', linewidth=1.5)
    ax1.set_title('User Behavior Type Distribution (Bar Chart)', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Behavior Type', fontsize=12)
    ax1.set_ylabel('Count', fontsize=12)
    ax1.tick_params(axis='x', rotation=15)
    
    # 在柱状图上标注数值
    for bar, count in zip(bars, df['count']):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{count/1e6:.1f}M', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # 饼图
    wedges, texts, autotexts = ax2.pie(
        df['count'], labels=df['behavior_cn'], autopct='%1.1f%%',
        colors=colors, startangle=90, textprops={'fontsize': 11}
    )
    ax2.set_title('User Behavior Type Distribution (Pie Chart)', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    filepath = os.path.join(OUTPUT_DIR, '01_behavior_distribution.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  已保存: {filepath}")
    
    # 打印数据
    print("  行为类型统计:")
    for _, row in df.iterrows():
        pct = row['count'] / df['count'].sum() * 100
        print(f"    {row['behavior_cn']}: {row['count']:,} ({pct:.1f}%)")

# ============================================================
# 图表2: 每日行为趋势
# ============================================================
def plot_daily_trend():
    """每日行为数量趋势"""
    print("\n[图表2] 生成每日行为趋势...")
    conn = get_connection()
    
    df = pd.read_sql_query(f'''
        SELECT 
            CAST(STRFTIME('%Y-%m-%d', datetime(timestamp, 'unixepoch', '+8 hours')) AS TEXT) as date,
            behavior_type,
            COUNT(*) as count
        FROM user_behavior
        {WHERE_CLAUSE}
        GROUP BY date, behavior_type
        ORDER BY date
    ''', conn)
    conn.close()
    
    # 透视表
    pivot = df.pivot(index='date', columns='behavior_type', values='count').fillna(0)
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    type_map = {'pv': 'Click(PV)', 'buy': 'Purchase', 'cart': 'Add to Cart', 'fav': 'Favorite'}
    colors = {'pv': '#FF6B6B', 'buy': '#4ECDC4', 'cart': '#45B7D1', 'fav': '#96CEB4'}
    
    for col in pivot.columns:
        label = type_map.get(col, col)
        ax.plot(range(len(pivot)), pivot[col], label=label, 
                color=colors.get(col, '#333'), linewidth=2.5, marker='o', markersize=8, markeredgewidth=1.5)
    
    ax.set_title('Daily User Behavior Trend (2017-11-25 to 2017-12-03)', fontsize=14, fontweight='bold')
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Count', fontsize=12)
    ax.set_xticks(range(len(pivot)))
    ax.set_xticklabels(pivot.index, rotation=30, ha='right', fontsize=10)
    ax.legend(fontsize=11, loc='upper left')
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    filepath = os.path.join(OUTPUT_DIR, '02_daily_trend.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  已保存: {filepath}")

# ============================================================
# 图表3: 每小时行为分布
# ============================================================
def plot_hourly_distribution():
    """24小时行为分布"""
    print("\n[图表3] 生成每小时行为分布...")
    conn = get_connection()
    
    df = pd.read_sql_query('''
        SELECT 
            CAST(STRFTIME('%H', datetime(timestamp, 'unixepoch', '+8 hours')) AS INTEGER) as hour,
            behavior_type,
            COUNT(*) as count
        FROM user_behavior
        GROUP BY hour, behavior_type
        ORDER BY hour
    ''', conn)
    conn.close()
    
    # 透视表
    pivot = df.pivot(index='hour', columns='behavior_type', values='count').fillna(0)
    
    fig, ax = plt.subplots(figsize=(14, 6))
    
    type_map = {'pv': 'Click(PV)', 'buy': 'Purchase', 'cart': 'Add to Cart', 'fav': 'Favorite'}
    colors = {'pv': '#FF6B6B', 'buy': '#4ECDC4', 'cart': '#45B7D1', 'fav': '#96CEB4'}
    
    bottom = np.zeros(len(pivot))
    for col in ['pv', 'cart', 'fav', 'buy']:
        if col in pivot.columns:
            label = type_map.get(col, col)
            ax.bar(range(24), pivot[col], bottom=bottom, label=label,
                   color=colors.get(col, '#333'), edgecolor='white', linewidth=0.5)
            bottom += pivot[col].values
    
    ax.set_title('Hourly User Behavior Distribution (UTC+8)', fontsize=14, fontweight='bold')
    ax.set_xlabel('Hour of Day', fontsize=12)
    ax.set_ylabel('Count', fontsize=12)
    ax.set_xticks(range(24))
    ax.set_xticklabels([f'{h}:00' for h in range(24)], rotation=45, ha='right', fontsize=8)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    filepath = os.path.join(OUTPUT_DIR, '03_hourly_distribution.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  已保存: {filepath}")

# ============================================================
# 图表4: 热门商品类目 Top 10
# ============================================================
def plot_top_categories():
    """热门商品类目 Top 10（按总行为数）"""
    print("\n[图表4] 生成热门商品类目 Top 10...")
    conn = get_connection()
    
    df = pd.read_sql_query('''
        SELECT 
            category_id,
            COUNT(*) as total_count,
            SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) as pv_count,
            SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) as buy_count,
            SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) as cart_count,
            SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) as fav_count
        FROM user_behavior
        GROUP BY category_id
        ORDER BY total_count DESC
        LIMIT 10
    ''', conn)
    conn.close()
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    x = range(len(df))
    width = 0.2
    
    bars1 = ax.bar([i - 1.5*width for i in x], df['pv_count'], width, label='Click(PV)', color='#FF6B6B')
    bars2 = ax.bar([i - 0.5*width for i in x], df['cart_count'], width, label='Add to Cart', color='#45B7D1')
    bars3 = ax.bar([i + 0.5*width for i in x], df['fav_count'], width, label='Favorite', color='#96CEB4')
    bars4 = ax.bar([i + 1.5*width for i in x], df['buy_count'], width, label='Purchase', color='#4ECDC4')
    
    ax.set_title('Top 10 Product Categories by Behavior Count', fontsize=14, fontweight='bold')
    ax.set_xlabel('Category ID', fontsize=12)
    ax.set_ylabel('Count', fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels([f'Cat-{cid}' for cid in df['category_id']], rotation=45, ha='right')
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    filepath = os.path.join(OUTPUT_DIR, '04_top_categories.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  已保存: {filepath}")
    
    # 打印 Top 10 信息
    print("  Top 10 热门类目:")
    for i, row in df.iterrows():
        print(f"    #{i+1} Category-{row['category_id']}: Total={row['total_count']:,}, "
              f"PV={row['pv_count']:,}, Buy={row['buy_count']:,}, Cart={row['cart_count']:,}, Fav={row['fav_count']:,}")

# ============================================================
# 图表5: 用户转化漏斗
# ============================================================
def plot_conversion_funnel():
    """用户行为转化漏斗图 - 优化版"""
    print("\n[图表5] 生成用户转化漏斗...")
    conn = get_connection()
    
    df = pd.read_sql_query('''
        SELECT behavior_type, COUNT(*) as count
        FROM user_behavior
        WHERE timestamp >= 1511539200 AND timestamp < 1512316800
        GROUP BY behavior_type
    ''', conn)
    conn.close()
    
    # 漏斗顺序: 点击 → 加购 → 收藏 → 购买
    funnel_order = ['pv', 'cart', 'fav', 'buy']
    funnel_labels = ['Click (PV)', 'Add to Cart', 'Favorite', 'Purchase']
    funnel_counts = []
    for bt in funnel_order:
        row = df[df['behavior_type'] == bt]
        funnel_counts.append(row['count'].values[0] if len(row) > 0 else 0)
    
    # 使用 sqrt 缩放让比例更均衡
    import numpy as np
    max_count = funnel_counts[0]
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    colors = ['#FF6B6B', '#45B7D1', '#96CEB4', '#4ECDC4']
    bar_height = 0.7
    n = len(funnel_labels)
    
    for i, (label, count) in enumerate(zip(funnel_labels, funnel_counts)):
        # 使用 sqrt 缩放宽度，避免底层过小
        width = np.sqrt(count / max_count)
        left = (1 - width) / 2
        y = n - 1 - i  # 从上到下
        
        # 绘制圆角矩形
        bar = ax.barh(y, width, left=left, height=bar_height, 
                      color=colors[i], edgecolor='white', linewidth=2,
                      zorder=2)
        
        # 右侧标注：行为名称、数量、转化率
        pct = count / max_count * 100
        label_x = 1.02  # 放在漏斗右侧
        
        # 行为名称（加粗）
        ax.text(label_x, y, label, ha='left', va='center', 
                fontsize=13, fontweight='bold', color='#333333')
        
        # 数量
        ax.text(label_x + 0.01, y - 0.12, f'{count:,}', ha='left', va='center',
                fontsize=11, color='#666666')
        
        # 转化率
        ax.text(label_x + 0.01, y - 0.24, f'{pct:.1f}%', ha='left', va='center',
                fontsize=11, color=colors[i], fontweight='bold')
        
        # 层级间转化率箭头标注
        if i > 0:
            prev_pct = funnel_counts[i-1] / max_count * 100
            step_pct = count / funnel_counts[i-1] * 100
            ax.text(0.5, y + 0.42, f'↓ {step_pct:.1f}%', 
                    ha='center', va='center', fontsize=9, color='#999999',
                    style='italic')
    
    ax.set_title('User Behavior Conversion Funnel (2017-11-25 to 2017-12-03)', 
                 fontsize=15, fontweight='bold', pad=20)
    ax.set_xlim(-0.05, 1.55)
    ax.set_ylim(-0.5, n - 0.5)
    ax.axis('off')
    
    # 添加整体转化率标注
    overall_pct = funnel_counts[-1] / funnel_counts[0] * 100
    fig.text(0.5, 0.02, f'Overall Conversion Rate (Click → Purchase): {overall_pct:.2f}%',
             ha='center', va='bottom', fontsize=12, fontweight='bold', color='#FF6B6B',
             bbox=dict(boxstyle='round,pad=0.5', facecolor='#FFF5F5', edgecolor='#FF6B6B', alpha=0.8))
    
    plt.tight_layout(rect=[0, 0.06, 1, 1])
    filepath = os.path.join(OUTPUT_DIR, '05_conversion_funnel.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  已保存: {filepath}")
    
    # 打印转化率
    print("  转化率分析:")
    for i, (label, count) in enumerate(zip(funnel_labels, funnel_counts)):
        pct = count / funnel_counts[0] * 100
        print(f"    {label}: {count:,} ({pct:.2f}%)")
    print(f"  点击→购买转化率: {funnel_counts[3]/funnel_counts[0]*100:.4f}%")

# ============================================================
# 图表6: 用户活跃度分布
# ============================================================
def plot_user_activity_distribution():
    """用户行为数量分布"""
    print("\n[图表6] 生成用户活跃度分布...")
    conn = get_connection()
    
    df = pd.read_sql_query('''
        SELECT user_id, COUNT(*) as behavior_count
        FROM user_behavior
        GROUP BY user_id
    ''', conn)
    conn.close()
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    # 左图: 用户行为数分布直方图（对数刻度）
    ax1 = axes[0]
    ax1.hist(df['behavior_count'], bins=100, color='#45B7D1', edgecolor='white', alpha=0.8)
    ax1.set_title('Distribution of User Activity (Behavior Count per User)', fontsize=12, fontweight='bold')
    ax1.set_xlabel('Number of Behaviors per User', fontsize=11)
    ax1.set_ylabel('Number of Users', fontsize=11)
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    ax1.grid(True, alpha=0.3)
    
    # 添加统计信息
    stats_text = (f"Total Users: {len(df):,}\n"
                  f"Mean: {df['behavior_count'].mean():.0f}\n"
                  f"Median: {df['behavior_count'].median():.0f}\n"
                  f"Max: {df['behavior_count'].max():,}")
    ax1.text(0.95, 0.95, stats_text, transform=ax1.transAxes,
             fontsize=9, verticalalignment='top', horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
    
    # 右图: 用户行为数分箱统计
    ax2 = axes[1]
    bins = [0, 10, 50, 100, 200, 500, 1000, 5000, float('inf')]
    bin_labels = ['1-10', '11-50', '51-100', '101-200', '201-500', '501-1000', '1001-5000', '5000+']
    df['bin'] = pd.cut(df['behavior_count'], bins=bins, labels=bin_labels)
    bin_counts = df['bin'].value_counts().sort_index()
    
    colors_bar = plt.cm.RdYlGn_r(np.linspace(0.2, 0.8, len(bin_counts)))
    bars = ax2.bar(range(len(bin_counts)), bin_counts.values, color=colors_bar, edgecolor='white', linewidth=1.5)
    ax2.set_title('User Activity Groups', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Behavior Count Range', fontsize=11)
    ax2.set_ylabel('Number of Users', fontsize=11)
    ax2.set_xticks(range(len(bin_counts)))
    ax2.set_xticklabels(bin_labels, rotation=45, ha='right')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # 标注数值
    for bar, count in zip(bars, bin_counts.values):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{count/1000:.0f}K', ha='center', va='bottom', fontsize=8)
    
    plt.tight_layout()
    filepath = os.path.join(OUTPUT_DIR, '06_user_activity.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  已保存: {filepath}")

# ============================================================
# 图表7: 商品热度分布 Top 20
# ============================================================
def plot_top_items():
    """热门商品 Top 20"""
    print("\n[图表7] 生成热门商品 Top 20...")
    conn = get_connection()
    
    df = pd.read_sql_query('''
        SELECT 
            item_id,
            COUNT(*) as total_count,
            SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) as buy_count,
            SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) as pv_count
        FROM user_behavior
        GROUP BY item_id
        ORDER BY total_count DESC
        LIMIT 20
    ''', conn)
    conn.close()
    
    fig, ax = plt.subplots(figsize=(16, 7))
    
    x = range(len(df))
    width = 0.35
    
    ax.bar([i - width/2 for i in x], df['pv_count'], width, label='Click(PV)', color='#FF6B6B', alpha=0.8)
    ax.bar([i + width/2 for i in x], df['buy_count'], width, label='Purchase', color='#4ECDC4', alpha=0.8)
    
    ax.set_title('Top 20 Products by Behavior Count', fontsize=14, fontweight='bold')
    ax.set_xlabel('Product ID', fontsize=12)
    ax.set_ylabel('Count', fontsize=12)
    ax.set_xticks(x)
    ax.set_xticklabels([f'Item-{iid}' for iid in df['item_id']], rotation=60, ha='right', fontsize=8)
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    filepath = os.path.join(OUTPUT_DIR, '07_top_items.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  已保存: {filepath}")

# ============================================================
# 图表8: 综合数据概览仪表板
# ============================================================
def plot_dashboard():
    """数据概览仪表板"""
    print("\n[图表8] 生成综合数据概览仪表板...")
    conn = get_connection()
    
    # 获取基本统计
    total_rows = pd.read_sql_query('SELECT COUNT(*) as cnt FROM user_behavior', conn).iloc[0]['cnt']
    total_users = pd.read_sql_query('SELECT COUNT(DISTINCT user_id) as cnt FROM user_behavior', conn).iloc[0]['cnt']
    total_items = pd.read_sql_query('SELECT COUNT(DISTINCT item_id) as cnt FROM user_behavior', conn).iloc[0]['cnt']
    total_categories = pd.read_sql_query('SELECT COUNT(DISTINCT category_id) as cnt FROM user_behavior', conn).iloc[0]['cnt']
    
    behavior_stats = pd.read_sql_query('''
        SELECT behavior_type, COUNT(*) as count FROM user_behavior GROUP BY behavior_type ORDER BY count DESC
    ''', conn)
    
    conn.close()
    
    fig = plt.figure(figsize=(16, 10))
    fig.suptitle('Taobao User Behavior Dataset - Overview Dashboard', fontsize=16, fontweight='bold', y=0.98)
    
    # 顶部: 关键指标卡片
    ax0 = fig.add_axes([0.05, 0.82, 0.9, 0.12])
    ax0.axis('off')
    
    metrics = [
        ('Total Records', f'{total_rows/1e6:.1f}M'),
        ('Total Users', f'{total_users/1e6:.2f}M'),
        ('Total Products', f'{total_items/1e6:.1f}M'),
        ('Categories', f'{total_categories:,}'),
        ('Date Range', '9 Days'),
    ]
    
    for i, (label, value) in enumerate(metrics):
        x = 0.1 + i * 0.18
        ax0.add_patch(plt.Rectangle((x-0.07, 0.1), 0.14, 0.8, 
                                     transform=ax0.transAxes, 
                                     facecolor='#f0f0f0', edgecolor='#ccc', linewidth=1))
        ax0.text(x, 0.65, value, transform=ax0.transAxes,
                ha='center', va='center', fontsize=16, fontweight='bold', color='#333')
        ax0.text(x, 0.25, label, transform=ax0.transAxes,
                ha='center', va='center', fontsize=9, color='#666')
    
    # 底部: 行为统计表格
    ax1 = fig.add_axes([0.1, 0.1, 0.8, 0.6])
    ax1.axis('off')
    
    type_map = {'pv': 'Click (Page View)', 'buy': 'Purchase', 'cart': 'Add to Cart', 'fav': 'Favorite'}
    
    table_data = []
    for _, row in behavior_stats.iterrows():
        pct = row['count'] / total_rows * 100
        table_data.append([
            type_map.get(row['behavior_type'], row['behavior_type']),
            f"{row['count']:,}",
            f"{pct:.2f}%"
        ])
    
    table = ax1.table(
        cellText=table_data,
        colLabels=['Behavior Type', 'Count', 'Percentage'],
        cellLoc='center',
        loc='center',
        colWidths=[0.4, 0.3, 0.3]
    )
    table.auto_set_font_size(False)
    table.set_fontsize(12)
    table.scale(1, 2)
    
    # 设置表头样式
    for j in range(3):
        table[0, j].set_facecolor('#45B7D1')
        table[0, j].set_text_props(color='white', fontweight='bold')
    
    # 设置数据行样式
    colors_row = ['#FFE0E0', '#E0FFE0', '#E0F0FF', '#F0FFE0']
    for i in range(1, len(table_data) + 1):
        for j in range(3):
            table[i, j].set_facecolor(colors_row[i-1])
    
    filepath = os.path.join(OUTPUT_DIR, '08_dashboard.png')
    plt.savefig(filepath, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  已保存: {filepath}")

# ============================================================
# 主函数
# ============================================================
def main():
    print("=" * 60)
    print("淘宝用户购物行为数据集 - 数据可视化")
    print("=" * 60)
    
    # 检查数据库是否存在
    if not os.path.exists(DB_PATH):
        print(f"错误: 数据库文件不存在: {DB_PATH}")
        print("请先运行 import_to_database.py 创建数据库")
        return
    
    ensure_output_dir()
    
    start_time = datetime.now()
    print(f"\n开始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 生成所有图表
    plot_behavior_distribution()     # 图表1: 行为类型分布
    plot_daily_trend()               # 图表2: 每日行为趋势
    plot_hourly_distribution()       # 图表3: 每小时行为分布
    plot_top_categories()            # 图表4: 热门商品类目 Top 10
    plot_conversion_funnel()         # 图表5: 用户转化漏斗
    plot_user_activity_distribution() # 图表6: 用户活跃度分布
    plot_top_items()                 # 图表7: 热门商品 Top 20
    plot_dashboard()                 # 图表8: 综合数据概览仪表板
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"\n{'=' * 60}")
    print(f"所有图表生成完成！")
    print(f"总耗时: {duration:.1f}s ({duration/60:.1f}分钟)")
    print(f"图表保存目录: {OUTPUT_DIR}")
    print(f"{'=' * 60}")
    
    # 列出所有生成的文件
    print("\n生成的图表文件:")
    for f in sorted(os.listdir(OUTPUT_DIR)):
        fpath = os.path.join(OUTPUT_DIR, f)
        size = os.path.getsize(fpath) / 1024
        print(f"  {f} ({size:.0f} KB)")

if __name__ == '__main__':
    main()
