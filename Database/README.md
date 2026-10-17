# 数据库与可视化模块

本模块负责将淘宝用户行为 CSV 数据导入 SQLite 数据库，并生成可视化分析图表。

---

## 数据库重现指南

> 由于数据库文件过大（7.49GB），无法直接上传 Git。请按照以下步骤自行生成本地数据库。
> 所有脚本已配置为**基于脚本所在目录的相对路径**，无需修改代码，只要文件结构正确即可运行。

### 前置条件

| 需要 | 说明 |
|------|------|
| `UserBehavior.csv` | 原始数据文件（约3.42GB），从下方获取 |
| 脚本文件 | `ImportToDatabase.java` 或 `import_to_database.py`（已在仓库中） |

### 第一步：获取原始数据文件

**方式一：自行下载（推荐）**

1. 访问天池数据集页面：https://tianchi.aliyun.com/dataset/649
2. 登录/注册阿里云账号
3. 下载 `UserBehavior.csv.zip`（约900MB）
4. 解压后将 `UserBehavior.csv`（约3.42GB）放到**本目录**（与脚本同级）

**方式二：从小组网盘获取**

- **百度网盘**: [链接待补充]
- **提取码**: [待补充]
- 下载后将 `UserBehavior.csv` 放到本目录

### 第二步：确认文件结构

确保你的目录结构如下：

```
Database/
── ImportToDatabase.java      ← Java导入脚本（二选一）
├── import_to_database.py      ← Python导入脚本（二选一）
├── visualizations.py          ← 可视化脚本
├── sqlite-jdbc-3.44.1.0.jar   ← Java方式需要此驱动
├── UserBehavior.csv           ← ★ 原始数据文件（需自行获取）
└── visualizations/            ← 图表输出目录
```

### 第三步：运行导入脚本

#### 方式A：Python脚本（简单，推荐新手）

```bash
# 1. 安装依赖
pip install pandas

# 2. 运行导入脚本（耗时约20-30分钟）
python import_to_database.py
```

**预期输出**：
```
开始导入数据 (每块 500,000 行)...
  块 1: 导入 500,000 行 | 累计 500,000 行 | ...
  块 2: 导入 500,000 行 | 累计 1,000,000 行 | ...
  ...
创建索引...
导入完成！总行数: 100,150,807
数据库文件大小: 7.49 GB
```

#### 方式B：Java脚本（性能更好）

**前提**: Java 17+ 和 Maven 3.6+

```bash
# 使用Maven运行（耗时约17分钟）
mvn compile
mvn exec:java
```

或直接编译运行：
```bash
# 编译
javac -cp "sqlite-jdbc-3.44.1.0.jar" ImportToDatabase.java

# 运行（Windows用分号，Mac/Linux用冒号）
java -cp ".;sqlite-jdbc-3.44.1.0.jar" ImportToDatabase
```

### 第四步：验证数据库

导入完成后，可用以下命令快速验证：

```python
import sqlite3
conn = sqlite3.connect("UserBehavior.db")
# 查看总行数
print(conn.execute("SELECT COUNT(*) FROM user_behavior").fetchone())
# 查看各行为类型统计
for row in conn.execute("SELECT behavior_type, COUNT(*) FROM user_behavior GROUP BY behavior_type"):
    print(row)
conn.close()
```

**预期结果**：总行数应为 **100,150,807**

### 第五步（可选）：生成可视化图表

```bash
pip install pandas matplotlib seaborn numpy
python visualizations.py
```

图表将保存在 `visualizations/` 目录。

### 常见问题排查

| 问题 | 解决方法 |
|------|----------|
| `FileNotFoundError: UserBehavior.csv` | 确认 CSV 文件在本目录（Database/），且文件名完全一致 |
| `ModuleNotFoundError: pandas` | 运行 `pip install pandas` |
| Java 报 `ClassNotFoundException` | 确认 `sqlite-jdbc-3.44.1.0.jar` 在本目录，且 classpath 分隔符正确（Windows `;` / Mac `:` ） |
| 导入很慢 | 正常，约1亿行数据需要20-30分钟 |
| 磁盘空间不足 | 需要约 **12GB** 可用空间（CSV 3.42GB + DB 7.49GB） |

---

## 数据库说明

### 表结构

```sql
CREATE TABLE user_behavior (
    user_id INTEGER NOT NULL,      -- 用户ID（序列化）
    item_id INTEGER NOT NULL,      -- 商品ID（序列化）
    category_id INTEGER NOT NULL,  -- 商品类目ID（序列化）
    behavior_type TEXT NOT NULL,   -- 行为类型：pv/buy/cart/fav
    timestamp INTEGER NOT NULL     -- Unix时间戳
);
```

### 索引

- `idx_behavior` - behavior_type字段索引
- `idx_timestamp` - timestamp字段索引
- `idx_user` - user_id字段索引
- `idx_category` - category_id字段索引

### 数据规模

- **总行数**: 100,150,807（约1亿）
- **文件大小**: 7.49 GB
- **时间范围**: 2017-11-25 到 2017-12-03（9天）

---

## Java使用示例

### 连接数据库

```java
import java.sql.*;

public class Example {
    public static void main(String[] args) {
        try {
            Class.forName("org.sqlite.JDBC");
            Connection conn = DriverManager.getConnection("jdbc:sqlite:UserBehavior.db");
            Statement stmt = conn.createStatement();
            
            // 查询示例（必须加时间戳过滤）
            String sql = """
                SELECT behavior_type, COUNT(*) as count
                FROM user_behavior
                WHERE timestamp >= 1511539200 AND timestamp < 1512316800
                GROUP BY behavior_type
            """;
            
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                System.out.println(rs.getString("behavior_type") + ": " + rs.getInt("count"));
            }
            
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### Maven依赖

```xml
<dependency>
    <groupId>org.xerial</groupId>
    <artifactId>sqlite-jdbc</artifactId>
    <version>3.44.1.0</version>
</dependency>
```

---

## 可视化图表

所有图表在 `visualizations/` 目录：

| 文件名 | 说明 |
|--------|------|
| `01_behavior_distribution.png` | 用户行为类型分布（柱状图+饼图） |
| `02_daily_trend.png` | 每日行为趋势（折线图） |
| `03_hourly_distribution.png` | 24小时行为分布（堆叠柱状图） |
| `04_top_categories.png` | 热门商品类目Top10（分组柱状图） |
| `05_conversion_funnel.png` | 用户转化漏斗（水平漏斗图） |
| `06_user_activity.png` | 用户活跃度分布（直方图+分箱统计） |
| `07_top_items.png` | 热门商品Top20（分组柱状图） |
| `08_dashboard.png` | 综合数据概览仪表板 |

---

## 注意事项

### 1. 时间戳过滤（重要！）

数据集中有约0.05%的异常时间戳（负值、未来时间等），**查询时必须添加过滤条件**：

```sql
WHERE timestamp >= 1511539200 AND timestamp < 1512316800
```

- `1511539200` = 2017-11-25 00:00:00 UTC+8
- `1512316800` = 2017-12-04 00:00:00 UTC+8

### 2. 时区转换

SQLite中转换Unix时间戳为北京时间（UTC+8）：

```sql
datetime(timestamp, 'unixepoch', '+8 hours')
STRFTIME('%Y-%m-%d', datetime(timestamp, 'unixepoch', '+8 hours'))
```

### 3. 数据匿名化

数据集中的ID字段（user_id, item_id, category_id）都经过序列化/匿名化处理：
- 无法映射到真实的用户、商品或类目名称
- 只能进行相对分析和趋势分析

### 4. 行为类型说明

| 行为类型 | 含义 |
|---------|------|
| `pv` | 浏览/点击（Page View） |
| `buy` | 购买 |
| `cart` | 加入购物车 |
| `fav` | 收藏 |
