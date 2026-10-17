# 淘宝用户行为数据分析项目

## 项目简介

基于阿里巴巴淘宝用户行为数据集（约1亿条记录）的大数据分析项目，用于大数据课程小组作业。

**数据集来源**: https://tianchi.aliyun.com/dataset/649

---

## 快速开始

### 1. 获取数据集

1. 访问天池：https://tianchi.aliyun.com/dataset/649
2. 下载 `UserBehavior.csv.zip`（约900MB）
3. 解压到项目根目录，得到 `UserBehavior.csv`（约3.42GB）

### 2. 生成数据库

#### 方式A：使用Java脚本（推荐）

**前提条件**:
- Java 17+
- Maven 3.6+

**步骤**:
```bash
# 编译项目
mvn compile

# 运行数据库导入脚本（耗时约17分钟）
mvn exec:java
```

或者直接编译运行：
```bash
# 下载SQLite JDBC驱动
# https://github.com/xerial/sqlite-jdbc/releases

# 编译
javac -cp "sqlite-jdbc-3.44.1.0.jar" ImportToDatabase.java

# 运行
java -cp ".;sqlite-jdbc-3.44.1.0.jar" ImportToDatabase
```

#### 方式B：使用Python脚本

**前提条件**:
- Python 3.8+
- pandas库

**步骤**:
```bash
pip install pandas
python import_to_database.py
```

#### 方式C：直接下载数据库

由于数据库文件过大（7.49GB），可以从网盘下载：
- **百度网盘**: [链接]
- **提取码**: xxxx

下载后将 `UserBehavior.db` 放到项目根目录即可。

### 3. 生成可视化图表（Python）

```bash
pip install pandas matplotlib seaborn numpy
python visualizations.py
```

生成的图表保存在 `visualizations/` 目录。

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
            // 加载驱动
            Class.forName("org.sqlite.JDBC");
            
            // 连接数据库
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

---

## 项目结构

```
group project/
├── ImportToDatabase.java      # Java数据库导入脚本
── import_to_database.py      # Python数据库导入脚本（参考）
├── visualizations.py          # Python可视化脚本
├── pom.xml                    # Maven配置文件
├── UserBehavior.csv           # 原始数据（需下载，不上传Git）
├── UserBehavior.db            # 数据库文件（需下载或生成，不上传Git）
├── visualizations/            # 可视化图表
│   ├── 01_behavior_distribution.png
│   ├── 02_daily_trend.png
│   ├── 03_hourly_distribution.png
│   ├── 04_top_categories.png
│   ├── 05_conversion_funnel.png
│   ├── 06_user_activity.png
│   └── 07_top_items.png
├── README.md                  # 本文件
└── .gitignore                 # Git忽略规则
```

---

## 小组成员

- [你的名字] - 数据库 + 可视化
- [组员1] - Java开发
- [组员2] - ...

---

## 常见问题

### Q: Java组员如何运行脚本？
A: 确保安装了Java 17+和Maven，然后运行 `mvn compile` 和 `mvn exec:java`

### Q: 数据库文件太大无法上传Git怎么办？
A: 使用网盘共享（百度网盘/Google Drive），在README中提供下载链接

### Q: 查询结果不对怎么办？
A: 检查是否添加了时间戳过滤条件 `WHERE timestamp >= 1511539200 AND timestamp < 1512316800`

### Q: 可视化图表看不清怎么办？
A: 图片分辨率是150 DPI，可以放大查看或重新生成更高分辨率的版本
