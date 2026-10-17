import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 淘宝用户购物行为数据集 - 数据库导入脚本（Java版本）
 * 将 UserBehavior.csv 导入 SQLite 数据库
 * 数据量: ~1亿行, 3.42GB
 * 
 * 依赖: sqlite-jdbc (Maven: org.xerial:sqlite-jdbc:3.44.1.0)
 */
public class ImportToDatabase {
    
    // === 配置 ===
    private static final String CSV_PATH = "UserBehavior.csv";
    private static final String DB_PATH = "UserBehavior.db";
    private static final int CHUNK_SIZE = 500_000;  // 每次处理50万行
    
    public static void main(String[] args) {
        try {
            createDatabase();
        } catch (Exception e) {
            System.err.println("错误: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * 创建SQLite数据库并导入数据
     */
    private static void createDatabase() throws Exception {
        // 加载SQLite JDBC驱动
        Class.forName("org.sqlite.JDBC");
        
        // 如果数据库已存在，先删除
        if (Files.exists(Paths.get(DB_PATH))) {
            Files.delete(Paths.get(DB_PATH));
            System.out.println("已删除旧数据库文件");
        }
        
        // 连接数据库
        Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_PATH);
        conn.setAutoCommit(false);  // 禁用自动提交，启用手动事务
        Statement stmt = conn.createStatement();
        
        // 创建表
        stmt.execute("""
            CREATE TABLE IF NOT EXISTS user_behavior (
                user_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                behavior_type TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            )
        """);
        conn.commit();
        System.out.println("数据表 user_behavior 已创建");
        
        // 分块读取CSV并导入
        System.out.println("\n开始导入数据 (每块 " + CHUNK_SIZE + " 行)...");
        long totalRows = 0;
        long startTime = System.currentTimeMillis();
        int chunkCount = 0;
        
        // 准备批量插入语句
        String insertSQL = "INSERT INTO user_behavior VALUES (?, ?, ?, ?, ?)";
        PreparedStatement pstmt = conn.prepareStatement(insertSQL);
        
        try (BufferedReader reader = new BufferedReader(new FileReader(CSV_PATH))) {
            String line;
            int lineCount = 0;
            long chunkStartTime = System.currentTimeMillis();
            
            while ((line = reader.readLine()) != null) {
                lineCount++;
                
                // 解析CSV行（逗号分隔）
                String[] parts = line.split(",", -1);
                if (parts.length != 5) {
                    System.err.println("警告: 第 " + lineCount + " 行格式错误，跳过");
                    continue;
                }
                
                try {
                    int userId = Integer.parseInt(parts[0].trim());
                    int itemId = Integer.parseInt(parts[1].trim());
                    int categoryId = Integer.parseInt(parts[2].trim());
                    String behaviorType = parts[3].trim();
                    long timestamp = Long.parseLong(parts[4].trim());
                    
                    pstmt.setInt(1, userId);
                    pstmt.setInt(2, itemId);
                    pstmt.setInt(3, categoryId);
                    pstmt.setString(4, behaviorType);
                    pstmt.setLong(5, timestamp);
                    pstmt.addBatch();
                    
                } catch (NumberFormatException e) {
                    System.err.println("警告: 第 " + lineCount + " 行数据格式错误，跳过: " + e.getMessage());
                }
                
                // 每CHUNK_SIZE行执行一次批量插入
                if (lineCount % CHUNK_SIZE == 0) {
                    chunkCount++;
                    pstmt.executeBatch();
                    conn.commit();
                    pstmt.clearBatch();
                    
                    totalRows += CHUNK_SIZE;
                    long chunkTime = System.currentTimeMillis() - chunkStartTime;
                    long elapsed = System.currentTimeMillis() - startTime;
                    
                    System.out.printf("  块 %d: 导入 %,d 行 | 累计 %,d 行 | 本块 %.1fs | 总耗时 %.1fs%n",
                        chunkCount, CHUNK_SIZE, totalRows, chunkTime / 1000.0, elapsed / 1000.0);
                    
                    chunkStartTime = System.currentTimeMillis();
                }
            }
            
            // 处理最后一批不足CHUNK_SIZE的数据
            if (lineCount % CHUNK_SIZE != 0) {
                chunkCount++;
                int remaining = lineCount % CHUNK_SIZE;
                pstmt.executeBatch();
                conn.commit();
                totalRows += remaining;
                
                long chunkTime = System.currentTimeMillis() - chunkStartTime;
                long elapsed = System.currentTimeMillis() - startTime;
                
                System.out.printf("  块 %d: 导入 %,d 行 | 累计 %,d 行 | 本块 %.1fs | 总耗时 %.1fs%n",
                    chunkCount, remaining, totalRows, chunkTime / 1000.0, elapsed / 1000.0);
            }
            
        } catch (IOException e) {
            System.err.println("读取CSV文件错误: " + e.getMessage());
            throw e;
        }
        
        // 创建索引以加速查询
        System.out.println("\n创建索引...");
        
        System.out.println("  创建 behavior_type 索引...");
        stmt.execute("CREATE INDEX idx_behavior ON user_behavior(behavior_type)");
        
        System.out.println("  创建 timestamp 索引...");
        stmt.execute("CREATE INDEX idx_timestamp ON user_behavior(timestamp)");
        
        System.out.println("  创建 user_id 索引...");
        stmt.execute("CREATE INDEX idx_user ON user_behavior(user_id)");
        
        System.out.println("  创建 category_id 索引...");
        stmt.execute("CREATE INDEX idx_category ON user_behavior(category_id)");
        
        conn.commit();
        
        // 验证导入结果
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM user_behavior");
        rs.next();
        long count = rs.getLong(1);
        System.out.printf("\n导入完成！总行数: %,d%n", count);
        
        // 显示各行为类型统计
        System.out.println("\n各行为类型统计:");
        rs = stmt.executeQuery("""
            SELECT behavior_type, COUNT(*) as cnt 
            FROM user_behavior 
            GROUP BY behavior_type 
            ORDER BY cnt DESC
        """);
        while (rs.next()) {
            System.out.printf("  %s: %,d%n", rs.getString("behavior_type"), rs.getLong("cnt"));
        }
        
        long totalTime = System.currentTimeMillis() - startTime;
        System.out.printf("\n总耗时: %.1fs (%.1f分钟)%n", totalTime / 1000.0, totalTime / 60000.0);
        
        // 显示数据库文件大小
        long dbSize = Files.size(Paths.get(DB_PATH));
        System.out.printf("数据库文件大小: %.2f GB%n", dbSize / (1024.0 * 1024 * 1024));
        
        // 关闭资源
        pstmt.close();
        stmt.close();
        conn.close();
        
        System.out.println("\n数据库已保存至: " + Paths.get(DB_PATH).toAbsolutePath());
    }
}
