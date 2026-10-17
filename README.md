# 淘宝用户行为数据分析项目

基于阿里巴巴淘宝用户行为数据集（约1亿条记录）的大数据分析项目，用于大数据课程小组作业。

**数据集来源**: https://tianchi.aliyun.com/dataset/649

---

## 项目结构

```
5488Project/
├── README.md                  ← 本文件（项目总览）
├── Database/                  ← 数据库与可视化模块
│   ├── README.md              ← 数据库模块说明与重现指南
│   ├── ImportToDatabase.java  ← Java导入脚本
│   ├── import_to_database.py  ← Python导入脚本
│   ├── visualizations.py      ← 可视化脚本
│   ├── pom.xml                ← Maven配置
│   ├── sqlite-jdbc-3.44.1.0.jar
│   ├── slf4j-api-2.0.9.jar
│   ├── slf4j-simple-2.0.9.jar
│   └── visualizations/        ← 可视化图表
│       └── 01~08_*.png
└── ...                        ← 其他组员模块（后续添加）
```

## 模块说明

| 模块 | 负责人 | 说明 |
|------|--------|------|
| [Database](Database/) | 汪成儒 | 数据库构建、数据导入脚本、可视化图表 |
| ... | ... | 其他模块（待补充） |

## 快速开始

1. Clone 本仓库
2. 进入对应模块目录，阅读该模块的 `README.md`
3. 按照指南获取数据并运行脚本

## 数据集说明

- **数据量**: 约1亿条用户行为记录
- **时间范围**: 2017-11-25 到 2017-12-03（9天）
- **行为类型**: pv（浏览）、buy（购买）、cart（加购）、fav（收藏）
- **原始数据**: 需从天池下载，不上传 Git
