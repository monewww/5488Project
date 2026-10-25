# 淘宝用户行为数据分析项目

基于阿里巴巴淘宝用户行为数据集（约 1 亿条记录）的大数据分析项目，用于 CS5488 大数据课程小组作业。

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
├── pagerank/                  ← PageRank 模块（图算法）
│   ├── README.md              ← ★ 方法、结果、两版运行方式
│   ├── local/                 ← 小批量版（Python，本地验证）
│   ├── mapreduce/             ← 全量版（Hadoop MapReduce / Java）
│   │   └── README.md          ← ★ 集群运行手册
│   ├── pig/                   ← 备选：Pig Latin 版（可选）
│   ├── data/                  ← 抽样脚本 + 小批量数据集
│   └── results/               ← 全量运行产出（Top-30、收敛曲线、图表）
└── ...
```

## 模块说明

| 模块 | 负责人 | 说明 |
|------|--------|------|
| [Database](Database/) | 汪成儒 | 数据库构建、数据导入脚本、可视化图表 |
| [PageRank](pagerank/) | — | 会话共现图 + 加权 PageRank，找枢纽商品；含本地验证版与 Hadoop MapReduce 全量版 |

## 快速开始

1. Clone 本仓库
2. 进入对应模块目录，阅读该模块的 `README.md`
3. 按照指南获取数据并运行脚本

> **只想验证 PageRank 算法？不需要下载 3.4GB 全量数据**：
> `pagerank/data/sample/` 里已经有 1.7MB 的确定性抽样数据集，直接
> `cd pagerank && ./local/run_local.ps1`（Windows）两秒出结果。

## 数据集说明

- **数据量**: 约 1 亿条用户行为记录（3.4GB）
- **时间范围**: 2017-11-25 到 2017-12-03（9 天）
- **行为类型**: pv（浏览）、buy（购买）、cart（加购）、fav（收藏）
- **字段**: `user_id, item_id, category_id, behavior_type, timestamp(秒)`
- **原始数据**: 需从天池下载，**不上传 Git**（单文件超 GitHub 100MB 限制）。
  见各模块 `README.md` 里的获取说明。
