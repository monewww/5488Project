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
├── mapreduce_data_tools/                 ← MapReduce 数据处理与验证工具集
│   ├── README.md                         ← 工具说明、使用方法、运行流程及注意事项
│   ├── data_quality_check.py             ← 原始数据质量检查（字段、缺失、异常、重复等）
│   ├── tiny_sample.py                    ← 按用户 ID 取模生成确定性小样本
│   ├── prepare_mr_input.py               ← 按 Java MR1 Mapper 规则检查和准备输入数据
│   ├── validate_session_edges.py         ← Python 模拟 MR1 Session 划分与商品共现计算，生成预期结果
│   ├── compare_mr_vs_expected.py         ← 对比本地预期结果与 Hadoop 实际结果（MR1、MR2、Nodes、PageRank、Top-K）
│   └── out/                              ← 工具默认输出目录
│       └── quality_report.txt            ← 数据质量检查报告
│
└── run_results/                          ← 项目实验结果与运行产物归档
    ├── data_tools/                       ← MapReduce 数据工具的运行结果与验证记录
    │   ├── cmp/                          ← Hadoop 实际结果与本地预期结果的对比报告
    │   │   ├── compare_mr1_*.txt         ← MR1 商品共现边对比报告
    │   │   ├── compare_mr2_*.txt         ← MR2 商品图构建结果对比报告
    │   │   ├── compare_nodes_*.txt       ← 商品节点集合对比报告
    │   │   └── compare_topk_*.txt        ← Top-K 排名对比报告
    │   ├── expected330/                  ← sampleMod=330 的本地预期结果
    │   │   ├── expected_edges_user.tsv   ← 按用户聚合的商品共现边
    │   │   ├── expected_edges_global.tsv ← 跨用户汇总的商品共现边
    │   │   ├── expected_nodes.tsv        ← 预期商品节点集合
    │   │   └── expected_summary.txt      ← 本地模拟统计摘要
    │   ├── tiny330/                      ← sampleMod=330 生成的小样本数据
    │   │   └── UserBehavior_tiny.csv
    │   ├── quality/                      ← 原始数据质量检查结果
    │   │   └── quality_report.txt
    │   └── expected_topk_correct.tsv     ← 修正后的 Top-K 参考结果
    │
    ├── kmeans/                           ← K-means 商品聚类实验结果
    │   ├── feature.tsv                   ← 商品聚类特征数据
    │   ├── labels.tsv                    ← 商品及其所属 Cluster 的标签
    │   ├── centers.txt                   ← 聚类中心数据
    │   ├── centers_k3.txt                ← K=3 时的聚类中心
    │   ├── README_centers.txt            ← 聚类中心说明
    │   ├── stats.txt                     ← 聚类统计结果
    │   ├── kmeans_run_k3.log             ← K=3 聚类运行日志
    │   ├── plot_cluster_size.png         ← 各 Cluster 的样本数量分布图
    │   ├── plot_k_selection.png          ← 聚类数量 K 的选择分析图
    │   ├── plot_pca.png                  ← PCA 降维后的聚类分布图
    │   └── plot_radar.png                ← 各 Cluster 特征对比雷达图
    │
    ├── mapreduce/                        ← Hadoop MapReduce 全量运行结果
    │   ├── edges/                        ← MR1 商品共现边输出
    │   │   ├── _SUCCESS
    │   │   └── part-r-00000
    │   ├── graph_45/                     ← MR3 第 45 轮 PageRank 结果
    │   │   ├── _SUCCESS
    │   │   └── part-r-00000
    │   ├── graph_46/                     ← MR3 第 46 轮 PageRank 最终结果（收敛）
    │   │   ├── _SUCCESS
    │   │   └── part-r-00000
    │   ├── topk/                         ← MR4 原始 Top-K 输出
    │   │   ├── _SUCCESS
    │   │   └── part-r-00000
    │   ├── topk_fixed.tsv                ← 独立保存的修正后 Top-K 结果
    │   └── pr_run.log                    ← Hadoop MR1–MR4 完整运行日志
    │
    └── pagerank_local/                   ← 本地 Python PageRank 实验结果
        ├── edges.csv                     ← 本地商品关联边数据
        ├── nodes.csv                     ← 本地商品节点数据
        ├── convergence.csv               ← PageRank 迭代收敛记录
        ├── stats.txt                     ← 本地 PageRank 统计摘要
        └── top_items.csv                 ← 本地 PageRank Top 商品结果

## 模块说明

| 模块 | 负责人 | 说明 |
|------|--------|------|
| [Database](Database/) | 汪成儒 | 数据库构建、数据导入脚本、可视化图表 |
| [PageRank](pagerank/) | — | 会话共现图 + 加权 PageRank，找枢纽商品；含本地验证版与 Hadoop MapReduce 全量版 |
| [mapreduce_data_tools]| 逯卿芝 | MapReduce 数据处理与验证工具集，包含数据质量检查、样本抽取、输入数据准备、MR1 本地模拟及 Hadoop 结果对比|
| [run_results]| 逯卿芝 |项目实验结果归档，集中保存 K-means、PageRank 本地版、Hadoop MapReduce 及数据验证工具的运行结果、日志和报告 |

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
