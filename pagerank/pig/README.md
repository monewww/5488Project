# Pig Latin 版（可选 · 未验证）

> **状态：未在集群上验证。** 仅作为备选/试跑参考保留。
> **正式交付请用 [`../mapreduce/`](../mapreduce/) 的 Java 版**，那份已编译通过并有完整运行手册。

四个脚本对应 Java 版的四个阶段，思路一致：

| 脚本 | 对应 | 说明 |
|---|---|---|
| `01_cooccurrence.pig` | `CooccurrenceJob` | 采样 → 按 `(uid, ts/gap)` 时间桶分组 → 桶内 `CROSS` 生成两两组合 → 按 pair 求和 |
| `02_graph_build.pig` | `GraphBuildJob` | 边表转邻接表 + 节点数 |
| `03_pagerank_iter.pig` | `PageRankJob` | 单轮质量传递（Lin & Dyer），并 `DUMP` 出本轮 L1 delta |
| `04_topk.pig` | `TopKJob` | 全量排序取 Top-K |

一键跑：`./run_pagerank.sh`

## 与 Java 版的差异（重要，别混用中间结果）

1. **会话切分不同**：Pig 没有 `lag` 算子，`01` 用**固定时间桶** `ts / gap` 近似精确的
   "间隔 > gap 就切" 规则。所以 Pig 版的 `N` / 边数 / Top-K **不会**精确等于 Java 版，
   只适合看趋势。这一点可以写进报告的"实现对比"里。
2. **记录格式不同**：`02` 的输出是 `id \t mass \t strength \t adj`（邻接表用**空格**分隔的
   `nbr:w`，`strength` 单独一列）；Java 版是 `id \t mass \t adj`（`adj` 里用空格分隔，
   strength 由邻接表现算）。**两套中间结果不能互相喂。**
3. Pig 版没有 `Counter`，收敛判断靠 `DUMP` 出来的 `L1_DELTA` 手动看。

所以：**要不要留这一个版本由组里决定**；如果集群上 Pig 版本装，直接删掉这个目录不影响
任何其他模块。
