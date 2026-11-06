# MapReduce / PageRank 数据处理工具

本目录是 CS5488 项目中 **MapReduce / PageRank 模块** 独立配套的数据处理、质量检查与验证工具集。

设计原则：**不修改、不重写、不优化任何现有 MapReduce Java 代码或脚本**。
现有 `pagerank/mapreduce/` 下的 MR1~MR4（`CooccurrenceJob` / `GraphBuildJob` /
`PageRankJob` / `TopKJob` / `PageRankDriver`）、`run_mr.sh`、`data_healthcheck.sh`
以及 `pagerank/data/`、`pagerank/results/`、`dist/`、jar 等全部保持原样。

本目录中的工具只做以下事情：

- **只读**原始 `UserBehavior.csv` / `.zip` 以及现有 MR 的输出目录；
- **只写** `mapreduce_data_tools/out/`（或用户通过 `--outdir` 明确指定的目录）；
- 输出目录允许已存在，但写入前检查目标文件是否已存在，**禁止覆盖同名文件**。

---

## 1. 目录结构

```
mapreduce_data_tools/
├── README.md                     # 本文件
├── data_quality_check.py         # 原始数据质量统计（只统计，不删除）
├── tiny_sample.py                # 按 UID 取模生成小样本（默认 mod=2200）
├── validate_session_edges.py     # 模拟 MR1 共现边逻辑：用户级边/全局边/节点/摘要
├── prepare_mr_input.py           # 按 MR1 Mapper 输入规则准备数据（默认不抽样/不过滤/不去重）
├── compare_mr_vs_expected.py     # 比较 Expected 与 Hadoop 实际输出（5 个阶段）
└── out/                          # 运行时自动创建；所有默认产物写在这里
```

> `out/` 目录不需要提前创建。**第一次运行任一脚本时**由脚本自动创建。

---

## 2. 各脚本职责

| 文件 | 职责 | 是否改变数据语义 |
|---|---|---|
| `data_quality_check.py` | **原始数据质量统计**：字段数、缺失、解析失败、负值、行为分布、非法行为、完全重复记录、uid 范围与 int32 溢出、时间范围越界。**只统计，不删除、不修改原始数据**。 | 否（纯统计） |
| `tiny_sample.py` | 按 `user_id % mod == 0` 的确定性抽样生成小样本，**默认 `mod=2200`**（与 `pagerank/data/make_sample.py` 一致）。保持原始五列、无表头、保序。 | 否（仅抽样） |
| `validate_session_edges.py` | **模拟 MR1 共现边逻辑**：复刻 `CooccurrenceJob` 的会话切分（gap=1800s）、会话内去重、商品上限 50、两两共现权重聚合。产出**用户级边、跨用户全局边、节点统计、摘要**四个文件。**不实现 PageRank，也不实现 Top-K。** | 否（独立参考，不改 MR） |
| `prepare_mr_input.py` | 按 **MR1 Mapper 输入解析规则**准备数据：默认只过滤格式坏行与非法行为（与 MR1 一致），**默认不抽样、不做时间过滤、不去重**。输出仍为原始五列、无表头、保序。 | 默认否；显式开启选项才可能改变 |
| `compare_mr_vs_expected.py` | 比较 **Expected** 与 **Hadoop 实际输出**，支持 `mr1` / `mr2` / `nodes` / `pagerank` / `topk` 五个阶段。 | 否（只读比对） |

---

## 3. 输入 / 输出路径与参数

以下路径均相对**项目根目录**。

### 3.1 通用

- 所有脚本支持 `--outdir`；默认输出到 `mapreduce_data_tools/out/` 的各子目录（见下表）。
- 输出文件禁止覆盖：目标已存在则报错退出。

### 3.2 data_quality_check.py

- 输入：`--input`（必填，原始 CSV 或 `.zip`）。
- 参数：`--outdir`、`--report-name`（默认 `quality_report.txt`）、
  `--check-duplicates`（默认开启）/ `--no-check-duplicates`（关闭重复检测，省内存）。
- 默认输出：`mapreduce_data_tools/out/quality_report.txt`。

### 3.3 tiny_sample.py

- 输入：`--input`（必填，原始 CSV 或 `.zip`）。
- 参数：`--mod`（默认 **2200**）、`--outdir`、`--filename`（默认 `UserBehavior_tiny.csv`）。
- 默认输出：`mapreduce_data_tools/out/tiny/UserBehavior_tiny.csv`。

### 3.4 validate_session_edges.py

- 输入：`--input`（**必填，没有默认输入路径**，五列 CSV 或 `.zip`）。
- 参数：
  - `--sample-mod`（默认 **1**，即不采样；`330` 表示复刻 MR1 默认抽样）
  - `--session-gap`（默认 1800）
  - `--max-items`（默认 50）
  - `--behaviors`（默认 `pv,buy,cart,fav`）
  - `--outdir`、`--max-rows`（默认 0，仅自测截断用）
- 默认输出目录 `mapreduce_data_tools/out/expected/`，实际生成**四个文件**：

| 文件 | 含义 |
|---|---|
| `expected_edges_user.tsv` | **MR1 用户级边权**：`i1<TAB>i2<TAB>w`，同一用户跨其所有 session 累加，**不同用户之间不合并**（同一对可能多行） |
| `expected_edges_global.tsv` | **跨用户汇总后的全局边权**：`i1<TAB>i2<TAB>w`，模拟 MR2 的跨用户合并结果 |
| `expected_nodes.tsv` | **节点集合**：`item<TAB>节点强度<TAB>邻居数`（全局） |
| `expected_summary.txt` | **统计摘要**：读取/过滤/会话/边/节点计数与语义边界说明 |

> **内存说明**：本工具用内存保存每个用户的事件列表、以及全局边权 `global_pair`，
> 内存随数据量线性增长，**仅适合小样本**，不建议直接处理全量 1 亿行数据。

### 3.5 prepare_mr_input.py

- 输入：`--input`（必填，原始 CSV 或 `.zip`）。
- 参数：`--outdir`、`--filename`（默认 `UserBehavior.clean.csv`）、
  `--filter-time-range MIN MAX`（默认关闭，必须显式给出）、
  `--no-count-duplicates`（默认统计重复，关闭可省内存）。
- 默认输出：`mapreduce_data_tools/out/clean/UserBehavior.clean.csv` 与同目录 `prepare_report.txt`。

### 3.6 compare_mr_vs_expected.py

- 参数（实际 argparse）：

| 参数 | 说明 |
|---|---|
| `--stage` | 必填，取值 `mr1` / `mr2` / `nodes` / `pagerank` / `topk` |
| `--expected` | 必填，期望文件路径（文件或目录） |
| `--actual` | 必填，实际输出路径（文件或目录；目录会读取其中的 `part-*`） |
| `--outdir` | 报告输出目录（默认 `mapreduce_data_tools/out`） |
| `--top-n` | 差异详情数量（默认 20） |
| `--tolerance` | 浮点误差容差（默认 `1e-6`，用于 `pagerank`/`topk`） |
| `--pagerank-node-count` | 可选：actual 为 `graph_k`（`id<TAB>mass<TAB>adj`）时，用 `mass/N` 转为 PR |

- **实际报告文件名**：`compare_<stage>_<timestamp>.txt`（默认写到 `out/`）。
- 各阶段的 Expected / Actual 来源：

| `--stage` | Expected | Actual |
|---|---|---|
| `mr1` | `expected_edges_user.tsv` | MR1 的 `edges/` 输出（`i1<TAB>i2<TAB>w`，按多重集比较） |
| `mr2` | `expected_edges_global.tsv` | MR2（`GraphBuildJob`）的 `graph_0`（**邻接表** `id<TAB>mass<TAB>adj`，脚本从邻接表还原无向全局边） |
| `nodes` | `expected_nodes.tsv` | 图文件，取每行第一列为节点 |
| `pagerank` | **需用户单独提供** PageRank Expected | 可为 `graph_k`（配合 `--pagerank-node-count N`）或已归一化的 PR 文件 |
| `topk` | **需用户单独提供** Top-K Expected | `TopKJob` 输出（`rank<TAB>id<TAB>pagerank<TAB>strength`） |

> 说明：
> - `pagerank`/`topk` 的 Expected **不由本目录任何脚本生成**，需用户自行提供
>   （例如本地 Python 版结果或另行计算）；缺失时工具返回 `CANNOT_COMPARE`。
> - 旧版 README 中的 `--mr-edges` / `--mr-topk` / `--expected-dir` **并不存在**，
>   请使用上表的 `--stage` / `--expected` / `--actual`。

---

## 4. 抽样参数一致性警告（重要）

不同工具/组件默认的抽样参数不同，**Expected 与 Actual 必须使用相同的输入数据范围与相同的抽样规则**：

| 组件 | 默认抽样参数 |
|---|---|
| `tiny_sample.py` | `mod=2200` |
| `validate_session_edges.py` | `--sample-mod 1`（不采样） |
| Java MR1（`CooccurrenceJob`） | `sampleMod=330` |

规则：

1. **Expected 与 Actual 必须基于相同的输入数据范围。**
2. **Expected 与 Actual 必须采用相同的抽样规则。**
3. 若使用未抽样的 `clean.csv`（`prepare_mr_input.py` 默认输出）去验证 **Java 默认 `sampleMod=330`**，
   则 `validate_session_edges.py` **必须指定 `--sample-mod 330`**，否则 expected 是全集、actual 是 330 采样，
   会产生大量缺失/多余边的假差异。
4. 若输入已经由 `tiny_sample.py` 按 `mod=2200` 抽样，则必须按 **Java MR1 实际使用的 `sampleMod`** 决定
   验证参数，避免二次抽样或口径不一致：
   - 若直接让 Java MR1 对该样本再按 330 采样，则 validate 仍需按实际有效范围对齐；
   - 更稳妥的做法是让 Java MR1 使用 `sampleMod=1`（不额外采样），validate 也用 `--sample-mod 1`。
5. **不要把不同抽样规则产生的结果差异误判成 MR1/MR2 算法错误。**

---

## 5. 内存与数据规模说明

| 工具 | 数据规模建议 |
|---|---|
| `data_quality_check.py` | 可流式处理；全量数据建议加 `--no-check-duplicates` 关闭重复统计（否则内存随唯一行数增长） |
| `tiny_sample.py` | 支持流式读取全量数据；但 `users` 集合仍随用户数占用内存 |
| `validate_session_edges.py` | **仅建议小样本**（事件与边权全量驻留内存） |
| `prepare_mr_input.py` | 可流式处理；全量数据建议加 `--no-count-duplicates` 关闭重复统计 |
| `compare_mr_vs_expected.py` | 边与节点会装入内存，适合**小到中等规模**对比 |

> **任何工具均未完成全量 1 亿行性能压测**；上述仅为设计层面的内存评估，不代表实测结果。

---

## 6. 语义边界与算法不变量

算法不变量（与现有 pipeline 一致）：

- session 时间间隔：**1800 秒**（切分条件为 `ts - lastTs > 1800`）；
- 会话内商品上限：**50**；
- 行为过滤：`pv/buy/cart/fav`；
- 采样：`user_id % mod == 0`；
- 商品 ID：保持字符串表示；
- 共现边权重：同一会话内两两商品共现次数累加。

语义边界：

- **相同 timestamp 的排序**：Java MR1 的二次排序键为 `(uid, timestamp)`，
  对 `timestamp` 完全相同的多条事件不保证稳定顺序；Python 参考实现对相同 ts 采用
  "输入出现顺序的稳定排序"。因此相同 timestamp 在会话内先后、50 上限截断边界处可能产生差异，
  比对时应视为语义边界，而非 pipeline 错误。
- **浮点差异**：参考实现与 Java 实现的浮点求和顺序可能不同，PageRank 数值末位可能有差异；
  以节点数、边数、Top-K 集合等结构性指标为主判据。

---

## 7. 工具之间的关系

- `tiny_sample.py` 与 `prepare_mr_input.py` 是**两种独立用途**，**不是必须串行执行**：
  - `tiny_sample.py`：生成小样本（按 UID 取模），用于快速本地验证；
  - `prepare_mr_input.py`：按 MR1 Mapper 的输入解析规则准备数据（默认不抽样/不过滤/不去重），
    用于喂给 Java MR1 或作为验证输入。
- 两者的输出均为**五列 CSV**，都可作为 `validate_session_edges.py` 的 `--input`；
  但**验证参数必须与实际 MR 的抽样口径一致**（见第 4 节）。
- `validate_session_edges.py` 生成 Expected（用户级边 / 全局边 / 节点 / 摘要）。
- `compare_mr_vs_expected.py` 负责把 Expected 与**实际 Hadoop 输出**比较。
- 二者不互相依赖运行顺序，但比较时需保证 Expected 与 Actual 来自**同一输入、同一抽样口径**。

---

## 8. 端到端命令示例

> 下列命令均在**项目根目录**执行。Hadoop 作业需用户亲自运行；本目录脚本**不自动运行 Hadoop**。
> 比较工具读取的是**本地文件**，Hadoop 输出需先从 HDFS 导出到本地。

### A. 原始数据质量检查

```bash
python3 mapreduce_data_tools/data_quality_check.py \
    --input pagerank/data/sample/UserBehavior_sample.csv

# 全量数据建议关闭重复统计：
# python3 mapreduce_data_tools/data_quality_check.py \
#     --input UserBehavior.csv --no-check-duplicates --outdir mapreduce_data_tools/out/quality_full
```

### B. 生成 tiny sample

```bash
python3 mapreduce_data_tools/tiny_sample.py \
    --input UserBehavior.csv --mod 2200
# 输出：mapreduce_data_tools/out/tiny/UserBehavior_tiny.csv
```

### C. 准备 MR1 输入

```bash
python3 mapreduce_data_tools/prepare_mr_input.py \
    --input UserBehavior.csv
# 输出：mapreduce_data_tools/out/clean/UserBehavior.clean.csv（默认不抽样/不过滤/不去重）
```

### D. 生成 expected 边和节点

```bash
# 用 tiny 样本（validate 默认 sample-mod=1）
python3 mapreduce_data_tools/validate_session_edges.py \
    --input mapreduce_data_tools/out/tiny/UserBehavior_tiny.csv

# 若用未抽样的 clean.csv 对齐 Java 默认 sampleMod=330，则必须：
# python3 mapreduce_data_tools/validate_session_edges.py \
#     --input mapreduce_data_tools/out/clean/UserBehavior.clean.csv --sample-mod 330
# 输出：expected_edges_user.tsv / expected_edges_global.tsv / expected_nodes.tsv / expected_summary.txt
```

### E. 用 compare 比较 MR1（用户级边）

```bash
# 先把 MR1 的 edges 输出从 HDFS 导出到本地，例如导出到 ./mr_edges
python3 mapreduce_data_tools/compare_mr_vs_expected.py \
    --stage mr1 \
    --expected mapreduce_data_tools/out/expected/expected_edges_user.tsv \
    --actual   ./mr_edges
# 报告：mapreduce_data_tools/out/compare_mr1_<timestamp>.txt
```

### F. 用 compare 比较 MR2（全局边）

```bash
# MR2 输出为邻接表 graph_0，先从 HDFS 导出到本地，例如 ./graph_0
python3 mapreduce_data_tools/compare_mr_vs_expected.py \
    --stage mr2 \
    --expected mapreduce_data_tools/out/expected/expected_edges_global.tsv \
    --actual   ./graph_0
# 报告：mapreduce_data_tools/out/compare_mr2_<timestamp>.txt
```

### G. nodes 比较

```bash
python3 mapreduce_data_tools/compare_mr_vs_expected.py \
    --stage nodes \
    --expected mapreduce_data_tools/out/expected/expected_nodes.tsv \
    --actual   ./graph_0
# 报告：mapreduce_data_tools/out/compare_nodes_<timestamp>.txt
```

### H. PageRank / Top-K 比较（需用户自行提供 Expected）

```bash
# PageRank：Expected 需用户自行提供（本目录不生成 PR expected）
#   若 actual 是 graph_k（id<TAB>mass<TAB>adj），用 --pagerank-node-count N 把 mass 转 PR
python3 mapreduce_data_tools/compare_mr_vs_expected.py \
    --stage pagerank \
    --expected /path/to/your_expected_pagerank.tsv \
    --actual   ./graph_k \
    --pagerank-node-count <N> \
    --tolerance 1e-6

# Top-K：Expected 需用户自行提供
python3 mapreduce_data_tools/compare_mr_vs_expected.py \
    --stage topk \
    --expected /path/to/your_expected_topk.tsv \
    --actual   ./topk \
    --tolerance 1e-6
# 报告：mapreduce_data_tools/out/compare_pagerank_<timestamp>.txt / compare_topk_<timestamp>.txt
```

---

## 9. 未验证事项（如实说明）

- `prepare_mr_input.py` 的默认清洗结果与**真实 Java MR1 输出**的等价性**尚未完成验证**——
  需实际用原始 CSV 与 clean.csv 分别跑 MR1 并比对 `edges/` 才能下结论。
- `compare_mr_vs_expected.py` **尚未使用真实 Hadoop 输出验证**（目前仅用构造的小样本自测）。
- **PageRank 与 Top-K 的 Expected 尚需单独生成**（本目录脚本不产出）。
- **所有工具均未完成全量 1 亿行性能压测。**

---

## 10. 如何验证新增脚本不影响现有 pipeline

1. **零侵入**：所有脚本只读原始数据与现有 MR 输出，只写 `out/` 或指定目录；运行前后对现有目录做
   时间戳/校验和对比即可证明未改动。
2. **不自动运行 Hadoop**：所有集群作业需用户手动执行；本目录脚本只做本地读取、统计与比对。
3. **统一输入格式**：各脚本接受五列 CSV（原始或 tiny/clean 产物），保证链路一致。
