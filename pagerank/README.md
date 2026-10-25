# PageRank 模块

淘宝用户行为数据（[天池 UserBehavior](https://tianchi.aliyun.com/dataset/649)，1 亿条）上的
**加权 PageRank**：把用户的浏览/加购/收藏/购买行为切成会话，用同一会话内商品的**共现**建一张
带权无向图，在图上跑 PageRank，找出"枢纽商品"。

这个模块提供**两个可分别执行的版本**：

| 版本 | 位置 | 数据规模 | 环境 | 用途 |
|---|---|---|---|---|
| 小批量版 | [`local/`](local/) | 1.7MB 样本（449 用户 / 3.34 万节点） | Python 3，**不需要 Hadoop** | 本地验证算法是否生效，秒级出结果 |
| 全量版 | [`mapreduce/`](mapreduce/) | 3.4GB / 1 亿行 | Hadoop MapReduce（Java） | 在集群上跑全量数据，产出报告用的结果 |

两版**抽样规则、会话切分、图构造、PageRank 公式完全一致**，所以小批量版跑出来的结论可以
用来校验全量版是否正确（见 §5）。

- 全量版运行手册（给负责集群的同学）：**[`mapreduce/README.md`](mapreduce/README.md)**
- 图表产出脚本：`local/visualize_hubs.py`、`local/compare_behaviors.py`
- 备选实现（Pig Latin，逻辑等价，适合试跑）：[`pig/`](pig/)

---

## 1. 目录结构

```
pagerank/
├── README.md                  # 本文件：方法、结果、如何跑两版
├── local/                     # 小批量版 (Python)
│   ├── pagerank_local.py      # 主程序: 建图 + 迭代 PR + Top-K
│   ├── run_local.ps1          # Windows 一键跑
│   ├── run_local.sh           # Linux/macOS 一键跑
│   ├── visualize_hubs.py      # 出图: 枢纽网络 / 幂律 / 收敛曲线 / 类目多样性
│   └── compare_behaviors.py   # 算 strength / n_neighbors / 类目熵 等对比指标
├── mapreduce/                 # 全量版 (Hadoop MapReduce, Java)
│   ├── README.md              # ★ 集群运行手册
│   ├── pom.xml
│   ├── run_mr.sh              # 一键提交
│   ├── dist/                  # 已编译好的 jar（直接 scp 到集群就能跑）
│   └── src/main/java/cs5488/pr/
│       ├── CooccurrenceJob.java   # MR1 会话化 + 二次排序建边
│       ├── GraphBuildJob.java     # MR2 边表 -> 邻接表 + 数节点数 N
│       ├── PageRankJob.java       # MR3 单轮迭代 (Counter 算 delta)
│       ├── TopKJob.java           # MR4 全局 Top-K
│       └── PageRankDriver.java    # 串起 MR1~MR4
├── pig/                       # 备选: Pig Latin 版 (可选, 未在集群验证)
├── data/
│   ├── make_sample.py         # 从全量 CSV 确定性抽样, 生成小批量数据集
│   └── sample/
│       └── UserBehavior_sample.csv   # 48,225 行 / 449 用户 / 1.7MB (已入库)
└── results/                   # 全量 330 采样的产出 (已入库)
    ├── top_items.csv          # Top-30 枢纽商品
    ├── convergence.csv        # 每轮 L1 变化
    ├── stats.txt              # 图规模/耗时等统计
    ├── behavior_comparison.txt / .png
    ├── hub_visualization.png
    └── panel_a~d_*.png
```

`nodes.csv` / `edges.csv`（原始图，约 36MB）**不入库**，跑一次就会重新生成。

---

## 2. 数据

`UserBehavior.csv`（3,672,347,465 B，100,150,807 行），字段：

```
user_id, item_id, category_id, behavior_type, timestamp(秒)
```

- 时间范围 2017-11-25 ~ 2017-12-03（9 天）
- `behavior_type ∈ {pv, buy, cart, fav}`
- **文件太大，不进 git**（GitHub 单文件限 100MB）。获取方式：
  - 天池 https://tianchi.aliyun.com/dataset/649 下载 `UserBehavior.csv.zip`（~900MB）解压；
  - 或直接用本仓库 `Database/` 模块 README 里同学提供的下载/入库方案。

小批量数据集 `data/sample/UserBehavior_sample.csv` 是从全量里**确定性抽样**得到的
（`uid % 2200 == 0`），所以：

- 不需要下载全量数据也能跑通代码；
- 抽样规则固定，任何人重跑都得到同一份样本。

重新生成（需要全量 CSV）：

```bash
python data/make_sample.py --input UserBehavior.csv --output data/sample/UserBehavior_sample.csv --mod 2200
```

---

## 3. 方法

### 3.1 会话化（sessionization）

原始数据是一条条"用户-商品"事件，没有会话标识。做法：

1. 按 `(user_id, timestamp)` 排序；
2. 相邻两条事件的时间差 `> session_gap`（默认 **1800 秒 = 30 分钟**）就切一个新会话；
3. 每个会话内按时间顺序保留去重后的商品序列，最多 **50** 个商品（超过就截断，防止"一口气
   浏览 500 个商品"的异常用户贡献O(n²) 条边）。

为什么用会话而不是"同一用户全时段共现"：同一用户 9 天里买的东西可能毫无关系（比如先买猫粮
再买手机），按会话切能近似"一次购物意图"。这是本项目做出的一个关键建模选择，也是
`compare_behaviors.py` 里做对比实验的对象。

### 3.2 共现图（co-occurrence graph）

同一会话内的商品两两之间连一条边，权重 = **共现次数**（同一对商品在多少个会话里一起出现）。
图是无向加权图：

```
edge(i, j) weight w_ij = #{sessions : i 和 j 同会话出现}
S(i) = Σ_j w_ij              # 节点 i 的加权度数 (strength)
```

淘宝 1 亿条原始事件、`mod=330` 采样后（312,780 行 / 3,005 用户 / 51,348 会话），得到的图是：

| 指标 | 值 |
|---|---|
| 节点数 N | **168,000** |
| 边数 | **1,763,568** |
| 平均度数 | 2 × 1,763,568 / 168,000 ≈ **21** |

### 3.3 加权 PageRank

标准 PageRank 的加权推广：

```
PR(i) = (1-d) + d · Σ_{j ∈ in(i)} PR(j) · w_ji / S(j)
```

- `d = 0.85`（阻尼系数）
- 边权 `w_ji` 归一化到 `S(j)`，即"j 把自己的分按边权比例分给邻居"
- 无出边节点（dangling，本图里没有）质量会均摊回全体

**实现细节（重要）**：代码里不直接存 `PR`，而存 `mass = N · PR`，初始 `mass = 1`：

```
new_mass(i) = (1-d) + d · Σ_j mass(j) · w_ji / S(j)
PR(i) = mass(i) / N
```

好处是**质量总和恒为 N**（浮点误差累积小得多，可以直接用 `Σmass ≈ N` 做守恒校验），
收敛判据用 `mean_i |Δmass_i| = (Σ_i |Δmass_i|) / N`，这正是 PR 尺度上的 `mean|ΔPR|`。

> 这一点在 MR 版里是踩过的坑：一开始直接对 `mass` 求和除以 1e9 当 delta，看着"收敛"了，
> 但实际严格程度是 Python 版的 N 倍，收敛轮数对不上。现在两版都换算到 PR 尺度。

### 3.4 Top-K 枢纽

收敛后按 `PR` 取 Top-K，同时输出 `strength`（加权度数）作为解释变量——
"这个商品 PR 高是因为它和很多东西一起出现，而不是因为它被某个超级用户买爆"。

---

## 4. 怎么跑

### 4.1 小批量版（本地验证，2 秒）

Windows：

```powershell
cd pagerank
.\local\run_local.ps1
```

Linux / macOS / WSL：

```bash
cd pagerank
chmod +x local/run_local.sh
./local/run_local.sh
```

也可以直接调：

```bash
python local/pagerank_local.py \
    --input data/sample/UserBehavior_sample.csv \
    --sample-mod 1 \
    --max-iter 10 \
    --top 30 \
    --outdir results/local_run
```

完整参数：

| 参数 | 默认 | 说明 |
|---|---|---|
| `--input` | 必填 | CSV 或 `.zip`（自动流式读 zip） |
| `--sample-mod` | `330` | 保留 `uid % m == 0` 的用户，`1` = 不采样 |
| `--max-rows` | `0` | 最多读多少行（`0` = 全部），用于快速冒烟 |
| `--behaviors` | `pv,buy,cart,fav` | 参与建图的行为类型 |
| `--session-gap` | `1800` | 会话切分间隔（秒） |
| `--max-session-items` | `50` | 单会话最多保留商品数 |
| `--damping` | `0.85` | 阻尼系数 |
| `--max-iter` | `50` | 最大迭代轮数 |
| `--tol` | `1e-10` | `mean|ΔPR|` 收敛阈值 |
| `--top` | `30` | Top-N |
| `--outdir` | `./pr_output` | 输出目录 |

### 4.2 全量版（集群）

见 **[`mapreduce/README.md`](mapreduce/README.md)**。一句话版本：

```bash
cd pagerank/mapreduce
mvn clean package -DskipTests
hadoop jar target/taobao-pagerank-1.0.jar cs5488.pr.PageRankDriver \
    <hdfs>/UserBehavior.csv <hdfs>/pr 330 1800 0.85 80 1e-6 30 pv,buy,cart,fav
```

或者用封装脚本：

```bash
INPUT=/user/hadoop/taobao/UserBehavior.csv ./run_mr.sh     # 330 采样 (推荐首跑)
INPUT=/user/hadoop/taobao/UserBehavior.csv SAMPLE_MOD=1 ./run_mr.sh   # 全量
```

---

## 5. 结果 & 两版一致性验收

### 5.1 小批量版实测（`data/sample`，10 轮）

```
rows 48,225 | users 449 | sessions 7,761 (截断 14) | nodes 33,400 | edges 269,521
建图 0.1s, PageRank 0.7s
```

Top-10：

| rank | item_id | category_id | pagerank | strength | n_neighbors |
|---|---|---|---|---|---|
| 1 | **812879** | 4756105 | 2.9082e-04 | 261 | 250 |
| 2 | 3845720 | 3607361 | 2.7534e-04 | 204 | 199 |
| 3 | 1583704 | 4801426 | 2.5328e-04 | 196 | 194 |
| 4 | 1143624 | 3607361 | 2.4770e-04 | 237 | 212 |
| 5 | 4189993 | 3607361 | 2.3469e-04 | 237 | 188 |
| 6 | 2009215 | 570735 | 2.2753e-04 | 103 | 78 |
| 7 | 307891 | 4756105 | 2.2218e-04 | 250 | 179 |
| 8 | 640975 | 2355072 | 2.1831e-04 | 119 | 116 |
| 9 | 59883 | 4818107 | 1.9909e-04 | 119 | 118 |
| 10 | 2032668 | 1080785 | 1.9454e-04 | 134 | 134 |

### 5.2 全量 330 采样实测（`results/`，80 轮上限，tol=1e-10）

```
input: UserBehavior.csv.zip (全量)
rows 100,150,807 -> sampled 312,780 | users 3,005 | sessions 51,348 (截断 161)
nodes 168,000 | edges 1,763,568
建图 0.7s, PageRank 55.0s (80 轮), final L1 1.979e-09
```

Top-5：

| rank | item_id | category_id | pagerank | strength | n_neighbors |
|---|---|---|---|---|---|
| 1 | **812879** | 4756105 | 2.0275e-04 | 1101 | 996 |
| 2 | 3845720 | 3607361 | 1.8961e-04 | 1041 | 968 |
| 3 | 2331370 | 3607361 | 1.6871e-04 | 842 | 802 |
| 4 | 2032668 | 1080785 | 1.5618e-04 | 867 | 822 |
| 5 | 4649427 | 4756105 | 1.5280e-04 | 878 | 814 |

完整 Top-30 见 [`results/top_items.csv`](results/top_items.csv)，收敛曲线见
[`results/convergence.csv`](results/convergence.csv)。

**收敛速度**（决定 MR 版要跑多少轮，以下是从 `results/convergence.csv` 实测数出来的）：

| 阈值 `tol` | 达到所需轮数 |
|---|---|
| `1e-2` | 7 轮 |
| `1e-3` | 14 轮 |
| `1e-4` | **23 轮** |
| `1e-5` | 34 轮 |
| `1e-6` | **46 轮** |
| `1e-7` | 58 轮 |
| `1e-8` | 71 轮 |

所以 `mapreduce/README.md` 里默认 `tol=1e-6, maxIter=80`，实际应在 **46 轮左右**停下。
每轮 delta 的衰减比从 0.74 缓升到 0.84（越接近收敛越慢），呈标准幂律尾部——
这条曲线本身就是报告里"收敛性"那张图（`results/panel_c_convergence.png`）。

### 5.3 一致性验收（两版对不对得上）

抽样是确定性的，所以两版必须给出同样的规模指标：

| 指标 | 期望值 | 小批量版（449 用户样本） | MapReduce（330 采样） |
|---|---|---|---|
| 节点数 N | — | 33,400（对应 449 用户） | **168,000** ← 黄金指标 |
| 边数 | — | 269,521 | **1,763,568** |
| Top-1 | **812879** | ✅ 812879 | ✅（验收时看 `topk` 首行） |

**只要 MR 版打印 `N = 168000` 且 `topk` 第一行是 `812879`，就说明两版一致。**

### 5.4 图表产出

```bash
python local/visualize_hubs.py --indir results --outdir results   # 4 张 panel
python local/compare_behaviors.py --indir results                 # 行为对比指标
```

---

## 6. 验证状态（诚实说明）

| 项 | 状态 |
|---|---|
| 小批量版在 `data/sample` 上端到端跑通 | ✅ 已验证（§5.1 的数字就是实跑输出） |
| 小批量版在全量 CSV + 330 采样上端到端跑通 | ✅ 已验证（§5.2，`results/` 就是它的输出） |
| MapReduce 版 `mvn package` 编译通过 | ✅ 已验证（`target/taobao-pagerank-1.0.jar`） |
| MapReduce 版在真实 Hadoop/YARN 上端到端跑通 | ⚠️ **未验证**——开发机是 Windows，缺 `winutils.exe` 起不了本地 MR；需要组员在集群上确认 |
| Pig 版 | ⚠️ 未验证（逻辑与 Java 版等价，仅作备选） |

所以：**算法正确性由小批量版 + 全量 330 采样的 Python 运行背书；分布式实现的可跑性需要
组员在集群上首次确认**（`mapreduce/README.md` §5 列了完整的验收清单）。

---

## 7. 与项目其它部分的关系

- `Database/`（组员负责）：把 CSV 导入 MySQL，供 k-means 和图探索用；
- `pagerank/`（本模块）：图算法部分；
- 报告里 PageRank 一节用到的数字/图都来自本目录 `results/`。
