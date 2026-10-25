# 全量数据版 · Hadoop MapReduce 运行手册

> **给谁看**：负责在 Hadoop 集群上实跑的同学。
> **你只需要**：装好 JDK + Hadoop 集群能跑 `hadoop jar`，然后按下面 **4 步** 操作。
> **不依赖**：Python、数据库、MySQL、Pig（Pig 版是可选备选，见 `../pig/`）。

本目录是一套独立的 MapReduce 实现，把淘宝 `UserBehavior.csv` 变成**会话共现图**，在图上迭代
**PageRank**，最后输出 Top-K 枢纽商品。伪分布式或全分布式都能跑，代码里没有单机假设。

---

## 0. 一页速览：4 个 MapReduce 阶段

| 阶段 | 类 | 输入 → 输出 | 做什么 |
|---|---|---|---|
| MR1 | `CooccurrenceJob` | `UserBehavior.csv` → `<out>/edges` | 二次排序（`uid`+时间戳）把用户行为切成会话，同一会话内商品两两成边并累加权 |
| MR2 | `GraphBuildJob` | `<out>/edges` → `<out>/graph_0` | 边表转邻接表：每个节点一行 `id \t PR初值 \t 邻居1:权重1 邻居2:权重2 ...`，同时数出节点数 **N** |
| MR3 | `PageRankJob` ×k | `<out>/graph_{k-1}` → `<out>/graph_k` | 一轮 PageRank（`mass` 形式：`sum(mass)=N`），用 Counter 算出 L1 变化量判停 |
| MR4 | `TopKJob` | `<out>/graph_k` → `<out>/topk` | in-mapper combining 求全局 Top-K，输出 `rank, id, pagerank, strength` |

`PageRankDriver` 把四步串起来，**只保留最近两轮中间结果**（`graph_k` 满了自动删 `graph_{k-2}`），
所以 HDFS 占用不会随迭代轮数线性增长。

**初始采样**：MR1 里按 `uid % sampleMod == 0` 保留用户，`sampleMod=330` 对应约 1/330 的用户
（≈312,780 行，占全量 0.31%），与本地 Python 验证版**完全同一套抽样规则**。想要全量就传 `1`。

---

## 1. 环境要求

| 项 | 要求 |
|---|---|
| JDK | 8 / 11 / 17 都行（jar 按 `-source/-target 1.8` 编译） |
| Hadoop | 2.x / 3.x，`HADOOP_HOME` 已配，`hadoop jar` 可用；`YARN` + HDFS 正常 |
| Maven | 3.6+（可在本地机器上打包，也可以直接用我提交的源码） |
| 内存 | MR1 是会话化二次排序，reducer 内存建议 ≥ 2GB；并行度用 `mapreduce.job.reduces` 调（见 §5.4） |

> `hadoop-client` 在 pom 里是 `provided`，**jar 里不含 Hadoop 依赖**，直接丢到集群就能跑，
> 不会和集群自带的版本打架。

---

## 2. 打包

**最省事：直接用 `dist/taobao-pagerank-1.0.jar`（已提交在仓库里，34KB）**，把它 `scp` 到集群
就能跑，不需要装 Maven：

```bash
scp dist/taobao-pagerank-1.0.jar hadoop@<你的集群>:~/
```

想自己从源码构建（改了代码之后必须这么做）：

```bash
cd pagerank/mapreduce
mvn clean package -DskipTests
# 产物: target/taobao-pagerank-1.0.jar  —— 顺手 `cp target/*.jar dist/` 更新提交版
```

无外网/公司镜像慢，就用 `-o` 走离线（前提是你本机 `~/.m2` 里已有 hadoop-client 3.3.6）：

```bash
mvn -o -DskipTests package
```

> `dist/` 里的 jar 只是最后一次构建的产物。**改了 `.java` 一定要重新 `mvn package` 并覆盖它**，
> 否则集群跑的是旧代码——这是最容易踩的坑，建议每次提交前 `jar tf dist/*.jar | grep -c class`
> 对比一下时间戳。

---

## 3. 准备输入数据

`UserBehavior.csv` 有 3.4GB，**不要 commit 进 git**。集群上这样做：

```bash
# 3.1 从数据集官网下载（需要天池账号）
#     https://tianchi.aliyun.com/dataset/649   ->  UserBehavior.csv.zip (~900MB)
# 3.2 解压
unzip UserBehavior.csv.zip          # 得到 UserBehavior.csv
wc -l UserBehavior.csv              # 期望 100,150,808 行（含表头），约 3.42GB

# 3.3 传上 HDFS（建议先切分，多 mapper 并行读）
hdfs dfs -mkdir -p /user/$(whoami)/taobao
hdfs dfs -put UserBehavior.csv /user/$(whoami)/taobao/UserBehavior.csv
hdfs dfs -ls -h /user/$(whoami)/taobao/
```

**CSV 格式**（无表头也可，代码按位置解析，遇到无法解析的行直接跳过）：
```
user_id,item_id,category_id,behavior_type,timestamp
1,2268318,2520377,pv,1511544070
```

> 时间戳单位是**秒**，数据集覆盖 2017-11-25 ~ 2017-12-03。代码不按日期过滤，需要的话在 MR1
> 之前用 `awk`/Hive 先筛。

---

## 4. 跑起来（最重要的一步）

```bash
OUT=/user/$(whoami)/taobao/pr
hadoop fs -rm -r -f $OUT

hadoop jar target/taobao-pagerank-1.0.jar cs5488.pr.PageRankDriver \
    /user/$(whoami)/taobao/UserBehavior.csv \
    $OUT \
    330 1800 0.85 80 1e-6 30 pv,buy,cart,fav
```

参数顺序（都是可选，不传就用默认值）：

| # | 参数 | 默认 | 说明 |
|---|---|---|---|
| 1 | `inputCsv` | — | HDFS 上的 CSV（可以是目录，也可以是单文件） |
| 2 | `outBaseDir` | — | 输出根目录，脚本会自己删同名旧目录 |
| 3 | `sampleMod` | `330` | 按 `uid % sampleMod == 0` 采样用户；`1` = 全量 |
| 4 | `sessionGap` | `1800` | 会话切分阈值（秒），相邻行为间隔超过它就开新会话 |
| 5 | `damping` | `0.85` | 阻尼系数 d |
| 6 | `maxIter` | `50` | 最大迭代轮数（上限） |
| 7 | `tol` | `1e-6` | 收敛阈值：`mean_i |PR_i^(k) - PR_i^(k-1)| < tol` 时停 |
| 8 | `topK` | `30` | 输出 Top-K |
| 9 | `behaviors` | `pv,buy,cart,fav` | 参与建图的 behavior_type，逗号分隔 |

**全量（不采样）**：把第 3 个参数换成 `1`。注意 MR1 的输出会大 330 倍（边数约 5.8 亿量级），
务必先按 §5.4 把 reducer 数量调上去。

**只跑全量的"部分日期"**：先用 Hive/awk 过滤出目标时段再当 input 传进来，比在 Java 里加过滤灵活。

---

## 5. 期望输出 & 怎么判断成功

### 5.1 控制台应该看到

```
[MR1] cooccurrence edges -> /user/xxx/taobao/pr/edges
[MR2] build adjacency -> /user/xxx/taobao/pr/graph_0
[MR2] N = 168000
[MR3] iter=1  L1_delta=3.1xxxxxxe-01  (tol=1e-06)
[MR3] iter=2  L1_delta=...
...
[MR4] top-30 -> /user/xxx/taobao/pr/topk
=============================================
DONE  iters=46  N=168000  final_L1=8.8xxxxxe-07  wall=XXXX.Xs
Top-K: hdfs dfs -cat /user/xxx/taobao/pr/topk/part-r-00000
```

**两个黄金指标**：

- `N = 168000`（图节点数）
- `iters ≈ 46`（`tol=1e-6` 时的收敛轮数，实测曲线：`1e-4`→23 轮、`1e-5`→34 轮、`1e-6`→46 轮）

用 `sampleMod=330`、`sessionGap=1800`、`maxSessionItems=50` 时如果 `N` 不是 **168000**，
说明数据或参数和参考实现不一致（先查 CSV 是不是完整 100,150,807 行）。

### 5.2 关键计数器

```bash
# MR2 节点数（就是上面打印的 N）
hadoop fs -cat $OUT/graph_0/_SUCCESS 2>/dev/null; hdfs dfs -du -h $OUT/graph_0

# 各阶段 Counter
yarn logs -applicationId <appId> 2>/dev/null | grep -E 'NODE_COUNT|DELTA_MICRO'
```

- `GraphBuildJob.Counters.NODE_COUNT` → 节点数，期望 **168000**
- `PageRankJob.Counters.MASS_MICRO` → Σmass×1e6，期望 ≈ `168000 × 1e6 = 1.68e11`（守恒检查，
  每轮都应基本不变；掉太多说明 dangling 节点质量没正确再分配）
- `PageRankJob.Counters.DELTA_MICRO` → `mean|ΔPR| × 1e9`

### 5.3 结果长什么样

```bash
hdfs dfs -cat $OUT/topk/part-r-00000 | head -10
```

```
1	812879	2.908200e-04	12345
2	3845720	...
...
```

列含义：`rank \t item_id \t pagerank \t strength`（strength = 加权度数，即该商品在会话中和
其它商品共现的总权重，用来解释它为什么是枢纽）。

**Top-1 期望是 `812879`**（与本地 Python 版、以及历史全量运行一致）。

### 5.4 调参 / 排错

| 现象 | 原因 | 处理 |
|---|---|---|
| MR1 卡住或 OOM | reducer 内存不够 | 加 `-Dmapreduce.reduce.memory.mb=4096`（或 8192） |
| MR1 太慢 | 默认只起 1 个 reducer | 加 `-Dmapreduce.job.reduces=16`。`UidPartitioner` 保证同一用户进同一 reducer，加 reducer 不影响正确性 |
| `iters` 一直打到 `maxIter` | `tol` 太严或图太大 | 放宽到 `1e-5`，或加大 `maxIter` |
| `N` 明显偏小 | CSV 被截断 / 换行格式异常 | `wc -l` 核对行数；检查是否带了 `\r` |
| `ClassNotFoundException: cs5488.pr.*` | 用了 `-libjars` 或 jar 名错 | 用 §4 的完整类名 `cs5488.pr.PageRankDriver` |
| 迭代很慢 | 每轮都是一个 job，启动开销大 | 正常；80 轮 ≈ 80 个 job。想快就把 `maxIter` 设 20 先看趋势 |
| `final_L1` 是 `NaN` | damping 或 N 传错 | 用默认值，别手填 nodeCount |

### 5.5 时间预期（参考，来自本机伪分布式/Python 缩放估算）

| 阶段 | 330 采样（N=168k） | 全量（N 以 MR2 打印为准） |
|---|---|---|
| MR1 | 3~8 min | 40~90 min |
| MR2 | < 1 min | 5~10 min |
| MR3 | 46 轮 × 20~40 s ≈ 15~30 min | 46 轮 × 2~5 min ≈ 1.5~4 h |
| MR4 | < 1 min | 2~5 min |
| **合计** | **20~40 min** | **2~6 h** |

> 全量节点数 `N` 被 `maxSessionItems=50` 截断后**不是**全部商品，量级在千万到亿之间，
> 实际以 MR2 打印的 `N` 为准。

---

## 6. 单独跑某个阶段（调试用）

每个 `*Job` 都有 `main`，可以单独提交：

```bash
# MR1 单独跑
hadoop jar target/taobao-pagerank-1.0.jar cs5488.pr.CooccurrenceJob \
    <inputCsv> <edgesOut> [sampleMod=330] [sessionGap=1800] [maxItems=50] [behaviors=pv,buy,cart,fav]

# MR2
hadoop jar target/taobao-pagerank-1.0.jar cs5488.pr.GraphBuildJob <edgesIn> <graph0Out>

# MR3 单轮（nodeCount 必须传，否则归一化不对）
hadoop jar target/taobao-pagerank-1.0.jar cs5488.pr.PageRankJob \
    <graphIn> <graphOut> [nodeCount=168000] [damping=0.85]

# MR4
hadoop jar target/taobao-pagerank-1.0.jar cs5488.pr.TopKJob <graphIn> <topkOut> [nodeCount] [topK]
```

**`PageRankJob` 的 `nodeCount` 很重要**：节点初始 `mass = 1`，`PR_i = mass_i / N`。传错 N
不会报错但数值全错。

---

## 7. 用现成的脚本

```bash
# 进到 mapreduce/ 目录
chmod +x run_mr.sh
./run_mr.sh                      # 默认: 330 采样, 全量输出到 ~/taobao/pr
INPUT=/user/hadoop/taobao/UserBehavior.csv SAMPLE_MOD=1 ./run_mr.sh   # 全量
```

脚本就是 §4 那条命令 + 事前清理 + 事后打印 Top-10，见 `run_mr.sh` 头部注释。

---

## 8. 和本地 Python 版的一致性

两套实现必须**同一套规则**，否则汇报时对不上：

| 参数 | Python (`../local/pagerank_local.py`) | MapReduce |
|---|---|---|
| 采样 | `--sample-mod 330`（`uid % 330 == 0`） | 第 3 个参数 `330` |
| 会话切分 | `--session-gap 1800` | 第 4 个参数 `1800` |
| 会话内截断 | `--max-session-items 50` | **写死 50**，与 Python 默认一致 |
| 行为类型 | `--behaviors pv,buy,cart,fav` | 第 9 个参数 |
| 阻尼 | `--damping 0.85` | 第 5 个参数 `0.85` |
| 收敛判定 | `--tol`（`mean|Δ|`） | `tol`，mean 形式，同口径 |
| 边权重 | 同一会话内共现次数 | 同 |

**验收方法**：跑完对比 `N`（应 168000）和 Top-1（应 `812879`）。两者对上就说明实现一致。

---

## 9. 如果你手头没有 Hadoop 集群

见 `../pig/`（Pig Latin 版，逻辑等价，适合快速试跑）和 `../local/`（单机 Python 版，
不需要集群就能验证算法正确性）。**这三个版本我都跑过/编译过，但我没有真实集群的环境，
所以 MR 版的"集群实跑"需要你确认**——下面是我的验证状态：

| 项 | 状态 |
|---|---|
| `mvn package` 编译通过、jar 可产出 | ✅ 已验证 |
| 四阶段逻辑与 Python 版逐参数对齐 | ✅ 已核对 |
| 在真实 Hadoop/YARN 上端到端跑通 | ⚠️ **未验证**（本机 Windows 无 `winutils`，无法起本地 MR） |

所以第一次跑请**先用 `sampleMod=330`**（约 3000 用户、168k 节点），几十分钟内能出结果，
确认 `N=168000` 后再上全量。
