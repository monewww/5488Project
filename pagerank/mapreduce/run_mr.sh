#!/usr/bin/env bash
# =============================================================================
# 全量数据版 · 一键运行 Hadoop MapReduce PageRank
# 用法:
#   ./run_mr.sh                                              # 默认 330 采样
#   INPUT=/user/hadoop/taobao/UserBehavior.csv SAMPLE_MOD=1 ./run_mr.sh   # 全量
# 环境变量 (都有默认值):
#   INPUT        HDFS 上的 UserBehavior.csv 路径   (必填, 无默认)
#   OUT          输出根目录                        (默认 /user/$USER/taobao/pr)
#   SAMPLE_MOD   采样模数, 1=全量                  (默认 330)
#   SESSION_GAP  会话间隔秒                        (默认 1800)
#   DAMPING      阻尼系数                          (默认 0.85)
#   MAX_ITER     最大迭代轮数                      (默认 80)
#   TOL          收敛阈值                          (默认 1e-6)
#   TOPK         Top-K                             (默认 30)
#   BEHAVIORS    参与建图的行为                    (默认 pv,buy,cart,fav)
#   REDUCES      MR1 reducer 数 (0=用 Hadoop 默认) (默认 0)
#   JAR          jar 路径                          (默认 优先 dist/, 回退 target/)
# =============================================================================
set -euo pipefail

INPUT="${INPUT:?请先设置 INPUT，例如 INPUT=/user/\$USER/taobao/UserBehavior.csv}"
OUT="${OUT:-/user/${USER:-hadoop}/taobao/pr}"
SAMPLE_MOD="${SAMPLE_MOD:-330}"
SESSION_GAP="${SESSION_GAP:-1800}"
DAMPING="${DAMPING:-0.85}"
MAX_ITER="${MAX_ITER:-80}"
TOL="${TOL:-1e-6}"
TOPK="${TOPK:-30}"
BEHAVIORS="${BEHAVIORS:-pv,buy,cart,fav}"
REDUCES="${REDUCES:-0}"

cd "$(dirname "$0")"
if [ -z "${JAR:-}" ]; then
    if   [ -f dist/taobao-pagerank-1.0.jar ];   then JAR=dist/taobao-pagerank-1.0.jar
    elif [ -f target/taobao-pagerank-1.0.jar ]; then JAR=target/taobao-pagerank-1.0.jar
    else echo "找不到 jar，请先 mvn clean package -DskipTests"; exit 1
    fi
fi

echo "=== 检查输入 ==="
hadoop fs -test -e "$INPUT" || { echo "找不到 HDFS 输入: $INPUT"; exit 1; }
hadoop fs -ls -h "$INPUT" | tail -n +2

echo "=== 检查 jar ==="
[ -f "$JAR" ] || { echo "找不到 jar: $JAR，请先执行 mvn clean package -DskipTests"; exit 1; }
ls -lh "$JAR"

echo "=== 清理旧输出 ==="
hadoop fs -rm -r -f "$OUT" || true

echo "=== 开始跑 (采样模数=$SAMPLE_MOD) ==="
EXTRA=()
[ "$REDUCES" != "0" ] && EXTRA+=(-Dmapreduce.job.reduces="$REDUCES")

hadoop jar "$JAR" cs5488.pr.PageRankDriver \
    "$INPUT" "$OUT" \
    "$SAMPLE_MOD" "$SESSION_GAP" "$DAMPING" \
    "$MAX_ITER" "$TOL" "$TOPK" "$BEHAVIORS" \
    "${EXTRA[@]:-}"

echo
echo "=== Top-10 结果 ==="
hadoop fs -cat "$OUT/topk/part-r-00000" | head -10

echo
echo "结果目录: $OUT"
echo "取回本地: hadoop fs -get $OUT/topk/part-r-00000 ./topk_${SAMPLE_MOD}.tsv"
