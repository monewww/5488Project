#!/usr/bin/env bash
# =============================================================================
# 小批量版 · 本地单机验证 PageRank (Python, 不需要 Hadoop)
# 用法:
#   ./run_local.sh                        # 跑仓库内置的 1.7MB 样本
#   ./run_local.sh /path/UserBehavior.csv # 跑你自己给的文件 (会按 --sample-mod 采样)
# 说明: 样本文件本身已经按 uid%2200==0 抽过, 所以这里用 --sample-mod 1 (不再采样)。
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

PY="${PYTHON:-python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python

SAMPLE="data/sample/UserBehavior_sample.csv"
INPUT="${1:-$SAMPLE}"
if [ "$INPUT" = "$SAMPLE" ]; then
    MOD=1        # 样本已抽样, 不再二次采样
    ITER=10
else
    MOD=330      # 全量 CSV -> 按同样规则抽样, 结果与 MR 版可对齐
    ITER=50
fi

echo "=== 输入: $INPUT  (sample-mod=$MOD, max-iter=$ITER) ==="
"$PY" local/pagerank_local.py \
    --input "$INPUT" \
    --sample-mod "$MOD" \
    --session-gap 1800 \
    --max-session-items 50 \
    --damping 0.85 \
    --max-iter "$ITER" \
    --tol 1e-6 \
    --behaviors pv,buy,cart,fav \
    --top 30 \
    --outdir results/local_run

echo
echo "=== 结果 ==="
echo "--- stats.txt ---";        cat results/local_run/stats.txt
echo "--- top_items.csv ---";    head -11 results/local_run/top_items.csv
echo "--- convergence.csv ---";  cat results/local_run/convergence.csv
