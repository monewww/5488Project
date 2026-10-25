#!/usr/bin/env bash
# run_pagerank.sh — Pig 版 PageRank 全流程驱动 (伪分布式: pig -x mapreduce)
# 用法: ./run_pagerank.sh <UserBehavior路径> <输出基目录> [迭代数=40] [采样mod=330]
set -e
INPUT=${1:-/user/hadoop/taobao/UserBehavior.csv}
BASE=${2:-/user/hadoop/taobao/pr_pig}
ITERS=${3:-40}
MOD=${4:-330}
D=0.85

echo "[pig] 01 cooccurrence ..."
hdfs dfs -rm -r -f -skipTrash ${BASE}/edges || true
pig -x mapreduce -param input=$INPUT -param out=${BASE}/edges \
    -param mod=$MOD -param gap=1800 01_cooccurrence.pig

echo "[pig] 02 graph build ..."
hdfs dfs -rm -r -f -skipTrash ${BASE}/graph_0 || true
pig -x mapreduce -param edges=${BASE}/edges -param out=${BASE}/graph_0 \
    02_graph_build.pig

for i in $(seq 1 $ITERS); do
  echo "[pig] 03 iteration $i ..."
  hdfs dfs -rm -r -f -skipTrash ${BASE}/graph_$i || true
  pig -x mapreduce -param in=${BASE}/graph_$((i-1)) -param out=${BASE}/graph_$i \
      -param d=$D 03_pagerank_iter.pig | tee /tmp/pr_iter_$i.log
  # 从日志抓 L1_DELTA 判收敛 (< 1e-6 提前停)
  L1=$(grep -oP 'L1_DELTA,\K[0-9.eE+-]+' /tmp/pr_iter_$i.log | head -1 || echo 1)
  echo "  iter $i L1=$L1"
  awk -v l1="$L1" 'BEGIN{exit !(l1<1e-6)}' && { echo "  converged at iter $i"; break; }
done

echo "[pig] 04 topk ..."
hdfs dfs -rm -r -f -skipTrash ${BASE}/topk || true
pig -x mapreduce -param in=${BASE}/graph_$i -param out=${BASE}/topk \
    -param k=30 04_topk.pig
echo "DONE -> ${BASE}/topk"