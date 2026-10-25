-- 04_topk.pig — 最终排名: PR = mass / N, 输出 Top-K
-- 参数: -param in=<graph_final> -param out=<topk路径> -param k=30
nodes = LOAD '$in' USING PigStorage('\t')
        AS (id:long, mass:double, strength:double, adj:chararray);

cnt = GROUP nodes ALL;
tot = FOREACH cnt GENERATE COUNT(nodes) AS n;      -- 单行小表

cx = CROSS nodes, tot;                              -- 广播 N (1 行, 代价可忽略)

pr = FOREACH cx GENERATE nodes::id AS id,
     nodes::mass / tot::n AS pagerank,
     nodes::strength AS strength;

srt = ORDER pr BY pagerank DESC PARALLEL 1;

topk = LIMIT srt $k;

STORE topk INTO '$out' USING PigStorage('\t');
DUMP topk;