-- 03_pagerank_iter.pig — 单轮 PageRank 迭代 (Lin & Dyer 质量传递, Pig 版)
-- 参数: -param in=<graph_k-1> -param out=<graph_k> -param d=0.85
-- 由 run_pagerank.sh/.bat 循环调用; 每轮后比较 mass 变化判收敛
nodes = LOAD '$in' USING PigStorage('\t')
        AS (id:long, mass:double, strength:double, adj:chararray);

-- 展开邻接表: "nbr:w" -> (nbr, w)
tok = FOREACH nodes GENERATE id, mass, strength,
      FLATTEN(TOKENIZE(adj, ' ')) AS t;

parsed = FOREACH tok GENERATE id, mass, strength,
         (long)(STRSPLIT(t, ':')#$0)   AS nbr,
         (double)(STRSPLIT(t, ':')#$1) AS w;

-- 分发质量: 邻居 j 得到 mass * d * w / S_i
sent = FOREACH parsed GENERATE nbr AS id, mass * $d * w / strength AS contrib;

agg = GROUP sent BY id;

inc = FOREACH agg GENERATE group AS id, SUM(sent.contrib) AS incoming;

-- join 回节点表: 保留邻接表与 strength 供下一轮使用
j = JOIN inc BY id, nodes BY id;

iter = FOREACH j GENERATE nodes::id AS id,
       (1.0 - $d) + inc::incoming AS mass,     -- new = (1-d) + 入质量
       nodes::strength AS strength,
       nodes::adj AS adj;

STORE iter INTO '$out' USING PigStorage('\t');

-- 收敛诊断: 本轮 L1 delta (可 grep 日志判断是否 < tol)
prev = FOREACH nodes GENERATE id AS pid, mass AS pmass;
cmp = JOIN iter BY id, prev BY pid;
delta = FOREACH cmp GENERATE (iter::mass > prev::pmass ?
          iter::mass - prev::pmass : prev::pmass - iter::mass) AS d;
gsum = GROUP delta ALL;
l1 = FOREACH gsum GENERATE 'L1_DELTA' AS tag, SUM(delta.d) AS l1;
DUMP l1;