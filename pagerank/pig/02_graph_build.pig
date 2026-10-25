-- 02_graph_build.pig — 边 -> 节点记录 (邻接表 + 边权和 strength + 初始质量)
-- 参数: -param edges=<01输出> -param out=<graph_0路径>
-- 输出格式 (4列, tab分隔): id  mass  strength  adj   (adj="nbr:w nbr:w ...")
edges = LOAD '$edges' USING PigStorage('\t') AS (i1:long, i2:long, w:long);

s1 = FOREACH edges GENERATE i1 AS id, i2 AS nbr, w;
s2 = FOREACH edges GENERATE i2 AS id, i1 AS nbr, w;   -- 无向图双向展开
all = UNION s1, s2;

g = GROUP all BY id;

nodes = FOREACH g {
    adjb = FOREACH all GENERATE CONCAT((chararray)nbr, CONCAT(':', (chararray)w));
    GENERATE group AS id,
             1.0 AS mass,                       -- 初始质量 1.0, 总和=N
             (double)SUM(all.w) AS strength,    -- 出边权和 S_i (迭代时直接查表)
             BagToString(adjb, ' ') AS adj;
};

STORE nodes INTO '$out' USING PigStorage('\t');