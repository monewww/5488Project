-- 01_cooccurrence.pig — 会话共现建边 (Pig 版)
-- 参数: -param input=<UserBehavior路径> -param out=<edges路径>
--       -param mod=330  -param gap=1800
-- 会话化简化: 时间桶 (ts/gap) 代替精确 gap 切分 —— Pig 无 lag 算子,
-- 用固定窗口是 Pig/SQL 生态的标准做法 (与 MR 版精确会话的对比可写进报告)
raw = LOAD '$input' USING PigStorage(',')
      AS (uid:long, item:long, cat:long, beh:chararray, ts:long);

sampled = FILTER raw BY (uid % $mod) == 0;

beh_f = FILTER sampled BY beh == 'pv' OR beh == 'buy' OR beh == 'cart' OR beh == 'fav';

b = FOREACH beh_f GENERATE uid, item, (long)(ts / $gap) AS bucket;

g = GROUP b BY (uid, bucket);

pairs = FOREACH g {
    it     = DISTINCT b.item;
    capped = LIMIT it 50;                 -- 截断超大会话, 防 O(n^2)
    a      = FOREACH capped GENERATE item AS i;
    bb     = FOREACH capped GENERATE item AS j;
    x      = CROSS a, bb;                 -- 嵌套块内 CROSS 生成会话内两两组合
    y      = FILTER x BY i < j;           -- 去重: 只保留 i<j
    GENERATE FLATTEN(y) AS (i1, i2);
};

w1 = FOREACH pairs GENERATE i1, i2, 1L AS one;

gpair = GROUP w1 BY (i1, i2);

edges = FOREACH gpair GENERATE FLATTEN(group) AS (i1, i2), SUM(w1.one) AS w;

STORE edges INTO '$out' USING PigStorage('\t');