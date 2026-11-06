#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
compare_mr_vs_expected.py — 将 MapReduce 实际输出与 Python 参考期望结果对比

用途:
  本地比对 Hadoop MapReduce 某阶段的输出 (actual) 与 validate_session_edges.py
  等参考实现生成的期望结果 (expected), 帮助验证 MR1 / MR2 / 节点集合 /
  PageRank / Top-K 各阶段正确性。

  本工具只读取本地已有文件: 不连接 HDFS, 不启动 Hadoop, 不修改任何现有文件。

支持阶段 (--stage):
  mr1       用户级共现边      expected: expected_edges_user.tsv (i1<TAB>i2<TAB>w)
                             actual:   MR1 输出目录 (i1<TAB>i2<TAB>w, 每用户一行)
  mr2       全局共现边        expected: expected_edges_global.tsv (i1<TAB>i2<TAB>w)
                             actual:   MR2(GraphBuildJob) 输出目录 (id<TAB>mass<TAB>adj)
                                       脚本从邻接表还原无向全局边再比较
  nodes     节点集合          expected: expected_nodes.tsv (item<TAB>strength<TAB>nbrs)
                             actual:   图文件, 取每行第一列为节点 id
  pagerank  PageRank 数值     expected/actual: 2 列以上 "id<TAB>score[<TAB>...]"
                             actual 若是 graph_k (id<TAB>mass<TAB>adj), 可用
                             --pagerank-node-count N 将 mass 转为 PR=mass/N
  topk      Top-K             expected/actual: "rank<TAB>id<TAB>pr[<TAB>strength]"
                             (也兼容无 rank 的 "id<TAB>pr[<TAB>strength]")

状态:
  EXACT_MATCH                 完全一致
  STRUCTURE_MATCH_NUMERIC_DIFF 结构一致但数值存在差异 (如 PageRank 超容差)
  MISSING_OR_EXTRA            存在缺失或多余数据
  CANNOT_COMPARE              文件缺失/空/格式不匹配/阶段不对应

输出:
  默认 mapreduce_data_tools/out/, 报告文件名含阶段与时间戳, 拒绝覆盖已有文件。

用法示例:
  python3 mapreduce_data_tools/compare_mr_vs_expected.py --stage mr2 \
      --expected mapreduce_data_tools/out/expected/expected_edges_global.tsv \
      --actual   /path/to/hdfs_pr/graph_0

  python3 mapreduce_data_tools/compare_mr_vs_expected.py --stage topk \
      --expected /path/to/expected_topk.tsv \
      --actual   /path/to/hdfs_pr/topk
"""

import argparse
import os
import sys
import time
import glob as globmod
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_DIR = os.path.join(HERE, "out")

STAGES = ("mr1", "mr2", "nodes", "pagerank", "topk")


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------
def read_lines(paths, label):
    """返回 (make_gen, n_files)。make_gen() 每次调用产生一个新的逐行迭代器 (可重复打开,
    流式, 不一次性读入内存)。路径不存在返回 (None, 0)。
    """
    files = []
    for p in paths:
        if os.path.isdir(p):
            # MR 输出目录: 读取 part-* 文件 (跳过 _SUCCESS/_logs)
            files.extend(sorted(
                f for f in globmod.glob(os.path.join(p, "part-*"))
                if os.path.isfile(f)))
        elif os.path.isfile(p):
            files.append(p)
        else:
            sys.stderr.write("[错误] {} 路径不存在: {}\n".format(label, p))
            return None, 0
    if not files:
        return None, 0

    def make_gen():
        for fp in files:
            with open(fp, "r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    yield line.rstrip("\n").rstrip("\r")
    return make_gen, len(files)


def parse_edge_tsv(line):
    """解析 "i1\t i2\t w" -> (a, b, int(w)) 或 None (跳过空行/坏行)。"""
    if line == "":
        return None
    parts = line.split("\t")
    if len(parts) < 3:
        return None
    a, b = parts[0].strip(), parts[1].strip()
    try:
        w = int(parts[2].strip())
    except ValueError:
        return None
    return a, b, w


def parse_pagerank_line(line):
    """解析 "id<TAB>score[rest]" -> (id, float(score)) 或 (id, score, rest_fields)。"""
    if line == "":
        return None
    parts = line.split("\t")
    if len(parts) < 2:
        return None
    nid = parts[0].strip()
    try:
        score = float(parts[1].strip())
    except ValueError:
        return None
    return nid, score


def count_lines(path_wrap):
    """在需要时统计行数 (用于空文件判断)。这里仅在比较函数内部按需使用。"""
    return None


# ---------------------------------------------------------------------------
# 边集合读取 (多重集, 用于 MR1 用户级)
# ---------------------------------------------------------------------------
def load_edge_multiset(gen):
    from collections import Counter
    c = Counter()
    for line in gen:
        e = parse_edge_tsv(line)
        if e is None:
            continue
        a, b = e[0], e[1]
        if a > b:
            a, b = b, a
        c[(a, b, e[2])] += 1
    return c


# ---------------------------------------------------------------------------
# 比较: 边 (mr1 多重集 / mr2 全局集合)
# ---------------------------------------------------------------------------
def compare_edges(expected_gen, actual_gen, top_n, multiset):
    """返回 (status, stats, diffs)。

    multiset=True: MR1 用户级, 同对多行代表不同用户, 按 (a,b,w) 多重集比较。
    multiset=False: MR2 全局, 按 (a,b)->w 单值比较。
    """
    if multiset:
        exp = load_edge_multiset(expected_gen)
        act = load_edge_multiset(actual_gen)
        exp_total = sum(exp.values())
        act_total = sum(act.values())
        keys = set(exp) | set(act)
        matched = 0
        missing = 0
        extra = 0
        weight_mismatch = 0
        diffs = []
        # 多重集: 每个 (a,b,w) 的计数应一致
        for k in keys:
            ce, ca = exp.get(k, 0), act.get(k, 0)
            if ce == ca and ce > 0:
                matched += ce
            else:
                if ce > ca:
                    missing += ce - ca
                else:
                    extra += ca - ce
                if ce and ca:
                    weight_mismatch += 1
                if len(diffs) < top_n:
                    diffs.append("({}, {}): expected x{}, actual x{} (w={})".format(
                        k[0], k[1], ce, ca, k[2]))
        stats = dict(exp_total=exp_total, act_total=act_total,
                     matched=matched, missing=missing, extra=extra,
                     weight_mismatch=weight_mismatch, exp_keys=len(exp), act_keys=len(act))
        if missing == 0 and extra == 0 and weight_mismatch == 0:
            return "EXACT_MATCH", stats, []
        return "MISSING_OR_EXTRA", stats, diffs

    # 全局: (a,b) -> w
    exp = {}
    for line in expected_gen:
        e = parse_edge_tsv(line)
        if e is None:
            continue
        a, b = e[0], e[1]
        if a > b:
            a, b = b, a
        exp[(a, b)] = e[2]
    act = {}
    for line in actual_gen:
        e = parse_edge_tsv(line)
        if e is None:
            continue
        a, b = e[0], e[1]
        if a > b:
            a, b = b, a
        act[(a, b)] = e[2]
    exp_total = len(exp)
    act_total = len(act)
    keys = set(exp) | set(act)
    matched = 0
    missing = 0
    extra = 0
    weight_mismatch = 0
    diffs = []
    for k in sorted(keys):
        in_e = k in exp
        in_a = k in act
        if in_e and in_a:
            if exp[k] == act[k]:
                matched += 1
            else:
                weight_mismatch += 1
                if len(diffs) < top_n:
                    diffs.append("({}, {}): weight expected {}, actual {}".format(
                        k[0], k[1], exp[k], act[k]))
        elif in_e:
            missing += 1
            if len(diffs) < top_n:
                diffs.append("({}, {}): missing (expected w={})".format(k[0], k[1], exp[k]))
        else:
            extra += 1
            if len(diffs) < top_n:
                diffs.append("({}, {}): extra (actual w={})".format(k[0], k[1], act[k]))
    stats = dict(exp_total=exp_total, act_total=act_total, matched=matched,
                 missing=missing, extra=extra, weight_mismatch=weight_mismatch)
    if missing == 0 and extra == 0 and weight_mismatch == 0:
        return "EXACT_MATCH", stats, []
    if missing == 0 and extra == 0:
        return "STRUCTURE_MATCH_NUMERIC_DIFF", stats, diffs
    return "MISSING_OR_EXTRA", stats, diffs


def parse_graph_adjacency(line):
    """解析 GraphBuildJob 输出 "id<TAB>mass<TAB>adj" -> (id, mass, [(nbr,w),...])。

    adj 形如 "nbr1:w1 nbr2:w2 ..."。若第 2 列不可解析为 float 视为坏行。"""
    if line == "":
        return None
    parts = line.split("\t")
    if len(parts) < 3:
        return None
    nid = parts[0].strip()
    try:
        mass = float(parts[1].strip())
    except ValueError:
        return None
    adj = []
    for tok in parts[2].split(" "):
        tok = tok.strip()
        if not tok:
            continue
        c = tok.rfind(":")
        if c <= 0:
            continue
        nbr = tok[:c]
        try:
            w = int(tok[c + 1:])
        except ValueError:
            continue
        adj.append((nbr, w))
    return nid, mass, adj


def graph_to_edges(gen):
    """从 MR2 邻接表还原无向全局边: (min,max)->w, 权重取一致值。"""
    edges = {}
    inconsistent = 0
    for line in gen:
        rec = parse_graph_adjacency(line)
        if rec is None:
            continue
        nid, _mass, adj = rec
        for nbr, w in adj:
            a, b = (nid, nbr) if nid <= nbr else (nbr, nid)
            key = (a, b)
            if key in edges and edges[key] != w:
                inconsistent += 1
            edges[key] = w
    return edges, inconsistent


# ---------------------------------------------------------------------------
# 比较: 节点集合
# ---------------------------------------------------------------------------
def load_node_ids_expected(gen):
    ids = set()
    for line in gen:
        if line == "":
            continue
        ids.add(line.split("\t")[0].strip())
    return ids


def load_node_ids_graph(gen):
    """从图文件 (id<TAB>... 邻接表或节点表) 取每行第一列为节点 id。"""
    ids = set()
    for line in gen:
        if line == "":
            continue
        ids.add(line.split("\t")[0].strip())
    return ids


def compare_node_sets(exp_ids, act_ids, top_n):
    missing = exp_ids - act_ids
    extra = act_ids - exp_ids
    matched = len(exp_ids & act_ids)
    stats = dict(exp_total=len(exp_ids), act_total=len(act_ids),
                 matched=matched, missing=len(missing), extra=len(extra))
    if not missing and not extra:
        return "EXACT_MATCH", stats, []
    diffs = []
    for x in sorted(missing)[:top_n]:
        diffs.append("缺失节点: {}".format(x))
    for x in sorted(extra)[:top_n]:
        diffs.append("多余节点: {}".format(x))
    return "MISSING_OR_EXTRA", stats, diffs


# ---------------------------------------------------------------------------
# 比较: PageRank 数值
# ---------------------------------------------------------------------------
def load_pagerank(gen, node_count=None):
    scores = {}
    bad = 0
    for line in gen:
        rec = parse_pagerank_line(line)
        if rec is None:
            if line.strip():
                bad += 1
            continue
        nid, score = rec
        if node_count:
            score = score / node_count
        scores[nid] = score
    return scores, bad


def compare_pagerank(exp, act, tolerance, top_n):
    missing = set(exp) - set(act)
    extra = set(act) - set(exp)
    common = set(exp) & set(act)
    matched = len(common)
    max_err = 0.0
    sum_err = 0.0
    over_tol = 0
    diffs = []
    for nid in common:
        err = abs(exp[nid] - act[nid])
        sum_err += err
        if err > max_err:
            max_err = err
        if err > tolerance:
            over_tol += 1
            if len(diffs) < top_n:
                diffs.append("{}: expected {:.10g}, actual {:.10g}, |diff|={:.3e}".format(
                    nid, exp[nid], act[nid], err))
    avg_err = (sum_err / matched) if matched else 0.0
    stats = dict(exp_total=len(exp), act_total=len(act), matched=matched,
                 missing=len(missing), extra=len(extra),
                 max_abs_err=max_err, avg_abs_err=avg_err, over_tolerance=over_tol,
                 tolerance=tolerance)
    if missing or extra:
        for x in sorted(missing)[:top_n]:
            diffs.append("缺失节点: {}".format(x))
        for x in sorted(extra)[:top_n]:
            diffs.append("多余节点: {}".format(x))
        return "MISSING_OR_EXTRA", stats, diffs
    if over_tol == 0:
        return "EXACT_MATCH", stats, []
    return "STRUCTURE_MATCH_NUMERIC_DIFF", stats, diffs


# ---------------------------------------------------------------------------
# 比较: Top-K
# ---------------------------------------------------------------------------
def load_topk(gen):
    """返回 (ranked_list, scores)。兼容有/无 rank 列。"""
    ranked = []
    scores = {}
    for line in gen:
        if line == "":
            continue
        parts = line.split("\t")
        if len(parts) >= 4:
            # rank, id, pr, [strength]
            nid = parts[1].strip()
            try:
                pr = float(parts[2].strip())
            except ValueError:
                continue
            scores[nid] = pr
            ranked.append(nid)
        elif len(parts) == 3:
            # 可能是 id, pr, strength 或 rank, id, pr; 以第 2 列能否解析 float 判断
            try:
                pr = float(parts[1].strip())
                nid = parts[0].strip()
                scores[nid] = pr
                ranked.append(nid)
            except ValueError:
                # rank, id, pr
                nid = parts[1].strip()
                try:
                    pr = float(parts[2].strip())
                except ValueError:
                    continue
                scores[nid] = pr
                ranked.append(nid)
        elif len(parts) == 2:
            nid = parts[0].strip()
            try:
                pr = float(parts[1].strip())
            except ValueError:
                continue
            scores[nid] = pr
            ranked.append(nid)
    return ranked, scores


def compare_topk(exp_ranked, exp_scores, act_ranked, act_scores, tolerance, top_n):
    exp_set = set(exp_ranked)
    act_set = set(act_ranked)
    overlap = exp_set & act_set
    overlap_ratio = (len(overlap) / len(exp_set)) if exp_set else 0.0
    exp_rank = {nid: i + 1 for i, nid in enumerate(exp_ranked)}
    act_rank = {nid: i + 1 for i, nid in enumerate(act_ranked)}
    rank_changes = []
    score_diffs = []
    for nid in overlap:
        if exp_rank[nid] != act_rank[nid]:
            rank_changes.append((nid, exp_rank[nid], act_rank[nid]))
        if nid in exp_scores and nid in act_scores:
            d = abs(exp_scores[nid] - act_scores[nid])
            if d > tolerance:
                score_diffs.append((nid, exp_scores[nid], act_scores[nid], d))
    stats = dict(exp_total=len(exp_ranked), act_total=len(act_ranked),
                 overlap=len(overlap), overlap_ratio=overlap_ratio,
                 only_expected=len(exp_set - act_set), only_actual=len(act_set - exp_set),
                 rank_changes=len(rank_changes), score_diffs=len(score_diffs),
                 tolerance=tolerance)
    diffs = []
    if overlap_ratio == 1.0 and not rank_changes and not score_diffs:
        return "EXACT_MATCH", stats, []
    for nid, r1, r2 in sorted(rank_changes, key=lambda x: abs(x[1] - x[2]), reverse=True)[:top_n]:
        diffs.append("排名变化: {} expected rank {}, actual rank {}".format(nid, r1, r2))
    for nid, s1, s2, d in sorted(score_diffs, key=lambda x: x[3], reverse=True)[:top_n]:
        diffs.append("分数差异: {} expected {:.8g}, actual {:.8g}, |diff|={:.3e}".format(
            nid, s1, s2, d))
    for nid in sorted(exp_set - act_set)[:top_n]:
        diffs.append("缺失在 actual 的 Top-K: {}".format(nid))
    for nid in sorted(act_set - exp_set)[:top_n]:
        diffs.append("actual 多出的 Top-K: {}".format(nid))
    if overlap_ratio < 1.0:
        status = "MISSING_OR_EXTRA"
    else:
        status = "STRUCTURE_MATCH_NUMERIC_DIFF"
    return status, stats, diffs


# ---------------------------------------------------------------------------
# 报告输出
# ---------------------------------------------------------------------------
def write_report(out_dir, stage, report_lines):
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(out_dir, "compare_{}_{}.txt".format(stage, ts))
    # 拒绝覆盖 (尽管时间戳基本唯一, 仍严格检查)
    if os.path.exists(report_path):
        sys.stderr.write("[错误] 报告文件已存在, 禁止覆盖: {}\n".format(report_path))
        sys.exit(2)
    tmp_path = report_path + ".tmp.{}".format(os.getpid())
    try:
        with open(tmp_path, "x", encoding="utf-8") as fh:
            fh.write("\n".join(report_lines) + "\n")
        # 严格不覆盖发布
        try:
            os.link(tmp_path, report_path)
        except FileExistsError:
            sys.stderr.write("[错误] 报告文件在运行期间已出现, 拒绝覆盖: {}\n".format(report_path))
            sys.exit(2)
    finally:
        try:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass
    return report_path


def main():
    ap = argparse.ArgumentParser(
        description="比较 MapReduce 实际输出与参考期望结果 (只读本地文件, 不运行 Hadoop)")
    ap.add_argument("--stage", required=True, choices=STAGES,
                    help="比较阶段: mr1/mr2/nodes/pagerank/topk")
    ap.add_argument("--expected", required=True,
                    help="期望文件路径 (文件或目录)")
    ap.add_argument("--actual", required=True,
                    help="实际输出路径 (文件或目录; 目录会读取其中的 part-* 文件)")
    ap.add_argument("--outdir", default=None,
                    help="报告输出目录 (默认 mapreduce_data_tools/out)")
    ap.add_argument("--top-n", type=int, default=20,
                    help="差异详情展示数量 (默认 20)")
    ap.add_argument("--tolerance", type=float, default=1e-6,
                    help="浮点误差容差 (默认 1e-6, 用于 pagerank/topk)")
    ap.add_argument("--pagerank-node-count", type=float, default=None,
                    help="可选: actual 为 graph_k(id,mass,adj) 时, 用 mass/N 转为 PR")
    args = ap.parse_args()

    out_dir = args.outdir if args.outdir else DEFAULT_OUT_DIR
    t_start = datetime.now()

    report = []
    report.append("=" * 72)
    report.append("MapReduce 实际输出 vs 参考期望 — 比较报告")
    report.append("=" * 72)
    report.append("比较时间: {}".format(t_start.strftime("%Y-%m-%d %H:%M:%S")))
    report.append("比较阶段: {}".format(args.stage))
    report.append("Expected: {}".format(os.path.abspath(args.expected)))
    report.append("Actual:   {}".format(os.path.abspath(args.actual)))
    report.append("容差 tolerance: {}".format(args.tolerance))
    report.append("Top-N 详情: {}".format(args.top_n))

    # 读取输入 (返回 make_gen 工厂, 每次调用重新流式打开)
    exp_make, exp_nf = read_lines([args.expected], "Expected")
    act_make, act_nf = read_lines([args.actual], "Actual")

    def finish(status, stats, diffs, rule, limitation=""):
        report.append("")
        report.append("解析规则: {}".format(rule))
        report.append("总体状态: {}".format(status))
        report.append("")
        report.append("--- 统计指标 ---")
        for k, v in stats.items():
            report.append("  {}: {}".format(k, v))
        report.append("")
        report.append("--- 前 {} 条差异详情 ---".format(args.top_n))
        if diffs:
            for d in diffs:
                report.append("  - {}".format(d))
        else:
            report.append("  无差异")
        report.append("")
        report.append("--- 可能差异原因 ---")
        report.append("  - 输入数据不同 (是否同一份原始 CSV / 采样参数 sampleMod 不同)")
        report.append("  - 相同 timestamp 的排序不确定性 (Java (uid,ts) 无 tie-breaker)")
        report.append("  - 行为白名单 / session gap / 商品上限参数不一致")
        report.append("  - 浮点求和顺序不同 (PageRank 末位差异属正常)")
        report.append("  - Top-K 并列分数导致排名不同 (非错误)")
        report.append("")
        report.append("--- 未验证内容与限制 ---")
        report.append("  - 本报告仅基于提供的本地文件做静态比对, 不验证 Hadoop 是否真正跑通")
        report.append("  - MR1 若在外部被全局聚合, 则不能当作用户级输出比较 (本工具按多重集处理)")
        if limitation:
            report.append("  - " + limitation)
        report.append("=" * 72)
        path = write_report(out_dir, args.stage, report)
        print("\n".join(report))
        print("[报告] {}".format(os.path.abspath(path)))

    # 阶段不对应 / 文件缺失 -> CANNOT_COMPARE
    if exp_make is None or act_make is None:
        missing_side = []
        if exp_make is None:
            missing_side.append("expected")
        if act_make is None:
            missing_side.append("actual")
        finish("CANNOT_COMPARE",
               {"reason": "缺失或不可读: {}".format(",".join(missing_side))},
               [],
               "文件缺失, 无法比较",
               "请确认路径正确且文件/目录存在")
        return

    # 空文件/无有效行 -> CANNOT_COMPARE
    # 各阶段/各侧的格式不同: mr2 的 actual 是邻接表, 其余按边/数值/文本判定
    def valid_expected(make_gen, stage):
        for line in make_gen():
            if stage in ("mr1", "mr2"):
                if parse_edge_tsv(line) is not None:
                    return True
            elif stage == "nodes":
                if line.strip():
                    return True
            elif stage == "pagerank":
                if parse_pagerank_line(line) is not None:
                    return True
            else:  # topk
                if line.strip():
                    return True
        return False

    def valid_actual(make_gen, stage):
        for line in make_gen():
            if stage == "mr1":
                if parse_edge_tsv(line) is not None:
                    return True
            elif stage == "mr2":
                # actual 为 GraphBuildJob 邻接表
                if parse_graph_adjacency(line) is not None:
                    return True
            elif stage == "nodes":
                if line.strip():
                    return True
            elif stage == "pagerank":
                if parse_pagerank_line(line) is not None:
                    return True
            else:  # topk
                if line.strip():
                    return True
        return False

    exp_ok = valid_expected(exp_make, args.stage)
    act_ok = valid_actual(act_make, args.stage)
    if not exp_ok or not act_ok:
        which = []
        if not exp_ok:
            which.append("expected")
        if not act_ok:
            which.append("actual")
        finish("CANNOT_COMPARE",
               {"reason": "空文件或无有效行: {}".format(",".join(which))},
               [],
               "空文件/无有效行, 无法比较 (或文件格式与阶段不匹配)",
               "请确认文件非空且格式与该阶段一致 (mr2 的 actual 应为邻接表 id<TAB>mass<TAB>adj)")
        return


    if args.stage == "mr1":
        rule = ("MR1 用户级边: expected_edges_user.tsv 与 actual MR1 edges/ 均按 "
                "'i1<TAB>i2<TAB>w' 解析, 按 (a,b,w) 多重集比较 (同对多行代表不同用户)。"
                "若 actual 实为全局聚合结果, 则本比较不成立。")
        status, stats, diffs = compare_edges(exp_make(), act_make(), args.top_n, multiset=True)

    elif args.stage == "mr2":
        # actual 是邻接表 (GraphBuildJob): 先还原全局边, 再与 expected 边表比较
        rule = ("MR2 全局边: expected_edges_global.tsv 按 'i1<TAB>i2<TAB>w' 解析; "
                "actual (GraphBuildJob) 按 'id<TAB>mass<TAB>adj' 解析并从邻接表还原"
                "无向全局边 (取 (min,max)->w), 再与 expected 比较。")
        exp_edges = {}
        for line in exp_make():
            e = parse_edge_tsv(line)
            if e is None:
                continue
            a, b = e[0], e[1]
            if a > b:
                a, b = b, a
            exp_edges[(a, b)] = e[2]
        act_edges, inconsistent = graph_to_edges(act_make())
        # 复用全局比较逻辑 (构造等价 gen)
        def gen_from(d):
            for (a, b) in sorted(d):
                yield "{}\t{}\t{}".format(a, b, d[(a, b)])
        status, stats, diffs = compare_edges(gen_from(exp_edges), gen_from(act_edges),
                                             args.top_n, multiset=False)
        stats["adj_inconsistent"] = inconsistent

    elif args.stage == "nodes":
        rule = ("节点集合: expected_nodes.tsv 取每行第一列为节点; "
                "actual 图文件取每行第一列为节点 id。仅比较集合, 不比 strength。")
        exp_ids = load_node_ids_expected(exp_make())
        act_ids = load_node_ids_graph(act_make())
        status, stats, diffs = compare_node_sets(exp_ids, act_ids, args.top_n)

    elif args.stage == "pagerank":
        rule = ("PageRank 数值: 两文件均按 'id<TAB>score[<TAB>...]' 解析第 2 列为分数; "
                "若给出 --pagerank-node-count, actual 的分数按 mass/N 归一。"
                "用 tolerance 判定浮点误差。")
        exp_scores, exp_bad = load_pagerank(exp_make(), node_count=None)
        act_scores, act_bad = load_pagerank(act_make(), node_count=args.pagerank_node_count)
        if not exp_scores or not act_scores:
            finish("CANNOT_COMPARE",
                   {"reason": "expected或actual无有效数值行",
                    "exp_bad": exp_bad, "act_bad": act_bad},
                   [], rule, "请确认格式为 2 列 id<TAB>score")
            return
        status, stats, diffs = compare_pagerank(exp_scores, act_scores, args.tolerance, args.top_n)

    else:  # topk
        rule = ("Top-K: 两文件按 'rank<TAB>id<TAB>pr[<TAB>strength]' 或 "
                "'id<TAB>pr[<TAB>strength]' 解析。比较 id 集合重合、重合率、排名变化与分数差。"
                "并列分数可能导致排名不同, 非错误。")
        exp_ranked, exp_scores = load_topk(exp_make())
        act_ranked, act_scores = load_topk(act_make())
        if not exp_ranked or not act_ranked:
            finish("CANNOT_COMPARE",
                   {"reason": "expected或actual无有效 Top-K 行"},
                   [], rule, "请确认 Top-K 文件格式")
            return
        status, stats, diffs = compare_topk(exp_ranked, exp_scores, act_ranked,
                                            act_scores, args.tolerance, args.top_n)

    finish(status, stats, diffs, rule)


if __name__ == "__main__":
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    main()
