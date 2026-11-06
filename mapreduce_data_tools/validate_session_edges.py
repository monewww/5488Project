#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
validate_session_edges.py — MR1 (CooccurrenceJob) 的独立 Python 参考实现

目的:
  在不修改 Java MR1 的前提下, 独立复刻 CooccurrenceJob 的会话化与共现建边语义,
  产出参考的 "用户级共现边" 与节点统计, 供后续 compare_mr_vs_expected.py 比对
  真实 MR 输出, 用于验证现有 pipeline。

  注意: 本脚本只是"参考实现", 它的正确性不等于 Java 实现正确; 反之亦然。
  二者需通过实际输出比对来交叉验证。

严格复刻的 Java 规则 (对照 CooccurrenceJob.java):
  - 字段: 行 split(","), f.length < 5 跳过 (EventMapper:141)
  - 行为过滤: f[3].trim() 必须属于 behaviors 白名单 (EventMapper:144-147)
  - uid / ts: Integer.parseInt / Long.parseLong, 失败跳过 (EventMapper:150-155)
  - 采样: sampleMod > 1 时 floorMod(uid, sampleMod) != 0 跳过 (EventMapper:156-158)
  - 商品: f[1].trim(), 始终按字符串处理 (EventMapper:161)
  - 排序: 按 (uid, ts) 升序 (UserTsKey.compareTo:71-75)
  - 会话切分: ts - lastTs > sessionGap (默认 1800) 时切分 (SessionReducer:212-215)
  - 会话内去重: 保留首次出现顺序 (LinkedHashSet, SessionReducer:204,216)
  - 会话内商品上限: maxItems (默认 50), 超出取前 50 (SessionReducer:184-186)
  - 两两组合: i<j; 以字符串 compareTo 排序成 (a, b), a<b; key = "a b"
              (SessionReducer:187-197)
  - 聚合口径: 同一用户跨其所有 session 累加 pairCount, 用户处理完毕后一次性输出
              (SessionReducer:205,220-225)
  - category_id 不参与任何计算
  - 边输出: "i1 \t i2 \t w" (i1<i2 字符串序), 与 MR1 输出格式一致

已知语义边界 (必须知晓):
  - 相同 timestamp 的排序: Java 的二次排序键为 (uid, ts), 对 ts 完全相同的记录
    顺序不确定 (取决于输入分片/读取顺序)。本 Python 参考实现对相同 ts 采用
    "输入文件出现顺序的稳定排序" (sorted 是稳定排序)。因此在相同 ts 存在时,
    session 边界、会话内去重顺序、50 上限截断的边界可能与 Java 输出不同。
    比对时应将此视为语义边界, 而非 pipeline 错误。
  - 内存型实现: 流式读取后按 uid 分组保存事件列表, 用户组内排序。内存占用与
    事件总数成正比, 不适合全量 1 亿行数据 (全量验证应交给集群上的 Java MR)。
  - 用户级 vs 全局: 本脚本输出的 edges 是 "用户级" 聚合 (MR1 职责), 同一商品对
    在不同用户间未合并; MR2 才做跨用户全局合并。二者职责边界明确区分。

用法:
  python3 mapreduce_data_tools/validate_session_edges.py \
      --input mapreduce_data_tools/out/tiny/UserBehavior_tiny.csv

  python3 mapreduce_data_tools/validate_session_edges.py \
      --input UserBehavior.csv --sample-mod 330 --session-gap 1800

输出 (默认写入 mapreduce_data_tools/out/expected/):
  - expected_edges_user.tsv    用户级边 "i1\t i2\t w" (i1<i2, 每用户跨 session 累加)
  - expected_edges_global.tsv  跨用户全局合并后的边 (模拟 MR2, 供参考; 与用户级区分)
  - expected_nodes.tsv         节点统计 "item_id\tn_strength\tn_neighbors" (全局)
  - expected_summary.txt       运行摘要 (行数/用户/session/边/截断/边界说明)

  默认输出目录允许已存在, 但禁止覆盖同名文件 (存在则报错退出)。
"""

import argparse
import os
import sys
import time
import zipfile
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_DIR = os.path.join(HERE, "out", "expected")

VALID_BEHAVIORS = ("pv", "buy", "cart", "fav")
DEFAULT_SESSION_GAP = 1800
DEFAULT_MAX_ITEMS = 50


def open_rows(input_path):
    """逐行产出 bytes。支持 .zip (流式读取第一个成员) 与普通 CSV。

    返回 (fileobj, filename, closer)。调用方必须在 finally 中调用 closer.close()。
    """
    if input_path.lower().endswith(".zip"):
        try:
            zf = zipfile.ZipFile(input_path)
        except zipfile.BadZipFile as e:
            sys.stderr.write("[错误] 无法打开 ZIP (可能已损坏): {}\n".format(e))
            sys.exit(2)
        infos = zf.infolist()
        if not infos:
            zf.close()
            sys.stderr.write("[错误] ZIP 为空, 没有任何成员文件: {}\n".format(input_path))
            sys.exit(2)
        member = infos[0]
        return zf.open(member), member.filename, zf
    f = open(input_path, "rb")
    return f, os.path.basename(input_path), f


def check_output_path(path):
    if os.path.exists(path):
        sys.stderr.write(
            "[错误] 目标文件已存在, 禁止覆盖: {}\n"
            "       请使用 --outdir 指定其他目录或先手动移走该文件。\n".format(path))
        sys.exit(2)


def parse_events(input_path, behaviors, sample_mod, max_rows):
    """流式读取, 复刻 EventMapper 的过滤, 返回 (uid -> [(ts, item_str), ...], 统计信息)。

    内存型: 按 uid 分组保存全部合法事件 (用于小样本验证)。
    """
    f, fname, closer = open_rows(input_path)
    users = defaultdict(list)
    n_total = 0          # 读取总行数
    n_bad_len = 0        # f.length < 5
    n_bad_beh = 0        # 行为不合法
    n_bad_num = 0        # uid/ts 解析失败
    n_sampled_out = 0    # 采样丢弃
    n_kept = 0           # 保留事件
    try:
        for raw in f:
            if max_rows and n_total >= max_rows:
                break
            n_total += 1
            try:
                line = raw.decode("utf-8")
            except UnicodeDecodeError:
                n_bad_num += 1
                continue
            line = line.rstrip("\n").rstrip("\r")
            if line == "":
                continue
            # Java: line.toString().split(",") —— 不 trim 整行, 按 split 结果逐字段 trim
            fields = line.split(",")
            if len(fields) < 5:
                n_bad_len += 1
                continue
            beh = fields[3].strip()   # Java: f[3].trim()
            if beh not in behaviors:
                n_bad_beh += 1
                continue
            uid_s = fields[0].strip()
            ts_s = fields[4].strip()
            try:
                uid = int(uid_s)
                ts = int(ts_s)
            except ValueError:
                n_bad_num += 1
                continue
            if sample_mod > 1 and (uid % sample_mod) != 0:
                n_sampled_out += 1
                continue
            item = fields[1].strip()   # 商品: f[1].trim(), 字符串
            users[uid].append((ts, item))
            n_kept += 1
    finally:
        closer.close()
    stats = dict(fname=fname, n_total=n_total, n_bad_len=n_bad_len,
                 n_bad_beh=n_bad_beh, n_bad_num=n_bad_num,
                 n_sampled_out=n_sampled_out, n_kept=n_kept,
                 n_users=len(users))
    return users, stats


def build_user_edges(events, session_gap, max_items, user_stats):
    """复刻 SessionReducer: 用户组内按 (ts) 稳定排序 -> 切 session -> 去重/上限/配对,
    同一用户跨 session 累加, 返回该用户的 pairCount{dict}, 并累计统计。"""
    # 稳定排序: 只按 ts 排序; 相同 ts 保持输入出现顺序 (Java 则为非确定顺序)
    events_sorted = sorted(events, key=lambda x: x[0])
    pair_count = {}
    session = []          # 用 list + 成员集合模拟 LinkedHashSet 保序去重
    in_session = set()
    last_ts = None
    n_sessions = 0
    n_truncated = 0

    def flush():
        nonlocal n_sessions, n_truncated
        n_sessions += 1
        items = session
        if len(items) > max_items:
            items = items[:max_items]      # subList(0, maxItems)
            n_truncated += 1
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                a = items[i]
                b = items[j]
                if a > b:                  # String.compareTo
                    a, b = b, a
                pk = a + " " + b
                pair_count[pk] = pair_count.get(pk, 0) + 1

    for ts, item in events_sorted:
        if last_ts is not None and ts - last_ts > session_gap:
            flush()
            session = []
            in_session = set()
        if item not in in_session:         # LinkedHashSet: 首次出现才加入
            session.append(item)
            in_session.add(item)
        last_ts = ts
    if session:
        flush()
    elif last_ts is not None:
        # 空会话不 flush; 正常情况下 session 非空
        pass

    user_stats["n_sessions"] += n_sessions
    user_stats["n_truncated"] += n_truncated
    return pair_count


def main():
    ap = argparse.ArgumentParser(
        description="MR1 CooccurrenceJob 的独立 Python 参考实现 (会话共现建边)")
    ap.add_argument("--input", required=True, help="原始 CSV 或 .zip (五列)")
    ap.add_argument("--sample-mod", type=int, default=1,
                    help="uid%%mod==0 采样; 1=不采样 (默认), 330=复刻 MR1 默认抽样")
    ap.add_argument("--session-gap", type=int, default=DEFAULT_SESSION_GAP,
                    help="会话间隔秒数 (默认 1800)")
    ap.add_argument("--max-items", type=int, default=DEFAULT_MAX_ITEMS,
                    help="单会话商品上限 (默认 50)")
    ap.add_argument("--behaviors", default="pv,buy,cart,fav",
                    help="参与建图的行为类型, 逗号分隔 (默认全部)")
    ap.add_argument("--outdir", default=None,
                    help="输出目录 (默认 mapreduce_data_tools/out/expected; 允许已存在, 禁止覆盖同名文件)")
    ap.add_argument("--max-rows", type=int, default=0,
                    help="最多读取多少行, 0=全部 (仅用于自测快速截断)")
    args = ap.parse_args()

    if args.session_gap < 0:
        sys.stderr.write("[错误] --session-gap 不能为负\n")
        sys.exit(2)
    if args.max_items <= 0:
        sys.stderr.write("[错误] --max-items 必须为正\n")
        sys.exit(2)
    behaviors = tuple(b.strip() for b in args.behaviors.split(",") if b.strip())
    if not behaviors:
        sys.stderr.write("[错误] --behaviors 至少需要一个行为类型\n")
        sys.exit(2)

    input_path = args.input
    if not os.path.exists(input_path):
        sys.stderr.write("[错误] 找不到输入文件: {}\n".format(input_path))
        sys.exit(2)

    out_dir = args.outdir if args.outdir else DEFAULT_OUT_DIR
    os.makedirs(out_dir, exist_ok=True)
    p_user_edges = os.path.join(out_dir, "expected_edges_user.tsv")
    p_global_edges = os.path.join(out_dir, "expected_edges_global.tsv")
    p_nodes = os.path.join(out_dir, "expected_nodes.tsv")
    p_summary = os.path.join(out_dir, "expected_summary.txt")
    for p in (p_user_edges, p_global_edges, p_nodes, p_summary):
        check_output_path(p)

    t0 = time.time()
    users, st = parse_events(input_path, behaviors, args.sample_mod, args.max_rows)
    t_parse = time.time() - t0

    # 用户级边 (MR1) + 跨用户全局边 (模拟 MR2 合并) + 节点统计
    global_pair = defaultdict(int)       # (a,b) -> 全局权重
    user_stats = defaultdict(int)        # n_sessions, n_truncated
    t1 = time.time()
    n_user_edge_rows = 0
    with open(p_user_edges, "w", encoding="utf-8", newline="") as fu:
        for uid in sorted(users.keys()):
            pair_count = build_user_edges(users[uid], args.session_gap,
                                          args.max_items, user_stats)
            # 用户级: 每用户一对一行 (每用户跨 session 累加, 不跨用户合并)
            for pk, w in pair_count.items():
                a, b = pk.split(" ", 1)
                fu.write("{}\t{}\t{}\n".format(a, b, w))
                global_pair[(a, b)] += w
                n_user_edge_rows += 1
    t_graph = time.time() - t1

    # 全局边 (跨用户合并, 模拟 MR2) + 节点统计
    node_strength = defaultdict(int)
    node_nbrs = defaultdict(set)
    with open(p_global_edges, "w", encoding="utf-8", newline="") as fg:
        for (a, b) in sorted(global_pair.keys()):
            w = global_pair[(a, b)]
            fg.write("{}\t{}\t{}\n".format(a, b, w))
            node_strength[a] += w
            node_strength[b] += w
            node_nbrs[a].add(b)
            node_nbrs[b].add(a)
    with open(p_nodes, "w", encoding="utf-8", newline="") as fn:
        for item in sorted(node_strength.keys()):
            fn.write("{}\t{}\t{}\n".format(item, node_strength[item],
                                           len(node_nbrs[item])))

    # 摘要
    n_global_edges = len(global_pair)
    lines = []
    lines.append("=" * 72)
    lines.append("MR1 CooccurrenceJob 参考实现 — 运行摘要")
    lines.append("=" * 72)
    lines.append("输入文件: {}".format(os.path.abspath(input_path)))
    lines.append("文件成员: {}".format(st["fname"]))
    lines.append("参数: sample_mod={} session_gap={}s max_items={} behaviors={}".format(
        args.sample_mod, args.session_gap, args.max_items, ",".join(behaviors)))
    lines.append("")
    lines.append("--- 读取与过滤 (复刻 EventMapper) ---")
    lines.append("读取行数: {}".format(st["n_total"]))
    lines.append("字段<5 跳过: {}".format(st["n_bad_len"]))
    lines.append("行为不合法跳过: {}".format(st["n_bad_beh"]))
    lines.append("uid/ts 解析失败跳过: {}".format(st["n_bad_num"]))
    lines.append("采样丢弃: {}".format(st["n_sampled_out"]))
    lines.append("保留事件: {}".format(st["n_kept"]))
    lines.append("用户数: {}".format(st["n_users"]))
    lines.append("")
    lines.append("--- 会话与边 (复刻 SessionReducer) ---")
    lines.append("会话总数: {}".format(user_stats["n_sessions"]))
    lines.append("超限截断会话数: {}".format(user_stats["n_truncated"]))
    lines.append("用户级边行数 (MR1, 每用户一对一行): {}".format(n_user_edge_rows))
    lines.append("全局边数 (MR2 合并后, 去重对): {}".format(n_global_edges))
    lines.append("节点数 (全局): {}".format(len(node_strength)))
    lines.append("")
    lines.append("--- 语义边界与限制 ---")
    lines.append("1) 相同 timestamp: Java 排序键 (uid,ts) 无 tie-breaker, 顺序不确定;")
    lines.append("   本参考实现对相同 ts 采用输入出现顺序的稳定排序, 可能导致")
    lines.append("   session 边界/去重顺序/50 上限截断与 Java 输出不同。比对时视为边界。")
    lines.append("2) 内存型实现: 事件按 uid 分组保存于内存, 不适合全量 1 亿行;")
    lines.append("   全量验证应使用集群上的 Java MR。")
    lines.append("3) 用户级 vs 全局: expected_edges_user.tsv 为 MR1 用户级输出")
    lines.append("   (不跨用户合并); expected_edges_global.tsv 为模拟 MR2 跨用户合并结果。")
    lines.append("4) category_id 不参与计算 (与 Java 一致)。")
    lines.append("")
    lines.append("耗时: 解析 {:.2f}s, 建图与输出 {:.2f}s".format(t_parse, t_graph))
    lines.append("=" * 72)
    with open(p_summary, "w", encoding="utf-8") as fs:
        fs.write("\n".join(lines) + "\n")

    print("\n".join(lines))
    print("[输出] {}".format(os.path.abspath(out_dir)))


if __name__ == "__main__":
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    main()
