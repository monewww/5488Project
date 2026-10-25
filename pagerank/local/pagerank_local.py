#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
pagerank_local.py — 淘宝 UserBehavior 商品共现网络 PageRank (单机模拟版)

流程:
  1. 流式读取 UserBehavior.csv (支持直接读 zip, 无需解压)
  2. 按 user_id % sample_mod == 0 做无偏 hash 采样 (文件按 user 排序, 不能用前缀采样)
  3. 每用户按时间排序, 以 session_gap (默认30分钟) 切分会话
  4. 会话内商品两两共现 -> 加权无向图
  5. 幂迭代法 PageRank (阻尼系数 0.85, 处理悬挂节点)
  6. 输出枢纽商品 Top-N + nodes.csv / edges.csv (可导入 Gephi) + 收敛曲线

用法:
  python pagerank_local.py --input E:\\edge下载\\UserBehavior.csv.zip
  python pagerank_local.py --input UserBehavior.csv --sample-mod 330 --top 30
"""

import argparse
import csv
import os
import sys
import time
import zipfile
from collections import defaultdict

BEHAVIORS_ALL = {"pv", "buy", "cart", "fav"}


def open_rows(input_path):
    """逐行产出 bytes。支持 .zip (流式解压第一个成员) 和普通 csv 文件。"""
    if input_path.lower().endswith(".zip"):
        zf = zipfile.ZipFile(input_path)
        member = zf.infolist()[0]
        return zf.open(member), member.filename
    return open(input_path, "rb"), os.path.basename(input_path)


def scan_and_sample(input_path, sample_mod, max_rows, behaviors):
    """流式扫描 + hash 采样, 返回 {user: [(ts, item, cat, behavior), ...]}"""
    f, fname = open_rows(input_path)
    users = defaultdict(list)
    n_read = n_kept = 0
    t0 = time.time()
    for raw in f:
        n_read += 1
        if max_rows and n_read > max_rows:
            n_read -= 1
            break
        # 快速格式: uid,item,cat,behavior,ts
        try:
            s = raw.decode("ascii")
            uid, item, cat, beh, ts = s.split(",")
            if beh not in behaviors:
                continue
            if int(uid) % sample_mod != 0:
                continue
            users[int(uid)].append((int(ts), item, cat, beh))
            n_kept += 1
        except (ValueError, UnicodeDecodeError):
            continue  # 跳过坏行 (含可能的表头行)
        if n_read % 10_000_000 == 0:
            print("  ...已扫描 {:,} 行, 命中 {:,} 行, 用时 {:.1f}s".format(
                n_read, n_kept, time.time() - t0))
    f.close()
    print("[scan] 文件={} 扫描={:,} 行, 采样命中={:,} 行, 用户数={:,}, 耗时={:.1f}s".format(
        fname, n_read, n_kept, len(users), time.time() - t0))
    return users, n_read, n_kept


def sessionize(user_events, session_gap):
    """按时间排序并切分会话, 返回 [ [item,...], ... ] (会话内商品去重、保序)"""
    user_events.sort(key=lambda x: x[0])
    sessions = []
    cur, last_ts = [], None
    for ts, item, cat, beh in user_events:
        if last_ts is not None and ts - last_ts > session_gap:
            if cur:
                sessions.append(cur)
            cur = []
        if item not in cur:
            cur.append(item)
        last_ts = ts
    if cur:
        sessions.append(cur)
    return sessions


def build_graph(users, session_gap, max_session_items):
    """构建加权无向共现图: edge_w[(a,b)] += 1 (a<b 按字符串序)"""
    edge_w = defaultdict(int)
    node_cat = {}   # item -> category (取首次出现)
    n_sessions = 0
    n_truncated = 0
    for uid, events in users.items():
        for ts, item, cat, beh in events:
            if item not in node_cat:
                node_cat[item] = cat
        for sess in sessionize(events, session_gap):
            n_sessions += 1
            if len(sess) > max_session_items:
                sess = sess[:max_session_items]   # 截断超大会话, 防 O(n^2) 爆炸
                n_truncated += 1
            for i in range(len(sess)):
                for j in range(i + 1, len(sess)):
                    a, b = sess[i], sess[j]
                    if a > b:
                        a, b = b, a
                    edge_w[(a, b)] += 1
    print("[graph] 会话数={:,} (截断 {:,} 个超大会话), 节点={:,}, 边={:,}".format(
        n_sessions, n_truncated, len(node_cat), len(edge_w)))
    return edge_w, node_cat, n_sessions, n_truncated

def pagerank(edge_w, damping=0.85, max_iter=50, tol=1e-10):
    """
    加权无向图 PageRank 幂迭代。
      PR(i) = (1-d)/N + d * sum_j [ w(j,i)/S_j * PR(j) ]   (S_j = j 的边权和)
    返回 (pr_dict, 每轮L1差值列表)。
    """
    adj = defaultdict(list)
    strength = defaultdict(float)          # S_j
    for (a, b), w in edge_w.items():
        adj[a].append((b, w))
        adj[b].append((a, w))
        strength[a] += w
        strength[b] += w
    nodes = list(adj.keys())
    N = len(nodes)
    if N == 0:
        return {}, []

    pr = {n: 1.0 / N for n in nodes}
    deltas = []
    t0 = time.time()
    for it in range(1, max_iter + 1):
        base = (1.0 - damping) / N
        new_pr = {}
        for n in nodes:
            acc = base
            for (nbr, w) in adj[n]:
                acc += damping * pr[nbr] * w / strength[nbr]
            new_pr[n] = acc
        total = sum(new_pr.values())
        if total > 0:
            inv = 1.0 / total
            for n in nodes:
                new_pr[n] *= inv
        delta = sum(abs(new_pr[n] - pr[n]) for n in nodes)
        deltas.append(delta)
        pr = new_pr
        if it <= 5 or it % 10 == 0:
            print("  [iter {:>2}] L1 delta = {:.3e}  ({:.1f}s)".format(
                it, delta, time.time() - t0))
        if delta < tol:
            print("  收敛于第 {} 轮 (delta={:.3e} < tol={:.1e})".format(it, delta, tol))
            break
    return pr, deltas


def write_outputs(outdir, pr, edge_w, node_cat, degree, deltas, top_n, stats):
    os.makedirs(outdir, exist_ok=True)
    ranked = sorted(pr.items(), key=lambda kv: kv[1], reverse=True)

    # 1) Top-N 枢纽商品
    top_path = os.path.join(outdir, "top_items.csv")
    with open(top_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["rank", "item_id", "category_id", "pagerank",
                    "weighted_degree", "n_neighbors"])
        for i, (item, score) in enumerate(ranked[:top_n], 1):
            wd, nn = degree.get(item, (0.0, 0))
            w.writerow([i, item, node_cat.get(item, ""),
                        "{:.8f}".format(score), wd, nn])

    # 2) 全量节点表 (Gephi 节点文件)
    nodes_path = os.path.join(outdir, "nodes.csv")
    with open(nodes_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "pagerank", "category_id", "weighted_degree", "n_neighbors"])
        for item, score in ranked:
            wd, nn = degree.get(item, (0.0, 0))
            w.writerow([item, "{:.8f}".format(score),
                        node_cat.get(item, ""), wd, nn])

    # 3) 边表 (Gephi 边文件)
    edges_path = os.path.join(outdir, "edges.csv")
    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source", "target", "weight"])
        for (a, b), wt in edge_w.items():
            w.writerow([a, b, wt])

    # 4) 收敛曲线
    conv_path = os.path.join(outdir, "convergence.csv")
    with open(conv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["iteration", "l1_delta"])
        for i, d in enumerate(deltas, 1):
            w.writerow([i, "{:.6e}".format(d)])

    # 5) 运行统计 (报告直接引用)
    stats_path = os.path.join(outdir, "stats.txt")
    with open(stats_path, "w", encoding="utf-8") as f:
        for k, v in stats.items():
            f.write("{}: {}\n".format(k, v))

    print("\n[输出] {}:".format(os.path.abspath(outdir)))
    for p in (top_path, nodes_path, edges_path, conv_path, stats_path):
        print("  - {}  ({:.1f} KB)".format(os.path.basename(p),
                                            os.path.getsize(p) / 1024))

def main():
    # Windows 控制台/重定向时避免中文编码崩溃
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    ap = argparse.ArgumentParser(
        description="Item co-occurrence PageRank on Taobao UserBehavior (local simulation)")
    ap.add_argument("--input", required=True, help="UserBehavior.csv 或其 zip 包路径")
    ap.add_argument("--sample-mod", type=int, default=330,
                    help="hash采样模数: 保留 user_id%%mod==0 (默认330≈3000用户)")
    ap.add_argument("--max-rows", type=int, default=0,
                    help="最多扫描多少行, 0=全部 (快速试跑可设 5000000)")
    ap.add_argument("--behaviors", default="pv,buy,cart,fav",
                    help="参与建图的行为类型, 逗号分隔 (默认全部)")
    ap.add_argument("--session-gap", type=int, default=1800,
                    help="会话切分间隔秒数 (默认1800=30分钟)")
    ap.add_argument("--max-session-items", type=int, default=50,
                    help="单会话最多参与配对的商品数, 防O(n^2)爆炸 (默认50)")
    ap.add_argument("--damping", type=float, default=0.85, help="阻尼系数 (默认0.85)")
    ap.add_argument("--max-iter", type=int, default=50, help="最大迭代轮数 (默认50)")
    ap.add_argument("--tol", type=float, default=1e-10,
                    help="收敛阈值 L1 delta (默认1e-10)")
    ap.add_argument("--top", type=int, default=30, help="输出Top-N枢纽商品 (默认30)")
    ap.add_argument("--outdir", default=None, help="输出目录 (默认 ./pr_output)")
    args = ap.parse_args()

    outdir = args.outdir or os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "pr_output")
    behaviors = set(b.strip() for b in args.behaviors.split(",") if b.strip())
    if not behaviors <= BEHAVIORS_ALL:
        print("[ERROR] 未知行为类型, 只允许: %s" % sorted(BEHAVIORS_ALL), file=sys.stderr)
        sys.exit(2)

    print("=" * 64)
    print("淘宝 UserBehavior 商品共现网络 PageRank — 单机模拟")
    print("  input=%s" % args.input)
    print("  sample_mod=%d  session_gap=%ds  damping=%.2f  max_iter=%d"
          % (args.sample_mod, args.session_gap, args.damping, args.max_iter))
    print("=" * 64)

    # 阶段1: 扫描采样
    users, n_read, n_kept = scan_and_sample(
        args.input, args.sample_mod, args.max_rows, behaviors)
    if not users:
        print("[ERROR] 没有采样到任何数据, 请检查输入路径/参数", file=sys.stderr)
        sys.exit(1)

    # 阶段2: 会话切分 + 建图
    t0 = time.time()
    edge_w, node_cat, n_sessions, n_trunc = build_graph(
        users, args.session_gap, args.max_session_items)
    t_graph = time.time() - t0

    # 加权度 / 邻居数统计
    degree = {}
    for (a, b), w in edge_w.items():
        wa, na = degree.get(a, (0.0, 0))
        wb, nb = degree.get(b, (0.0, 0))
        degree[a] = (wa + w, na + 1)
        degree[b] = (wb + w, nb + 1)

    # 阶段3: PageRank
    t0 = time.time()
    pr, deltas = pagerank(edge_w, args.damping, args.max_iter, args.tol)
    t_pr = time.time() - t0

    # 阶段4: 输出
    ranked = sorted(pr.items(), key=lambda kv: kv[1], reverse=True)
    stats = {
        "input": args.input,
        "rows_scanned": n_read,
        "rows_sampled": n_kept,
        "sample_mod": args.sample_mod,
        "n_users": len(users),
        "n_sessions": n_sessions,
        "n_truncated_sessions": n_trunc,
        "n_items(nodes)": len(node_cat),
        "n_edges": len(edge_w),
        "session_gap_sec": args.session_gap,
        "damping": args.damping,
        "iterations": len(deltas),
        "final_l1_delta": deltas[-1] if deltas else "NA",
        "time_graph_sec": round(t_graph, 1),
        "time_pagerank_sec": round(t_pr, 1),
    }
    write_outputs(outdir, pr, edge_w, node_cat, degree, deltas, args.top, stats)

    # 控制台 Top-N
    print("\nTop {} 枢纽商品 (hub items):".format(args.top))
    print("{:>4} {:>12} {:>10} {:>12} {:>8} {:>6}".format(
        "rank", "item_id", "category", "pagerank", "w_degree", "nbrs"))
    for i, (item, score) in enumerate(ranked[:args.top], 1):
        wd, nn = degree.get(item, (0.0, 0))
        print("{:>4} {:>12} {:>10} {:>12.8f} {:>8} {:>6}".format(
            i, item, node_cat.get(item, ""), score, wd, nn))

    print("\n[done] " + ", ".join("{}={}".format(k, v) for k, v in stats.items()))


if __name__ == "__main__":
    main()
