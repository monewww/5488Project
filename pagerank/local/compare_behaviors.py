#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
compare_behaviors.py — 分行为 PageRank 枢纽对比 (任务3实验)
对比 4 张图: 全行为(pr_output) / 仅pv(pr_pv) / 仅cart(pr_cart) / 仅buy(pr_buy)
输出: behavior_comparison.txt + behavior_comparison.png
"""
import csv
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei"]
plt.rcParams["axes.unicode_minus"] = False

HERE = os.path.dirname(os.path.abspath(__file__))
RUNS = [("全行为", "pr_output"), ("仅pv", "pr_pv"),
        ("仅cart", "pr_cart"), ("仅buy", "pr_buy")]
TOP_N = 30


def load_top(d):
    items = []
    with open(os.path.join(HERE, d, "top_items.csv"), encoding="utf-8") as f:
        for row in csv.DictReader(f):
            items.append((row["item_id"], float(row["pagerank"]),
                          row["category_id"]))
    return items


def load_stats(d):
    st = {}
    p = os.path.join(HERE, d, "stats.txt")
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            for line in f:
                if ":" in line:
                    k, v = line.split(":", 1)
                    st[k.strip()] = v.strip()
    return st


def jac(a, b):
    A, B = set(a), set(b)
    return len(A & B) / len(A | B) if A | B else 0.0


def main():
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    tops = {}
    lines = []
    lines.append("=" * 72)
    lines.append("分行为 PageRank 枢纽对比 (sample_mod=330, 与全行为同一采样)")
    lines.append("=" * 72)
    for name, d in RUNS:
        if not os.path.exists(os.path.join(HERE, d, "top_items.csv")):
            lines.append("[跳过] {}: 目录不存在".format(name))
            continue
        tops[name] = load_top(d)
        st = load_stats(d)
        lines.append("\n### {} ({}): 节点={} 边={} 会话={}".format(
            name, d, st.get("n_items(nodes)", "?"), st.get("n_edges", "?"),
            st.get("n_sessions", "?")))
        lines.append("  Top-10 枢纽: " + ", ".join(
            "{}(pr={:.2e})".format(i, p) for i, p, _ in tops[name][:10]))

    names = [n for n, _ in RUNS if n in tops]
    lines.append("\n### Top-{} 枢纽集合重叠 (交集数 / Jaccard)".format(TOP_N))
    header = "".ljust(10) + "".join(n.ljust(12) for n in names)
    lines.append(header)
    overlap_counts = {}
    for a in names:
        row = a.ljust(10)
        ids_a = [i for i, _, _ in tops[a]]
        for b in names:
            ids_b = [i for i, _, _ in tops[b]]
            inter = len(set(ids_a) & set(ids_b))
            j = jac(ids_a, ids_b)
            row += "{}/{}".format(inter, round(j, 2)).ljust(12)
            if a != b:
                overlap_counts[(a, b)] = inter
        lines.append(row)

    txt = "\n".join(lines)
    print(txt)
    out_txt = os.path.join(HERE, "behavior_comparison.txt")
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write(txt + "\n")
    print("\n已保存:", out_txt)

    # 图: 两两 Jaccard 热力条
    pairs = [(a, b) for i, a in enumerate(names) for b in names[i + 1:]]
    vals = [jac([x for x, _, _ in tops[a]], [x for x, _, _ in tops[b]])
            for a, b in pairs]
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(["{} vs {}".format(a, b) for a, b in pairs], vals,
                  color="#1f77b4", alpha=0.85)
    for bar, v in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width() / 2, v + 0.005,
                "{:.3f}".format(v), ha="center", fontsize=9)
    ax.set_ylabel("Top-{} Jaccard 重叠".format(TOP_N))
    ax.set_title("不同行为图之间的枢纽重叠度\n"
                 "重叠低 => 浏览枢纽 ≠ 购买枢纽, 分开计算有独立信息量",
                 fontsize=12)
    plt.xticks(rotation=20, fontsize=8)
    fig.tight_layout()
    out_png = os.path.join(HERE, "behavior_comparison.png")
    fig.savefig(out_png, dpi=200)
    print("已保存:", out_png)
    print("DONE")


if __name__ == "__main__":
    main()