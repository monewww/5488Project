#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
visualize_hubs.py — 枢纽节点意义可视化 (4 联图)
  (A) Top-8 枢纽商品的 ego-network: 枢纽=连接大量商品/跨类目的"桥"
  (B) PageRank vs 度 双对数散点: 重尾分布, 少数枢纽占据绝大部分权重
  (C) PageRank 幂迭代收敛曲线
  (D) 枢纽 vs 普通商品的"邻居类目多样性"对比: 枢纽桥接更多类目
输入: pr_output/{nodes.csv, edges.csv, convergence.csv}
输出: hub_visualization.png (以及 4 张单独 panel PNG)
"""
import csv
import os
import random
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei"]
plt.rcParams["axes.unicode_minus"] = False

HERE = os.path.dirname(os.path.abspath(__file__))
PR_DIR = os.path.join(HERE, "pr_output")


def load_data():
    nodes = {}   # id -> dict(pr, cat, wdeg, nbrs)
    with open(os.path.join(PR_DIR, "nodes.csv"), encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            nodes[row["id"]] = {
                "pr": float(row["pagerank"]),
                "cat": row["category_id"],
                "wdeg": float(row["weighted_degree"]),
                "nn": int(row["n_neighbors"]),
            }
    adj = {}     # id -> list of (nbr, w)
    with open(os.path.join(PR_DIR, "edges.csv"), encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            s, t, w = row["source"], row["target"], float(row["weight"])
            adj.setdefault(s, []).append((t, w))
            adj.setdefault(t, []).append((s, w))
    conv = []
    with open(os.path.join(PR_DIR, "convergence.csv"), encoding="utf-8") as f:
        for row in csv.DictReader(f):
            conv.append((int(row["iteration"]), float(row["l1_delta"])))
    n_edges = sum(len(v) for v in adj.values()) // 2
    return nodes, adj, conv, n_edges


def panel_a(ax, nodes, adj, top_k=8, nbr_per_hub=8):
    """Top-K 枢纽的 ego-network"""
    ranked = sorted(nodes.items(), key=lambda kv: kv[1]["pr"], reverse=True)
    hubs = [h for h, _ in ranked[:top_k]]
    G = nx.Graph()
    hub_set = set(hubs)
    for h in hubs:
        G.add_node(h, is_hub=True)
        top_nbrs = sorted(adj.get(h, []), key=lambda x: x[1], reverse=True)[:nbr_per_hub]
        for nb, w in top_nbrs:
            G.add_node(nb, is_hub=nb in hub_set)
            G.add_edge(h, nb, weight=w)
    pos = nx.spring_layout(G, seed=42, k=0.6)
    hub_nodes = [n for n, d in G.nodes(data=True) if d["is_hub"]]
    leaf_nodes = [n for n in G.nodes() if n not in hub_set]
    cats = sorted({nodes[n]["cat"] for n in leaf_nodes if n in nodes})
    cmap = plt.get_cmap("tab20")
    cdict = {c: cmap(i % 20) for i, c in enumerate(cats)}
    colors = [cdict.get(nodes[n]["cat"], "gray") for n in leaf_nodes if n in nodes]
    nx.draw_networkx_nodes(G, pos, nodelist=hub_nodes, node_color="#d62728",
                           node_size=900, node_shape="*", edgecolors="k", ax=ax)
    nx.draw_networkx_nodes(G, pos, nodelist=leaf_nodes, node_color=colors,
                           node_size=90, ax=ax)
    nx.draw_networkx_edges(G, pos, width=0.6, alpha=0.4, ax=ax)
    nx.draw_networkx_labels(G, pos, labels={h: h for h in hub_nodes},
                            font_size=7, font_color="black", ax=ax)
    ax.set_title("(A) Top-{} 枢纽商品(红星)及其最强 {} 个邻居\n"
                 "颜色=商品类目: 枢纽同时连接大量不同类目的商品"
                 .format(top_k, nbr_per_hub), fontsize=11)
    ax.axis("off")


def panel_b(ax, nodes, top_k=30):
    """PR vs 度 双对数散点 + 幂律拟合"""
    import numpy as np
    xs = [v["nn"] for v in nodes.values()]
    ys = [v["pr"] for v in nodes.values()]
    ax.scatter(xs, ys, s=3, alpha=0.25, color="#1f77b4", label="全部商品")
    ranked = sorted(nodes.items(), key=lambda kv: kv[1]["pr"], reverse=True)
    hx = [v["nn"] for _, v in ranked[:top_k]]
    hy = [v["pr"] for _, v in ranked[:top_k]]
    ax.scatter(hx, hy, s=35, color="#d62728", zorder=3,
               label="Top-{} 枢纽".format(top_k))
    lx, ly = np.log(xs), np.log(ys)
    slope, intercept = np.polyfit(lx, ly, 1)
    xr = np.array([min(xs), max(xs)])
    ax.plot(xr, np.exp(intercept) * xr ** slope, "k--", lw=1,
            label="幂律拟合: PR $\\propto$ degree$^{{{:,.2f}}}$".format(slope))
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("邻居数 (度)"); ax.set_ylabel("PageRank")
    ax.legend(fontsize=8, loc="upper left")
    ax.set_title("(B) PageRank 呈重尾分布: 度与 PR 呈幂律关系\n"
                 "极少数枢纽商品获得远高于均匀值 (1/N) 的权重", fontsize=11)
    ax.grid(alpha=0.3, which="both")


def panel_c(ax, conv):
    its = [c[0] for c in conv]
    ds = [c[1] for c in conv]
    ax.plot(its, ds, "o-", color="#2ca02c", ms=3)
    ax.set_yscale("log")
    ax.set_xlabel("迭代轮数"); ax.set_ylabel("L1 delta (对数)")
    ax.set_title("(C) 幂迭代收敛曲线: L1 delta 指数级下降\n"
                 "{} 轮后收敛至 {:.1e}".format(len(ds), ds[-1]), fontsize=11)
    ax.grid(alpha=0.3, which="both")
def panel_d(ax, nodes, adj, top_k=10, n_random=1000):
    """枢纽 vs 随机普通商品: 邻居类目多样性"""
    ranked = sorted(nodes.items(), key=lambda kv: kv[1]["pr"], reverse=True)
    hubs = [h for h, _ in ranked[:top_k]]

    def cat_diversity(n):
        return len({nodes[nb]["cat"] for nb, _ in adj.get(n, []) if nb in nodes})

    hub_div = [(h, cat_diversity(h)) for h in hubs]
    candidates = [n for n, v in nodes.items() if 5 <= v["nn"] <= 30]
    random.seed(42)
    sample = random.sample(candidates, min(n_random, len(candidates)))
    rand_div = [cat_diversity(n) for n in sample]
    avg_rand = sum(rand_div) / len(rand_div)

    labels = [h for h, _ in hub_div]
    vals = [d for _, d in hub_div]
    bars = ax.bar(range(len(vals)), vals, color="#d62728", alpha=0.85)
    ax.axhline(avg_rand, color="#1f77b4", ls="--", lw=1.5,
               label="普通商品平均 (随机{}个, 度5-30): {:.1f} 类".format(n_random, avg_rand))
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, fontsize=7)
    ax.set_ylabel("邻居覆盖的不同类目数")
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v + 1, str(v),
                ha="center", fontsize=7)
    ax.legend(fontsize=8)
    ax.set_title("(D) 枢纽商品的邻居类目多样性是普通商品的 {:.1f} 倍\n"
                 "→ 枢纽 = 跨类目的'桥', 这正是 PageRank 高的结构原因"
                 .format((sum(vals) / len(vals)) / avg_rand), fontsize=11)
    ax.grid(alpha=0.3, axis="y")


def main():
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    print("加载数据 ...")
    nodes, adj, conv, n_edges = load_data()
    print("  节点={:,}  边={:,}  收敛记录={} 轮".format(
        len(nodes), n_edges, len(conv)))

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    panel_a(axes[0][0], nodes, adj)
    panel_b(axes[0][1], nodes)
    panel_c(axes[1][0], conv)
    panel_d(axes[1][1], nodes, adj)
    fig.suptitle("淘宝 UserBehavior 商品共现网络 — 枢纽节点 (Hub) 分析  "
                 "[3,005 用户采样 / {:,} 节点 / {:,} 边]".format(
                     len(nodes), n_edges), fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    out = os.path.join(HERE, "hub_visualization.png")
    fig.savefig(out, dpi=200)
    print("已保存:", out)

    # 单独保存 4 张 panel (报告灵活取用)
    for name, fn in (("panel_a_egonetwork", lambda a: panel_a(a, nodes, adj)),
                     ("panel_b_powerlaw", lambda a: panel_b(a, nodes)),
                     ("panel_c_convergence", lambda a: panel_c(a, conv)),
                     ("panel_d_diversity", lambda a: panel_d(a, nodes, adj))):
        f2, ax2 = plt.subplots(figsize=(8, 6))
        fn(ax2)
        f2.tight_layout()
        p = os.path.join(HERE, name + ".png")
        f2.savefig(p, dpi=200)
        print("已保存:", p)
    print("DONE")


if __name__ == "__main__":
    main()