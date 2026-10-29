import sys

# 加载标准化参数
mean, std = [], []
with open('stats.txt') as f:
    for line in f:
        parts = line.strip().split('\t')
        if parts[0] == 'MEAN':
            mean = [float(x) for x in parts[1:]]
        elif parts[0] == 'STD':
            std = [float(x) for x in parts[1:]]

# 加载当前中心
centers = []
with open('centers.txt') as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) != 2:
            continue
        centers.append((int(parts[0]), [float(x) for x in parts[1].split(',')]))

def dist2(a, b):
    return sum((x - y) ** 2 for x, y in zip(a, b))

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 11:
        continue
    raw = [float(x) for x in parts[2:]]
    vec = [(raw[i] - mean[i]) / std[i] for i in range(len(raw))]

    best_cid, best_d = -1, float('inf')
    for cid, c in centers:
        d = dist2(vec, c)
        if d < best_d:
            best_d, best_cid = d, cid

    print(f"{best_cid}\t" + ",".join(f"{x:.6f}" for x in vec))