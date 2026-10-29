import sys
import random
import numpy as np

k = int(sys.argv[1]) if len(sys.argv) > 1 else 3
seed = int(sys.argv[2]) if len(sys.argv) > 2 else 42
random.seed(seed)

rows = []
for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 11:
        continue
    vals = [float(x) for x in parts[2:]]  # 9 维数值特征
    rows.append(vals)

if not rows:
    print("ERROR: no feature rows", file=sys.stderr)
    sys.exit(1)

arr = np.array(rows)
mean = arr.mean(axis=0)
std = arr.std(axis=0)
std[std == 0] = 1.0

with open('stats.txt', 'w') as f:
    f.write("MEAN\t" + "\t".join(f"{x:.6f}" for x in mean) + "\n")
    f.write("STD\t" + "\t".join(f"{x:.6f}" for x in std) + "\n")

k = min(k, len(rows))
idx = random.sample(range(len(rows)), k)
with open('centers.txt', 'w') as f:
    for cid, i in enumerate(idx):
        vec = (arr[i] - mean) / std
        f.write(f"{cid}\t" + ",".join(f"{x:.6f}" for x in vec) + "\n")

print(f"k={k}, items={len(rows)}, centers initialized", file=sys.stderr)