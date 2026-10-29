import sys
import math

def load(path):
    d = {}
    with open(path) as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) != 2:
                continue
            d[int(parts[0])] = [float(x) for x in parts[1].split(',')]
    return d

old = load(sys.argv[1])
new = load(sys.argv[2])
tol = float(sys.argv[3]) if len(sys.argv) > 3 else 0.001

if set(old.keys()) != set(new.keys()):
    sys.exit(1)

total = math.sqrt(sum(
    sum((x - y) ** 2 for x, y in zip(old[cid], new[cid]))
    for cid in old
))
print(f"delta={total:.6f}", file=sys.stderr)
sys.exit(0 if total < tol else 1)