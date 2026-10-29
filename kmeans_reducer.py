import sys

current_cid = None
sum_vec = None
count = 0

def emit():
    if count == 0:
        return
    avg = [s / count for s in sum_vec]
    print(f"{current_cid}\t" + ",".join(f"{x:.6f}" for x in avg))

for line in sys.stdin:
    parts = line.strip().split('\t')
    if len(parts) != 2:
        continue
    cid = int(parts[0])
    vec = [float(x) for x in parts[1].split(',')]
    if cid != current_cid:
        if current_cid is not None:
            emit()
        current_cid = cid
        sum_vec = [0.0] * len(vec)
        count = 0
    for i, v in enumerate(vec):
        sum_vec[i] += v
    count += 1

if current_cid is not None:
    emit()