from collections import Counter
import matplotlib.pyplot as plt

LABELS = 'labels.tsv'

cnt = Counter()
with open(LABELS) as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) == 2:
            cnt[int(parts[0])] += 1

cids = sorted(cnt.keys())
sizes = [cnt[c] for c in cids]

plt.figure(figsize=(7, 5))
bars = plt.bar([str(c) for c in cids], sizes, color='steelblue')
for b, s in zip(bars, sizes):
    plt.text(b.get_x() + b.get_width() / 2, b.get_height(),
             str(s), ha='center', va='bottom')
plt.xlabel('Cluster')
plt.ylabel('Number of items')
plt.title('Cluster Size Distribution')
plt.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig('plot_cluster_size.png', dpi=150)
print("saved plot_cluster_size.png")