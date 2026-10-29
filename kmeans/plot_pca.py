import numpy as np
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA

FEATURE = 'feature.tsv'
LABELS = 'labels.tsv'

def load_features(path):
    items, rows = [], []
    with open(path) as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) != 11:
                continue
            items.append(parts[0])
            rows.append([float(x) for x in parts[2:]])
    return items, np.array(rows)

def load_labels(path):
    labels = {}
    with open(path) as f:
        for i, line in enumerate(f):
            parts = line.strip().split('\t')
            if len(parts) == 2:
                labels[i] = int(parts[0])
    return labels

items, X_raw = load_features(FEATURE)
labels = load_labels(LABELS)

mean = X_raw.mean(axis=0)
std = X_raw.std(axis=0)
std[std == 0] = 1.0
X = (X_raw - mean) / std

pca = PCA(n_components=2)
X_pca = pca.fit_transform(X)

y = np.array([labels.get(i, -1) for i in range(len(X))])
mask = y >= 0

plt.figure(figsize=(8, 6))
for cid in sorted(set(y[mask])):
    idx = (y == cid) & mask
    plt.scatter(X_pca[idx, 0], X_pca[idx, 1],
                label=f'cluster {cid}', alpha=0.7, s=30)
plt.xlabel(f'PC1 ({pca.explained_variance_ratio_[0]*100:.1f}%)')
plt.ylabel(f'PC2 ({pca.explained_variance_ratio_[1]*100:.1f}%)')
plt.title('KMeans Clusters (PCA projection)')
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('plot_pca.png', dpi=150)
print("saved plot_pca.png")