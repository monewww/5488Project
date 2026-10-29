import subprocess
import numpy as np
import matplotlib.pyplot as plt

FEATURE = 'feature.tsv'
K_RANGE = range(2, 9)

def load_features(path):
    rows = []
    with open(path) as f:
        for line in f:
            parts = line.strip().split('\t')
            if len(parts) != 11:
                continue
            rows.append([float(x) for x in parts[2:]])
    return np.array(rows)

def run_kmeans(k):
    subprocess.run(['python3', 'init_centers.py', str(k)],
                   stdin=open(FEATURE), check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(20):
        with open(FEATURE) as fin, open('centers_new.txt', 'w') as fout:
            p1 = subprocess.Popen(['python3', 'kmeans_mapper.py'],
                                  stdin=fin, stdout=subprocess.PIPE)
            p2 = subprocess.Popen(['sort', '-k1,1n'],
                                  stdin=p1.stdout, stdout=subprocess.PIPE)
            p3 = subprocess.Popen(['python3', 'kmeans_reducer.py'],
                                  stdin=p2.stdout, stdout=fout)
            p3.wait()
        ret = subprocess.run(['python3', 'compare_centers.py',
                              'centers.txt', 'centers_new.txt', '0.001'],
                             stderr=subprocess.DEVNULL)
        subprocess.run(['mv', 'centers_new.txt', 'centers.txt'])
        if ret.returncode == 0:
            break

    # 用最终中心算 SSE
    centers = []
    with open('centers.txt') as f:
        for line in f:
            parts = line.strip().split('\t')
            centers.append(np.array([float(x) for x in parts[1].split(',')]))
    centers = np.array(centers)

    arr = load_features(FEATURE)
    mean = arr.mean(axis=0)
    std = arr.std(axis=0)
    std[std == 0] = 1.0
    X = (arr - mean) / std

    d2 = ((X[:, None, :] - centers[None, :, :]) ** 2).sum(axis=2)
    return d2.min(axis=1).sum()

ks, sses = [], []
for k in K_RANGE:
    if k > len(load_features(FEATURE)):
        break
    sse = run_kmeans(k)
    ks.append(k)
    sses.append(sse)
    print(f"k={k}, SSE={sse:.4f}")

plt.figure(figsize=(7, 5))
plt.plot(ks, sses, 'o-', linewidth=2)
plt.xlabel('k')
plt.ylabel('SSE (within-cluster sum of squares)')
plt.title('Elbow Method for KMeans')
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig('plot_k_selection.png', dpi=150)
print("saved plot_k_selection.png")