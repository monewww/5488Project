import numpy as np
import matplotlib.pyplot as plt

CENTERS = 'centers.txt'
FEATURE_NAMES = ['log_pv', 'log_fav', 'log_cart', 'log_buy',
                 'log_uv', 'log_buy_uv', 'fav_rate', 'cart_rate', 'buy_rate']

centers = []
with open(CENTERS) as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) != 2:
            continue
        centers.append([float(x) for x in parts[1].split(',')])
centers = np.array(centers)

N = len(FEATURE_NAMES)
angles = np.linspace(0, 2 * np.pi, N, endpoint=False).tolist()
angles += angles[:1]

fig, axes = plt.subplots(1, len(centers), figsize=(4 * len(centers), 4),
                         subplot_kw=dict(polar=True))
if len(centers) == 1:
    axes = [axes]

for cid, ax in enumerate(axes):
    vals = centers[cid].tolist()
    vals += vals[:1]
    ax.plot(angles, vals, 'o-', linewidth=2)
    ax.fill(angles, vals, alpha=0.25)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(FEATURE_NAMES, fontsize=8)
    ax.set_title(f'Cluster {cid}', fontsize=11)

plt.tight_layout()
plt.savefig('plot_radar.png', dpi=150)
print("saved plot_radar.png")