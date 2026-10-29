set -e
echo "[1/4] k selection..."
python3 plot_k_selection.py
echo "[2/4] pca..."
python3 plot_pca.py
echo "[3/4] radar..."
python3 plot_radar.py
echo "[4/4] cluster size..."
python3 plot_cluster_size.py
echo "done: plot_k_selection.png plot_pca.png plot_radar.png plot_cluster_size.png"