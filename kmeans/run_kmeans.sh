set -e

K=${1:-3}
MAX_ITER=${2:-20}
TOL=${3:-0.001}
FEATURE=${4:-feature.tsv}

echo "[init] k=$K, max_iter=$MAX_ITER, tol=$TOL"
python3 init_centers.py $K < $FEATURE

for iter in $(seq 1 $MAX_ITER); do
    echo "[iter $iter] mapping..."
    cat $FEATURE \
      | python3 kmeans_mapper.py \
      | sort -k1,1n \
      | python3 kmeans_reducer.py > centers_new.txt

    if python3 compare_centers.py centers.txt centers_new.txt $TOL; then
        echo "[iter $iter] converged"
        rm -f centers_new.txt
        break
    fi
    mv centers_new.txt centers.txt
done

echo "[final] assign labels..."
cat $FEATURE | python3 kmeans_mapper.py > labels.tsv

echo "[done] centers:"
cat centers.txt
echo "[done] label distribution:"
cut -f1 labels.tsv | sort | uniq -c