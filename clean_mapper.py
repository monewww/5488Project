import sys
from datetime import datetime, timezone, timedelta

# 数据集时间范围：2017-11-25 00:00:00 ~ 2017-12-04 00:00:00 (CST)
CST = timezone(timedelta(hours=8))
MIN_TS = 1511539200   # 2017-11-25 00:00:00 CST
MAX_TS = 1512316800   # 2017-12-04 00:00:00 CST
VALID_BEHAVIOR = {'pv', 'fav', 'cart', 'buy'}

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split(',')
    if len(parts) != 5:
        continue
    user_id, item_id, category_id, behavior, ts = parts
    if not all([user_id, item_id, category_id, behavior, ts]):
        continue
    if behavior not in VALID_BEHAVIOR:
        continue
    try:
        user_id = int(user_id)
        item_id = int(item_id)
        category_id = int(category_id)
        ts = int(ts)
    except ValueError:
        continue
    if ts < MIN_TS or ts >= MAX_TS:
        continue

    dt = datetime.fromtimestamp(ts, tz=CST)
    date_str = dt.strftime('%Y-%m-%d')
    hour = dt.hour

    # 输出 TSV：user_id, item_id, category_id, behavior, ts, date, hour
    print(f"{user_id}\t{item_id}\t{category_id}\t{behavior}\t{ts}\t{date_str}\t{hour}")