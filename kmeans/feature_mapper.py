import sys

for line in sys.stdin:
    parts = line.rstrip('\n').split('\t')
    if len(parts) != 7:
        continue
    user_id, item_id, category_id, behavior, ts, date_str, hour = parts
    # key = item_id，value = category_id, behavior, user_id
    print(f"{item_id}\t{category_id}\t{behavior}\t{user_id}")