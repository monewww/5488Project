import sys
import math

current_item = None
category_id = None
pv = fav = cart = buy = 0
users = set()
buy_users = set()

def emit():
    uv = len(users)
    buy_uv = len(buy_users)
    fav_rate = fav / pv if pv else 0.0
    cart_rate = cart / pv if pv else 0.0
    buy_rate = buy / pv if pv else 0.0
    # 输出：item_id, category_id, log1p(pv), log1p(fav), log1p(cart), log1p(buy),
    #       log1p(uv), log1p(buy_uv), fav_rate, cart_rate, buy_rate
    print(
        f"{current_item}\t{category_id}\t"
        f"{math.log1p(pv):.6f}\t{math.log1p(fav):.6f}\t"
        f"{math.log1p(cart):.6f}\t{math.log1p(buy):.6f}\t"
        f"{math.log1p(uv):.6f}\t{math.log1p(buy_uv):.6f}\t"
        f"{fav_rate:.6f}\t{cart_rate:.6f}\t{buy_rate:.6f}"
    )

for line in sys.stdin:
    parts = line.rstrip('\n').split('\t')
    if len(parts) != 4:
        continue
    item_id, cat, behavior, user_id = parts
    if item_id != current_item:
        if current_item is not None:
            emit()
        current_item = item_id
        category_id = cat
        pv = fav = cart = buy = 0
        users = set()
        buy_users = set()
    users.add(user_id)
    if behavior == 'pv':
        pv += 1
    elif behavior == 'fav':
        fav += 1
    elif behavior == 'cart':
        cart += 1
    elif behavior == 'buy':
        buy += 1
        buy_users.add(user_id)

if current_item is not None:
    emit()