#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_sample.py — 从完整 UserBehavior.csv 抽出一个可提交的小样本

为什么要抽样而不是直接取前 N 行?
  UserBehavior.csv 按 user_id 排序, 直接取前 N 行 = 只保留最早的几百个用户,
  时间上只覆盖 1 天, 会话/共现结构会严重失真。
  本脚本按 user_id % mod == 0 做**确定性 hash 抽样**, 用户在全文件随机分布,
  9 天时间跨度都能覆盖, 且结果可复现 (同样的 mod 永远得到同样的样本)。

用法:
  python make_sample.py --input ../Database/UserBehavior.csv --mod 2200 \
      --output sample/UserBehavior_sample.csv

  mod=330  (≈3005 用户, 11MB)  -> 与论文/报告实验完全一致, 但文件偏大
  mod=2200 (≈450 用户, 1.7MB)  -> 仓库内提交的冒烟测试样本 (默认)
  mod=22000(≈45 用户, 170KB)   -> 极小样本, 只用来点一下流程
"""

import argparse
import os
import sys
import time
import zipfile


def open_rows(path):
    if path.lower().endswith(".zip"):
        zf = zipfile.ZipFile(path)
        return zf.open(zf.infolist()[0]), zf.infolist()[0].filename
    return open(path, "rb"), os.path.basename(path)


def main():
    ap = argparse.ArgumentParser(description="hash-sample UserBehavior.csv into a small csv")
    ap.add_argument("--input", required=True, help="完整 UserBehavior.csv 或其 .zip")
    ap.add_argument("--output", required=True, help="输出小样本 csv 路径")
    ap.add_argument("--mod", type=int, default=2200, help="保留 user_id %% mod == 0 (默认 2200)")
    ap.add_argument("--max-users", type=int, default=0, help="最多保留多少个用户, 0=不限")
    args = ap.parse_args()

    out_dir = os.path.dirname(os.path.abspath(args.output))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    f, fname = open_rows(args.input)
    t0 = time.time()
    n_read = n_kept = 0
    users = set()
    stop = False
    with open(args.output, "w", encoding="utf-8", newline="") as out:
        for raw in f:
            n_read += 1
            try:
                s = raw.decode("ascii").strip()
                uid_s, _, _ = s.partition(",")
                uid = int(uid_s)
            except (ValueError, UnicodeDecodeError):
                continue          # 跳过表头/坏行
            if uid % args.mod != 0:
                continue
            if args.max_users and uid not in users and len(users) >= args.max_users:
                stop = True
                break
            users.add(uid)
            out.write(s + "\n")
            n_kept += 1
    f.close()
    dt = time.time() - t0
    print("[sample] 源文件={} 扫描={:,} 行 保留={:,} 行 用户={:,} 模数={} 耗时={:.1f}s".format(
        fname, n_read, n_kept, len(users), args.mod, dt))
    print("[sample] 输出={} ({:.2f} MB){}".format(
        os.path.abspath(args.output),
        os.path.getsize(args.output) / 1024.0 / 1024.0,
        "  [已按 --max-users 提前截断]" if stop else ""))


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(errors="replace")
    main()
