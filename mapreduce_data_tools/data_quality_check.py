#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
data_quality_check.py — MapReduce / PageRank 模块数据质量统计（只统计，不删除）

用途:
  对原始 UserBehavior 数据做流式统计, 为报告"数据质量"一节提供依据。
  本脚本只读取输入, 只把统计报告写入 out/ (或 --outdir 指定的新目录),
  绝不删除、修改、覆盖任何原始数据或现有项目文件。

统计内容:
  1) 总行数、空行数
  2) 字段数 != 5 的行数
  3) 空字段行数
  4) user_id / item_id / category_id / timestamp 无法解析为整数的行数
  5) 负值行数 (uid/item/ts 为负)
  6) behavior_type 分布 (pv/buy/cart/fav) 及非法行为行数
  7) 完全重复记录 (5 字段完全相同) 统计 —— 只统计, 不删除
  8) (user_id, item_id, timestamp) 重复统计 —— 只统计, 不删除
  9) user_id / item_id 最大值, 判断是否超过 Java int32 上限 (2147483647)
  10) 时间范围越界统计 (以 K-means 口径 [1511539200, 1512316800) 为参照, 只统计)
  11) timestamp 最小/最大值

设计:
  - 流式逐行处理, 内存占用与重复检测相关 (需保存已见 key 的集合/计数),
    对 1 亿行全量建议在内存充足的机器上运行, 或仅对样本运行。
  - 所有统计口径与现有 MR1 (CooccurrenceJob.EventMapper) 一致之处已注明。

用法:
  python3 mapreduce_data_tools/data_quality_check.py \
      --input pagerank/data/sample/UserBehavior_sample.csv

  python3 mapreduce_data_tools/data_quality_check.py \
      --input UserBehavior.csv --outdir mapreduce_data_tools/out/quality_full

重复检测:
  full_seen / uit_seen 会保存所有唯一 key, 全量 1 亿行可能占用大量内存。
  因此提供 --check-duplicates (默认开启) / --no-check-duplicates 开关:
  关闭时不维护这两张表, 内存 O(1), 适合全量运行 (报告中会注明"已关闭")。

输入:
  --input  原始 CSV 或 .zip (必填)
输出:
  默认 mapreduce_data_tools/out/quality_report.txt
  --outdir 指定的目录: 允许目录已存在, 但禁止覆盖同名的报告文件
  (脚本不会删除、修改任何原始数据)
"""

import argparse
import os
import sys
import zipfile
from collections import Counter

# K-means 口径的时间区间 (仅作越界统计参照, 不删除任何数据)
MIN_TS = 1511539200   # 2017-11-25 00:00:00 CST
MAX_TS = 1512316800   # 2017-12-04 00:00:00 CST
INT32_MAX = 2147483647
VALID_BEHAVIORS = ("pv", "buy", "cart", "fav")

# 本脚本所在目录及其父目录 (项目根), 用于推断默认路径
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_BASE = os.path.join(HERE, "out")


def open_rows(input_path):
    """逐行产出 bytes。支持 .zip (流式读取第一个成员) 与普通 CSV。

    返回 (fileobj, filename, closer)。调用方必须在 finally 中调用 closer.close(),
    以可靠关闭底层文件句柄 (普通 CSV 时 closer 即 fileobj; ZIP 时 closer 为 ZipFile)。
    """
    if input_path.lower().endswith(".zip"):
        try:
            zf = zipfile.ZipFile(input_path)
        except zipfile.BadZipFile as e:
            sys.stderr.write("[错误] 无法打开 ZIP (可能已损坏): {}\n".format(e))
            sys.exit(2)
        infos = zf.infolist()
        if not infos:
            zf.close()
            sys.stderr.write("[错误] ZIP 为空, 没有任何成员文件: {}\n".format(input_path))
            sys.exit(2)
        member = infos[0]
        return zf.open(member), member.filename, zf
    f = open(input_path, "rb")
    return f, os.path.basename(input_path), f


def fmt_int(n):
    return "{:,}".format(n)


def check_output_path(path):
    """若目标文件已存在则报错, 禁止覆盖。"""
    if os.path.exists(path):
        sys.stderr.write(
            "[错误] 目标文件已存在, 禁止覆盖: {}\n"
            "       请使用 --outdir 指定其他新目录。\n".format(path))
        sys.exit(2)


def main():
    ap = argparse.ArgumentParser(
        description="UserBehavior 数据质量统计 (只统计, 不删除)")
    ap.add_argument("--input", required=True, help="原始 CSV 或 .zip 路径")
    ap.add_argument("--outdir", default=None,
                    help="输出目录 (默认 mapreduce_data_tools/out; 允许目录已存在, 但禁止覆盖同名报告文件)")
    ap.add_argument("--report-name", default="quality_report.txt",
                    help="报告文件名 (默认 quality_report.txt)")
    ap.add_argument("--check-duplicates", dest="check_duplicates",
                    action="store_true", default=True,
                    help="统计完全重复与 (uid,item,ts) 重复记录 (默认开启)")
    ap.add_argument("--no-check-duplicates", dest="check_duplicates",
                    action="store_false",
                    help="关闭重复检测, 内存 O(1), 适合全量运行")
    args = ap.parse_args()

    input_path = args.input
    if not os.path.exists(input_path):
        sys.stderr.write("[错误] 找不到输入文件: {}\n".format(input_path))
        sys.exit(2)

    # 输出目录: 默认 out/, 或用户指定 (方案 A: 允许目录已存在, 仅禁止覆盖同名报告文件)
    out_dir = args.outdir if args.outdir else DEFAULT_OUT_BASE
    os.makedirs(out_dir, exist_ok=True)
    report_path = os.path.join(out_dir, args.report_name)
    check_output_path(report_path)

    # 计数器
    n_total = 0            # 总行数 (含空行)
    n_empty = 0            # 空行
    n_bad_field_count = 0  # 字段数 != 5
    n_empty_field = 0      # 空字段
    n_parse_error = 0      # 无法解析为整数
    n_negative = 0         # 负值 (uid/item/ts)
    n_illegal_behavior = 0  # 非法行为
    n_valid = 0            # 干净行 (字段数=5, 非空, 可解析, 非负, 行为合法)
    beh_counter = Counter()
    # 重复检测表: 仅当 check_duplicates 开启时维护, 否则保持为空 (内存 O(1))
    full_seen = Counter()
    uit_seen = Counter()
    max_uid = -1
    max_item = -1
    ts_min = None
    ts_max = None
    n_ts_below = 0         # ts < MIN_TS
    n_ts_above = 0         # ts >= MAX_TS
    n_uid_overflow = 0     # uid > INT32_MAX
    n_item_overflow = 0    # item > INT32_MAX

    f, fname, closer = open_rows(input_path)
    try:
        for raw in f:
            n_total += 1
            try:
                line = raw.decode("utf-8").rstrip("\n").rstrip("\r")
            except UnicodeDecodeError:
                n_parse_error += 1
                continue
            if line.strip() == "":
                n_empty += 1
                continue
            fields = line.split(",")
            if len(fields) != 5:
                n_bad_field_count += 1
                continue
            uid_s, item_s, cat_s, beh_s, ts_s = [x.strip() for x in fields]
            if not (uid_s and item_s and cat_s and beh_s and ts_s):
                n_empty_field += 1
                continue
            try:
                uid = int(uid_s)
                item = int(item_s)
                int(cat_s)
                ts = int(ts_s)
            except ValueError:
                n_parse_error += 1
                continue
            if uid < 0 or item < 0 or ts < 0:
                # 负值行: 单独计数后跳过, 不计入干净行/行为分布/时间越界/重复统计
                n_negative += 1
                continue
            if beh_s not in VALID_BEHAVIORS:
                n_illegal_behavior += 1
                # 非法行为不计入"干净行"的重复统计, 但计入行为分布
                beh_counter[beh_s] += 1
                continue
            # 到这里是干净行
            n_valid += 1
            beh_counter[beh_s] += 1
            if uid > max_uid:
                max_uid = uid
            if item > max_item:
                max_item = item
            if ts_min is None or ts < ts_min:
                ts_min = ts
            if ts_max is None or ts > ts_max:
                ts_max = ts
            if ts < MIN_TS:
                n_ts_below += 1
            elif ts >= MAX_TS:
                n_ts_above += 1
            if uid > INT32_MAX:
                n_uid_overflow += 1
            if item > INT32_MAX:
                n_item_overflow += 1
            if args.check_duplicates:
                full_seen[line] += 1
                uit_seen[(uid_s, item_s, ts_s)] += 1
    finally:
        closer.close()

    dup_full_extra = sum(c - 1 for c in full_seen.values() if c > 1)
    dup_full_keys = sum(1 for c in full_seen.values() if c > 1)
    dup_uit_extra = sum(c - 1 for c in uit_seen.values() if c > 1)
    dup_uit_keys = sum(1 for c in uit_seen.values() if c > 1)

    lines = []
    lines.append("=" * 72)
    lines.append("UserBehavior 数据质量报告 (只统计, 不删除)")
    lines.append("=" * 72)
    lines.append("输入文件: {}".format(os.path.abspath(input_path)))
    lines.append("文件成员: {}".format(fname))
    lines.append("")
    lines.append("--- 行级统计 ---")
    lines.append("总行数: {}".format(fmt_int(n_total)))
    lines.append("空行数: {}".format(fmt_int(n_empty)))
    lines.append("字段数 != 5: {}".format(fmt_int(n_bad_field_count)))
    lines.append("空字段行: {}".format(fmt_int(n_empty_field)))
    lines.append("无法解析为整数: {}".format(fmt_int(n_parse_error)))
    lines.append("负值行 (uid/item/ts 任一为负): {}".format(fmt_int(n_negative)))
    lines.append("  说明: 负值行单独计数后跳过, 不计入干净行/行为分布/时间越界/重复统计")
    lines.append("非法行为行: {}".format(fmt_int(n_illegal_behavior)))
    lines.append("干净行 (合法 5 字段, 非负, 合法行为): {}".format(fmt_int(n_valid)))
    lines.append("")
    lines.append("--- 行为类型分布 ---")
    for b in VALID_BEHAVIORS:
        lines.append("  {}: {}".format(b, fmt_int(beh_counter.get(b, 0))))
    illegal_beh = {k: v for k, v in beh_counter.items() if k not in VALID_BEHAVIORS}
    lines.append("  非法行为明细: {}".format(
        dict(illegal_beh) if illegal_beh else "无"))
    lines.append("")
    lines.append("--- 重复记录统计 (仅统计, 不删除) ---")
    if args.check_duplicates:
        lines.append("完全重复记录: {} 个额外行 ({} 个重复 key)".format(
            fmt_int(dup_full_extra), fmt_int(dup_full_keys)))
        lines.append("(uid,item,ts) 重复: {} 个额外行 ({} 个重复 key)".format(
            fmt_int(dup_uit_extra), fmt_int(dup_uit_keys)))
        lines.append("  说明: 完全重复可能代表真实行为, 本脚本不删除; 是否处理需人工判断")
    else:
        lines.append("重复检测: 已关闭 (--no-check-duplicates), 未统计")
        lines.append("  说明: 全量运行时关闭可保持内存 O(1); 需要时请单独对样本开启")
    lines.append("")
    lines.append("--- ID 范围与 int32 溢出 ---")
    lines.append("user_id 最大值: {} ({} int32 上限)".format(
        fmt_int(max_uid) if max_uid >= 0 else "无",
        "<= " if max_uid <= INT32_MAX else ">"))
    lines.append("item_id 最大值: {} ({} int32 上限)".format(
        fmt_int(max_item) if max_item >= 0 else "无",
        "<= " if max_item <= INT32_MAX else ">"))
    lines.append("uid > int32 上限的行数: {}".format(fmt_int(n_uid_overflow)))
    lines.append("item > int32 上限的行数: {}".format(fmt_int(n_item_overflow)))
    lines.append("")
    lines.append("--- 时间范围 (仅统计, 不删除) ---")
    lines.append("参照区间 (K-means 口径): [{}, {})".format(MIN_TS, MAX_TS))
    lines.append("timestamp 最小值: {}".format(ts_min if ts_min is not None else "无"))
    lines.append("timestamp 最大值: {}".format(ts_max if ts_max is not None else "无"))
    lines.append("timestamp < {} 的行数: {}".format(MIN_TS, fmt_int(n_ts_below)))
    lines.append("timestamp >= {} 的行数: {}".format(MAX_TS, fmt_int(n_ts_above)))
    lines.append("越界合计: {}".format(fmt_int(n_ts_below + n_ts_above)))
    lines.append("")
    lines.append("--- 与现有 MR1 的关系 ---")
    lines.append("MR1 EventMapper 会跳过: 字段数<5、uid/ts 解析失败、非法行为")
    lines.append("MR1 不做过时间过滤、不做过完全重复去重 (与本统计口径无关, 仅说明)")
    lines.append("=" * 72)

    report = "\n".join(lines) + "\n"
    with open(report_path, "w", encoding="utf-8") as out:
        out.write(report)

    print(report)
    print("[输出] 报告已写入: {}".format(os.path.abspath(report_path)))


if __name__ == "__main__":
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    main()
