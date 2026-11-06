#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
prepare_mr_input.py — 为现有 Java MR1 (CooccurrenceJob) 准备清理后的输入 CSV

目的:
  生成一个"把 MR1 Mapper 本来就会丢弃的行提前去掉"的干净 CSV, 方便统计与复现。
  默认清理范围与 Java MR1 Mapper 的过滤逻辑**完全一致**, 因此默认模式下
  喂给 MR1 的结果应与直接喂原始 CSV 等价 (此等价性需实际边输出比对后才能断言,
  在此之前本工具不宣称等价)。

  本工具不修改、不重写 Java MR1; 只生成新的输入文件。

严格对齐的 Java MR1 规则 (对照 CooccurrenceJob.EventMapper):
  - line.toString().split(",") 后 f.length < 5 -> 跳过
  - f[3].trim() 不在行为白名单 -> 跳过
  - Integer.parseInt(f[0].trim()) 失败 -> 跳过
  - Long.parseLong(f[4].trim()) 失败 -> 跳过
  - sampleMod 由 Java MR1 自己处理, 本工具默认不抽样 (不擅自使用 uid%330)
  - 时间范围: 默认不过滤 (README 里的时间范围不作为默认过滤依据)

不做的处理 (与 MR1 一致, 本工具也不做):
  - 不删除重复记录 (默认只统计; 可用 --no-count-duplicates 关闭统计)
  - 不进行 session 划分 / 商品对计算 (那是 MR1 Reducer 与 MR2 的职责)
  - 不修改 item_id / 不删除 category_id
  - 不对有效记录的字段做重新格式化 (保留原始五列内容与顺序)

关于 Java split 的细节:
  Java String.split(",") 会**丢弃末尾的空字符串**, Python str.split(",") 不丢弃。
  为忠实复刻 "f.length < 5" 的判定, 本工具在统计字段数时模拟该行为
  (剥离末尾空字段后再计数), 但写出时仍保留原始整行内容不变。

用法:
  python3 mapreduce_data_tools/prepare_mr_input.py \
      --input UserBehavior.csv

  # 显式开启时间范围过滤 (必须给出起止, 无隐含默认值)
  python3 mapreduce_data_tools/prepare_mr_input.py \
      --input UserBehavior.csv --filter-time-range 1511539200 1512316800

输出:
  默认 mapreduce_data_tools/out/clean/UserBehavior.clean.csv
  (目录允许已存在, 但禁止覆盖同名文件)
  统计写入同目录 prepare_report.txt
"""

import argparse
import os
import sys
import time
import zipfile
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_DIR = os.path.join(HERE, "out", "clean")
DEFAULT_FILENAME = "UserBehavior.clean.csv"

VALID_BEHAVIORS = ("pv", "buy", "cart", "fav")


def open_rows(input_path):
    """逐行产出 bytes。支持 .zip (流式读取第一个成员) 与普通 CSV。

    返回 (fileobj, filename, closer)。调用方必须在 finally 中调用 closer.close()。
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


def check_output_path(path):
    if os.path.exists(path):
        sys.stderr.write(
            "[错误] 目标文件已存在, 禁止覆盖: {}\n"
            "       请使用 --outdir/--filename 指定其他路径或先手动移走该文件。\n".format(path))
        sys.exit(2)


def java_split_len(s):
    """模拟 Java String.split(',') 的字段数: 丢弃末尾的空字符串后再计数。

    仅用于复刻 "f.length < 5" 判定; 不用于改写输出。"""
    if s == "":
        return 1   # Java: "".split(",") -> [""]? 实际返回 [""]，长度 1
    parts = s.split(",")
    # Java 丢弃所有末尾空字符串
    while parts and parts[-1] == "":
        parts.pop()
    return len(parts)


def main():
    ap = argparse.ArgumentParser(
        description="为 Java MR1 准备清理后输入 (默认清理范围与 MR1 一致)")
    ap.add_argument("--input", required=True, help="原始 CSV 或 .zip")
    ap.add_argument("--outdir", default=None,
                    help="输出目录 (默认 mapreduce_data_tools/out/clean; 允许已存在, 禁止覆盖同名文件)")
    ap.add_argument("--filename", default=DEFAULT_FILENAME,
                    help="输出文件名 (默认 {})".format(DEFAULT_FILENAME))
    ap.add_argument("--filter-time-range", nargs=2, type=int, default=None,
                    metavar=("MIN_TS", "MAX_TS"),
                    help="可选: 仅保留 MIN_TS <= ts < MAX_TS 的记录 (默认关闭, 无隐含默认值)")
    ap.add_argument("--no-count-duplicates", dest="count_duplicates",
                    action="store_false", default=True,
                    help="不统计完全重复记录 (默认统计; 统计需保存唯一行, 全量时可关闭以省内存)")
    args = ap.parse_args()

    input_path = args.input
    if not os.path.exists(input_path):
        sys.stderr.write("[错误] 找不到输入文件: {}\n".format(input_path))
        sys.exit(2)

    use_time_filter = args.filter_time_range is not None
    if use_time_filter:
        fmin, fmax = args.filter_time_range
        if fmin >= fmax:
            sys.stderr.write("[错误] --filter-time-range 要求 MIN_TS < MAX_TS\n")
            sys.exit(2)

    out_dir = args.outdir if args.outdir else DEFAULT_OUT_DIR
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, args.filename)
    report_path = os.path.join(out_dir, "prepare_report.txt")
    check_output_path(out_path)
    check_output_path(report_path)

    f, fname, closer = open_rows(input_path)
    t0 = time.time()

    n_total = 0            # 总扫描行 (含空行)
    n_empty = 0            # 空行
    n_bad_field_count = 0  # 模拟 Java split 后 length<5
    n_bad_uid = 0          # uid 解析失败
    n_bad_ts = 0           # timestamp 解析失败
    n_bad_behavior = 0     # 非法行为
    n_time_filtered = 0    # 被时间范围过滤 (仅显式开启时)
    n_kept = 0             # 保留写出
    dup_counter = Counter() if args.count_duplicates else None

    tmp_path = out_path + ".tmp.{}".format(os.getpid())
    created_tmp = False
    try:
        try:
            out = open(tmp_path, "x", encoding="utf-8", newline="")
        except FileExistsError:
            sys.stderr.write("[错误] 临时文件已存在, 请先清理: {}\n".format(tmp_path))
            sys.exit(2)
        created_tmp = True
        with out:
            for raw in f:
                n_total += 1
                try:
                    line = raw.decode("utf-8")
                except UnicodeDecodeError:
                    n_bad_uid += 1   # 无法解码, 计入坏行 (无法解析任何字段)
                    continue
                line = line.rstrip("\n").rstrip("\r")
                if line == "":
                    n_empty += 1
                    continue
                # 复刻 Java split 的字段数判定
                if java_split_len(line) < 5:
                    n_bad_field_count += 1
                    continue
                fields = line.split(",")
                # 行为过滤 (f[3].trim())
                beh = fields[3].strip()
                if beh not in VALID_BEHAVIORS:
                    n_bad_behavior += 1
                    continue
                # uid / ts 解析 (f[0].trim() / f[4].trim())
                try:
                    uid = int(fields[0].strip())
                except ValueError:
                    n_bad_uid += 1
                    continue
                try:
                    ts = int(fields[4].strip())
                except ValueError:
                    n_bad_ts += 1
                    continue
                # 可选时间范围过滤 (默认关闭)
                if use_time_filter and not (fmin <= ts < fmax):
                    n_time_filtered += 1
                    continue
                # 保留: 写出原始整行 (不重新格式化字段, 保持顺序)
                out.write(line + "\n")
                n_kept += 1
                if dup_counter is not None:
                    dup_counter[line] += 1

        # 严格不覆盖地原子发布: os.link 在目标存在时抛 FileExistsError
        try:
            os.link(tmp_path, out_path)
        except FileExistsError:
            sys.stderr.write(
                "[错误] 目标文件在运行期间已出现, 拒绝覆盖: {}\n".format(out_path))
            sys.exit(2)
        except OSError as e:
            sys.stderr.write("[错误] 发布输出文件失败: {}\n".format(e))
            sys.exit(2)
    finally:
        closer.close()
        if created_tmp:
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except OSError:
                pass

    dup_extra = 0
    dup_keys = 0
    if dup_counter is not None:
        dup_extra = sum(c - 1 for c in dup_counter.values() if c > 1)
        dup_keys = sum(1 for c in dup_counter.values() if c > 1)

    dt = time.time() - t0
    size_mb = os.path.getsize(out_path) / 1024.0 / 1024.0

    lines = []
    lines.append("=" * 72)
    lines.append("为 Java MR1 准备清理后输入 — 统计报告")
    lines.append("=" * 72)
    lines.append("输入文件: {}".format(os.path.abspath(input_path)))
    lines.append("文件成员: {}".format(fname))
    lines.append("输出文件: {}".format(os.path.abspath(out_path)))
    lines.append("时间范围过滤: {}".format(
        "开启 [{}, {})".format(fmin, fmax) if use_time_filter else "关闭 (默认)"))
    lines.append("抽样: 无 (交由 Java MR1 的 sampleMod 处理)")
    lines.append("")
    lines.append("--- 清理统计 (对齐 MR1 EventMapper) ---")
    lines.append("总扫描行数: {}".format(n_total))
    lines.append("空行: {}".format(n_empty))
    lines.append("字段数<5 (按 Java split 口径): {}".format(n_bad_field_count))
    lines.append("uid 解析失败: {}".format(n_bad_uid))
    lines.append("timestamp 解析失败: {}".format(n_bad_ts))
    lines.append("非法 behavior_type: {}".format(n_bad_behavior))
    lines.append("时间范围过滤掉: {}".format(n_time_filtered))
    lines.append("有效保留行数: {}".format(n_kept))
    lines.append("")
    lines.append("--- 重复记录 (仅统计, 不删除) ---")
    if dup_counter is not None:
        lines.append("完全重复记录: {} 个额外行 ({} 个重复 key)".format(dup_extra, dup_keys))
        lines.append("  说明: 重复记录未被删除; 是否代表真实行为需人工判断")
    else:
        lines.append("重复统计: 已关闭 (--no-count-duplicates), 未统计; 重复记录未删除")
    lines.append("")
    lines.append("--- 说明 ---")
    lines.append("- 默认清理范围与 MR1 EventMapper 一致; 默认模式与直接喂原始 CSV 的")
    lines.append("  等价性需通过实际 MR1 边输出比对验证, 本工具不单方面宣称等价。")
    lines.append("- 输出保持原始五列顺序、逗号分隔、无表头, 未修改有效记录内容与顺序。")
    lines.append("- 未做 session 划分 / 商品对计算 / item_id 修改 / category_id 删除。")
    lines.append("")
    lines.append("耗时: {:.2f}s, 输出大小: {:.2f} MB".format(dt, size_mb))
    lines.append("=" * 72)

    report = "\n".join(lines) + "\n"
    try:
        with open(report_path, "x", encoding="utf-8") as fs:
            fs.write(report)
    except FileExistsError:
        sys.stderr.write(
            "[错误] 报告文件已存在, 禁止覆盖: {}\n".format(report_path))
        sys.exit(2)
    print(report)
    print("[输出] {}".format(os.path.abspath(out_path)))
    print("[报告] {}".format(os.path.abspath(report_path)))


if __name__ == "__main__":
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    main()
