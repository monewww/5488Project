#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tiny_sample.py — 为 MapReduce / PageRank 模块生成确定性小样本

功能:
  从原始 UserBehavior CSV 或 .zip 中, 按 user_id % mod == 0 的确定性 hash 抽样规则
  生成小样本, 用于本地快速自测。抽样规则复刻现有 pagerank/data/make_sample.py
  (uid % mod == 0), 但本脚本独立实现, 不修改也不依赖该文件。

规则与约束:
  - 保留原始 5 列格式 (user_id,item_id,category_id,behavior_type,timestamp)
  - 不增加表头, 不改变字段顺序
  - 流式处理, 不把整个数据集读入内存
  - 不删除、不覆盖任何原始数据或已有输出; 目标文件已存在则报错退出

与 make_sample.py 的差异 (见 README):
  - 默认输出到 mapreduce_data_tools/out/tiny/, 支持 --outdir (make_sample 用必填 --output)
  - 增强 ZIP 异常处理 (空 ZIP / 损坏 ZIP) 与资源关闭
  - 输出前检查目标文件是否存在, 禁止覆盖
  - 不实现 --max-users (本工具按方案未要求)
  - 行内容保留原样, 仅去除行尾换行符 (合法 CSV 行与 make_sample 的 strip 结果一致)

用法:
  python3 mapreduce_data_tools/tiny_sample.py \
      --input UserBehavior.csv --mod 2200

  python3 mapreduce_data_tools/tiny_sample.py \
      --input UserBehavior.csv.zip --mod 330 --outdir mapreduce_data_tools/out/tiny330

输出:
  默认 mapreduce_data_tools/out/tiny/UserBehavior_tiny.csv
  (输出目录允许已存在, 但禁止覆盖同名文件)
"""

import argparse
import os
import sys
import time
import zipfile

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT_DIR = os.path.join(HERE, "out", "tiny")
DEFAULT_FILENAME = "UserBehavior_tiny.csv"


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
    """若目标文件已存在则报错, 禁止覆盖。"""
    if os.path.exists(path):
        sys.stderr.write(
            "[错误] 目标文件已存在, 禁止覆盖: {}\n"
            "       请使用 --outdir 指定其他目录或先手动移走该文件。\n".format(path))
        sys.exit(2)


def main():
    ap = argparse.ArgumentParser(
        description="UserBehavior 确定性 uid%%mod 小样本生成 (只读原始, 不覆盖)")
    ap.add_argument("--input", required=True, help="原始 CSV 或 .zip 路径")
    ap.add_argument("--mod", type=int, default=2200,
                    help="保留 user_id %% mod == 0 的用户 (默认 2200, 与 make_sample.py 一致)")
    ap.add_argument("--outdir", default=None,
                    help="输出目录 (默认 mapreduce_data_tools/out/tiny; 允许已存在, 但禁止覆盖同名文件)")
    ap.add_argument("--filename", default=DEFAULT_FILENAME,
                    help="输出文件名 (默认 {})".format(DEFAULT_FILENAME))
    args = ap.parse_args()

    if args.mod <= 0:
        sys.stderr.write("[错误] --mod 必须为正整数, 当前: {}\n".format(args.mod))
        sys.exit(2)

    input_path = args.input
    if not os.path.exists(input_path):
        sys.stderr.write("[错误] 找不到输入文件: {}\n".format(input_path))
        sys.exit(2)

    out_dir = args.outdir if args.outdir else DEFAULT_OUT_DIR
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, args.filename)
    # 目标已存在则报错 (在打开输入前先检查, 避免无谓读取)
    check_output_path(out_path)

    f, fname, closer = open_rows(input_path)
    t0 = time.time()
    n_read = 0
    n_kept = 0
    users = set()

    # 临时文件名: 同目录, 含 PID, 独占创建, 不会覆盖任何已存在文件
    tmp_path = "{}.tmp.{}".format(out_path, os.getpid())
    created_tmp = False          # 标记本次运行是否成功创建了临时文件
    try:
        # 以独占方式创建临时文件 ("x"): 若同名已存在则抛 FileExistsError, 绝不覆盖
        try:
            out = open(tmp_path, "x", encoding="utf-8", newline="")
        except FileExistsError:
            sys.stderr.write(
                "[错误] 临时文件已存在, 请先清理: {}\n".format(tmp_path))
            sys.exit(2)
        created_tmp = True
        with out:
            for raw in f:
                n_read += 1
                try:
                    line = raw.decode("utf-8")
                except UnicodeDecodeError:
                    continue
                # 只去除行尾换行符, 保留字段内容原样
                line = line.rstrip("\n").rstrip("\r")
                if line == "":
                    continue
                uid_s = line.split(",", 1)[0].strip()
                try:
                    uid = int(uid_s)
                except ValueError:
                    continue
                if uid % args.mod != 0:
                    continue
                out.write(line + "\n")
                users.add(uid)
                n_kept += 1

        # 原子且严格不覆盖地发布: os.link 在目标已存在时抛 FileExistsError,
        # 不会像 os.replace/os.rename 那样覆盖已存在文件。
        try:
            os.link(tmp_path, out_path)
        except FileExistsError:
            sys.stderr.write(
                "[错误] 目标文件在运行期间已出现, 拒绝覆盖: {}\n".format(out_path))
            sys.exit(2)
        except OSError as e:
            sys.stderr.write(
                "[错误] 发布输出文件失败: {}\n".format(e))
            sys.exit(2)
    finally:
        # 无论成功与否都关闭输入 (ZIP 时关闭 ZipFile)
        closer.close()
        # 只清理本次运行创建的临时文件名; 绝不删除其他进程/遗留文件
        if created_tmp:
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except OSError:
                pass

    dt = time.time() - t0
    size_mb = os.path.getsize(out_path) / 1024.0 / 1024.0
    print("[sample] 源文件={} 扫描={:,} 行 保留={:,} 行 用户={:,} 模数={} 耗时={:.1f}s".format(
        fname, n_read, n_kept, len(users), args.mod, dt))
    print("[sample] 输出={} ({:.2f} MB)".format(os.path.abspath(out_path), size_mb))


if __name__ == "__main__":
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(errors="replace")
    main()
