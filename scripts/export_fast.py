#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第④步 批文件导出（快速版）。

export_v3.py 每个线程要跑 2 次 SQL，1.1 万线程要 2 小时。
本版一次性把「线程 -> 消息 -> 正文」全读进内存，纯 Python 组装，秒级完成。

规则与 export_v3 一致：
- 线程输入：第一封 + 最后 6 封全文，中间每封前 300 字，总量上限 10,000 字
- 顺序：2025 及以后 -> 2024 -> 2023；段内重点客户优先，再按最后日期倒序
- 只导出正文齐全的线程（--ready-only），支持多轮增量
- 已有卡片/已标跳过的线程不再导出
"""

import argparse
import json
import os
import re
import sqlite3

import pipeline_config as PC
_P = PC.paths()
DB = _P["db"]
OUT_DIR = _P["out_dir"]

THREAD_CAP = 10000
MID_HEAD = 300
TAIL_N = 6

KEY_CUSTOMER_DOMAINS = {
    "dolby.com", "midwich.com", "midwich.us", "broadfield.com", "jbanda.com",
    "bhphotovideo.com", "adorama.com", "sfm.ca", "vizrt.com", "telescript.com",
    "fortinge.com", "artisight.com", "novomatic.com", "nv5.com", "nbcuni.com",
    "janestreet.com", "starin.biz", "gpinnacle.com",
}


def seg_of(date_str):
    y = (date_str or "")[:4]
    return "2025plus" if y >= "2025" else (y or "2023")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chars", type=int, default=250000)
    ap.add_argument("--tag", default="p1_")
    ap.add_argument("--seg")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--ready-only", action="store_true")
    ap.add_argument("--out", default=OUT_DIR)
    ap.add_argument("--db", default=DB)
    ap.add_argument("--exclude-index", help="已有的 _index.json：其中出现过的线程一律不再导出")
    ap.add_argument("--only", help="JSON 文件，内含线程 id 列表：只导出这些线程，已出卡的也重导（用于增量里有新回复的老线程）")
    args = ap.parse_args()

    con = sqlite3.connect(args.db, timeout=180)
    con.row_factory = sqlite3.Row
    c = con.cursor()

    # 1) 线程
    threads = {r["thread_id"]: dict(r) for r in c.execute("""
        SELECT thread_id, subject, first_date, last_date, message_count,
               external_domains, need_triage, folder_label
        FROM threads""")}

    # 2) 分诊标签
    labels = {r["thread_id"]: r["label"] for r in c.execute(
        "SELECT thread_id,label FROM triage WHERE label IS NOT NULL")}

    # 3) 已出卡 / 已跳过
    done = {r[0] for r in c.execute("SELECT DISTINCT record_id FROM cards")}
    done |= {r[0] for r in c.execute("SELECT DISTINCT record_id FROM card_skips")}

    # 3b) 已在既有批文件里的线程（防止补导出时重复抽）
    if args.exclude_index and os.path.exists(args.exclude_index):
        try:
            idx = json.load(open(args.exclude_index, encoding="utf-8"))
            for v in idx.values():
                done |= set(v.get("ids") or [])
            print("已排除既有批次中的 %d 个线程" % len(done), flush=True)
        except Exception as e:
            print("读排除索引失败：%s" % e, flush=True)

    # 4) 全部保留邮件（一次读完）
    #    「正文为空」的邮件（只有附件/日历、确无文本）不算缺口，否则它所在线程永远导不出去
    empty_ok = {(r[0], r[1]) for r in c.execute(
        "SELECT folder, uid FROM errors WHERE error LIKE '%正文为空%'")}
    msgs = {}
    missing = set()
    cur = c.execute("""
        SELECT m.thread_id, m.folder, m.uid, m.date_utc, m.from_addr, m.from_name,
               b.text, b.char_count
        FROM messages m
        LEFT JOIN bodies b ON m.folder=b.folder AND m.uid=b.uid
        WHERE m.kept=1 AND m.is_dup=0""")
    for r in cur:
        tid = r["thread_id"]
        txt = r["text"]
        if txt is None and (r["folder"], r["uid"]) not in empty_ok:
            missing.add(tid)
        msgs.setdefault(tid, []).append((r["date_utc"] or "", r["folder"], r["uid"],
                                         r["from_addr"] or "-", txt or ""))

    for k in msgs:
        msgs[k].sort(key=lambda x: x[0])

    # 5) 选线程
    only = None
    if args.only:
        only = set(json.load(open(args.only, encoding="utf-8")))
        done = set()
    todo = []
    for tid, t in threads.items():
        if only is not None and tid not in only:
            continue
        if tid in done:
            continue
        if t["need_triage"] == 1 and labels.get(tid) not in ("业务", "拿不准"):
            continue
        if args.ready_only and tid in missing:
            continue
        if args.seg and seg_of(t["first_date"]) != args.seg:
            continue
        todo.append(t)

    def sort_key(t):
        seg = seg_of(t["first_date"])
        order = {"2025plus": 0, "2024": 1, "2023": 2}.get(seg, 3)
        try:
            doms = set(json.loads(t["external_domains"] or "[]"))
        except Exception:
            doms = set()
        return (order, 0 if (doms & KEY_CUSTOMER_DOMAINS) else 1, t["last_date"] or "")

    todo.sort(key=sort_key)
    if args.limit:
        todo = todo[:args.limit]
    print("待导出线程 %d（总 %d，已出卡 %d，正文未齐 %d）" % (
        len(todo), len(threads), len(done), len(missing)), flush=True)

    os.makedirs(args.out, exist_ok=True)
    index = {}
    # 续号：避免再次补导出时覆盖已有批次（覆盖会造成 txt/jsonl 对不上）
    batch_no = 0
    import glob as _glob
    for p in _glob.glob(os.path.join(args.out, "%sbatch_*.txt" % args.tag)):
        m = re.search(r"batch_(\d+)\.txt$", p)
        if m:
            batch_no = max(batch_no, int(m.group(1)))
    if batch_no:
        print("已有 %d 个 %s 批次，从第 %d 号继续" % (batch_no, args.tag, batch_no + 1), flush=True)
    chunk, chars = [], 0
    seg_stat = {}

    def flush():
        nonlocal chunk, chars, batch_no
        if not chunk:
            return
        batch_no += 1
        name = "%sbatch_%03d.txt" % (args.tag, batch_no)
        text = "\n\n".join(x["rec"] for x in chunk)
        with open(os.path.join(args.out, name), "w", encoding="utf-8") as f:
            f.write(text)
        index[name] = {"ids": [x["id"] for x in chunk], "chars": len(text),
                       "segments": sorted({seg_of(x["first_date"]) for x in chunk})}
        print("  %s: %d 线程 / %d 字符" % (name, len(chunk), len(text)), flush=True)
        chunk, chars = [], 0

    for t in todo:
        tid = t["thread_id"]
        rows = msgs.get(tid) or []
        if not rows:
            continue
        n = len(rows)
        head = "### RECORD %s | email_thread" % tid
        meta = ["Subject: %s" % (t["subject"] or "(无主题)"),
                "Folder: %s" % (t["folder_label"] or "-"),
                "Span: %s ~ %s | %d messages" % (
                    (t["first_date"] or "-")[:10], (t["last_date"] or "-")[:10], n),
                ""]
        budget = THREAD_CAP - (len(head) + sum(len(x) + 1 for x in meta))
        parts = []
        for idx, (dt, folder, uid, frm, text) in enumerate(rows, 1):
            text = (text or "").strip() or "(无正文)"
            body = text if (idx == 1 or idx > n - TAIL_N) else (
                text[:MID_HEAD] + ("…" if len(text) > MID_HEAD else ""))
            block = "--- [%d/%d] %s | from: %s | folder: %s\n%s" % (
                idx, n, dt[:16], frm, folder, body)
            if len(block) > budget:
                parts.append(block[:max(budget, 0)])
                budget = 0
                break
            parts.append(block)
            budget -= len(block) + 2
            if budget <= 0:
                break
        rec = "\n".join([head] + meta + ["\n\n".join(parts)])
        seg_stat[seg_of(t["first_date"])] = seg_stat.get(seg_of(t["first_date"]), 0) + 1
        if chunk and chars + len(rec) > args.chars:
            flush()
        chunk.append({"id": tid, "first_date": t["first_date"], "rec": rec})
        chars += len(rec)

    flush()
    idx_path = os.path.join(args.out, "_index.json")
    merged = {}
    if os.path.exists(idx_path):
        try:
            merged = json.load(open(idx_path, encoding="utf-8"))
        except Exception:
            merged = {}
    merged.update(index)
    json.dump(merged, open(idx_path, "w", encoding="utf-8"), ensure_ascii=False, indent=1)

    total = sum(v["chars"] for v in index.values())
    n_threads = sum(len(v["ids"]) for v in index.values())
    print("共 %d 批，%d 线程，%s 字符｜年段 %s" % (
        batch_no, n_threads, f"{total:,}", seg_stat), flush=True)
    con.close()


if __name__ == "__main__":
    main()
