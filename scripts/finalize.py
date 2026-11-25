#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全链路收尾：只在「所有批单元都做完」之后跑。

做四件事：
  1) 用 run_batches 的真实状态算完成度，插进第④步报告开头（## 0）
  2) 重新生成报告（report_step4.py）
  3) 数据库 WAL 落盘
  4) 数据库与报告 chmod 600（含客户名/金额，属敏感文件）

不发布任何在线链接，不发送任何邮件。
"""

import csv
import json
import os
import sqlite3
import subprocess
import sys

import pipeline_config as PC
_P = PC.paths()
BASE = _P["workspace"]
HERE = _P["program_dir"]
DB = _P["db"]
REPORT = os.path.join(BASE, "第4步_全量抽取报告.md")
OUTDIR = _P["out_dir"]
PY = sys.executable

sys.path.insert(0, HERE)
import run_batches as RB  # noqa: E402

CARD_COLS = [
    "record_id", "thread_id", "date_first", "date_last", "company", "domain", "country",
    "company_type", "industry", "end_customer", "signals", "products", "competitors",
    "quantity_or_value", "summary", "open_question", "status", "owner_involved",
    "evidence", "folder", "batch",
]
JSON_COLS = {"signals", "products", "competitors", "evidence"}


def export_cards():
    """把卡片导成 JSONL + 公司汇总 CSV（任务书要求的交付物）"""
    os.makedirs(OUTDIR, exist_ok=True)
    con = sqlite3.connect(DB, timeout=180)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT %s FROM cards ORDER BY date_last DESC" % ",".join(CARD_COLS)).fetchall()
    jl = os.path.join(OUTDIR, "cards_all.jsonl")
    with open(jl, "w", encoding="utf-8") as f:
        for r in rows:
            d = {}
            for k in CARD_COLS:
                v = r[k]
                if k in JSON_COLS:
                    try:
                        v = json.loads(v) if v else []
                    except Exception:
                        v = []
                d[k] = v
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    agg = con.execute("""
        SELECT company, MAX(domain) domain, COUNT(*) cards,
               MIN(date_first) first_date, MAX(date_last) last_date,
               SUM(CASE WHEN status='已结束' THEN 1 ELSE 0 END) closed,
               SUM(CASE WHEN open_question IS NOT NULL AND open_question<>'' THEN 1 ELSE 0 END) has_todo
        FROM cards WHERE company IS NOT NULL AND company<>''
        GROUP BY company ORDER BY cards DESC, last_date DESC
    """).fetchall()
    csvp = os.path.join(OUTDIR, "公司汇总.csv")
    with open(csvp, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["公司", "域名", "卡片数", "首次往来", "最近往来", "已结束", "有待办"])
        for r in agg:
            w.writerow([r["company"], r["domain"] or "", r["cards"], r["first_date"] or "",
                        r["last_date"] or "", r["closed"], r["has_todo"]])
    con.close()
    for p in (jl, csvp):
        try:
            os.chmod(p, 0o600)
        except Exception:
            pass
    print("已导出卡片 %d 张 → %s，公司汇总 %d 家 → %s" % (len(rows), jl, len(agg), csvp))
    return len(rows)


def batch_progress():
    imported = RB.load_state()
    us = RB.units()
    st = {"pending": 0, "partial": 0, "ready": 0, "done": 0}
    done_threads = total_threads = 0
    for u in us:
        s, _, _ = RB.scan(u, imported)
        st[s] += 1
        total_threads += u["n"]
        if s == "done":
            done_threads += u["n"]
    return st, len(us), done_threads, total_threads


def main():
    st, n_units, done_thr, total_thr = batch_progress()

    # 1) 重新生成报告
    subprocess.run([PY, os.path.join(HERE, "report_step4.py")], check=False)

    # 2) 采集数字
    con = sqlite3.connect(DB, timeout=120)
    c = con.cursor()
    cards = c.execute("select count(*) from cards").fetchone()[0]
    skips = c.execute("select count(*) from card_skips").fetchone()[0]
    cov = c.execute("select count(distinct record_id) from (select record_id from cards "
                    "union all select record_id from card_skips)").fetchone()[0]
    threads = c.execute("select count(*) from threads").fetchone()[0]
    bodies = c.execute("select count(*) from bodies").fetchone()[0]
    kept = c.execute("select count(*) from messages where kept=1 and is_dup=0 "
                     "and exclude_reason is null").fetchone()[0]
    empty = c.execute("select count(*) from errors where error like '%正文为空%'").fetchone()[0]
    con.close()

    pct = cov * 100.0 / max(threads, 1)
    head = [
        "## 0. 完成度",
        "",
        "| 项 | 数值 |",
        "|---|---|",
        "| 批单元 | **%d / %d 完成**（pending %d / partial %d / ready %d） |" % (
            st["done"], n_units, st["pending"], st["partial"], st["ready"]),
        "| 线程覆盖 | **%d / %d（%.1f%%）** |" % (cov, threads, pct),
        "| 卡片 | %s 张 / 跳过 %s 条 |" % (f"{cards:,}", f"{skips:,}"),
        "| 正文覆盖 | %s / %s 封（%.1f%%）；确无正文 %s 封 |" % (
            f"{bodies:,}", f"{kept:,}", bodies * 100.0 / max(kept, 1), f"{empty:,}"),
        "",
    ]
    if st["pending"] or st["partial"] or st["ready"]:
        head.insert(0, "> ⚠️ 本次运行**尚未跑完全部批次**，下表为当前进度。")
        head.insert(1, "")

    # 3) 插到标题与「## 1. 总量」之间
    if os.path.exists(REPORT):
        txt = open(REPORT, encoding="utf-8").read()
        marker = "## 1. 总量"
        block = "\n".join(head) + "\n"
        if "## 0. 完成度" in txt:
            pre, rest = txt.split("## 0. 完成度", 1)
            # 先清掉上一轮可能写进去的「尚未跑完」提示，否则全部完成后还留着
            pre = "\n".join(ln for ln in pre.split("\n")
                            if not ln.startswith("> ⚠️ 本次运行"))
            pre = pre.rstrip() + "\n\n"
            _, rest = rest.split(marker, 1)
            txt = pre + block + marker + rest
        elif marker in txt:
            pre, rest = txt.split(marker, 1)
            txt = pre + block + marker + rest
        else:
            txt = block + txt
        open(REPORT, "w", encoding="utf-8").write(txt)

    # 4) 导出交付物（卡片 JSONL + 公司汇总）
    try:
        export_cards()
    except Exception as e:
        print("导出交付物失败：%s" % e)

    # 5) 落盘 + 收紧权限
    con = sqlite3.connect(DB, timeout=180)
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    for p in (DB, REPORT):
        try:
            os.chmod(p, 0o600)
        except Exception as e:
            print("chmod 失败 %s: %s" % (p, e))

    print("收尾完成：批单元 %d/%d，线程覆盖 %.1f%%，卡片 %d，报告已重生成并 chmod 600" % (
        st["done"], n_units, pct, cards))
    return 0 if not (st["pending"] or st["partial"] or st["ready"]) else 1


if __name__ == "__main__":
    sys.exit(main())
