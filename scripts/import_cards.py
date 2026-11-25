#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第④步 · 导入：把抽取出的 JSONL 卡片导回数据库 cards / card_skips 表。

- 只读 JSONL，写 cards / card_skips 表（record_id 即 thread_id，直接映射）
- 不做任何 AI，不校验字段语义（校验由抽检环节人工做）
"""
import sqlite3
import json
import os
import argparse
from datetime import datetime, timezone

import pipeline_config as PC
_P = PC.paths()
DB = _P["db"]
OUT_DIR = _P["out_dir"]

CARD_FIELDS = [
    "source_type", "date_first", "date_last", "company", "domain", "country",
    "contacts", "company_type", "industry", "end_customer", "signals", "products",
    "competitors", "quantity_or_value", "product_feedback", "trend", "summary",
    "open_question", "status", "our_people", "owner_involved", "evidence", "notes",
]


def init_db():
    con = sqlite3.connect(DB)
    con.execute("""
        CREATE TABLE IF NOT EXISTS cards (
            card_id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id TEXT NOT NULL,
            thread_id TEXT,
            source_type TEXT,
            date_first TEXT,
            date_last TEXT,
            company TEXT,
            domain TEXT,
            country TEXT,
            contacts TEXT,
            company_type TEXT,
            industry TEXT,
            end_customer TEXT,
            signals TEXT,
            products TEXT,
            competitors TEXT,
            quantity_or_value TEXT,
            product_feedback TEXT,
            trend TEXT,
            summary TEXT,
            open_question TEXT,
            status TEXT,
            our_people TEXT,
            owner_involved TEXT,
            evidence TEXT,
            notes TEXT,
            raw TEXT,
            batch TEXT,
            extracted_at TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS card_skips (
            record_id TEXT PRIMARY KEY,
            skip TEXT,
            batch TEXT,
            extracted_at TEXT
        )
    """)
    con.commit()
    return con


def store_card(con, obj, batch):
    rid = obj.get('record_id')
    if not rid:
        return 0
    cols = CARD_FIELDS
    values = []
    for c in cols:
        v = obj.get(c)
        if isinstance(v, (list, dict)):
            v = json.dumps(v, ensure_ascii=False)
        values.append(v)
    con.execute(
        f"INSERT OR REPLACE INTO cards (record_id, thread_id, {','.join(cols)}, raw, batch, extracted_at) "
        f"VALUES (?, ?, {','.join(['?']*len(cols))}, ?, ?, ?)",
        [rid, rid] + values + [json.dumps(obj, ensure_ascii=False), batch,
                               datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')],
    )
    return 1


def store_skip(con, obj, batch):
    rid = obj.get('record_id')
    if not rid:
        return 0
    con.execute(
        "INSERT OR REPLACE INTO card_skips (record_id, skip, batch, extracted_at) VALUES (?,?,?,?)",
        [rid, obj.get('skip'), batch, datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')],
    )
    return 1


def main(path=None):
    """path 可为绝对路径（编排器调用）或 None（命令行用 batch 参数）。返回 0 成功 / 1 失败。"""
    if path is None:
        p = argparse.ArgumentParser()
        p.add_argument('batch', help='JSONL 文件名，如 m_batch_001.jsonl')
        args = p.parse_args()
        path = args.batch
        if not os.path.isabs(path):
            path = os.path.join(OUT_DIR, path)
    batch = os.path.basename(path)[:-6] if path.endswith('.jsonl') else os.path.basename(path)
    if not os.path.exists(path):
        print("文件不存在:", path)
        return 1

    con = init_db()
    cards = skips = bad = 0
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                bad += 1
                print("跳过坏行:", line[:80])
                continue
            if 'batch_stats' in obj:
                continue
            if 'skip' in obj:
                skips += store_skip(con, obj, batch)
            else:
                cards += store_card(con, obj, batch)
    con.commit()
    con.close()
    print(f"导入完成：卡片 {cards}，跳过 {skips}，坏行 {bad}")
    return 1 if bad else 0


if __name__ == '__main__':
    main()
