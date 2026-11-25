#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第③步补抓（v3d）：只补「第③步：未取到正文」的那批邮件。

v3c 的坑：大量邮件 text/plain 是 2 字节空壳（Outlook/PowerPoint 导出常见），
真正的正文在 text/html（20~46 KB）。v3c 无条件优先 text/plain，取到空壳就判失败。
本版规则：
  1) text/plain 且 size >= 200  -> 用它
  2) 否则 text/html 且 size >= 200 -> 用它
  3) 否则 text/calendar 且 size >= 100 -> 用它（会议邀请正文在日历里）
  4) 都没有 -> 该邮件确实无正文，记 "正文为空"，不再重试

成功入库后删除对应 errors 记录，进度以 bodies 实际行数为准。

只读约束：EXAMINE(readonly=True) + BODY.PEEK，绝不 STORE/COPY/MOVE/EXPUNGE/APPEND。
"""

import argparse
import os
import random
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from step3_bodies import (  # noqa: E402
    clean, load_env, log, mailbox_arg, parse_bs, text_parts,
)
from step3c_bodies import connect, decode_part, parse_bodies  # noqa: E402

import pipeline_config as PC
DB_PATH = PC.paths()["db"]
BATCH = 100
PEEK_BYTES = 16384
PAUSE = (0.05, 0.15)
MIN_PLAIN = 200
MIN_HTML = 200
MIN_CAL = 100


def pick_part(bs):
    """按「非空壳」规则挑正文节点。返回 (part, reason)。"""
    parts = [p for p in text_parts(bs) if p.get("part")]
    if not parts:
        return None, "无 text 节点"

    def biggest(kind, minimum):
        cand = [p for p in parts if p["type"] == kind and (p["size"] or 0) >= minimum]
        if not cand:
            return None
        cand.sort(key=lambda p: -(p["size"] or 0))
        return cand[0]

    p = biggest("text/plain", MIN_PLAIN)
    if p:
        return p, "plain"
    p = biggest("text/html", MIN_HTML)
    if p:
        return p, "html"
    p = biggest("text/calendar", MIN_CAL)
    if p:
        return p, "calendar"
    return None, "正文为空"


def main():
    global DB_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--db")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--folders")
    ap.add_argument("--batch", type=int, default=BATCH)
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--of", type=int, default=1)
    args = ap.parse_args()
    if args.db:
        DB_PATH = args.db
    bs_size = args.batch

    env = load_env()
    con = sqlite3.connect(DB_PATH, timeout=180)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.row_factory = sqlite3.Row
    c = con.cursor()

    # 候选：保留邮件 - 已有正文 - 已判「正文为空」的
    q = """
      SELECT m.folder, m.uid, m.thread_id
      FROM messages m
      LEFT JOIN bodies b ON m.folder=b.folder AND m.uid=b.uid
      WHERE m.kept=1 AND m.is_dup=0 AND m.exclude_reason IS NULL
        AND b.uid IS NULL
        AND NOT EXISTS (SELECT 1 FROM errors e WHERE e.folder=m.folder AND e.uid=m.uid
                        AND e.error LIKE '%正文为空%')
    """
    params = []
    if args.folders:
        fs = [x.strip() for x in args.folders.split(",") if x.strip()]
        q += " AND m.folder IN (%s)" % ",".join("?" * len(fs))
        params += fs
    q += " ORDER BY m.folder, m.date_utc DESC"
    if args.limit:
        q += " LIMIT %d" % args.limit
    todo = [dict(r) for r in c.execute(q, params)]
    if args.of > 1:
        todo = [r for i, r in enumerate(todo) if i % args.of == args.shard]
    log("待补正文 %d 封%s" % (len(todo), " [分片 %d/%d]" % (args.shard, args.of) if args.of > 1 else ""))
    if not todo:
        con.close()
        return

    by_folder = {}
    for r in todo:
        by_folder.setdefault(r["folder"], []).append(r)
    order = sorted(by_folder.items(), key=lambda kv: -len(kv[1]))

    client = connect(env)
    client.login(env["IMAP_USER"], env["IMAP_PASSWORD"])

    stat = {"plain": 0, "html": 0, "calendar": 0, "empty": 0, "fail": 0}
    t_start = time.time()
    for folder, items in order:
        uid_map = {r["uid"]: r for r in items}
        uids = [r["uid"] for r in items]
        log("== %s（%d 封）" % (folder, len(uids)))
        try:
            typ, _ = client.select(mailbox_arg(folder), readonly=True)
        except Exception:
            try:
                client = connect(env)
                client.login(env["IMAP_USER"], env["IMAP_PASSWORD"])
                typ, _ = client.select(mailbox_arg(folder), readonly=True)
            except Exception:
                log("  重连失败，跳过该文件夹")
                continue
        if typ != "OK":
            log("  EXAMINE 失败，跳过")
            continue

        f_ok = 0
        for bi in range(0, len(uids), bs_size):
            chunk = uids[bi:bi + bs_size]
            ins, empties, fails = [], [], []

            bs_map = {}
            try:
                typ, data = client.uid("fetch", ",".join(map(str, chunk)), "(BODYSTRUCTURE)")
                if typ == "OK":
                    bs_map = parse_bs(data)
            except Exception as e:
                log("  BS 批异常 %s" % type(e).__name__)
                time.sleep(2)
                try:
                    client = connect(env)
                    client.login(env["IMAP_USER"], env["IMAP_PASSWORD"])
                    client.select(mailbox_arg(folder), readonly=True)
                except Exception:
                    pass

            by_part = {}
            for uid in chunk:
                bs = bs_map.get(uid)
                if not bs:
                    fails.append(uid)
                    continue
                p, why = pick_part(bs)
                if not p:
                    empties.append(uid)
                    continue
                by_part.setdefault(p["part"], []).append((uid, p))

            for part, plist in sorted(by_part.items(), key=lambda kv: -len(kv[1])):
                puids = [u for u, _ in plist]
                meta_by_uid = {u: p for u, p in plist}
                for gi in range(0, len(puids), bs_size):
                    g = puids[gi:gi + bs_size]
                    try:
                        typ, data = client.uid(
                            "fetch", ",".join(map(str, g)),
                            "(BODY.PEEK[%s]<0.%d>)" % (part, PEEK_BYTES))
                        if typ != "OK":
                            continue
                        got = parse_bodies(data)
                    except Exception as e:
                        log("  正文批异常 %s" % type(e).__name__)
                        time.sleep(2)
                        try:
                            client = connect(env)
                            client.login(env["IMAP_USER"], env["IMAP_PASSWORD"])
                            client.select(mailbox_arg(folder), readonly=True)
                        except Exception:
                            pass
                        continue

                    for uid in g:
                        raw = got.get(uid)
                        if not raw:
                            fails.append(uid)
                            continue
                        meta = meta_by_uid[uid]
                        try:
                            txt = decode_part(raw, meta)
                        except Exception:
                            txt = None
                        if not txt or not txt.strip():
                            fails.append(uid)
                            continue
                        body, title, company, fb = clean(txt)
                        if not body.strip():
                            empties.append(uid)
                            continue
                        ins.append((
                            folder, uid, uid_map[uid]["thread_id"], meta["type"],
                            body, len(body),
                            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            title, company, fb, len(txt),
                        ))
                        k = "calendar" if meta["type"] == "text/calendar" else (
                            "html" if meta["type"] == "text/html" else "plain")
                        stat[k] += 1
                    time.sleep(random.uniform(*PAUSE))

            if ins:
                c.executemany("""
                    INSERT OR REPLACE INTO bodies
                    (folder, uid, thread_id, content_type, text, char_count, fetched_at,
                     sender_title, sender_company, quoted_fallback, raw_chars)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, ins)
                c.executemany("DELETE FROM errors WHERE folder=? AND uid=?", [(r[0], r[1]) for r in ins])
            if empties:
                c.executemany(
                    "INSERT OR REPLACE INTO errors (folder, uid, error, time) VALUES (?,?,?,?)",
                    [(folder, u, "第③步：正文为空（无 text 正文，可能只有附件）",
                      datetime.now().isoformat()) for u in set(empties)])
                stat["empty"] += len(set(empties))
            if fails:
                stat["fail"] += len(fails)
            con.commit()

            f_ok += len(ins)
            done_n = bi + len(chunk)
            rate = (stat["plain"] + stat["html"] + stat["calendar"] + stat["empty"]
                    + stat["fail"]) / max(time.time() - t_start, 1)
            log("  %d/%d 入库 %d（plain %d html %d cal %d）空 %d 失败 %d | %.2f 封/秒" % (
                done_n, len(uids), f_ok, stat["plain"], stat["html"], stat["calendar"],
                stat["empty"], stat["fail"], rate))

    try:
        client.logout()
    except Exception:
        pass
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    total = c.execute("SELECT COUNT(*) FROM bodies").fetchone()[0]
    con.close()
    log("补抓完成：plain %d / html %d / 日历 %d / 真空 %d / 仍失败 %d｜bodies 累计 %d｜用时 %.1f 分" % (
        stat["plain"], stat["html"], stat["calendar"], stat["empty"], stat["fail"],
        total, (time.time() - t_start) / 60))


if __name__ == "__main__":
    main()
