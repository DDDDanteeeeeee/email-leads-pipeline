#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第③步健壮补抓（step3e）：专补 step3d 因长连接断开而跳过的剩余邮件。

与 step3d 的区别：
  1) 每个文件夹开始前主动重连一个新 IMAP 连接（避免长连接累积断连）
  2) BS 批 / 正文批失败时最多重试 5 次（递增退避 2/4/6/8/10 秒），重连后重新 select
  3) 优先处理待抓量大的文件夹（Sent Messages 先）
  4) 每 N 个文件夹强制重连一次

复用 step3d/step3c/step3_bodies 的解析与清洗函数，判据与 step3d 完全一致：
  候选 = kept=1 且 is_dup=0 且无 exclude_reason 且 bodies 无记录 且 errors 无「正文为空」

只读约束：EXAMINE(readonly=True) + BODY.PEEK，绝不 STORE/COPY/MOVE/EXPUNGE/APPEND。
"""
import argparse
import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from step3_bodies import clean, load_env, log, mailbox_arg, parse_bs, text_parts  # noqa: E402
from step3c_bodies import connect, decode_part, parse_bodies  # noqa: E402
from step3d_retry_bodies import MIN_CAL, MIN_HTML, MIN_PLAIN, PEEK_BYTES, pick_part  # noqa: E402

import pipeline_config as PC
DB_PATH = PC.paths()["db"]
BATCH = 20
MAX_RETRY = 5
REFRESH_EVERY = 8  # 每处理 N 个文件夹强制重连


def fresh_client(env, folder):
    """建新连接 + login + select(readonly)，失败返回 None。"""
    try:
        cl = connect(env)
        cl.login(env["IMAP_USER"], env["IMAP_PASSWORD"])
        typ, _ = cl.select(mailbox_arg(folder), readonly=True)
        if typ == "OK":
            return cl
        try:
            cl.logout()
        except Exception:
            pass
    except Exception:
        pass
    return None


def main():
    global DB_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--db")
    ap.add_argument("--batch", type=int, default=BATCH)
    ap.add_argument("--folders")
    ap.add_argument("--limit", type=int)
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
    log("step3e 待补正文 %d 封" % len(todo))
    if not todo:
        con.close()
        return

    by_folder = {}
    for r in todo:
        by_folder.setdefault(r["folder"], []).append(r)
    order = sorted(by_folder.items(), key=lambda kv: -len(kv[1]))  # 大的先

    stat = {"plain": 0, "html": 0, "calendar": 0, "empty": 0, "fail": 0}
    t_start = time.time()
    client = None
    folder_count = 0

    for folder, items in order:
        uid_map = {r["uid"]: r for r in items}
        uids = [r["uid"] for r in items]
        log("== %s（%d 封）" % (folder, len(uids)))

        # 每个文件夹都建新连接（健壮性优先，牺牲一点连接开销）
        client = fresh_client(env, folder)
        if client is None:
            for attempt in range(1, MAX_RETRY + 1):
                log("  select 失败，重连 %d/%d" % (attempt, MAX_RETRY))
                time.sleep(2 * attempt)
                client = fresh_client(env, folder)
                if client is not None:
                    break
            if client is None:
                log("  放弃该文件夹（%d 封未取）" % len(uids))
                stat["fail"] += len(uids)
                continue
        folder_count += 1

        f_ok = 0
        for bi in range(0, len(uids), bs_size):
            chunk = uids[bi:bi + bs_size]
            ins, empties, fails = [], [], []

            # ---- BS 批（带重试）----
            bs_map = {}
            for attempt in range(1, MAX_RETRY + 1):
                try:
                    typ, data = client.uid("fetch", ",".join(map(str, chunk)), "(BODYSTRUCTURE)")
                    if typ == "OK":
                        bs_map = parse_bs(data)
                        if bs_map:
                            break
                except Exception as e:
                    log("  BS批异常 %s (尝试%d)" % (type(e).__name__, attempt))
                time.sleep(2 * attempt)
                if attempt < MAX_RETRY:
                    nc = fresh_client(env, folder)
                    if nc is not None:
                        try:
                            client.logout()
                        except Exception:
                            pass
                        client = nc

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

            # ---- 正文批（带重试）----
            for part, plist in sorted(by_part.items(), key=lambda kv: -len(kv[1])):
                meta_by_uid = {u: p for u, p in plist}
                puids = [u for u, _ in plist]
                for gi in range(0, len(puids), bs_size):
                    g = puids[gi:gi + bs_size]
                    got = {}
                    for attempt in range(1, MAX_RETRY + 1):
                        try:
                            typ, data = client.uid(
                                "fetch", ",".join(map(str, g)),
                                "(BODY.PEEK[%s]<0.%d>)" % (part, PEEK_BYTES))
                            if typ == "OK":
                                got = parse_bodies(data)
                                if got:
                                    break
                        except Exception as e:
                            log("  正文批异常 %s (尝试%d)" % (type(e).__name__, attempt))
                        time.sleep(2 * attempt)
                        if attempt < MAX_RETRY:
                            nc = fresh_client(env, folder)
                            if nc is not None:
                                try:
                                    client.logout()
                                except Exception:
                                    pass
                                client = nc

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
                    time.sleep(random.uniform(0.05, 0.15))

            if ins:
                c.executemany("""
                    INSERT OR REPLACE INTO bodies
                    (folder, uid, thread_id, content_type, text, char_count, fetched_at,
                     sender_title, sender_company, quoted_fallback, raw_chars)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, ins)
                c.executemany("DELETE FROM errors WHERE folder=? AND uid=?",
                              [(r[0], r[1]) for r in ins])
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
            log("  %d/%d 入库 %d（累计本夹失败 %d）" % (done_n, len(uids), f_ok, stat["fail"]))

        if client is not None:
            try:
                client.logout()
            except Exception:
                pass
            client = None

    try:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        pass
    total = c.execute("SELECT COUNT(*) FROM bodies").fetchone()[0]
    con.close()
    log("step3e 完成：plain %d / html %d / 日历 %d / 真空 %d / 仍失败 %d｜bodies 累计 %d｜用时 %.1f 分" % (
        stat["plain"], stat["html"], stat["calendar"], stat["empty"], stat["fail"],
        total, (time.time() - t_start) / 60))


if __name__ == "__main__":
    main()
