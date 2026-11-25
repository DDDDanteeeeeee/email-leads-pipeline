#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第③步（v3c）：取正文 + 清洗。分批闭环、增量入库、可断点续跑。

每 100 封一个闭环：
  1) UID FETCH (BODYSTRUCTURE)      —— 只回结构，几十 KB
  2) 每封选正文叶子节点（text/plain 优先，其次 text/html）
  3) UID FETCH (BODY.PEEK[part]<0.12288>) —— 每封最多 12 KB 原文
  4) 解码 -> 清洗 -> 立即入库 -> commit

为什么不再全量预取：v3 版本把 5 万封的结构全攒内存、只在文件夹末尾入库，
一旦连接中断整批归零，且内存会涨到数百 MB。本版每 100 封落盘一次。

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
    clean, connect_db as _unused, decode_charset, encode_mutf7, html_to_text,
    load_env, log, mailbox_arg, parse_bs, text_parts,
)

import imaplib  # noqa: E402

import pipeline_config as PC
DB_PATH = PC.paths()["db"]
BATCH = 150
PEEK_BYTES = 8192   # 清洗后只留 2500 字，8 KB 原文足够；越少越省服务端带宽
PAUSE = (0.05, 0.15)
TIMEOUT = 90

UID_ANY = re.compile(rb"UID (\d+)")


def connect(env):
    return imaplib.IMAP4_SSL(env.get("IMAP_HOST", "imap.exmail.qq.com"), 993, timeout=TIMEOUT)


def parse_bodies(data):
    """UID FETCH (BODY[1]<0> {n}...) -> {uid: bytes}"""
    out = {}
    for item in data:
        if not isinstance(item, tuple):
            continue
        line, lit = item
        if not isinstance(line, (bytes, bytearray)):
            continue
        m = UID_ANY.search(line)
        if m and isinstance(lit, (bytes, bytearray)):
            out[int(m.group(1))] = bytes(lit)
    return out


B64_LIKE = re.compile(r"^[A-Za-z0-9+/=\r\n]{100,}$")


def b64_decode_loose(raw):
    """截断的 base64 长度常不是 4 的倍数，先补齐再解，否则会整段失败。"""
    import base64
    s = re.sub(rb"[^A-Za-z0-9+/=]", b"", raw).rstrip(b"=")
    if len(s) % 4 == 1:
        s = s[:-1]
    s += b"=" * ((-len(s)) % 4)
    return base64.b64decode(s)


def decode_part(raw, meta):
    """按 part 的 encoding/charset 解码出纯文本；失败返回 None（不落库垃圾）"""
    enc = (meta.get("encoding") or "").lower()
    if enc in ("base64", "b64"):
        try:
            import base64  # noqa: F401
            raw = b64_decode_loose(raw)
        except Exception:
            return None
    elif enc in ("quoted-printable", "quotedprintable", "qp"):
        import quopri
        try:
            raw = quopri.decodestring(raw)
        except Exception:
            return None
    # 兜底：没声明编码但内容本身就是 base64
    elif B64_LIKE.match(raw[:400].decode("ascii", "ignore")):
        try:
            raw = b64_decode_loose(raw)
        except Exception:
            return None
    txt = decode_charset(raw, meta.get("charset"))
    if (meta.get("type") or "").lower() == "text/html":
        txt = html_to_text(txt)
    else:
        txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    # 解码后仍是 base64 块 -> 视为失败，避免把乱码当正文
    probe = txt.strip()[:400]
    if len(probe) > 100 and B64_LIKE.match(probe):
        return None
    return txt


def pick_part(bs):
    parts = text_parts(bs)
    plain = [p for p in parts if p["type"] == "text/plain" and p["part"]]
    if plain:
        plain.sort(key=lambda p: -(p["size"] or 0))
        return plain[0]
    htmlp = [p for p in parts if p["type"] == "text/html" and p["part"]]
    if htmlp:
        htmlp.sort(key=lambda p: -(p["size"] or 0))
        return htmlp[0]
    return None


def main():
    global DB_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int)
    ap.add_argument("--folders")
    ap.add_argument("--db")
    ap.add_argument("--batch", type=int, default=BATCH)
    ap.add_argument("--shard", type=int, default=0, help="分片序号（0 起）")
    ap.add_argument("--of", type=int, default=1, help="分片总数")
    args = ap.parse_args()
    if args.db:
        DB_PATH = args.db
    batch_size = args.batch

    env = load_env()
    con = sqlite3.connect(DB_PATH, timeout=180)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.row_factory = sqlite3.Row
    c = con.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS bodies (
            folder TEXT, uid INTEGER, thread_id TEXT, content_type TEXT,
            text TEXT, char_count INTEGER, fetched_at TEXT,
            sender_title TEXT, sender_company TEXT, quoted_fallback INTEGER DEFAULT 0,
            raw_chars INTEGER DEFAULT 0,
            PRIMARY KEY (folder, uid)
        )
    """)
    con.commit()

    q = """SELECT folder, uid, thread_id FROM messages
           WHERE kept=1 AND is_dup=0 AND exclude_reason IS NULL"""
    params = []
    if args.folders:
        fs = [x.strip() for x in args.folders.split(",") if x.strip()]
        q += " AND folder IN (%s)" % ",".join("?" * len(fs))
        params += fs
    q += " ORDER BY folder, date_utc DESC"
    if args.limit:
        q += " LIMIT %d" % args.limit
    rows = c.execute(q, params).fetchall()

    done = {(r["folder"], r["uid"]) for r in c.execute(
        "SELECT folder, uid FROM bodies WHERE char_count > 0")}
    todo = [r for r in rows if (r["folder"], r["uid"]) not in done]
    if args.of > 1:
        todo = [r for i, r in enumerate(todo) if i % args.of == args.shard]
    log("待取正文 %d 封（已有 %d 封）%s" % (
        len(todo), len(done),
        " [分片 %d/%d]" % (args.shard, args.of) if args.of > 1 else ""))
    if not todo:
        con.close()
        return

    by_folder = {}
    for r in todo:
        by_folder.setdefault(r["folder"], []).append(r)
    order = sorted(by_folder.items(), key=lambda kv: -len(kv[1]))

    client = connect(env)
    client.login(env["IMAP_USER"], env["IMAP_PASSWORD"])

    tot_ok = tot_fail = 0
    t_start = time.time()
    for folder, items in order:
        uid_map = {r["uid"]: r for r in items}
        uids = [r["uid"] for r in items]
        log("== %s（%d 封）" % (folder, len(uids)))
        try:
            typ, _ = client.select(mailbox_arg(folder), readonly=True)
        except Exception as e:
            log("  EXAMINE 异常 %s，重连" % type(e).__name__)
            try:
                client = connect(env)
                client.login(env["IMAP_USER"], env["IMAP_PASSWORD"])
                typ, _ = client.select(mailbox_arg(folder), readonly=True)
            except Exception as e2:
                log("  重连失败 %s，跳过文件夹" % type(e2).__name__)
                continue
        if typ != "OK":
            log("  EXAMINE 失败，跳过")
            continue

        f_ok = f_fail = 0
        t0 = time.time()
        for bi in range(0, len(uids), batch_size):
            chunk = uids[bi:bi + batch_size]
            ins, fails = [], []

            # ---- 1) BODYSTRUCTURE ----
            bs_map = {}
            try:
                typ, data = client.uid(
                    "fetch", ",".join(map(str, chunk)), "(BODYSTRUCTURE)")
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

            # ---- 2) 选 part ----
            by_part = {}
            for uid in chunk:
                bs = bs_map.get(uid)
                if not bs:
                    fails.append(uid)
                    continue
                p = pick_part(bs)
                if not p:
                    fails.append(uid)
                    continue
                by_part.setdefault(p["part"], []).append((uid, p))

            # ---- 3) 按 part 批量定点取正文 ----
            for part, plist in sorted(by_part.items(), key=lambda kv: -len(kv[1])):
                puids = [u for u, _ in plist]
                meta_by_uid = {u: p for u, p in plist}
                for gi in range(0, len(puids), batch_size):
                    g = puids[gi:gi + batch_size]
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
                            continue
                        meta = meta_by_uid[uid]
                        try:
                            txt = decode_part(raw, meta)
                        except Exception:
                            continue
                        if not txt:
                            continue
                        body, title, company, fb = clean(txt)
                        ins.append((
                            folder, uid, uid_map[uid]["thread_id"], meta["type"],
                            body, len(body),
                            datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                            title, company, fb, len(txt),
                        ))
                    time.sleep(random.uniform(*PAUSE))

            if ins:
                c.executemany("""
                    INSERT OR REPLACE INTO bodies
                    (folder, uid, thread_id, content_type, text, char_count, fetched_at,
                     sender_title, sender_company, quoted_fallback, raw_chars)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, ins)
            got_uids = {r[1] for r in ins}
            real_fail = [u for u in chunk if u not in got_uids]
            if real_fail:
                c.executemany(
                    "INSERT OR REPLACE INTO errors (folder, uid, error, time) VALUES (?,?,?,?)",
                    [(folder, u, "第③步：未取到正文", datetime.now().isoformat())
                     for u in real_fail])
            con.commit()

            f_ok += len(ins)
            f_fail += len(real_fail)
            done_n = bi + len(chunk)
            el = time.time() - t0
            rate = done_n / max(el, 1)
            eta = (len(uids) - done_n) / rate if rate > 0 else 0
            log("  %d/%d 入库 %d 失败 %d | %.2f 封/秒 ETA %.0f 分" % (
                done_n, len(uids), f_ok, f_fail, rate, eta / 60))

        tot_ok += f_ok
        tot_fail += f_fail
        log("  %s 完成：入库 %d，失败 %d" % (folder, f_ok, f_fail))

    try:
        client.logout()
    except Exception:
        pass
    con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    con.close()
    log("第③步完成：入库 %d，失败 %d，用时 %.1f 分" % (
        tot_ok, tot_fail, (time.time() - t_start) / 60))


if __name__ == "__main__":
    main()
