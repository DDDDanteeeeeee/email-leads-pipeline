#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第①步（v3）：只读抓取 2023-01-01 至今全部文件夹的邮件头，建立索引。

相对 v2 的变化
- 范围：UID SEARCH SINCE 1-Jan-2023（不再受 30 天保留期限制）
- 新增字段：has_calendar（BODYSTRUCTURE 含 text/calendar）、folders_all（同一 Message-ID
  出现过的所有文件夹 JSON 数组）、is_dup（1=非代表封）、dedup_key
- 去重：同一 Message-ID 只保留一封代表，其余标 is_dup=1
- Junk / Deleted Messages / Drafts 只记条数，不抓头
- 从新到旧抓（UID 降序）
- 上次 errors 表里的失败 UID 强制重试一次

【只读硬规则（代码中不得出现以下命令）】
    STORE、COPY、MOVE、EXPUNGE、APPEND、DELETE、RENAME、CREATE、SUBSCRIBE、UNSUBSCRIBE
- 打开文件夹一律 select(readonly=True)（= IMAP EXAMINE）
- 取内容一律 BODY.PEEK[...]，不改变已读状态
- 不连 SMTP；不下载正文与附件；不调用任何大模型
- 不打印/记录/上传授权码

授权码从 邮件归档程序/.env 的 IMAP_PASSWORD 读取。
"""

import argparse
import base64
import json
import os
import random
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from email.header import decode_header
from email.utils import getaddresses, parseaddr, parsedate_to_datetime

import imaplib

import pipeline_config as PC
_P = PC.paths()
BASE = _P["program_dir"]
DB_PATH = _P["db"]
LOG_DIR = os.path.join(BASE, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

SINCE = PC.cfg.get("since") or "01-Jan-2023"
HEADER_FIELDS = (
    "MESSAGE-ID IN-REPLY-TO REFERENCES DATE FROM TO CC SUBJECT "
    "LIST-UNSUBSCRIBE LIST-ID PRECEDENCE AUTO-SUBMITTED X-MAILER"
)

BATCH_SIZE = 200
PAUSE_MIN, PAUSE_MAX = 0.8, 1.8
RECONNECT_DELAY = 30
MAX_RETRIES = 5

# 只记条数、不抓头的文件夹
COUNT_ONLY_MARKERS = ("junk", "spam", "垃圾", "deleted", "trash", "已删除", "draft", "草稿")

# 客户子文件夹前缀（用于选去重代表：优先保留带客户标签的那封）
CUSTOMER_PREFIX = "其他文件夹/"


def log(msg):
    print("[%s] %s" % (datetime.now().strftime("%H:%M:%S"), msg), flush=True)


# ---------------------------------------------------------------------------
# 环境
# ---------------------------------------------------------------------------
def load_env():
    env = {}
    with open(PC.paths()["env_file"], encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


# ---------------------------------------------------------------------------
# IMAP modified UTF-7（base64 段为 UTF-16-BE）
# ---------------------------------------------------------------------------
def decode_mutf7(data):
    if isinstance(data, str):
        data = data.encode("ascii", "replace")
    out = []
    i, n = 0, len(data)
    while i < n:
        c = data[i]
        if c == 0x26:
            j = data.find(b"-", i)
            if j == -1:
                out.append(data[i:].decode("utf-8", "replace"))
                break
            if j == i + 1:
                out.append("&")
            else:
                seg = data[i + 1:j].replace(b",", b"/")
                seg += b"=" * ((4 - len(seg) % 4) % 4)
                try:
                    out.append(base64.b64decode(seg).decode("utf-16-be"))
                except Exception:
                    out.append(data[i:j + 1].decode("utf-8", "replace"))
            i = j + 1
        else:
            out.append(chr(c))
            i += 1
    return "".join(out)


def encode_mutf7(text):
    out = bytearray()
    i, n = 0, len(text)
    while i < n:
        code = ord(text[i])
        if 0x20 <= code <= 0x7E:
            if text[i] == "&":
                out += b"&-"
            else:
                out += text[i].encode("ascii")
            i += 1
        else:
            j = i
            buf = bytearray()
            while j < n and not (0x20 <= ord(text[j]) <= 0x7E):
                buf += text[j].encode("utf-16-be")
                j += 1
            b64 = base64.b64encode(bytes(buf)).rstrip(b"=")
            out += b"&" + b64.replace(b"/", b",") + b"-"
            i = j
    return bytes(out)


def mailbox_arg(folder):
    return b'"' + encode_mutf7(folder) + b'"'


# ---------------------------------------------------------------------------
# 数据库
# ---------------------------------------------------------------------------
NEW_COLUMNS = [
    ("has_calendar", "INTEGER DEFAULT 0"),
    ("folders_all", "TEXT"),
    ("is_dup", "INTEGER DEFAULT 0"),
    ("dedup_key", "TEXT"),
]


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = sqlite3.connect(DB_PATH)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            folder TEXT NOT NULL,
            uidvalidity INTEGER,
            uid INTEGER NOT NULL,
            message_id TEXT,
            in_reply_to TEXT,
            references_hdr TEXT,
            date_utc TEXT,
            from_name TEXT,
            from_addr TEXT,
            from_domain TEXT,
            to_addrs TEXT,
            cc_addrs TEXT,
            subject TEXT,
            is_bulk INTEGER DEFAULT 0,
            attachment_names TEXT,
            attachment_total_bytes INTEGER DEFAULT 0,
            size_bytes INTEGER,
            fetched_at TEXT,
            kept INTEGER,
            thread_id TEXT,
            PRIMARY KEY (folder, uid)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS folders (
            folder TEXT PRIMARY KEY,
            uidvalidity INTEGER,
            server_count INTEGER,
            fetched_count INTEGER DEFAULT 0,
            oldest_date TEXT,
            newest_date TEXT
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS errors (
            folder TEXT, uid INTEGER, error TEXT, time TEXT
        )
    """)
    # 增量加列
    have = {r[1] for r in con.execute("PRAGMA table_info(messages)")}
    for name, ddl in NEW_COLUMNS:
        if name not in have:
            con.execute("ALTER TABLE messages ADD COLUMN %s %s" % (name, ddl))
    con.execute("CREATE INDEX IF NOT EXISTS ix_messages_dedup ON messages(dedup_key)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_messages_msgid ON messages(message_id)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_messages_date ON messages(date_utc)")
    con.execute("CREATE INDEX IF NOT EXISTS ix_messages_domain ON messages(from_domain)")
    con.commit()
    con.close()


def connect_db():
    con = sqlite3.connect(DB_PATH, timeout=60)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    return con


def done_uids(folder, retry_uids):
    """已抓过的 UID（但 retry_uids 里的除外，强制重试）"""
    con = connect_db()
    rows = con.execute("SELECT uid FROM messages WHERE folder=?", (folder,)).fetchall()
    con.close()
    done = {r[0] for r in rows}
    return done - set(retry_uids)


def upsert_messages(rows):
    """批量写入；冲突时只更新抓取字段，保留 kept / thread_id。
    用 try/finally 确保连接一定关闭，避免异常时残留写锁导致后续 database is locked。"""
    if not rows:
        return
    con = connect_db()
    try:
        con.executemany("""
        INSERT INTO messages (
            folder, uidvalidity, uid, message_id, in_reply_to, references_hdr,
            date_utc, from_name, from_addr, from_domain, to_addrs, cc_addrs,
            subject, is_bulk, attachment_names, attachment_total_bytes,
            size_bytes, fetched_at, has_calendar, dedup_key
        ) VALUES (
            :folder, :uidvalidity, :uid, :message_id, :in_reply_to, :references_hdr,
            :date_utc, :from_name, :from_addr, :from_domain, :to_addrs, :cc_addrs,
            :subject, :is_bulk, :attachment_names, :attachment_total_bytes,
            :size_bytes, :fetched_at, :has_calendar, :dedup_key
        )
        ON CONFLICT(folder, uid) DO UPDATE SET
            uidvalidity=excluded.uidvalidity,
            message_id=excluded.message_id,
            in_reply_to=excluded.in_reply_to,
            references_hdr=excluded.references_hdr,
            date_utc=COALESCE(excluded.date_utc, messages.date_utc),
            from_name=excluded.from_name,
            from_addr=excluded.from_addr,
            from_domain=excluded.from_domain,
            to_addrs=excluded.to_addrs,
            cc_addrs=excluded.cc_addrs,
            subject=excluded.subject,
            is_bulk=excluded.is_bulk,
            attachment_names=excluded.attachment_names,
            attachment_total_bytes=excluded.attachment_total_bytes,
            size_bytes=excluded.size_bytes,
            fetched_at=excluded.fetched_at,
            has_calendar=excluded.has_calendar,
            dedup_key=excluded.dedup_key
    """, rows)
        con.commit()
    except Exception:
        try:
            con.rollback()
        except Exception:
            pass
        raise
    finally:
        con.close()


def upsert_folder(folder, uidvalidity, server_count):
    con = connect_db()
    con.execute("""
        INSERT INTO folders (folder, uidvalidity, server_count)
        VALUES (?, ?, ?)
        ON CONFLICT(folder) DO UPDATE SET
            uidvalidity=excluded.uidvalidity,
            server_count=excluded.server_count
    """, (folder, uidvalidity, server_count))
    con.commit()
    con.close()


def update_folder_stats(folder):
    con = connect_db()
    row = con.execute(
        "SELECT COUNT(*), MIN(date_utc), MAX(date_utc) FROM messages WHERE folder=?", (folder,)
    ).fetchone()
    con.execute(
        "UPDATE folders SET fetched_count=?, oldest_date=?, newest_date=? WHERE folder=?",
        (row[0], row[1], row[2], folder),
    )
    con.commit()
    con.close()


def record_errors(items):
    if not items:
        return
    con = connect_db()
    con.executemany(
        "INSERT INTO errors (folder, uid, error, time) VALUES (?,?,?,?)",
        [(f, u, e, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")) for f, u, e in items],
    )
    con.commit()
    con.close()


# ---------------------------------------------------------------------------
# 头部解析
# ---------------------------------------------------------------------------
def decode_mime(value):
    """务必返回 str：msg.get() 有时返回 email.header.Header 对象，
    直接塞给 sqlite3 会报 "type 'Header' is not supported"。"""
    if not value:
        return ""
    try:
        out = []
        for data, enc in decode_header(value):
            if isinstance(data, bytes):
                out.append(data.decode(enc or "utf-8", "replace"))
            else:
                out.append(str(data))
        return "".join(out)
    except Exception:
        try:
            return str(value)
        except Exception:
            return ""


def parse_from(value):
    if not value:
        return "", ""
    name, addr = parseaddr(value)
    return decode_mime(name), addr.lower().strip()


def parse_addrs(value):
    if not value:
        return []
    return [a.lower().strip() for _, a in getaddresses([value]) if a]


def parse_date_utc(value):
    if not value:
        return None
    try:
        dt = parsedate_to_datetime(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def norm_msgid(mid):
    """Message-ID 归一化：去空白、去尖括号、小写域名部分"""
    if not mid:
        return ""
    m = re.search(r"<([^>]+)>", mid)
    s = (m.group(1) if m else mid).strip()
    return s.lower()


# ---------------------------------------------------------------------------
# BODYSTRUCTURE 解析（附件 + text/calendar 检测）
# ---------------------------------------------------------------------------
def _tokenize_bs(s):
    tokens = []
    i, n = 0, len(s)
    while i < n:
        c = s[i]
        if c in "()":
            tokens.append(c)
            i += 1
        elif c == '"':
            j = i + 1
            buf = []
            while j < n:
                if s[j] == "\\" and j + 1 < n:
                    buf.append(s[j + 1])
                    j += 2
                elif s[j] == '"':
                    break
                else:
                    buf.append(s[j])
                    j += 1
            tokens.append(("str", "".join(buf)))
            i = j + 1
        elif c.isspace():
            i += 1
        else:
            j = i
            while j < n and s[j] not in "()" and not s[j].isspace():
                j += 1
            tokens.append(("atom", s[i:j]))
            i = j
    return tokens


def _parse_list(tokens, idx):
    idx += 1
    items = []
    while idx < len(tokens) and tokens[idx] != ")":
        if tokens[idx] == "(":
            sub, idx = _parse_list(tokens, idx)
            items.append(sub)
        else:
            items.append(tokens[idx])
            idx += 1
    return items, idx + 1


def _atomval(x):
    if isinstance(x, tuple):
        return x[1]
    return str(x)


def _walk_parts(node, out):
    """out: {'attachments': [(name,size)], 'has_calendar': bool}"""
    if not isinstance(node, list) or not node:
        return
    first = node[0]
    if isinstance(first, list):
        for child in node:
            if isinstance(child, list):
                _walk_parts(child, out)
        return

    mtype = _atomval(node[0]).upper() if len(node) > 0 else ""
    subtype = _atomval(node[1]).upper() if len(node) > 1 else ""
    if mtype == "TEXT" and subtype == "CALENDAR":
        out["has_calendar"] = True
    # multipart/report 或 message/rfc822 里可能嵌 calendar
    if mtype == "MULTIPART" and subtype == "ALTERNATIVE":
        pass  # 已在递归里处理

    disposition = None
    filename = None
    size = None
    params = node[2] if len(node) > 2 and isinstance(node[2], list) else None
    if len(node) > 6:
        s6 = node[6]
        if isinstance(s6, tuple) and s6[0] == "atom" and s6[1].isdigit():
            size = int(s6[1])
    for i in range(7, len(node)):
        el = node[i]
        if isinstance(el, list) and el:
            dval = _atomval(el[0]).upper()
            if dval == "ATTACHMENT":
                disposition = "attachment"
                if len(el) > 1 and isinstance(el[1], list):
                    for k in range(0, len(el[1]) - 1, 2):
                        if _atomval(el[1][k]).upper() == "FILENAME":
                            filename = _atomval(el[1][k + 1])
    if disposition == "attachment" and filename is None and params:
        for k in range(0, len(params) - 1, 2):
            if _atomval(params[k]).upper() in ("NAME", "FILENAME"):
                filename = _atomval(params[k + 1])
    if disposition == "attachment":
        out["attachments"].append((decode_mime(filename) if filename else None, size))


def parse_bodystructure(bs):
    out = {"attachments": [], "has_calendar": False}
    if not bs:
        return out
    try:
        tokens = _tokenize_bs(bs)
        if not tokens:
            return out
        root, _ = _parse_list(tokens, 0)
        _walk_parts(root, out)
    except Exception:
        pass
    return out


# ---------------------------------------------------------------------------
# 连接 / 文件夹
# ---------------------------------------------------------------------------
def connect(env):
    # timeout 必给：imaplib 默认无超时，服务器不响应时会永久挂起
    c = imaplib.IMAP4_SSL(env.get("IMAP_HOST", "imap.exmail.qq.com"), 993, timeout=90)
    c.login(env["IMAP_USER"], env["IMAP_PASSWORD"])
    return c


def connect_with_retry(env):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return connect(env)
        except Exception as e:
            log("连接失败 (%d/%d): %s" % (attempt, MAX_RETRIES, type(e).__name__))
            if attempt < MAX_RETRIES:
                time.sleep(RECONNECT_DELAY)
    raise RuntimeError("无法连接 IMAP 服务器")


def list_folders(client):
    typ, data = client.list()
    if typ != "OK":
        raise RuntimeError("LIST 失败")
    folders = []
    for line in data:
        m = re.search(rb'\(([^)]*)\)\s+"([^"]*)"\s+(.+)$', line)
        if not m:
            continue
        flags = m.group(1).decode("ascii", "replace")
        name_raw = m.group(3).strip()
        if name_raw.startswith(b'"'):
            nm = re.search(rb'"((?:[^"\\]|\\.)*)"', name_raw)
            nb = nm.group(1) if nm else name_raw[1:-1]
            nb = nb.replace(b'\\"', b'"').replace(b"\\\\", b"\\")
        else:
            nb = name_raw
        folders.append((flags, decode_mutf7(nb)))
    return folders


def get_status(client, folder):
    typ, data = client.status(mailbox_arg(folder), "(MESSAGES UIDVALIDITY)")
    if typ != "OK" or not data:
        return None, None
    m = re.search(rb"MESSAGES (\d+)", data[0])
    u = re.search(rb"UIDVALIDITY (\d+)", data[0])
    return (int(m.group(1)) if m else 0), (int(u.group(1)) if u else None)


def is_count_only(folder):
    low = folder.lower()
    return any(k in low for k in COUNT_ONLY_MARKERS)


# ---------------------------------------------------------------------------
# fetch 解析 + 行构造
# ---------------------------------------------------------------------------
def parse_fetch(data):
    results = {}
    current_uid = None
    pending_header = None
    for item in data:
        if isinstance(item, tuple):
            line, literal = item
            m = re.search(rb"UID (\d+)", line)
            if m:
                current_uid = int(m.group(1))
                pending_header = literal
        else:
            if current_uid is None:
                continue
            if b"RFC822.SIZE" in item:
                rec = results.setdefault(
                    current_uid, {"header": pending_header, "size": None, "bs": ""}
                )
                m = re.search(rb"RFC822.SIZE (\d+)", item)
                if m:
                    rec["size"] = int(m.group(1))
                idx = item.find(b"BODYSTRUCTURE")
                if idx >= 0:
                    rec["bs"] = item[idx + len(b"BODYSTRUCTURE"):].decode("ascii", "replace")
            elif current_uid in results:
                results[current_uid]["bs"] += item.decode("ascii", "replace")
    return results


def build_row(folder, uidvalidity, uid, rec):
    import email as email_mod

    msg = email_mod.message_from_bytes(rec.get("header") or b"")
    message_id = (msg.get("Message-ID") or "").strip()
    subject = decode_mime(msg.get("Subject"))
    from_name, from_addr = parse_from(msg.get("From"))
    bs_info = parse_bodystructure(rec.get("bs") or "")
    names = [a[0] for a in bs_info["attachments"] if a[0]]
    total = sum(a[1] or 0 for a in bs_info["attachments"])

    list_unsub = (msg.get("List-Unsubscribe") or "").strip()
    list_id = (msg.get("List-Id") or "").strip()
    precedence = (msg.get("Precedence") or "").strip().lower()
    auto = (msg.get("Auto-Submitted") or "").strip()
    is_bulk = 1 if (list_unsub or list_id or precedence == "bulk" or auto) else 0

    mid = norm_msgid(message_id)
    row = {
        "folder": folder,
        "uidvalidity": uidvalidity,
        "uid": uid,
        "message_id": message_id,
        "in_reply_to": (msg.get("In-Reply-To") or "").strip(),
        "references_hdr": (msg.get("References") or "").strip(),
        "date_utc": parse_date_utc(msg.get("Date")),
        "from_name": from_name,
        "from_addr": from_addr,
        "from_domain": from_addr.split("@")[-1] if "@" in from_addr else "",
        "to_addrs": json.dumps(parse_addrs(msg.get("To")), ensure_ascii=False),
        "cc_addrs": json.dumps(parse_addrs(msg.get("Cc")), ensure_ascii=False),
        "subject": subject,
        "is_bulk": is_bulk,
        "attachment_names": json.dumps(names, ensure_ascii=False),
        "attachment_total_bytes": total,
        "size_bytes": rec.get("size"),
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "has_calendar": 1 if bs_info["has_calendar"] else 0,
        "dedup_key": mid if mid else "%s#%d" % (folder, uid),
    }
    # 兜底：sqlite3 只接受 int/float/str/bytes/None，其余一律转 str
    for k, v in list(row.items()):
        if v is not None and not isinstance(v, (int, float, str, bytes)):
            row[k] = str(v)
    return row


# ---------------------------------------------------------------------------
# 文件夹抓取
# ---------------------------------------------------------------------------
def fetch_batch(client, env, folder, uidvalidity, batch):
    uids_str = ",".join(str(u) for u in batch)
    # 第一阶段只取头 + 大小：BODYSTRUCTURE 对带附件的邮件体积巨大，是抓取瓶颈，
    # 改由 enrich_bodystructure() 只对"可能是会议/单据"的邮件补取。
    cmd = "(BODY.PEEK[HEADER.FIELDS (%s)] RFC822.SIZE)" % HEADER_FIELDS
    data, typ = None, None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            typ, data = client.uid("fetch", uids_str, cmd)
            if typ == "OK":
                break
        except Exception as e:
            log("  fetch 异常 (第%d次): %s" % (attempt, type(e).__name__))
            if attempt < MAX_RETRIES:
                time.sleep(RECONNECT_DELAY)
                try:
                    client.logout()
                except Exception:
                    pass
                client = connect(env)
                client.select(mailbox_arg(folder), readonly=True)
    if typ != "OK":
        record_errors([(folder, u, "fetch 重试耗尽") for u in batch])
        return client

    parsed = parse_fetch(data)
    rows, errs = [], []
    for u in batch:
        if u not in parsed:
            errs.append((folder, u, "fetch 响应中缺失该 UID"))
            continue
        try:
            rows.append(build_row(folder, uidvalidity, u, parsed[u]))
        except Exception as e:
            errs.append((folder, u, "%s: %s" % (type(e).__name__, e)))
    upsert_messages(rows)
    record_errors(errs)
    return client


def process_folder(client, env, folder, retry_map, max_per_folder=None, shard=0, nshard=1):
    server_count, uidvalidity = get_status(client, folder)
    if server_count is None:
        log("  无法获取 STATUS，跳过")
        return client
    if shard == 0:
        upsert_folder(folder, uidvalidity, server_count)
    if server_count == 0:
        if shard == 0:
            update_folder_stats(folder)
        return client
    typ, _ = client.select(mailbox_arg(folder), readonly=True)  # 只读 EXAMINE
    if typ != "OK":
        log("  EXAMINE 失败，跳过")
        return client

    typ, data = client.uid("search", None, "SINCE", SINCE)
    uids = [int(x) for x in data[0].split()] if data and data[0] else []
    uids.sort(reverse=True)  # 从新到旧
    if nshard > 1:
        uids = [u for u in uids if u % nshard == shard]
    if max_per_folder:
        uids = uids[:max_per_folder]

    done = done_uids(folder, retry_map.get(folder, []))
    todo = [u for u in uids if u not in done]
    log("  server=%s SINCE2023=%d(本片) 已抓=%d 本次待抓=%d"
        % (server_count, len(uids), len(done), len(todo)))
    if not todo:
        if shard == 0:
            update_folder_stats(folder)
        return client

    for i in range(0, len(todo), BATCH_SIZE):
        batch = todo[i:i + BATCH_SIZE]
        client = fetch_batch(client, env, folder, uidvalidity, batch)
        if (i // BATCH_SIZE) % 5 == 0 or i + BATCH_SIZE >= len(todo):
            log("    进度 %d/%d" % (min(i + BATCH_SIZE, len(todo)), len(todo)))
        time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))

    update_folder_stats(folder)
    return client


# ---------------------------------------------------------------------------
# 第二阶段：只对"可能是会议/单据"的邮件补取 BODYSTRUCTURE
# ---------------------------------------------------------------------------
ENRICH_HINTS = [
    "invitation", "accepted", "declined", "tentative", "cancel", "canceled",
    "meeting", "webinar", "reminder", "邀请", "会议", "已接受", "已拒绝", "暂定",
    "invoice", "statement", "remittance", "payment advice", "purchase order",
    "p.o.", "proforma", "packing list", "quotation", "quote", "发票", "对账",
    "付款", "订单", "rma",
]


def enrich_bodystructure(client, env):
    con = connect_db()
    conds = " OR ".join(["m.subject LIKE ?" for _ in ENRICH_HINTS])
    q = """SELECT m.folder, m.uid FROM messages m
           WHERE m.is_dup=0 AND (m.has_calendar=0)
             AND (%s OR m.from_domain='exmail.weixin.qq.com')
           ORDER BY m.folder, m.uid""" % conds
    rows = con.execute(q, ["%%%s%%" % h for h in ENRICH_HINTS]).fetchall()
    con.close()
    log("第二阶段：候选 %d 封需补 BODYSTRUCTURE" % len(rows))
    if not rows:
        return

    by_folder = {}
    for f, u in rows:
        by_folder.setdefault(f, []).append(u)

    for folder, uids in by_folder.items():
        try:
            typ, _ = client.select(mailbox_arg(folder), readonly=True)
            if typ != "OK":
                continue
            for i in range(0, len(uids), BATCH_SIZE):
                chunk = uids[i:i + BATCH_SIZE]
                try:
                    typ, data = client.uid(
                        "fetch", ",".join(map(str, chunk)), "(BODYSTRUCTURE)")
                    if typ != "OK":
                        continue
                except Exception as e:
                    log("    %s 批次异常(%s)，跳过该批" % (folder, type(e).__name__))
                    continue
                bs_map = {}
                buf = b""
                for item in data:
                    buf += item if isinstance(item, bytes) else (item[0] if isinstance(item, tuple) else b"")
                for m in re.finditer(rb"UID (\d+) BODYSTRUCTURE\s*\(", buf):
                    uid = int(m.group(1))
                    start = m.end() - 1
                    depth, j, q_, n = 0, start, False, len(buf)
                    while j < n:
                        ch = buf[j:j + 1]
                        if ch == b"\\":
                            j += 2
                            continue
                        if ch == b'"':
                            q_ = not q_
                        elif not q_:
                            if ch == b"(":
                                depth += 1
                            elif ch == b")":
                                depth -= 1
                                if depth == 0:
                                    bs_map[uid] = buf[start:j + 1].decode("ascii", "replace")
                                    break
                        j += 1
                upd = []
                for uid, bs in bs_map.items():
                    info = parse_bodystructure(bs)
                    names = [a[0] for a in info["attachments"] if a[0]]
                    upd.append((1 if info["has_calendar"] else 0,
                                json.dumps(names, ensure_ascii=False),
                                sum(a[1] or 0 for a in info["attachments"]),
                                folder, uid))
                if upd:
                    con = connect_db()
                    try:
                        con.executemany("""UPDATE messages SET has_calendar=?, attachment_names=?,
                                           attachment_total_bytes=? WHERE folder=? AND uid=?""", upd)
                        con.commit()
                    finally:
                        con.close()
                if (i // BATCH_SIZE) % 5 == 0:
                    log("    %s %d/%d" % (folder, min(i + BATCH_SIZE, len(uids)), len(uids)))
                time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))
            log("  %s: %d 封已补" % (folder, len(uids)))
        except Exception as e:
            log("  %s 补取失败: %s" % (folder, type(e).__name__))
    log("第二阶段完成")


# ---------------------------------------------------------------------------
# 去重：同一 Message-ID 只留一封代表
# ---------------------------------------------------------------------------
def priority(folder):
    """去重代表优先级：客户子文件夹 > INBOX > Sent > Archive > 其他"""
    if folder.startswith(CUSTOMER_PREFIX):
        return 0
    low = folder.lower()
    if folder == "INBOX":
        return 1
    if "sent" in low or "已发送" in folder:
        return 2
    if "archive" in low or "归档" in folder:
        return 4
    return 3


def backfill_dedup_key():
    """旧记录（试跑数据）dedup_key 为 NULL，先按 message_id 回填，避免被当成同一组。"""
    con = connect_db()
    rows = con.execute(
        "SELECT folder, uid, message_id FROM messages WHERE dedup_key IS NULL OR dedup_key=''"
    ).fetchall()
    if not rows:
        con.close()
        return 0
    updates = []
    for folder, uid, mid in rows:
        key = norm_msgid(mid) if mid else "%s#%d" % (folder, uid)
        updates.append((key, folder, uid))
    con.executemany("UPDATE messages SET dedup_key=? WHERE folder=? AND uid=?", updates)
    con.commit()
    con.close()
    log("回填 dedup_key: %d 条" % len(updates))
    return len(updates)


def dedup():
    backfill_dedup_key()
    log("开始去重（按 Message-ID）...")
    con = connect_db()
    rows = con.execute(
        "SELECT folder, uid, dedup_key FROM messages"
    ).fetchall()
    groups = {}
    for folder, uid, key in rows:
        if not key:
            key = "%s#%d" % (folder, uid)
        groups.setdefault(key, []).append((folder, uid))

    reps = 0
    dups = 0
    con.execute("UPDATE messages SET is_dup=0, folders_all=NULL")
    for key, members in groups.items():
        if len(members) == 1:
            f, u = members[0]
            con.execute(
                "UPDATE messages SET is_dup=0, folders_all=? WHERE folder=? AND uid=?",
                (json.dumps([f], ensure_ascii=False), f, u),
            )
            reps += 1
            continue
        members_sorted = sorted(members, key=lambda x: (priority(x[0]), x[0], x[1]))
        rep_f, rep_u = members_sorted[0]
        allf = json.dumps(sorted({m[0] for m in members}), ensure_ascii=False)
        con.execute(
            "UPDATE messages SET is_dup=0, folders_all=? WHERE folder=? AND uid=?",
            (allf, rep_f, rep_u),
        )
        reps += 1
        for f, u in members_sorted[1:]:
            con.execute(
                "UPDATE messages SET is_dup=1, folders_all=? WHERE folder=? AND uid=?",
                (allf, f, u),
            )
            dups += 1
    con.commit()
    total = con.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    uniq = con.execute("SELECT COUNT(*) FROM messages WHERE is_dup=0").fetchone()[0]
    con.close()
    log("去重完成：原始记录 %d，唯一邮件 %d，重复记录 %d" % (total, uniq, dups))


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--folders", help="只抓这些文件夹（逗号分隔）")
    p.add_argument("--limit", type=int, help="每个文件夹最多抓 N 封（调试）")
    p.add_argument("--dedup-only", action="store_true", help="只跑去重，不抓取")
    p.add_argument("--shard", type=int, default=0, help="当前分片序号（0 起）")
    p.add_argument("--of", type=int, default=1, help="总分片数")
    p.add_argument("--enrich-only", action="store_true", help="只补 BODYSTRUCTURE")
    p.add_argument("--list-only", action="store_true")
    args = p.parse_args()

    env = load_env()
    init_db()

    if args.dedup_only:
        dedup()
        return

    client = connect_with_retry(env)
    folders = list_folders(client)

    if args.enrich_only:
        enrich_bodystructure(client, env)
        client.logout()
        dedup()
        return

    if args.list_only:
        for flags, name in folders:
            cnt, uidv = get_status(client, name)
            log("%-50s %s 封 (uidvalidity=%s)" % (name[:50], cnt, uidv))
        client.logout()
        return

    # 上次失败的 UID（强制重试）
    con = connect_db()
    retry_map = {}
    for f, u in con.execute("SELECT folder, uid FROM errors").fetchall():
        retry_map.setdefault(f, []).append(u)
    old_err = con.execute("SELECT COUNT(*) FROM errors").fetchone()[0]
    if args.shard == 0:
        con.execute("DELETE FROM errors")
        con.commit()
    con.close()
    log("[分片 %d/%d] 待重试的旧失败记录: %d 条" % (args.shard, args.of, old_err))

    # 只记条数
    count_only = []
    targets = []
    for flags, name in folders:
        if "NoSelect" in flags:
            continue
        if is_count_only(name):
            count_only.append(name)
            continue
        targets.append(name)

    log("[分片 %d/%d] 文件夹：需抓 %d 个，仅记条数 %d 个" % (args.shard, args.of, len(targets), len(count_only)))
    if args.shard == 0:
        for name in count_only:
            cnt, uidv = get_status(client, name)
            upsert_folder(name, uidv, cnt)
            con = connect_db()
            con.execute(
                "UPDATE folders SET fetched_count=0, oldest_date='(仅记条数)', newest_date='(仅记条数)' WHERE folder=?",
                (name,),
            )
            con.commit()
            con.close()
            log("  [仅记条数] %-30s %s 封" % (name, cnt))

    if args.folders:
        wanted = [x.strip() for x in args.folders.split(",") if x.strip()]
        targets = [t for t in targets if t in wanted]

    # 大文件夹优先（不查 STATUS，用已知顺序：INBOX > Sent > 其他）
    def folder_weight(f):
        low = f.lower()
        if f == "INBOX":
            return 0
        if "sent" in low or "已发送" in f:
            return 1
        if f.startswith(CUSTOMER_PREFIX):
            return 3
        return 2

    targets.sort(key=lambda f: (folder_weight(f), f))
    log("抓取顺序: %s" % " > ".join(targets[:8]))

    for name in targets:
        log("== %s" % name)
        try:
            client = process_folder(client, env, name, retry_map, args.limit, args.shard, args.of)
        except Exception as e:
            log("!! 文件夹失败 %s: %s: %s" % (name, type(e).__name__, e))
            try:
                client = connect_with_retry(env)
            except Exception:
                log("重连失败，终止")
                break

    try:
        client.logout()
    except Exception:
        pass

    log("第一阶段抓取结束，开始第二阶段（补 BODYSTRUCTURE）")
    try:
        client = connect_with_retry(env)
        enrich_bodystructure(client, env)
        client.logout()
    except Exception as e:
        log("第二阶段失败（可稍后单独跑 --enrich-only）: %s" % e)

    dedup()
    log("第①步完成（含去重）")


if __name__ == "__main__":
    main()
