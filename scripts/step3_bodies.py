#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第③步（v3）：只对进第④步的邮件取正文并清洗。

取正文
- text/plain 优先；没有则 text/html 转纯文本
- 只 BODY.PEEK 正文叶子章节，绝不下载附件
- 每封最多取 32 KB 原文
- 性能：先批量取 BODYSTRUCTURE（200 封/批），再按"章节号"分组批量取正文，
  把 6 万封的往返次数从 12 万降到约 1 千

清洗
- 截掉引用历史（> 行 / On…wrote: / 在…写道：/ From:+Sent: / 发件人：+发送时间：/
  -----Original Message----- / -----原始邮件----- / 下划线分隔线）
- 删除含 confidential / intended recipient / privileged / 免责声明 / 保密 的段落
- 落款后 <=12 行且为联系方式类内容则删除，并提取 sender_title / sender_company
- 每封清洗后最多 2500 字
- 清洗后 <80 字则回退引用部分前 1500 字，标 quoted_fallback=1
"""

import argparse
import base64
import html
import imaplib
import json
import os
import quopri
import random
import re
import sqlite3
import time
from datetime import datetime, timezone

import pipeline_config as PC
_P = PC.paths()
BASE = _P["program_dir"]
DB_PATH = _P["db"]

MAX_RAW = 32 * 1024
MAX_CLEAN = 2500
FALLBACK_MIN = 80
FALLBACK_QUOTE = 1500
BATCH = 200
PAUSE = (0.5, 1.2)

OUR_DOMAIN = PC.our_domain()


def log(m):
    print("[%s] %s" % (datetime.now().strftime("%H:%M:%S"), m), flush=True)


def load_env():
    env = {}
    with open(os.path.join(BASE, ".env"), encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def connect_db():
    con = sqlite3.connect(DB_PATH, timeout=60)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.row_factory = sqlite3.Row
    return con


# ---------------------------------------------------------------------------
# modified UTF-7
# ---------------------------------------------------------------------------
def encode_mutf7(text):
    out = bytearray()
    i, n = 0, len(text)
    while i < n:
        code = ord(text[i])
        if 0x20 <= code <= 0x7E:
            out += b"&-" if text[i] == "&" else text[i].encode("ascii")
            i += 1
        else:
            j = i
            buf = bytearray()
            while j < n and not (0x20 <= ord(text[j]) <= 0x7E):
                buf += text[j].encode("utf-16-be")
                j += 1
            out += b"&" + base64.b64encode(bytes(buf)).rstrip(b"=").replace(b"/", b",") + b"-"
            i = j
    return bytes(out)


def mailbox_arg(folder):
    return b'"' + encode_mutf7(folder) + b'"'


# ---------------------------------------------------------------------------
# BODYSTRUCTURE 解析
# ---------------------------------------------------------------------------
def _tokenize(s):
    toks, i, n = [], 0, len(s)
    while i < n:
        c = s[i]
        if c in "()":
            toks.append(c)
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
            toks.append(("str", "".join(buf)))
            i = j + 1
        elif c.isspace():
            i += 1
        else:
            j = i
            while j < n and s[j] not in "()" and not s[j].isspace():
                j += 1
            toks.append(("atom", s[i:j]))
            i = j
    return toks


def _parse_list(toks, idx):
    idx += 1
    items = []
    while idx < len(toks) and toks[idx] != ")":
        if toks[idx] == "(":
            sub, idx = _parse_list(toks, idx)
            items.append(sub)
        else:
            items.append(toks[idx])
            idx += 1
    return items, idx + 1


def _tv(t):
    return t[1] if isinstance(t, tuple) else None


def walk_text(node, prefix, out):
    if not isinstance(node, list) or not node:
        return
    if isinstance(node[0], list):
        idx = 1
        for child in node:
            if isinstance(child, list):
                walk_text(child, prefix + str(idx) + ".", out)
                idx += 1
        return
    ctype = (_tv(node[0]) or "").lower()
    if ctype != "text":
        return
    csub = (_tv(node[1]) or "").lower()
    encoding = (_tv(node[5]) or "").lower() if len(node) > 5 else ""
    size = None
    if len(node) > 6 and isinstance(node[6], tuple) and str(node[6][1]).isdigit():
        size = int(node[6][1])
    charset = None
    params = node[2] if len(node) > 2 and isinstance(node[2], list) else None
    if params:
        for k in range(0, len(params) - 1, 2):
            if (_tv(params[k]) or "").upper() == "CHARSET":
                charset = _tv(params[k + 1])
    out.append({
        "part": prefix.rstrip(".") or "1",
        "type": "%s/%s" % (ctype, csub),
        "encoding": encoding, "charset": charset, "size": size,
    })


def text_parts(bs):
    if not bs:
        return []
    try:
        root, _ = _parse_list(_tokenize(bs), 0)
    except Exception:
        return []
    out = []
    walk_text(root, "", out)
    return out


def _balanced(s, start):
    depth, i, n, q = 0, start, len(s), False
    while i < n:
        c = s[i:i + 1]
        if c == b"\\":
            i += 2
            continue
        if c == b'"':
            q = not q
        elif not q:
            if c == b"(":
                depth += 1
            elif c == b")":
                depth -= 1
                if depth == 0:
                    return s[start:i + 1]
        i += 1
    return None


def parse_bs(data):
    out, buf = {}, b""
    for item in data:
        buf += item if isinstance(item, bytes) else (item[0] if isinstance(item, tuple) else b"")
        while True:
            m = re.search(rb"UID (\d+) BODYSTRUCTURE\s*", buf)
            if not m:
                break
            start = m.end()
            while start < len(buf) and buf[start:start + 1] in (b" ", b"\t", b"\r", b"\n"):
                start += 1
            if start >= len(buf) or buf[start:start + 1] != b"(":
                break
            bs = _balanced(buf, start)
            if bs is None:
                break
            out[int(m.group(1))] = bs.decode("ascii", "replace")
            buf = buf[start + len(bs):]
    return out


# ---------------------------------------------------------------------------
# 解码
# ---------------------------------------------------------------------------
def decode_body(raw, encoding):
    enc = (encoding or "").lower()
    try:
        if enc in ("quoted-printable", "quotedprintable", "qp"):
            return quopri.decodestring(raw)
        if enc in ("base64", "b64"):
            return base64.b64decode(raw)
    except Exception:
        pass
    return raw


def decode_charset(raw, charset):
    for cs in ([charset] if charset else []) + ["utf-8", "gb18030", "gbk", "big5", "latin-1"]:
        try:
            return raw.decode(cs)
        except (UnicodeDecodeError, LookupError):
            continue
    return raw.decode("utf-8", "replace")


_TAG = re.compile(r"<[^>]+>")
_BR = re.compile(r"(?i)<(br|/p|/div|/tr|/li|/h[1-6])[^>]*>")
# PowerPoint / Excel 导出的 HTML 会带整段 <style> CSS，转文字后会变成
# "table { border-collapse: collapse; }" 这类垃圾混进正文，必须先删掉
_STYLE_BLOCK = re.compile(r"(?is)<(style|script)\b[^>]*>.*?</\1\s*>")
_IE_COND = re.compile(r"(?is)<!--\[if[^\]]*\]>.*?<!\[endif\]-->")
_CSS_RULE_LINE = re.compile(r"^[\s\w.#:,\-\[\]()>@*\"'=/;]*\{[^{}]*\}\s*$")
_CSS_AT_LINE = re.compile(r"^\s*@(page|media|font-face|import|charset)\b", re.I)


def html_to_text(s):
    s = _STYLE_BLOCK.sub(" ", s)
    s = _IE_COND.sub(" ", s)
    s = _BR.sub("\n", s)
    s = _TAG.sub(" ", s)
    s = html.unescape(s)
    s = s.replace("\xa0", " ").replace("\u200b", "").replace("\r\n", "\n").replace("\r", "\n")
    lines = [ln.strip() for ln in s.split("\n")]
    out, blank = [], False
    for ln in lines:
        # 残留的 CSS 规则整行丢弃
        if ln and (_CSS_RULE_LINE.match(ln) or _CSS_AT_LINE.match(ln)):
            continue
        if not ln:
            if not blank:
                out.append("")
            blank = True
        else:
            out.append(ln)
            blank = False
    return "\n".join(out).strip()


# ---------------------------------------------------------------------------
# 清洗
# ---------------------------------------------------------------------------
QUOTE_START = [
    re.compile(r"^\s*On\s+.{0,200}\bwrote\s*:\s*$", re.I),
    re.compile(r"^\s*在\s*.{0,120}\s*写道\s*[:：]\s*$"),
    re.compile(r"^\s*-+\s*Original Message\s*-+\s*$", re.I),
    re.compile(r"^\s*-+\s*原始邮件\s*-+\s*$"),
    re.compile(r"^\s*_{30,}\s*$"),
    re.compile(r"^\s*From\s*:\s*.{0,120}$", re.I),
    re.compile(r"^\s*发件人\s*[:：]\s*.{0,80}$"),
    re.compile(r"^\s*_{20,}\s*$"),
]
OUTLOOK_FROM = re.compile(r"^\s*From\s*:\s*\S", re.I)
OUTLOOK_SENT = re.compile(r"^\s*Sent\s*:\s*\S", re.I)
CN_FROM = re.compile(r"^\s*发件人\s*[:：]")
CN_SENT = re.compile(r"^\s*发送时间\s*[:：]")

DISCLAIMER_WORDS = ["confidential", "intended recipient", "privileged",
                    "免责声明", "保密", "disclaimer"]

SIGNOFF = re.compile(
    r"^\s*(best\s+regards|best\s+wishes|kind\s+regards|warm\s+regards|regards|"
    r"thanks(\s+and\s+regards)?|thank\s+you|cheers|best|br|sincerely|"
    r"此致|敬礼|祝好|谢谢|顺祝商祺)\s*[,，!！.]?\s*$", re.I)

TITLE_WORDS = ["manager", "director", "engineer", "sales", "account", "president",
               "ceo", "cto", "coo", "vp", "vice president", "head of", "specialist",
               "consultant", "coordinator", "supervisor", "owner", "partner",
               "representative", "executive", "officer", "总监", "经理", "工程师",
               "销售", "主管", "专员", "负责人"]

# 公司后缀必须按"完整词"匹配（禁止子串，避免 manager 里的 ag 被当成德国公司后缀）
COMPANY_RE = re.compile(
    r"\b(ltd|limited|inc|llc|gmbh|corp|corporation|company|group|systems|"
    r"technologies|technology|solutions|industries|enterprises|holdings|"
    r"pty|aps|pte|ag|ab|oy|kg|bv|nv|srl|spa|sas|sarl|kk)\b\.?", re.I)
COMPANY_CN_RE = re.compile(r"(有限公司|科技|集团|股份|实业|电子)")
COMPANY_EXCLUDE = ("department", "dept", "team", "division", "unit", "purchasing")


def _is_company(s):
    low = s.lower()
    if any(w in low for w in COMPANY_EXCLUDE):
        return False
    return bool(COMPANY_RE.search(low)) or bool(COMPANY_CN_RE.search(s))


def _is_title(s):
    low = s.lower()
    return any(w in low for w in TITLE_WORDS) and not re.search(r"@|http|www\.", low, re.I)


def _pick_title_company(lines):
    title = company = None
    for ln in lines:
        for seg in [x.strip() for x in re.split(r"[|·•]", ln) if x.strip()]:
            if len(seg) > 90 or re.search(r"@|http|www\.", seg, re.I):
                continue
            if title is None and _is_title(seg):
                title = seg
            if company is None and _is_company(seg):
                company = seg
        if title and company:
            break
    if title and company and title == company:
        company = None
    return title, company


def cut_quotes(text):
    """从第一个引用标记开始截断，返回 (正文, 被截掉的引用部分)"""
    lines = text.split("\n")
    cut = len(lines)
    for i, ln in enumerate(lines):
        if ln.lstrip().startswith(">"):
            cut = i
            break
        for p in QUOTE_START:
            if p.match(ln):
                cut = i
                break
        else:
            continue
        break
    # Outlook 式：From: 行紧跟 Sent: 行
    if cut == len(lines):
        for i in range(len(lines) - 1):
            if OUTLOOK_FROM.match(lines[i]) and OUTLOOK_SENT.match(lines[i + 1]):
                cut = i
                break
            if CN_FROM.match(lines[i]) and CN_SENT.match(lines[i + 1]):
                cut = i
                break
    body = "\n".join(lines[:cut]).strip()
    quoted = "\n".join(lines[cut:]).strip()
    return body, quoted


def drop_disclaimer(text):
    paras = re.split(r"\n\s*\n", text)
    keep = []
    for p in paras:
        low = p.lower()
        if any(w in low for w in DISCLAIMER_WORDS) and len(p) < 1600:
            continue
        keep.append(p)
    return "\n\n".join(keep).strip()


def strip_signature(text):
    """落款后若 <=12 行且都是联系方式类内容，删除；同时提取 title / company"""
    lines = text.split("\n")
    sig_idx = None
    for i in range(len(lines) - 1, max(len(lines) - 30, -1), -1):
        if SIGNOFF.match(lines[i]):
            sig_idx = i
            break
    if sig_idx is None:
        return text, None, None
    tail = [ln for ln in lines[sig_idx + 1:] if ln.strip()]
    if not tail:
        return "\n".join(lines[:sig_idx]).strip(), None, None
    if len(tail) > 12:
        return text, None, None
    # 判断是否都是联系方式类（含数字/邮箱/网址/@/电话/地址词，且无长句）
    contact_like = 0
    for ln in tail:
        if len(ln) > 90:
            return text, None, None
        if re.search(r"[\d@:/]|tel|phone|mobile|fax|www\.|http|地址|电话|手机|邮箱", ln, re.I):
            contact_like += 1
        elif _is_title(ln) or _is_company(ln):
            contact_like += 1
    if contact_like < max(1, len(tail) * 0.6):
        return text, None, None

    title, company = _pick_title_company(tail)
    return "\n".join(lines[:sig_idx]).strip(), title, company


def clean(text):
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    body, quoted = cut_quotes(t)
    body = drop_disclaimer(body)
    body, title, company = strip_signature(body)
    # 压缩空行
    body = re.sub(r"\n{3,}", "\n\n", body).strip()
    fallback = 0
    if len(body) < FALLBACK_MIN and len(quoted) > FALLBACK_MIN:
        q = drop_disclaimer(quoted)
        q = re.sub(r"^(\s*>\s?)+", "", q, flags=re.M)
        body = q[:FALLBACK_QUOTE]
        fallback = 1
    if len(body) > MAX_CLEAN:
        body = body[:MAX_CLEAN]
    return body, title, company, fallback


# ---------------------------------------------------------------------------
# 抓取
# ---------------------------------------------------------------------------
def fetch_bodystructures(client, folder, uids):
    out = {}
    for i in range(0, len(uids), BATCH):
        chunk = uids[i:i + BATCH]
        try:
            typ, data = client.uid("fetch", ",".join(map(str, chunk)), "(BODYSTRUCTURE)")
            if typ == "OK":
                out.update(parse_bs(data))
        except Exception as e:
            log("  BS 批失败: %s" % type(e).__name__)
            time.sleep(3)
        time.sleep(random.uniform(*PAUSE))
    return out


def fetch_parts_group(client, folder, part, uids):
    """同一章节号批量取正文，返回 {uid: bytes}"""
    out = {}
    for i in range(0, len(uids), BATCH):
        chunk = uids[i:i + BATCH]
        try:
            typ, data = client.uid(
                "fetch", ",".join(map(str, chunk)), "(BODY.PEEK[%s])" % part)
            if typ != "OK":
                continue
            for item in data:
                if isinstance(item, tuple):
                    line, lit = item
                    m = re.search(rb"UID (\d+)", line)
                    if m:
                        out[int(m.group(1))] = lit
                    if len(lit) > MAX_RAW * 4:
                        pass
        except Exception as e:
            log("  part %s 批失败: %s" % (part, type(e).__name__))
            time.sleep(3)
        time.sleep(random.uniform(*PAUSE))
    return out


def main():
    global DB_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="最多处理 N 封（调试）")
    ap.add_argument("--folders", help="只处理这些文件夹")
    ap.add_argument("--db", help="指定数据库路径（调试用）")
    args = ap.parse_args()
    if args.db:
        DB_PATH = args.db

    env = load_env()
    con = connect_db()
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
    have = {r[1] for r in c.execute("PRAGMA table_info(bodies)")}
    for nm, ddl in [("sender_title", "TEXT"), ("sender_company", "TEXT"),
                    ("quoted_fallback", "INTEGER DEFAULT 0"), ("raw_chars", "INTEGER DEFAULT 0")]:
        if nm not in have:
            c.execute("ALTER TABLE bodies ADD COLUMN %s %s" % (nm, ddl))
    con.commit()

    # 待取正文的邮件（kept=1），按文件夹分组
    q = """SELECT folder, uid, thread_id FROM messages
           WHERE kept=1 AND is_dup=0 AND exclude_reason IS NULL"""
    params = []
    if args.folders:
        fs = [x.strip() for x in args.folders.split(",")]
        q += " AND folder IN (%s)" % ",".join("?" * len(fs))
        params += fs
    q += " ORDER BY folder, date_utc DESC"
    if args.limit:
        q += " LIMIT %d" % args.limit
    rows = c.execute(q, params).fetchall()
    done = {(r["folder"], r["uid"]) for r in c.execute(
        "SELECT folder, uid FROM bodies WHERE char_count > 0")}
    todo = [r for r in rows if (r["folder"], r["uid"]) not in done]
    log("待取正文 %d 封（已有 %d 封）" % (len(todo), len(done)))
    if not todo:
        con.close()
        return

    by_folder = {}
    for r in todo:
        by_folder.setdefault(r["folder"], []).append(r)

    client = imaplib.IMAP4_SSL(env.get("IMAP_HOST", "imap.exmail.qq.com"), 993, timeout=90)
    client.login(env["IMAP_USER"], env["IMAP_PASSWORD"])

    total_ok = total_fail = 0
    for folder, items in by_folder.items():
        log("== %s（%d 封）" % (folder, len(items)))
        typ, _ = client.select(mailbox_arg(folder), readonly=True)
        if typ != "OK":
            log("  EXAMINE 失败，跳过")
            continue
        uids = [r["uid"] for r in items]
        tid_map = {r["uid"]: r["thread_id"] for r in items}

        bs_map = fetch_bodystructures(client, folder, uids)
        log("  取到 BODYSTRUCTURE %d/%d" % (len(bs_map), len(uids)))

        # 每封选一个正文 part（plain 优先）
        pick = {}
        for uid, bs in bs_map.items():
            parts = text_parts(bs)
            plain = [p for p in parts if p["type"] == "text/plain" and p["part"]]
            htmlp = [p for p in parts if p["type"] == "text/html" and p["part"]]
            chosen = None
            if plain:
                plain.sort(key=lambda p: -(p["size"] or 0))
                chosen = plain[0]
            elif htmlp:
                htmlp.sort(key=lambda p: -(p["size"] or 0))
                chosen = htmlp[0]
            if chosen:
                pick[uid] = chosen

        # 按 part 号分组批量取
        by_part = {}
        for uid, p in pick.items():
            by_part.setdefault(p["part"], []).append(uid)

        raw_map = {}
        for part, us in sorted(by_part.items(), key=lambda x: -len(x[1])):
            got = fetch_parts_group(client, folder, part, us)
            raw_map.update({u: (b, pick[u]) for u, b in got.items()})
            log("    part %-6s %d 封 -> 取到 %d" % (part, len(us), len(got)))

        # 解码 + 清洗 + 入库
        batch_rows, fails = [], []
        for uid in uids:
            if uid not in raw_map:
                c.execute("INSERT OR REPLACE INTO errors (folder, uid, error, time) VALUES (?,?,?,?)",
                          (folder, uid, "第③步：未取到正文 part", datetime.now().isoformat()))
                continue
            raw, meta = raw_map[uid]
            try:
                dec = decode_body(raw, meta["encoding"])
                txt = decode_charset(dec, meta["charset"])
                if meta["type"] == "text/html":
                    txt = html_to_text(txt)
                else:
                    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
                raw_chars = len(txt)
                body, title, company, fb = clean(txt)
                batch_rows.append((
                    folder, uid, tid_map.get(uid), meta["type"],
                    body, len(body), datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                    title, company, fb, raw_chars,
                ))
            except Exception as e:
                fails.append((folder, uid, "第③步：%s" % type(e).__name__))

        con.executemany("""
            INSERT OR REPLACE INTO bodies
            (folder, uid, thread_id, content_type, text, char_count, fetched_at,
             sender_title, sender_company, quoted_fallback, raw_chars)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, batch_rows)
        if fails:
            con.executemany("INSERT INTO errors (folder, uid, error, time) VALUES (?,?,?,?)",
                            [(f, u, e, datetime.now().isoformat()) for f, u, e in fails])
        con.commit()
        total_ok += len(batch_rows)
        total_fail += len(fails)
        log("  %s 完成：入库 %d，失败 %d" % (folder, len(batch_rows), len(fails)))

    client.logout()
    log("第③步完成：入库 %d，失败 %d" % (total_ok, total_fail))
    con.close()


if __name__ == "__main__":
    main()
