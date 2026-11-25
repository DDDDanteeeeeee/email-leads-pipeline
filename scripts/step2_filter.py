#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第②步（v3）：三层过滤 + 结构化保存 + 串线程。

第一层 直接排除（只看头）
  1. is_bulk=1，或发件人本地名含 noreply/no-reply/donotreply/notification(s)/
     newsletter/mailer-daemon/postmaster/bounce（info@ / sales@ / support@ 不算）
  2. 域名排除名单：必须"完整域名"或"完整标签"匹配，禁止子串匹配
  3. 纯内部邮件排除；命中重点客户名/商机关键词/转发前缀的保留并标 internal_keep=1

第二层 结构化保存（不花 Token）
  4. 会议记录 -> meetings 表
  5. 单据通知 -> documents 表

第三层 AI 分诊（只用于"没把握"的单封外部来信）
  6. 单封 + 外部 + 我方从未给该域名发过信 -> 写 triage 表待分诊
  7. 否则直接进第④步

串线程
  A. Message-ID / In-Reply-To / References 并查集
  B. 规范化主题相同 + 外部域名重叠 + 时间差 <= 90 天 再并
"""

import argparse
import json
import re
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta

import pipeline_config as PC
DB_PATH = PC.paths()["db"]

# ---------------------------------------------------------------------------
# 规则 1：noreply 类本地名
# ---------------------------------------------------------------------------
NOREPLY_HINTS = [
    "noreply", "no-reply", "donotreply", "do-not-reply",
    "notification", "notifications", "newsletter",
    "mailer-daemon", "postmaster", "bounce",
]
# 明确不算的（真实客户可能用）
NOREPLY_WHITELIST_LOCAL = ["info", "sales", "support"]

# ---------------------------------------------------------------------------
# 规则 2：域名排除名单
# 匹配方式：完整域名相等，或域名按 "." 切分后存在完整标签相等
# ---------------------------------------------------------------------------
BLOCK_DOMAINS = [
    # 既有（试跑版）
    "linkedin.com", "facebook.com", "instagram.com", "twitter.com", "x.com", "youtube.com",
    "tiktok.com", "amazon.com", "alibaba.com", "aliexpress.com", "ebay.com", "taobao.com",
    "tmall.com", "jd.com", "1688.com", "aws.amazon.com", "aliyun.com", "azure.com",
    "qcloud.com", "tencentcloud.com", "notion.so", "slack.com", "asana.com", "trello.com",
    "zoom.us", "dropbox.com", "onedrive.com", "google.com", "googlegroups.com",
    "dhl.com", "fedex.com", "ups.com", "sf-express.com", "ems.com.cn",
    "indeed.com", "glassdoor.com", "monster.com", "51job.com", "zhaopin.com",
    "lagou.com", "liepin.com", "eventbrite.com", "mailchimp.com", "sendgrid.net",
    "constantcontact.com", "hubspot.com", "salesforce.com",
    # 新增：出行与酒店
    "uber.com", "lyft.com", "didi.com", "ctrip.com", "trip.com", "booking.com",
    "expedia.com", "airbnb.com", "marriott.com", "hilton.com", "hyatt.com", "ihg.com",
    "accor.com", "ana.co.jp", "jal.co.jp", "united.com", "delta.com", "aa.com",
    "cathaypacific.com", "airchina.com.cn", "csair.com", "ceair.com", "emirates.com",
    "qatarairways.com", "lufthansa.com", "klm.com", "airfrance.com",
    # 新增：会议记录与调研工具
    "read.ai", "otter.ai", "fireflies.ai", "qualtrics.com", "surveymonkey.com",
    "typeform.com", "service-now.com",
    # 新增：行业媒体群发
    "avinteractive.com", "apb-news.com", "mediakwest.com", "creatorhandbook.com",
    "tvtechnology.com", "svgeurope.org", "sportsvideo.org", "digitalmediaworld.tv",
    "interfuels.com",
    # 新增：付款与财务平台（客户本人发的付款通知另行豁免）
    "paypal.com", "stripe.com", "wise.com", "payoneer.com",
]

# 短标签白名单：这些标签虽然出现在 BLOCK_DOMAINS 里，但不能单独按标签匹配
# （例如 "aa" / "trip" / "ana" / "delta" / "united" / "wise" 单独作标签风险高）
# 处理方式：对长度 <= 4 的标签，要求"标签 + TLD"整体就是名单域名的后缀匹配，
# 即 domain == label + "." + rest 且 rest 属于常见 TLD。
COMMON_TLDS = {"com", "net", "org", "cn", "co", "io", "ai", "us", "uk", "de", "jp",
               "fr", "au", "ca", "tv", "biz", "info", "co.jp", "co.uk", "com.cn",
               "com.au", "co.kr", "com.br", "com.tw", "com.hk", "com.sg", "eu"}

# 付款平台豁免：发件人本人是客户公司财务（本地名不含 noreply）且主题含付款字样
PAYMENT_PLATFORMS = {"paypal.com", "stripe.com", "wise.com", "payoneer.com"}
PAYMENT_WORDS = ["payment", "invoice", "remittance", "transfer", "付款", "汇款", "invoice paid"]

# ---------------------------------------------------------------------------
# 规则 3：纯内部邮件的保留白名单
# ---------------------------------------------------------------------------
KEY_CUSTOMER_WORDS = [
    "dolby", "midwich", "broadfield", "jb&a", "jbanda", "jbanda", "b&h",
    "bhphotovideo", "adorama", "sfm", "vizrt", "telescript", "fortinge",
    "mediaedge", "artisight", "novomatic", "ava", "comline", "nv5", "nbc",
    "jane street", "ask", "starin", "pinnacle",
]
INTERNAL_KEEP_WORDS = [
    "询盘", "inquiry", "rfq", "rfp", "quote", "quotation", "报价",
    "项目", "project", "tender", "po", "样机", "sample", "demo",
    "rma", "投诉", "complaint",
]
FORWARD_PREFIX = re.compile(r"^\s*(fwd?|转发)\s*[:：]", re.I)

# ---------------------------------------------------------------------------
# 规则 4：会议
# ---------------------------------------------------------------------------
MEETING_PREFIX = [
    "accepted:", "declined:", "tentative:", "invitation:", "updated invitation:",
    "canceled:", "cancelled:", "已接受:", "已拒绝:", "暂定:", "邀请:", "会议提醒",
]
WECHAT_CAL_DOMAIN = "exmail.weixin.qq.com"

# ---------------------------------------------------------------------------
# 规则 5：单据
# ---------------------------------------------------------------------------
DOC_WORDS = [
    "invoice", "statement", "remittance", "payment advice", "purchase order",
    "p.o.", "proforma", "packing list", "发票", "对账", "付款", "订单确认",
]

# 重点客户域名（运行时用 cards 表反查后自动补充）
EXTRA_KEY_DOMAINS = [
    "dolby.com", "midwich.com", "midwich.us", "midwich.com.au", "broadfield.com",
    "jbanda.com", "bhphotovideo.com", "adorama.com", "sfm.ca", "vizrt.com",
    "telescript.com", "fortinge.com", "artisight.com", "novomatic.com",
    "comline.de", "nv5.com", "nbcuni.com", "janestreet.com", "starin.biz",
    "gpinnacle.com", "ask-corp.jp", "askgroup.co.jp", "medialink.co.jp",
]

CUSTOMER_PREFIX = PC.cfg.get("customer_folder_prefix") or "其他文件夹/"
OUR_DOMAIN = PC.our_domain()


# ---------------------------------------------------------------------------
def log(m):
    print("[%s] %s" % (datetime.now().strftime("%H:%M:%S"), m), flush=True)


def connect():
    con = sqlite3.connect(DB_PATH, timeout=120)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    con.row_factory = sqlite3.Row
    return con


def local_part(addr):
    return (addr or "").split("@")[0].lower()


def is_noreply_local(addr):
    lp = local_part(addr)
    if not lp:
        return False
    if lp in NOREPLY_WHITELIST_LOCAL:
        return False
    return any(h in lp for h in NOREPLY_HINTS)


def domain_blocked(domain):
    """完整域名或完整标签匹配。禁止子串匹配。"""
    d = (domain or "").lower().strip()
    if not d:
        return False
    for b in BLOCK_DOMAINS:
        if d == b:
            return True
        if d.endswith("." + b):
            return True
        # 完整标签匹配：b 作为 d 的一个 label
        if "." not in b and b in d.split("."):
            # 短标签额外收紧：必须是 label.tld 形式（避免 xx.com 之外的复杂子域误判）
            if len(b) <= 4:
                parts = d.split(".")
                idx = parts.index(b)
                rest = ".".join(parts[idx + 1:])
                if rest in COMMON_TLDS or any(rest == t for t in COMMON_TLDS):
                    return True
            else:
                return True
    return False


def norm_subject(s):
    """去掉开头所有 Re:/Fw:/转发:/[EXT] 等前缀与多余空格"""
    if not s:
        return ""
    x = s
    pat = re.compile(
        r"^\s*(\[\s*(ext|external|外部)\s*\]|(re|fw|fwd|回复|答复|转发|转)\s*\[\s*\d+\s*\]|"
        r"(re|fw|fwd|回复|答复|转发|转))\s*[:：]\s*",
        re.I,
    )
    for _ in range(12):
        y = pat.sub("", x)
        y = re.sub(r"^\s*\[\s*(ext|external|外部)\s*\]\s*", "", y, flags=re.I)
        if y == x:
            break
        x = y
    return re.sub(r"\s+", " ", x).strip()


class UF:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def extract_msgids(s):
    if not s:
        return []
    return [m.lower() for m in re.findall(r"<([^>]+)>", s)]


# ---------------------------------------------------------------------------
def main():
    global DB_PATH
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, help="只用前 N 封调试")
    ap.add_argument("--db", help="指定数据库路径（调试用）")
    args = ap.parse_args()
    if args.db:
        DB_PATH = args.db

    con = connect()
    c = con.cursor()

    # 建表
    c.executescript("""
        CREATE TABLE IF NOT EXISTS meetings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, subject TEXT, organizer TEXT, attendees TEXT,
            external_domains TEXT, response TEXT, folder TEXT, message_key TEXT
        );
        CREATE TABLE IF NOT EXISTS documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT, from_domain TEXT, subject TEXT, attachment_names TEXT,
            folder TEXT, message_key TEXT
        );
        CREATE TABLE IF NOT EXISTS triage (
            message_key TEXT PRIMARY KEY,
            thread_id TEXT, subject TEXT, from_domain TEXT, from_addr TEXT,
            date_utc TEXT, preview TEXT, label TEXT, decided_at TEXT
        );
        CREATE TABLE IF NOT EXISTS rule_stats (
            rule TEXT PRIMARY KEY, n INTEGER
        );
    """)
    c.execute("DELETE FROM meetings")
    c.execute("DELETE FROM documents")
    con.commit()

    # 新增列
    have = {r[1] for r in c.execute("PRAGMA table_info(messages)")}
    for name, ddl in [("internal_keep", "INTEGER DEFAULT 0"),
                      ("exclude_reason", "TEXT"),
                      ("ext_domains", "TEXT")]:
        if name not in have:
            c.execute("ALTER TABLE messages ADD COLUMN %s %s" % (name, ddl))
    con.commit()

    # 重点客户域名：cards 表反查 + 手工补充
    key_domains = set(EXTRA_KEY_DOMAINS)
    try:
        for (d,) in c.execute("SELECT DISTINCT domain FROM cards WHERE domain IS NOT NULL"):
            if d and OUR_DOMAIN not in d:
                key_domains.add(d.lower())
    except Exception:
        pass

    # 加载邮件
    q = """SELECT folder, uid, message_id, in_reply_to, references_hdr, date_utc,
                  from_name, from_addr, from_domain, to_addrs, cc_addrs, subject,
                  is_bulk, attachment_names, has_calendar, folders_all, dedup_key
           FROM messages WHERE is_dup=0"""
    if args.sample:
        q += " LIMIT %d" % args.sample
    rows = [dict(r) for r in c.execute(q)]
    for r in rows:
        r["internal_keep"] = 0
        r["exclude_reason"] = None
    log("载入唯一邮件 %d 封" % len(rows))

    # 我方发出的所有收件域名（判断"从未给该域名发过信"）
    outbound = set()
    for r in rows:
        if (r["from_domain"] or "") == OUR_DOMAIN:
            for fld in ("to_addrs", "cc_addrs"):
                for a in json.loads(r[fld] or "[]"):
                    d = a.split("@")[-1].lower()
                    if d and OUR_DOMAIN not in d:
                        outbound.add(d)
    log("我方曾发信的域名数: %d" % len(outbound))

    # 域名来信总数（判断 >=3 封）
    domain_in = defaultdict(int)
    for r in rows:
        d = (r["from_domain"] or "").lower()
        if d and OUR_DOMAIN not in d:
            domain_in[d] += 1

    stats = defaultdict(int)
    keep, excl = [], []

    # ---------------- 第一层 ----------------
    for r in rows:
        tos = json.loads(r["to_addrs"] or "[]")
        ccs = json.loads(r["cc_addrs"] or "[]")
        all_addrs = [r["from_addr"]] + tos + ccs
        all_addrs = [a for a in all_addrs if a]
        internal = bool(all_addrs) and all(a.lower().endswith("@" + OUR_DOMAIN) for a in all_addrs)
        subj = r["subject"] or ""
        dom = (r["from_domain"] or "").lower()

        # 外部域名集合（供串线程用）
        ext = set()
        if dom and OUR_DOMAIN not in dom:
            ext.add(dom)
        for a in tos + ccs:
            d = a.split("@")[-1].lower()
            if d and OUR_DOMAIN not in d:
                ext.add(d)
        r["ext_domains"] = json.dumps(sorted(ext), ensure_ascii=False)

        # 1) bulk / noreply
        if r["is_bulk"] == 1:
            excl.append((r, "bulk"))
            stats["bulk"] += 1
            continue
        if is_noreply_local(r["from_addr"]):
            excl.append((r, "noreply"))
            stats["noreply"] += 1
            continue

        # 2) 域名黑名单
        if dom and domain_blocked(dom):
            # 付款平台豁免：客户本人（非 noreply）发的付款通知
            if dom in PAYMENT_PLATFORMS and any(w in subj.lower() for w in PAYMENT_WORDS):
                pass
            else:
                excl.append((r, "domain_block"))
                stats["domain_block"] += 1
                continue

        # 3) 纯内部
        if internal:
            hit_customer = any(w in subj.lower() for w in KEY_CUSTOMER_WORDS)
            hit_kw = any(w in subj.lower() for w in INTERNAL_KEEP_WORDS)
            hit_fw = bool(FORWARD_PREFIX.match(subj))
            if hit_customer or hit_kw or hit_fw:
                r["internal_keep"] = 1
                keep.append(r)
                stats["internal_keep"] += 1
            else:
                excl.append((r, "internal"))
                stats["internal"] += 1
            continue

        keep.append(r)
        stats["layer1_pass"] += 1

    log("第一层：保留 %d，排除 %d（bulk=%d noreply=%d 域名=%d 内部=%d，其中内部保留=%d）"
        % (len(keep), len(excl), stats["bulk"], stats["noreply"], stats["domain_block"],
           stats["internal"], stats["internal_keep"]))

    # ---------------- 第二层：会议 / 单据 ----------------
    meetings, docs, rest = [], [], []
    for r in keep:
        subj = r["subject"] or ""
        low = subj.lower().strip()
        dom = (r["from_domain"] or "").lower()

        is_meeting = False
        if any(low.startswith(p) for p in MEETING_PREFIX):
            is_meeting = True
        elif r["has_calendar"] == 1:
            is_meeting = True
        elif dom == WECHAT_CAL_DOMAIN:
            is_meeting = True

        if is_meeting:
            if "declined" in low or "已拒绝" in subj:
                resp = "拒绝"
            elif "accepted" in low or "已接受" in subj:
                resp = "接受"
            elif "tentative" in low or "暂定" in subj:
                resp = "暂定"
            else:
                resp = "邀请"
            meetings.append(r)
            stats["meeting"] += 1
            r["exclude_reason"] = "meeting"
            r["_resp"] = resp
            continue

        if dom and OUR_DOMAIN not in dom:
            hit = [w for w in DOC_WORDS if w in low]
            if hit:
                docs.append(r)
                stats["document"] += 1
                r["exclude_reason"] = "document"
                continue

        rest.append(r)

    log("第二层：会议 %d 条 -> meetings，单据 %d 条 -> documents，余 %d"
        % (len(meetings), len(docs), len(rest)))

    # 写 meetings / documents
    con.executemany("""
        INSERT INTO meetings (date, subject, organizer, attendees, external_domains, response, folder, message_key)
        VALUES (?,?,?,?,?,?,?,?)
    """, [(
        r["date_utc"], r["subject"], r["from_addr"],
        json.dumps(sorted(set(json.loads(r["to_addrs"] or "[]") + json.loads(r["cc_addrs"] or "[]"))),
                   ensure_ascii=False),
        r["ext_domains"], r.get("_resp", "邀请"), r["folder"], r["dedup_key"],
    ) for r in meetings])
    con.executemany("""
        INSERT INTO documents (date, from_domain, subject, attachment_names, folder, message_key)
        VALUES (?,?,?,?,?,?)
    """, [(r["date_utc"], r["from_domain"], r["subject"],
           r["attachment_names"], r["folder"], r["dedup_key"]) for r in docs])
    con.commit()

    # ---------------- 串线程 A：Message-ID / In-Reply-To / References ----------------
    by_msgid = {}
    for r in rest:
        mid = r["dedup_key"]
        by_msgid.setdefault(mid, r)

    uf = UF()
    for r in rest:
        uf.find(r["dedup_key"])

    for r in rest:
        refs = extract_msgids(r["in_reply_to"]) + extract_msgids(r["references_hdr"])
        for ref in refs:
            if ref in by_msgid:
                uf.union(ref, r["dedup_key"])

    # ---------------- 串线程 B：主题相同 + 域名重叠 + 90 天内 ----------------
    def parse_d(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    groups = defaultdict(list)
    for r in rest:
        r["_norm"] = norm_subject(r["subject"]).lower()
    for r in rest:
        groups[r["_norm"]].append(r)

    merged_b = 0
    for subj, items in groups.items():
        if len(items) < 2:
            continue
        # 按域名建索引，只在共享域名的邮件之间比较
        by_dom = defaultdict(list)
        for it in items:
            for d in json.loads(it["ext_domains"] or "[]"):
                by_dom[d].append(it)
        pairs = set()
        for d, its in by_dom.items():
            if len(its) < 2 or len(its) > 60:
                continue
            for i in range(len(its)):
                for j in range(i + 1, len(its)):
                    a, b = its[i], its[j]
                    da, db = parse_d(a["date_utc"]), parse_d(b["date_utc"])
                    if not da or not db:
                        continue
                    if abs((da - db).days) <= 90:
                        pairs.add((min(a["dedup_key"], b["dedup_key"]),
                                   max(a["dedup_key"], b["dedup_key"])))
        for x, y in pairs:
            if uf.find(x) != uf.find(y):
                uf.union(x, y)
                merged_b += 1
    log("串线程：A 阶段按引用链，B 阶段额外合并 %d 次" % merged_b)

    # ---------------- 生成 thread_id ----------------
    members = defaultdict(list)
    for r in rest:
        members[uf.find(r["dedup_key"])].append(r)

    threads = []
    for root, items in members.items():
        items.sort(key=lambda x: (x["date_utc"] or "", x["dedup_key"]))
        dates = [i["date_utc"] for i in items if i["date_utc"]]
        first, last = (min(dates), max(dates)) if dates else (None, None)
        tid = "T" + (first or "0000")[0:10].replace("-", "") + "_" + root[:40]
        doms, people, folders = set(), set(), set()
        for i in items:
            doms.update(json.loads(i["ext_domains"] or "[]"))
            if i["from_domain"] == OUR_DOMAIN:
                people.add(i["from_addr"])
            for fld in ("to_addrs", "cc_addrs"):
                for a in json.loads(i[fld] or "[]"):
                    if a.endswith("@" + OUR_DOMAIN):
                        people.add(a)
            folders.add(i["folder"])
        last_ext = 0
        if items[-1]["from_domain"] and items[-1]["from_domain"] != OUR_DOMAIN:
            last_ext = 1
        threads.append({
            "thread_id": tid,
            "subject": norm_subject(items[0]["subject"]) or items[0]["subject"],
            "first_date": first, "last_date": last,
            "message_count": len(items),
            "external_domains": json.dumps(sorted(doms), ensure_ascii=False),
            "our_people": json.dumps(sorted(people), ensure_ascii=False),
            "last_sender_is_external": last_ext,
            "folders": json.dumps(sorted(folders), ensure_ascii=False),
            "members": items,
        })

    # ---------------- 第三层：判断是否需分诊 ----------------
    triage_rows = []
    for t in threads:
        doms = json.loads(t["external_domains"])
        has_ours = any(i["from_domain"] == OUR_DOMAIN for i in t["members"])
        is_key = any(d in key_domains for d in doms)
        in_customer_folder = any(f.startswith(CUSTOMER_PREFIX) for f in json.loads(t["folders"]))
        dom_freq3 = any(domain_in.get(d, 0) >= 3 for d in doms)

        need_triage = (
            t["message_count"] == 1
            and bool(doms)
            and not any(d in outbound for d in doms)
            and not has_ours
            and not is_key
            and not in_customer_folder
            and not dom_freq3
        )
        t["need_triage"] = need_triage
        t["auto_pass"] = not need_triage

    log("第三层：需分诊线程 %d，免分诊 %d"
        % (sum(1 for t in threads if t["need_triage"]),
           sum(1 for t in threads if t["auto_pass"])))

    # ---------------- 落库 ----------------
    c.executescript("""
        DROP TABLE IF EXISTS threads;
        CREATE TABLE threads (
            thread_id TEXT PRIMARY KEY,
            subject TEXT, first_date TEXT, last_date TEXT, message_count INTEGER,
            external_domains TEXT, our_people TEXT, last_sender_is_external INTEGER,
            folders TEXT, has_ours INTEGER DEFAULT 0, need_triage INTEGER DEFAULT 0,
            status TEXT, folder_label TEXT
        );
    """)

    # 回写 messages.kept / thread_id / exclude_reason
    c.execute("UPDATE messages SET kept=0, thread_id=NULL, exclude_reason=NULL, internal_keep=0")
    upd_keep = []
    for t in threads:
        for i in t["members"]:
            upd_keep.append((i["internal_keep"], i["ext_domains"], t["thread_id"],
                             i["folder"], i["uid"]))
    c.executemany(
        "UPDATE messages SET kept=1, internal_keep=?, ext_domains=?, thread_id=? "
        "WHERE folder=? AND uid=?", upd_keep)
    upd_excl = [(r, r["folder"], r["uid"]) for r, _ in excl]
    # 逐条回写排除原因
    c.executemany("UPDATE messages SET exclude_reason=? WHERE folder=? AND uid=?",
                  [(why, r["folder"], r["uid"]) for r, why in excl])
    c.executemany("UPDATE messages SET exclude_reason=? WHERE folder=? AND uid=?",
                  [(r["exclude_reason"], r["folder"], r["uid"]) for r in meetings + docs])

    c.executemany("""
        INSERT OR REPLACE INTO threads
        (thread_id, subject, first_date, last_date, message_count, external_domains,
         our_people, last_sender_is_external, folders, has_ours, need_triage, status, folder_label)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [(
        t["thread_id"], t["subject"], t["first_date"], t["last_date"], t["message_count"],
        t["external_domains"], t["our_people"], t["last_sender_is_external"],
        t["folders"],
        1 if any(i["from_domain"] == OUR_DOMAIN for i in t["members"]) else 0,
        1 if t["need_triage"] else 0,
        "待分诊" if t["need_triage"] else "待取正文",
        next((f for f in json.loads(t["folders"]) if f.startswith(CUSTOMER_PREFIX)), None),
    ) for t in threads])

    # 分诊清单（正文前 300 字由第③步的 preview 逻辑填，这里先占位）
    for t in threads:
        if t["need_triage"]:
            m = t["members"][0]
            triage_rows.append((m["dedup_key"], t["thread_id"], m["subject"],
                                m["from_domain"], m["from_addr"], m["date_utc"], None, None, None))
    c.executemany("""
        INSERT OR REPLACE INTO triage
        (message_key, thread_id, subject, from_domain, from_addr, date_utc, preview, label, decided_at)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, triage_rows)

    # 规则统计
    for k, v in stats.items():
        c.execute("INSERT OR REPLACE INTO rule_stats (rule, n) VALUES (?,?)", (k, v))
    c.execute("INSERT OR REPLACE INTO rule_stats (rule, n) VALUES (?,?)",
              ("threads", len(threads)))
    con.commit()

    # 汇总
    sizes = defaultdict(int)
    for t in threads:
        sizes[min(t["message_count"], 10)] += 1
    log("最终线程 %d 个，线程大小分布: %s"
        % (len(threads), dict(sorted(sizes.items()))))
    log("规则统计: %s" % dict(stats))
    con.close()


if __name__ == "__main__":
    main()
