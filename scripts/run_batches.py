#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
第④步 编排器：给「无人值守自动续跑」用的确定性状态机。

用法：
  python3 run_batches.py status         # 总览：完成/待做/缺漏
  python3 run_batches.py next -n 6      # 打印接下来该做的批号（一行一个）
  python3 run_batches.py gaps           # 给有缺漏的批次生成 *_gap.txt（只含缺的线程）
  python3 run_batches.py import         # 导入所有已产出但未导入的 jsonl（幂等）
  python3 run_batches.py mark <bid>     # 手工标记某批已导入（一般不手工用）

批单元 = 目录里每个 *.txt（含 *_gap.txt）。
状态：pending（无 jsonl）/ partial（jsonl 有缺漏）/ ready（齐了但没导入）/ done（已导入）
"""

import argparse
import glob
import json
import os
import re
import sqlite3
import sys

import pipeline_config as PC
_P = PC.paths()
BASE = _P["workspace"]
DIR = _P["out_dir"]
DB = _P["db"]
STATE = os.path.join(DIR, "_imported.json")

REC = re.compile(r"^### RECORD (.+?)\s*(?:\|\s*\w+)?\s*$", re.M)
SIGNALS_OK = {
    "询价", "项目机会", "渠道合作", "样机测试", "定制OEM", "采购",
    "风险投诉", "引荐", "产品需求", "竞品情报", "技术问题", "市场趋势",
}
STATUS_OK = {"未回复", "回了但未闭环", "中断", "已结束", "待确认"}


def load_state():
    if os.path.exists(STATE):
        try:
            return json.load(open(STATE, encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_state(s):
    json.dump(s, open(STATE, "w", encoding="utf-8"), ensure_ascii=False, indent=1)


def units():
    """返回 [{'bid','txt','jsonl','n','ids'}]"""
    out = []
    for p in sorted(glob.glob(os.path.join(DIR, "*.txt"))):
        bid = os.path.basename(p)[:-4]
        ids = set(REC.findall(open(p, encoding="utf-8").read()))
        out.append({
            "bid": bid, "txt": p,
            "jsonl": p[:-4] + ".jsonl",
            "n": len(ids), "ids": ids,
        })
    return out


def _seg_map():
    """txt 文件名 -> 年段；来自 _index.json。用于按「2025+ -> 2024 -> 2023」排优先级。"""
    out = {}
    p = os.path.join(DIR, "_index.json")
    if os.path.exists(p):
        try:
            for k, v in json.load(open(p, encoding="utf-8")).items():
                out[k[:-4] if k.endswith(".txt") else k] = v.get("segments") or []
        except Exception:
            pass
    return out


_SEGS = _seg_map()


def kind(bid):
    """排序键：年段优先（2025+ 先做），同段内按批号，补漏批排在同号之后。"""
    segs = _SEGS.get(bid) or _SEGS.get(bid.replace("_gap", "")) or []
    seg_rank = 0 if "2025plus" in segs else (1 if "2024" in segs else 2)
    m = re.match(r"p(\d+)_batch_(\d+)(_gap)?$", bid)
    if not m:
        return (3, 9, 0, 0)
    return (seg_rank, int(m.group(1)), int(m.group(2)), 1 if m.group(3) else 0)


def scan(bid_unit, imported):
    u = bid_unit
    if not os.path.exists(u["jsonl"]):
        return "pending", set(), 0
    got, bad = set(), 0
    for line in open(u["jsonl"], encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except Exception:
            bad += 1
            continue
        rid = d.get("record_id")
        if rid:
            got.add(rid)
    # 补漏批已产出的记录，也算这个批单元补齐了（否则会永远判 partial、永远不再收尾）
    if "_gap" not in u["bid"]:
        gj = u["jsonl"][:-6] + "_gap.jsonl"
        if os.path.exists(gj):
            for line in open(gj, encoding="utf-8"):
                line = line.strip()
                if not line:
                    continue
                try:
                    d = json.loads(line)
                except Exception:
                    continue
                if d.get("record_id"):
                    got.add(d["record_id"])
    miss = u["ids"] - got
    if miss:
        return "partial", miss, bad
    if u["bid"] not in imported:
        return "ready", set(), bad
    return "done", set(), bad


def write_board(stat, n_units, done_thr, total_thr, pend, part, ready, cards, skips, cov,
                threads, bodies, kept):
    from datetime import datetime
    p = os.path.join(BASE, "第4步_进度看板.md")
    lines = [
        "# 邮件商机库 第④步 抽卡进度看板",
        "",
        "> 自动更新：%s　（每次自动运行都会刷新）" % datetime.now().strftime("%Y-%m-%d %H:%M"),
        "",
        "## 完成度",
        "",
        "| 项 | 数值 |",
        "|---|---|",
        "| 批单元 | **%d / %d 完成** |" % (stat["done"], n_units),
        "| 待做 | pending %d，缺漏待补 %d，已产出待导入 %d |" % (
            stat["pending"], stat["partial"], stat["ready"]),
        "| 线程覆盖 | **%d / %d（%.1f%%）** |" % (cov, threads, cov * 100.0 / max(threads, 1)),
        "| 卡片 | %s 张（另跳过 %s 条） |" % (f"{cards:,}", f"{skips:,}"),
        "| 正文覆盖 | %s / %s 封（%.1f%%） |" % (
            f"{bodies:,}", f"{kept:,}", bodies * 100.0 / max(kept, 1)),
        "",
    ]
    if pend:
        lines += ["## 接下来要抽的批次", "",
                  "按「2025 年至今 → 2024 → 2023」排序，先做新商机：", "",
                  "`%s`" % " ".join(sorted(pend, key=kind)[:15]), ""]
    if part:
        lines += ["## 有缺漏待补的批次", ""]
        for b, m_, bad in part:
            lines.append("- %s：缺 %d 个线程，坏行 %d" % (b, m_, bad))
        lines.append("")
    lines += ["## 怎么接着跑", "",
              "自动化「邮件第④步·周末自动续抽」每小时运行一次，配额恢复后自动继续；",
              "全部批次完成后会自动重生成报告、落盘数据库并暂停自己。", "",
              "手工介入时看：`邮件归档程序/第4步_续跑说明.md`", ""]
    open(p, "w", encoding="utf-8").write("\n".join(lines))
    try:
        os.chmod(p, 0o600)
    except Exception:
        pass


def cmd_status(args):
    imported = load_state()
    us = units()
    stat = {"pending": 0, "partial": 0, "ready": 0, "done": 0}
    pend, part, ready = [], [], []
    threads_total = threads_done = 0
    for u in us:
        st, miss, bad = scan(u, imported)
        stat[st] += 1
        threads_total += u["n"]
        if st == "done":
            threads_done += u["n"]
        elif st == "pending":
            pend.append(u["bid"])
        elif st == "partial":
            part.append((u["bid"], len(miss), bad))
        elif st == "ready":
            ready.append(u["bid"])
    print("批单元 %d｜pending %d / partial %d / ready %d / done %d" % (
        len(us), stat["pending"], stat["partial"], stat["ready"], stat["done"]))
    print("线程覆盖：%d / %d（%.1f%%）" % (
        threads_done, threads_total, threads_done * 100.0 / max(threads_total, 1)))
    if ready:
        print("待导入（已产出未入库）：%s" % " ".join(ready[:20]))
    if part:
        print("有缺漏：")
        for b, m, bad in part[:15]:
            print("  %-22s 缺 %d 线程，坏行 %d" % (b, m, bad))
    if pend:
        print("下一批待做：%s" % " ".join(sorted(pend, key=kind)[:12]))
    try:
        c = sqlite3.connect(DB, timeout=60)
        cards = c.execute("select count(*) from cards").fetchone()[0]
        skips = c.execute("select count(*) from card_skips").fetchone()[0]
        cov = c.execute("select count(distinct record_id) from (select record_id from cards "
                        "union all select record_id from card_skips)").fetchone()[0]
        threads = c.execute("select count(*) from threads").fetchone()[0]
        bodies = c.execute("select count(*) from bodies").fetchone()[0]
        kept = c.execute("select count(*) from messages where kept=1 and is_dup=0 "
                         "and exclude_reason is null").fetchone()[0]
        print("库内：cards %d / skips %d / 覆盖线程 %d" % (cards, skips, cov))
        c.close()
        write_board(stat, len(us), threads_done, threads_total, pend, part, ready,
                    cards, skips, cov, threads, bodies, kept)
        print("看板已更新：%s" % os.path.join(BASE, "第4步_进度看板.md"))
    except Exception as e:
        print("读库失败：%s" % e)


def load_defer():
    """被暂停的批次（例如内容审批拦住、子代理读不了）：不在 next 里派发。"""
    p = os.path.join(DIR, "_defer.json")
    try:
        return set(json.load(open(p, encoding="utf-8")))
    except Exception:
        return set()


def cmd_next(args):
    imported = load_state()
    defer = load_defer()
    pend = []
    for u in units():
        st, _, _ = scan(u, imported)
        if st == "pending" and u["bid"] not in defer:
            pend.append(u["bid"])
    # 顺带提醒：ready 未导入的先导入
    ready = [u["bid"] for u in units() if scan(u, imported)[0] == "ready"]
    if ready:
        print("# 提示：先跑 import，以下已产出未入库：%s" % " ".join(ready), file=sys.stderr)
    for b in sorted(pend, key=kind)[:args.n]:
        print(b)


def cmd_gaps(args):
    imported = load_state()
    made = 0
    for u in units():
        st, miss, bad = scan(u, imported)
        if st != "partial":
            continue
        if "_gap" in u["bid"]:
            continue  # 补漏批本身有缺，不再无限套娃
        gap_txt = u["txt"][:-4] + "_gap.txt"
        gap_jsonl = gap_txt[:-4] + ".jsonl"
        if os.path.exists(gap_jsonl):
            continue  # 已经补过
        blocks = {}
        for blk in re.split(r"(?=^### RECORD )", open(u["txt"], encoding="utf-8").read(),
                            flags=re.M):
            m = REC.match(blk)
            if m:
                blocks[m.group(1)] = blk
        sel = [blocks[i] for i in miss if i in blocks]
        if not sel:
            print("%s：缺的线程在源文件里找不到，跳过" % u["bid"])
            continue
        open(gap_txt, "w", encoding="utf-8").write("".join(sel))
        made += 1
        print("生成 %s（%d 线程 / %d 字符）" % (
            os.path.basename(gap_txt), len(sel), sum(len(x) for x in sel)))
    if not made:
        print("没有需要补的批次")


BS = chr(92)


def _ok_json(s):
    try:
        json.loads(s)
        return True
    except Exception:
        return False


def _fix_struct(x):
    """把结构位置上被写成「反斜杠 + n」的地方改回逗号/花括号。"""
    for pat, rep in (("," + BS + "n\"", ",\""), ("}" + BS + "n\"", "},\""),
                     ("]" + BS + "n\"", "],\""), ("{" + BS + "n\"", "{\"")):
        x = x.replace(pat, rep)
    return x


def salvage_line(s):
    """修复子代理输出的结构性写坏，返回可解析的字符串或 None。

    实测见到的坏法（可能同时出现）：
      1) 字段之间该是逗号的地方写成了「反斜杠 + n」  -> 换回逗号
      2) 用「反斜杠 + n 两个字符」当换行，把两个对象粘成一行 -> 拆成两行
    只在原行解析失败时才调用，且只有修完能解析才采用。
    """
    glue = "}" + BS + "n{"
    for base in (s, _fix_struct(s)):
        if glue in base:
            parts = [x for x in base.replace(glue, "}\n{").split("\n") if x.strip()]
            if parts and all(_ok_json(x) for x in parts):
                return "\n".join(parts)
        if _ok_json(base):
            return base
    return None


def repair_jsonl(path, max_join=4):
    """修复坏行（子代理输出偶发）。

    原则：**只处理坏行，绝不吞掉相邻的好行**。
    - 单行能解析 -> 直接用
    - 单行不能 -> 先试结构性修复（salvage_line）
    - 再不能 -> 试与下一行拼接（最多 max_join 次）；拼成功就吃两行
    - 都不行 -> 该行原样保留（后续按坏行处理），继续看下一行
    返回 (输出行数, 仍坏的行数)。
    """
    raw = open(path, encoding="utf-8").read()
    lines = raw.split("\n")
    out, changed = [], False
    i = 0

    while i < len(lines):
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        if _ok_json(line):
            out.append(line)
            i += 1
            continue
        fixed = salvage_line(line)
        if fixed:
            out.extend(x for x in fixed.split("\n") if x.strip())
            changed = True
            i += 1
            continue
        # 尝试拼接后续行（被真换行截断的字符串）
        buf, j, joined = line, i, False
        while j + 1 < len(lines) and (j - i) < max_join:
            j += 1
            buf = buf + BS + "n" + lines[j]
            if _ok_json(buf):
                out.append(buf)
                changed = True
                joined = True
                break
        if joined:
            i = j + 1
        else:
            out.append(line)   # 坏行原样留着，只丢它自己
            i += 1
    if changed:
        open(path, "w", encoding="utf-8").write("\n".join(out) + "\n")
    return len(out), sum(0 if _ok_json(x) else 1 for x in out)


def cmd_import(args):
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from import_cards import main as import_one
    imported = load_state()
    n = 0
    for u in units():
        st, _, _ = scan(u, imported)
        if st not in ("ready", "partial"):
            continue
        if not os.path.exists(u["jsonl"]):
            continue
        if st == "partial":
            total, still = repair_jsonl(u["jsonl"])
            if still:
                print("  %s 修复后仍有 %d 行损坏，交 gaps 补抽" % (u["bid"], still))
        rc = import_one(u["jsonl"])
        if rc == 0:
            imported[u["bid"]] = True
            save_state(imported)
            n += 1
        else:
            print("  %s 有坏行，未标记为已导入（补漏后再导）" % u["bid"])
    # 同一线程若同时存在试跑卡与 v3 卡，以 v3 为准（公司名写法不同会并存两张）
    try:
        con = sqlite3.connect(DB, timeout=120)
        cur = con.execute("""
            DELETE FROM cards
            WHERE COALESCE(batch,'') NOT LIKE 'p1_%' AND COALESCE(batch,'') NOT LIKE 'p2_%'
              AND record_id IN (SELECT record_id FROM cards
                                WHERE batch LIKE 'p1_%' OR batch LIKE 'p2_%')
        """)
        if cur.rowcount and cur.rowcount > 0:
            print("  清理试跑重复卡 %d 张（同线程已有 v3 卡）" % cur.rowcount)
        con.commit()
        con.close()
    except Exception as e:
        print("  去重跳过：%s" % e)
    print("本轮导入 %d 个批单元" % n)


def cmd_verify(args):
    """检查 jsonl 的字段合法性（给自动化做最后一道闸）"""
    imported = load_state()
    bad_total = 0
    for u in units():
        if not os.path.exists(u["jsonl"]):
            continue
        bad = []
        for i, line in enumerate(open(u["jsonl"], encoding="utf-8"), 1):
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                bad.append((i, "坏 JSON"))
                continue
            if "skip" in d:
                continue
            sg = d.get("signals") or []
            if sg and not set(sg) <= SIGNALS_OK:
                bad.append((i, "signals 越界 %s" % (set(sg) - SIGNALS_OK)))
            if d.get("status") and d["status"] not in STATUS_OK:
                bad.append((i, "status 越界 %s" % d["status"]))
            if d.get("status") == "已结束" and d.get("open_question"):
                bad.append((i, "已结束但有 open_question"))
        if bad:
            bad_total += len(bad)
            print("%-22s %d 处问题：%s" % (u["bid"], len(bad), bad[:3]))
    print("校验完成，问题 %d 处" % bad_total)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("status")
    p = sub.add_parser("next")
    p.add_argument("-n", type=int, default=6)
    sub.add_parser("gaps")
    sub.add_parser("import")
    sub.add_parser("verify")
    args = ap.parse_args()
    {"status": cmd_status, "next": cmd_next, "gaps": cmd_gaps,
     "import": cmd_import, "verify": cmd_verify}[args.cmd](args)


if __name__ == "__main__":
    main()
