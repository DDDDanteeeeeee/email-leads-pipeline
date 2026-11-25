#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
单批 JSONL 自检（抽卡子代理必跑）。

用法：
    python3 verify_jsonl.py <批文件.txt> <产出.jsonl>

检查：
1. 每行都能 json.loads
2. 卡片数 + 跳过数 == txt 里 ### RECORD 的线程数
3. 每个 ### RECORD id 都出现在某行 record_id
4. signals 全在 12 类词表内；status 在 5 值内
5. 已结束状态无 open_question
6. 无银行账号 / license key 等敏感串

退出码 0=全过，1=有问题（会打印具体问题）。
"""
import json
import re
import sys

SIGNALS_OK = {
    "询价", "项目机会", "渠道合作", "样机测试", "定制OEM", "采购",
    "风险投诉", "引荐", "产品需求", "竞品情报", "技术问题", "市场趋势",
}
STATUS_OK = {"未回复", "回了但未闭环", "中断", "已结束", "待确认"}
REC = re.compile(r"^### RECORD (.+?)\s*(?:\|\s*\w+)?\s*$", re.M)


def looks_sensitive(s):
    if not isinstance(s, str):
        return False
    # 连续 12 位以上数字（卡号/账号），或 20 位以上含 +/ = 的串（license key）
    if re.search(r"(?:\d[\s-]?){12,}", s):
        return True
    if re.search(r"[A-Za-z0-9+/=]{28,}", s) and re.search(r"[+/=]", s):
        return True
    return False


def main():
    if len(sys.argv) != 3:
        print("用法: python3 verify_jsonl.py <批.txt> <产出.jsonl>")
        sys.exit(2)
    txt_path, jsonl_path = sys.argv[1], sys.argv[2]
    ids = set(REC.findall(open(txt_path, encoding="utf-8").read()))
    problems = []
    got = set()
    cards = skips = 0
    for ln, line in enumerate(open(jsonl_path, encoding="utf-8"), 1):
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except Exception as e:
            problems.append("第%d行 JSON 非法: %s" % (ln, e))
            continue
        rid = d.get("record_id")
        if rid:
            got.add(rid)
        if "skip" in d:
            skips += 1
            continue
        cards += 1
        sg = set(d.get("signals") or [])
        if sg - SIGNALS_OK:
            problems.append("第%d行 signals 越界: %s" % (ln, sorted(sg - SIGNALS_OK)))
        if not sg:
            problems.append("第%d行 signals 为空" % ln)
        if d.get("status") and d["status"] not in STATUS_OK:
            problems.append("第%d行 status 越界: %s" % (ln, d["status"]))
        if d.get("status") == "已结束" and d.get("open_question"):
            problems.append("第%d行 已结束但有 open_question" % ln)
        for k, v in d.items():
            if looks_sensitive(v) and k != "evidence":
                problems.append("第%d行字段 %s 疑似敏感凭据" % (ln, k))
                break
    if cards + skips != len(ids):
        problems.append("行数 %d != 线程数 %d（卡%d+跳%d）" % (
            cards + skips, len(ids), cards, skips))
    missing = ids - got
    if missing:
        problems.append("缺失 %d 个 record_id: %s" % (len(missing), list(missing)[:5]))
    extra = got - ids
    if extra:
        problems.append("多余 %d 个 record_id: %s" % (len(extra), list(extra)[:5]))

    if problems:
        print("自检未通过（%d 项）：" % len(problems))
        for p in problems[:30]:
            print("  -", p)
        sys.exit(1)
    print("自检通过：线程 %d / 卡片 %d / 跳过 %d / 无缺失 / 词表合法 / 无敏感凭据" % (
        len(ids), cards, skips))


if __name__ == "__main__":
    main()
