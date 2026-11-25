#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
环境自检：验证配置文件存在、路径可写、IMAP 能只读连通。绝不打印授权码。
"""
import imaplib
import os
import sys

import pipeline_config as PC


def main():
    c = PC.get_cfg()
    if not c.get("_config_path"):
        print("× 未找到 pipeline_config.json —— 请先运行 setup.py")
        sys.exit(1)
    print("√ 配置文件:", c["_config_path"])
    p = PC.paths()
    for key in ("workspace", "out_dir", "program_dir"):
        d = p[key]
        ok = os.path.isdir(d)
        print(("√ " if ok else "× ") + "%s: %s" % (key, d))
    e = PC.env()
    need = ["IMAP_HOST", "IMAP_USER", "IMAP_PASSWORD"]
    miss = [k for k in need if not e.get(k)]
    if miss:
        print("× .env 缺少字段:", ", ".join(miss))
        sys.exit(1)
    print("√ .env 凭据齐全（值不显示）")
    print("  登录账号:", e["IMAP_USER"])
    print("  我方域名:", PC.our_domain() or "(未识别)")

    host = e["IMAP_HOST"]
    port = int(c.get("imap_port") or 993)
    print("正在只读连接 %s:%d ..." % (host, port))
    try:
        cli = imaplib.IMAP4_SSL(host, port, timeout=30)
        cli.login(e["IMAP_USER"], e["IMAP_PASSWORD"])
        typ, data = cli.list()
        folders = [x for x in data if x]
        print("√ IMAP 登录成功，可见 %d 个文件夹" % len(folders))
        # 只读打开 INBOX 验证
        cli.select('"INBOX"', readonly=True)
        print("√ INBOX 只读打开成功（readonly）")
        cli.logout()
    except Exception as ex:
        print("× IMAP 连接失败:", type(ex).__name__, str(ex)[:120])
        sys.exit(1)
    print("\n全部检查通过，可以开始第①步。")


if __name__ == "__main__":
    main()
