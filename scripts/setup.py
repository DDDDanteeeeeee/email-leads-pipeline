#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
首次配置向导：交互式收集邮箱与业务信息，生成本地配置。

产出（都在用户自己的工作目录）：
    pipeline_config.json   工作路径 / 公司名 / 域名 / 日期范围 / 词表   0600
    <program_dir>/.env     IMAP 服务器 / 账号 / 授权码                0600

安全约束
- 授权码只在本机写入 .env，绝不打印、绝不进 config.json、绝不上传。
- IMAP 全程只读（readonly + BODY.PEEK），不写服务器、不发邮件。
"""
import getpass
import json
import os
import stat
import sys

DEFAULT_HOST = "imap.exmail.qq.com"


def ask(prompt, default=""):
    tip = " [%s]" % default if default else ""
    v = input("%s%s: " % (prompt, tip)).strip()
    return v if v else default


def yes(prompt, default="y"):
    v = input("%s [y/N]: " % prompt).strip().lower()
    return (v or default).lower().startswith("y")


def write_secret(path, kv):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    body = "\n".join("%s=%s" % (k, v) for k, v in kv.items()) + "\n"
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)  # 600
    print("  已写入 %s（权限 600，仅你可读）" % path)


def write_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    print("  已写入 %s（权限 600）" % path)


def main():
    print("=" * 60)
    print("邮箱商机库 · 首次配置向导")
    print("=" * 60)
    print("说明：所有配置只保存在你本机；授权码不会被打印或上传。\n")

    ws = ask("① 工作目录（存放数据库/抽取文件/配置）", os.getcwd())
    ws = os.path.abspath(os.path.expanduser(ws))
    os.makedirs(ws, exist_ok=True)

    print("\n--- 邮箱连接（IMAP，只读）---")
    host = ask("  IMAP 服务器", DEFAULT_HOST)
    port = ask("  端口（一般 993）", "993")
    user = ask("  邮箱账号（完整地址）")
    while not user or "@" not in user:
        user = ask("  邮箱账号（需含 @）")
    pwd = getpass.getpass("  授权码/密码（输入不显示）: ")
    while not pwd:
        pwd = getpass.getpass("  授权码不能为空，请重输: ")

    print("\n--- 你的业务身份（用于卡片字段，可稍后改）---")
    our_domain = user.split("@")[-1]
    our_company = ask("  你方公司名称", our_domain.split(".")[0])
    owner_name = ask("  负责人称呼（写进卡片的参与人）", "负责人")
    owner_email = ask("  负责人邮箱（可留空）", user)

    print("\n--- 抽取范围 ---")
    since = ask("  起始日期（IMAP 格式，如 01-Jan-2023）", "01-Jan-2023")
    prefix = ask("  客户归档文件夹前缀（如 其他文件夹/，没有就留空）", "其他文件夹/")
    key_domains = ask("  重点客户域名（逗号分隔，可留空）", "")

    out_name = ask("  抽取产物文件夹名", "抽取")
    prog_name = ask("  程序/日志文件夹名", "邮件归档程序")
    db_name = ask("  数据库文件名", "mail_index.sqlite")
    tag = ask("  批次文件名前缀（如 m_）", "m_")

    cfg = {
        "workspace": ws,
        "db_name": db_name,
        "out_dir_name": out_name,
        "program_dir_name": prog_name,
        "tag": tag,
        "since": since,
        "our_domain": our_domain,
        "our_company": our_company,
        "owner_name": owner_name,
        "owner_email": owner_email,
        "customer_folder_prefix": prefix,
        "key_customer_domains": [d.strip() for d in key_domains.split(",") if d.strip()],
        "env_file": os.path.join(ws, prog_name, ".env"),
        "immediate_imap_host": host,
        "imap_port": port,
        "_note": "本文件含业务信息，权限 600；授权码不在此文件。",
    }

    cfg_path = os.path.join(ws, "pipeline_config.json")
    env_path = os.path.join(ws, prog_name, ".env")
    write_json(cfg_path, cfg)
    write_secret(env_path, {
        "IMAP_HOST": host,
        "IMAP_PORT": port,
        "IMAP_USER": user,
        "IMAP_PASSWORD": pwd,
    })

    print("\n" + "=" * 60)
    print("配置完成！工作目录：%s" % ws)
    print("接下来可让 WorkBuddy 开始跑第①步建索引。")
    print("提示：授权码如变更，重跑本向导或直接编辑 .env。")


if __name__ == "__main__":
    main()
