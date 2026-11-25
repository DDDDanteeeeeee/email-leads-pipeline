#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
通用配置层：所有脚本从这里读取路径、业务身份与词表。

设计原则
- 路径、公司名、域名、日期范围等全部来自工作目录的 config.json，无硬编码绝对路径。
- 邮箱凭据不进 config.json，只从 config.json 指定的 env_file（.env）读取，且仅运行时读取。
- 只读 IMAP：所有脚本一律 select(readonly=True) + BODY.PEEK，绝不写服务器。

用法（其它脚本顶部）：
    from pipeline_config import cfg, env, paths
    DB   = paths["db"]
    OUT  = paths["out_dir"]
    HOST = env().get("IMAP_HOST", "imap.exmail.qq.com")
"""
import json
import os
import sys

# 配置文件查找顺序：显式环境变量 > 当前工作目录 > 本脚本上级目录（skill 根）
CONFIG_NAME = "pipeline_config.json"


def _find_config(explicit=None):
    candidates = []
    if explicit:
        candidates.append(explicit)
    if os.environ.get("LEADS_CONFIG"):
        candidates.append(os.environ["LEADS_CONFIG"])
    candidates.append(os.path.join(os.getcwd(), CONFIG_NAME))
    candidates.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), CONFIG_NAME))
    for c in candidates:
        if c and os.path.exists(c):
            return c
    return None


_DEFAULT = {
    "workspace": "",
    "db_name": "mail_index.sqlite",
    "out_dir_name": "抽取",
    "program_dir_name": "邮件归档程序",
    "tag": "m_",
    "since": "01-Jan-2023",
    "our_domain": "",
    "our_company": "",
    "owner_name": "负责人",
    "owner_email": "",
    "customer_folder_prefix": "其他文件夹/",
    "key_customer_domains": [],
    "env_file": "",
    "immediate_imap_host": "imap.exmail.qq.com",
}


class _Cfg(dict):
    pass


def load_config(explicit=None):
    path = _find_config(explicit)
    data = dict(_DEFAULT)
    if path:
        with open(path, encoding="utf-8") as f:
            user = json.load(f)
        data.update({k: v for k, v in user.items() if v not in (None, "") or k not in data})
    workspace = data.get("workspace") or os.getcwd()
    data["_config_path"] = path
    data["_workspace"] = os.path.abspath(workspace)
    return _Cfg(data)


cfg = None


def get_cfg(explicit=None, reload=False):
    global cfg
    if cfg is None or reload:
        cfg = load_config(explicit)
    return cfg


def paths(explicit=None):
    c = get_cfg(explicit)
    ws = c["_workspace"]
    out_name = c.get("out_dir_name") or "抽取"
    prog_name = c.get("program_dir_name") or "邮件归档程序"
    return {
        "workspace": ws,
        "db": os.path.join(ws, c.get("db_name") or "mail_index.sqlite"),
        "out_dir": os.path.join(ws, out_name),
        "program_dir": os.path.join(ws, prog_name),
        "env_file": c.get("env_file") or os.path.join(ws, prog_name, ".env"),
    }


def env(explicit=None):
    """只读取邮箱凭据字段，绝不打印/上传。"""
    c = get_cfg(explicit)
    p = paths(explicit)
    result = {}
    ef = p["env_file"]
    if ef and os.path.exists(ef):
        with open(ef, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                result[k.strip()] = v.strip().strip('"').strip("'")
    return result


def our_domain(explicit=None):
    d = get_cfg(explicit).get("our_domain") or ""
    if d:
        return d
    e = env(explicit).get("IMAP_USER", "")
    return e.split("@")[-1] if "@" in e else ""


def card_fields():
    """商机卡片字段（保留现成口径，我方标识用配置注入）。"""
    return [
        "source_type", "date_first", "date_last", "company", "domain", "country",
        "contacts", "company_type", "industry", "end_customer", "signals", "products",
        "competitors", "quantity_or_value", "product_feedback", "trend", "summary",
        "open_question", "status", "our_people", "owner_involved", "evidence", "notes",
    ]


if __name__ == "__main__":
    c = get_cfg()
    print("配置:", c.get("_config_path") or "(未找到，使用默认值)")
    for k, v in paths().items():
        mask = "***" if k == "env_file" else v
        print("  %-12s %s" % (k, mask))
    print("  我方域名     ", our_domain() or "(未配置)")
