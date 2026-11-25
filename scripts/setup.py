"""自动配置邮箱商机库。"""
import argparse
import getpass
import json
import os
import re
import stat
from pathlib import Path

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


HOSTS = {
    "qq.com": "imap.qq.com", "foxmail.com": "imap.qq.com",
    "163.com": "imap.163.com", "126.com": "imap.126.com",
    "gmail.com": "imap.gmail.com",
}


def configure(config_path, explicit_host=None):

    cfg = json.loads(config_path.read_text(encoding="utf-8-sig")) if config_path.exists() else {}
    root = Path(cfg.get("workspace") or config_path.parent).resolve()
    config_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    (root / (cfg.get("out_dir_name") or "抽取")).mkdir(parents=True, exist_ok=True)
    program = root / cfg.get("program_dir_name", "邮件归档程序")
    secret_path = Path(cfg.get("env_file") or program / ".env")
    saved = {}
    if secret_path.exists():
        for line in secret_path.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.partition("=")
            if sep and key in ("IMAP_USER", "IMAP_HOST"):
                saved[key] = value
    previous = saved.get("IMAP_USER", "")
    user = previous
    while not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", user):
        user = input("邮箱地址：").strip()
    domain = user.rsplit("@", 1)[1].lower()
    host = explicit_host or saved.get("IMAP_HOST") or HOSTS.get(domain) or cfg.get("immediate_imap_host")
    while not host:
        candidate = input("请填写邮箱服务商提供的 IMAP 服务器地址: ").strip()
        if re.fullmatch(r"(?:[A-Za-z0-9-]+\.)+[A-Za-z0-9-]+", candidate):
            host = candidate
    print(f"连接设置 {host}:{cfg.get('imap_port', 993)}（SSL），数据目录 {root}")
    password = ""
    while not password:
        password = getpass.getpass("邮箱授权码（输入隐藏）: ")
    cfg.update(workspace=str(root), env_file=str(secret_path),
               immediate_imap_host=host)
    cfg.setdefault("imap_port", "993")
    cfg.setdefault("owner_email", user)
    cfg.setdefault("our_domain", domain)
    write_json(config_path, cfg)
    write_secret(str(secret_path), dict(IMAP_HOST=host, IMAP_PORT=str(cfg["imap_port"]),
                                      IMAP_USER=user, IMAP_PASSWORD=password))
    print("本地配置已保存。")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--host", help="更正或指定 IMAP 主机")
    args = parser.parse_args()
    current = Path.cwd() / "pipeline_config.json"
    if args.workspace:
        config = args.workspace / current.name
    elif os.environ.get("LEADS_CONFIG"):
        config = Path(os.environ["LEADS_CONFIG"])
    elif current.exists():
        config = current
    else:
        config = Path.home() / "email-leads-data" / current.name
    configure(config.resolve(), args.host)

