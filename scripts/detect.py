#!/usr/bin/env python3
"""只读探测：验证连接、LIST 文件夹、STATUS 条数。不下载、不改任何状态、不打印授权码。"""
import imaplib
import re
import base64
import os

import pipeline_config as PC
BASE = PC.paths()["program_dir"]


def load_env():
    env = {}
    with open(PC.paths()["env_file"], encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def decode_mutf7(data: bytes) -> str:
    """IMAP modified UTF-7 -> unicode。"""
    result = bytearray()
    i, n = 0, len(data)
    while i < n:
        c = data[i]
        if c == ord('&'):
            j = data.find(b'-', i)
            if j == -1:
                result += data[i:]
                break
            if j == i + 1:
                result += b'&'
            else:
                seg = data[i + 1:j].replace(b',', b'/')
                seg += b'=' * ((4 - len(seg) % 4) % 4)
                try:
                    result += base64.b64decode(seg)
                except Exception:
                    result += data[i:j + 1]
            i = j + 1
        else:
            result.append(c)
            i += 1
    return result.decode('utf-8', errors='replace')


def encode_mutf7(text: str) -> bytes:
    """unicode -> IMAP modified UTF-7。"""
    result = bytearray()
    i, n = 0, len(text)
    while i < n:
        code = ord(text[i])
        if 0x20 <= code <= 0x7E:
            if text[i] == '&':
                result += b'&-'
            else:
                result += text[i].encode('ascii')
            i += 1
        else:
            j = i
            buf = bytearray()
            while j < n and not (0x20 <= ord(text[j]) <= 0x7E):
                buf += text[j].encode('utf-16-be')
                j += 1
            b64 = base64.b64encode(bytes(buf)).rstrip(b'=')
            result += b'&' + b64.replace(b'/', b',') + b'-'
            i = j
    return bytes(result)


def main():
    env = load_env()
    host = env.get("IMAP_HOST", "imap.exmail.qq.com")
    user = env.get("IMAP_USER")
    client = imaplib.IMAP4_SSL(host, 993)
    typ, _ = client.login(user, env["IMAP_PASSWORD"])
    print("LOGIN:", typ)

    typ, data = client.list()
    print("LIST status:", typ, "count:", len(data))
    folders = []
    for line in data:
        print("RAW:", line)
        m = re.search(rb'"([^"]*)"\s*$', line)
        name_bytes = m.group(1) if m else line
        name = decode_mutf7(name_bytes)
        folders.append(name)

    print("\n=== 解码后的文件夹名 ===")
    for f in folders:
        print("  ", repr(f))

    print("\n=== STATUS (MESSAGES UIDVALIDITY) ===")
    for f in folders:
        try:
            mb = '"' + encode_mutf7(f).decode('ascii') + '"'
            typ, sd = client.status(mb, "(MESSAGES UIDVALIDITY)")
            print("STATUS", repr(f), "->", typ, sd)
        except Exception as e:
            print("STATUS ERR", repr(f), "->", type(e).__name__, e)

    client.logout()


if __name__ == "__main__":
    main()
