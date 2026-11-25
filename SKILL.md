---
name: email-leads-pipeline
description: 把一个邮箱的邮件批量整理成商机基础数据库（SQLite）+ 中文商机卡片报告。端到端六阶段：IMAP 只读抓邮件头建索引 → 线程化过滤 → 抓正文 → 导出批 → 并行派 AI 子代理抽成商机卡片 → 导入 + 出报告。当用户想把某个邮箱的历史邮件做成商机库/客户线索库/邮件基础数据整理，或提到"把我的邮箱邮件整理成数据库""从邮件抽商机""批量分析邮件找客户线索"时使用。全程 IMAP 只读、不发邮件、不发布在线链接；授权码仅本地 .env 保存。
agent_created: true
---

# 邮箱商机库流水线（Email → Leads Database）

## 用途

把配置里指定的一个邮箱，端到端整理成可查询的商机基础数据：
SQLite 数据库（邮件头 / 线程 / 正文 / 商机卡片）+ 中文报告。
核心是把大量邮件线程用并行的 AI 子代理抽成结构化的中文商机卡片。

**安全基线（不可突破）**
- IMAP 全程只读：`readonly=True` + `BODY.PEEK`，绝不 STORE/COPY/MOVE/EXPUNGE/APPEND，不发邮件。
- 授权码只在本地 `.env`（权限 600），不打印、不写进产出、不上传、不发布在线链接。
- 卡片不写银行账号/IBAN/SWIFT/密码/license key/授权码；含凭据被审批拦截只记 skip。

## 何时使用

- "把我的邮箱邮件整理成商机库 / 客户线索库 / 基础数据库"
- "从邮件里抽商机 / 找客户线索 / 分析历史邮件"
- 已有半成品数据库要"继续跑 / 接着抽卡"

## 核心原则：先判阶段，再动手

每轮对话都**先查当前处于哪个阶段**，只执行第一个未完成阶段，完成即进入下一阶段。
不要从头重跑。详细判据、SQL、并发检测见 `references/全流程手册.md`（**开工前必读**）。

## 环境约定

- `PY` = 本机 python3。WorkBuddy 环境优先用管理的 python3（形如 `env -u PYTHONPATH <绝对路径>`）。
- 所有脚本在 `<skill 目录>/scripts/` 下；`cd` 进去运行，或把该目录加入 PYTHONPATH。
- 路径、公司名、域名、日期范围、批次前缀都来自工作目录的 `pipeline_config.json`，无硬编码绝对路径。

## 工作流（六阶段）

### 阶段 0 · 配置向导（只在首次）
1. 若无 `pipeline_config.json`：运行 `python3 scripts/setup.py`，交互式收集
   工作目录、IMAP 服务器/账号/授权码、你方公司/域名/负责人、起始日期、客户文件夹前缀。
   生成 `pipeline_config.json` + `.env`（均 600）。授权码用 `getpass` 输入，不回显。
2. 运行 `python3 scripts/check_setup.py` 验证：配置齐全、只读登录成功、INBOX 可只读打开。
   全绿才继续。任何失败先修配置，勿往下走。

### 阶段 A · 索引（step1_index.py）
- 完成判据：日志末尾出现「第①步完成」。日志 mtime < 15 分钟表示后台在跑，**本轮不动，直接结束**。
- 执行：`$PY scripts/step1_index.py`（按 folder+uid 跳过已抓，幂等）。
- 首次可小试：`--folders INBOX --limit 50`。

### 阶段 B · 过滤线程化（step2_filter.py）
- 执行：`$PY scripts/step2_filter.py --db <工作目录>/mail_index.sqlite`
- 完成判据：连跑两次 `threads` 表行数不再增长。

### 阶段 C · 抓正文（step3 系列）
- 完成判据见手册 SQL（排除已判空正文）。为 0 或 < 200 即视为完成。
- 常规：`$PY scripts/step3e_resume_bodies.py --db <工作目录>/mail_index.sqlite`（健壮版，每文件夹新连接、重试 5 次）。
- 后台在跑时勿并发启动（看日志 mtime）。

### 阶段 D · 导出批（export_fast.py）
- 执行：`$PY scripts/export_fast.py --db <库> --out <工作目录>/抽取 --tag <tag> --ready-only --chars 250000`

### 阶段 E · 抽卡循环（核心）
1. `run_batches.py import → gaps → status → next -n 4`
2. 每个批号派**一个**子代理（模板 `references/子代理任务模板.md`，规格 `references/抽取规格.md`）。
   一条消息最多并行 4 个。子代理产出 `<批名>.jsonl` 并跑
   `python3 scripts/verify_jsonl.py <批.txt> <批.jsonl>` 自检通过。
3. 全返回后 `run_batches.py import → verify → status`；本轮最多 2 轮。
4. 429/网络错：jsonl 已生成就直接 import，不重抽；额度恢复再派新批。
5. 大量「审批拦截」skip 的批：把该批 txt 拆成小块重抽（小块不易触发审批超时）。
6. `pending/partial/ready` 全为 0 → 回阶段 D 再导出一轮；确无新批 → 阶段 F。

### 阶段 F · 收尾
1. 备份数据库：`cp <库> <库>.PRE$(date +%Y%m%d_%H%M%S)`
2. `$PY scripts/finalize.py`（重生成报告、WAL 落盘、chmod 600）
3. 校验产物权限与卡片行数，向用户汇报完成度。

## 编排器 run_batches.py

| 子命令 | 作用 |
|---|---|
| `status` | 总览 pending/partial/ready/done + 线程覆盖 + 刷新进度看板 |
| `next -n K` | 打印接下来 K 个待做批号 |
| `gaps` | 给缺漏批生成 `*_gap.txt`（只含缺的线程） |
| `import` | 幂等导入所有已产出 jsonl，自动修复结构性坏行 |
| `verify` | 校验所有 jsonl 字段合法性 |

## 资源

- `scripts/setup.py` — 首次配置向导（生成配置 + .env，600）
- `scripts/check_setup.py` — 只读连通性自检（不打印授权码）
- `scripts/pipeline_config.py` — 公共配置层（路径/域名/词表，所有脚本共用）
- `scripts/step1_index.py` — ① 建索引（IMAP 只读抓头 + 去重）
- `scripts/step2_filter.py` — ② 线程化过滤
- `scripts/step3_bodies.py` / `step3c` / `step3d` / `step3e_resume_bodies.py` — ③ 正文抓取各版本（step3e 最健壮）
- `scripts/export_fast.py` — ④ 导出批（全内存秒级）
- `scripts/run_batches.py` — ⑤ 编排状态机（status/next/gaps/import/verify）
- `scripts/import_cards.py` — ⑥ JSONL 导入 cards/card_skips
- `scripts/finalize.py` — 收尾报告 + 权限
- `scripts/verify_jsonl.py` — 单批自检（子代理必跑）
- `references/全流程手册.md` — 各阶段判据、SQL、并发检测、排错（**开工前读**）
- `references/抽取规格.md` — 商机卡片唯一口径（子代理读）
- `references/子代理任务模板.md` — 派子代理的提示词模板
- `references/字段与词表.md` — 字段/受控词表/敏感黑名单速查
- `assets/pipeline_config.example.json` — 配置模板示例

## 自定义词表

保留现成商机口径（12 类信号 / 5 类 status / 10 类 company_type）。
`our_company` / `our_domain` / `owner_name` / `owner_email` 从配置注入，替换卡片中的"我方/负责人"语义。
如需增删信号词或字段，改 `references/抽取规格.md`、`references/字段与词表.md`、
`scripts/verify_jsonl.py` 的 `SIGNALS_OK`，以及 `scripts/pipeline_config.py` 的 `card_fields()`，保持四处一致。
