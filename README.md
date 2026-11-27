# ThreadPass

跨宿主 AI 会话连续性内核与参考应用薄壳（`Pure Core ← Local-Offline Profile ← Application Shell`），支持纯 Node.js（`>= 22.18.0`）零第三方运行时依赖跨平台运行（Windows / macOS / Linux）。

- **核心概念区分（避免混淆）：**
  - **逻辑主会话（`thp` Main）**：ThreadPass 内核中由 `main_designated` 锚定的唯一权威逻辑主线。**任何时候，任何外部 Agent 框架里的原生会话文件都不是主会话。**
  - **宿主原生会话**：IDE/CLI/Web 里的具体窗口或本地/云端对话记录，只是临时承载逻辑流的工作载体。
  - **当前主 Agent**：此时此刻正在替逻辑主会话值班干活的那一个 AI 实例（哪怕全场只有 1 个 AI，它也只是“当前主 Agent”，换窗口/换模型不换主会话）。
- **宿主会话日志定位、Web 可选上传与可达性标记：** 支持在 `adopt` / `ingest` / `reconcile commit` / `fork` 时记录带机器归属（`hostname + device_id + file_path`）的原生会话文件路径（`--transcript-file`）、从 Web 上传并自动脱敏归档会话副本（`--upload-transcript`）或标注纯云端来源（`--cloud-host` / `--host-session-id`）。打包信封时会自动标注本机可 `grep`、异机、已上传或仅云端占位等可达性状态，溢出时写入 `transcript-index.json`。
- **双向分片、身份保持与低误报 Prompt 封装：** 支持超大交棒信封按 `<handoff_chunk>` 自动分片输出，并支持模型回传**对象级多片 JSON**（`part_group_id` / `part_index` / `total_parts` / `is_final`，CLI 可重复传多个 `--file`、`thp web` 可追加分片文件，缺片或冲突自动 `fail-closed` 拒绝）；顶部 `Session Handoff Overview` 保留模型原生身份并清晰隔离前任历史归因，同时采用对高敏感模型友好的 XML `<archived_reference_...>` 参考围栏，避免触发防注入误拒。

## 三层目录结构

- `src/core/` — **无偏向纯核心（`Pure Core`）**：三层 IR 类型、规范化 JSON 与 SHA-256、抽象端口、CALM 单调/冲突 Journal 投影、`Escrow` 配额与不可逆撤销、因果生命周期投影、三态异步信箱、祖先预分析（严禁导入 `node:fs` / `node:sqlite` / `node:http` 或上层模块）。
- `src/profile/local-offline/` — **默认离线配置档（`Local-Offline Profile`）**：跨平台独占 ACL（POSIX `0700`/`0600` 与 Windows `icacls`）、硬链接启动探针、`PublishImmutable`（同盘临时文件 + `fsync` + `no-replace` `fs.linkSync`）、`node:sqlite`（`WAL` + `BEGIN IMMEDIATE`）、Ed25519 `trusted_device_keys`、跨块滑动窗口脱敏与出站门禁、5 步事务引擎（证书即提交）、4 步 `fsck` 校验与删库重建（含 `divergent_heads` 检测）。
- `src/shell/` — **ThreadPass 应用薄壳（`thp` CLI + `thp web`）**：三层 IR 导入解析、来源信任分级与哈希绑定确认凭证、三态 `Workspace State Manifest`（Git 严格可选）、双层预算与分片投喂、`Current Role Briefing` 身份重框定、`adopt` / `reconcile` / `handoff` / `ingest` / `fork` / `mailbox` / `fsck` 全链路，以及仅监听 `127.0.0.1` 的临时本机交互网页 `thp web`。
- `test/` — 核心状态机、故障注入与 `fsck` 恢复、7-AI 合并与三阶段 `thp web` 端到端测试。

## 环境要求与构建测试

- **Node.js**：`>= 22.18.0`（覆盖 LTS `22.x`、`24.x`、`26.x`）
- **运行时依赖**：零第三方运行时依赖（`"dependencies": {}`）

```bash
npm install
npm run typecheck
npm run build
npm test
node dist/src/shell/cli.js --help
```

## 快速用法（7-AI 合并 → 备份回 Main → 拆 Subagent）

启动按需本机临时交互台（仅监听 `127.0.0.1`、带一次性高熵 Token、无后台守护进程）：

```bash
node dist/src/shell/cli.js web --project ThreadPass --thread main_dev
```

或使用纯 CLI：

```bash
# 1. 初始化项目与 Main
node dist/src/shell/cli.js init --project ThreadPass --thread main_dev --workspace .

# 2. 复制提取模板并收编 7 个外部 AI（亦支持直接粘贴原始对话兜底）
node dist/src/shell/cli.js prompt adopt-extract --source ai_1
node dist/src/shell/cli.js adopt --project ThreadPass --thread main_dev --source ai_1 --file ai_1.txt

# 3. 多源 Reconcile 打包与回执落盘
node dist/src/shell/cli.js reconcile pack --project ThreadPass --thread main_dev --attachment
node dist/src/shell/cli.js reconcile commit --project ThreadPass --thread main_dev --sources s1,s2 --file receipt.json --authorize --confirm-interactive

# 4. 阶段备份回 Main（支持 prompt checkpoint 生成）
node dist/src/shell/cli.js prompt checkpoint --project ThreadPass --thread main_dev
node dist/src/shell/cli.js ingest --project ThreadPass --thread main_dev --stream <main_stream_id> --file ckpt.json --authorize --confirm-interactive --workspace .

# 5. 原子派发独立 Subagent
node dist/src/shell/cli.js fork --project ThreadPass --thread main_dev --worker sub_a --task "Subtask A" --workspace ./wt-a
node dist/src/shell/cli.js fork --project ThreadPass --thread main_dev --worker sub_b --task "Subtask B" --workspace ./wt-b

# 6. 四步校验与 SQLite 重建
node dist/src/shell/cli.js fsck --project ThreadPass --thread main_dev --rebuild
```
