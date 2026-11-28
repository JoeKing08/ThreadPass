# ThreadPass

跨宿主 AI 会话连续性内核与参考应用薄壳（`Pure Core ← Local-Offline Profile ← Application Shell`），支持纯 Node.js（`>= 22.18.0`）零第三方运行时依赖跨平台运行（Windows / macOS / Linux）。

- **核心概念区分（避免混淆）：**
  - **逻辑主会话（`thp` Main）**：ThreadPass 内核中由 `main_designated` 锚定的唯一权威逻辑主线。**任何时候，任何外部 Agent 框架里的原生会话文件都不是主会话。**
  - **宿主原生会话**：IDE/CLI/Web 里的具体窗口或本地/云端对话记录，只是临时承载逻辑流的工作载体。
  - **当前主 Agent**：此时此刻正在替逻辑主会话值班干活的那一个 AI 实例（哪怕全场只有 1 个 AI，它也只是“当前主 Agent”，换窗口/换模型不换主会话）。
- **宿主会话日志定位、四档来源分级与 OS 机器指纹：** 支持在 `adopt` / `ingest` / `reconcile commit` / `fork` 时按四档路径来源（`operator_verified` / `system_prompt_provided` / `model_probed` / `unknown_null`）记录带可靠 OS 安装级机器指纹（`mach_<12hex>` + `hostname` + `device_id`，跨 `THREADPASS_HOME` 稳定且强制写入 `recorded_by_machine_fingerprint`）的原生会话文件路径（`--transcript-file`）、从 Web 上传并自动脱敏归档会话副本（`--upload-transcript`）或标注纯云端来源（`--cloud-host` / `--host-session-id`）。打包信封时会自动标注本机可 `grep`、异机、未核验来源、已上传或仅云端/未知占位等可达性状态，溢出时写入 `transcript-index.json`。
- **双端交互闭环与全量功能齐平（`thp` CLI / `thp web`）：** 支持统一粘贴往返闭环，并提供按需唤起、用完即退的本机临时图形壳 `thp web`（仅监听 `127.0.0.1`、高熵会话 Token），完整覆盖 `init` / `status` / `adopt` / `reconcile` / `handoff` / `ingest` / `fork` / `mailbox` / `grant` / `peek` / `gc` / `device` / `fsck` 全部工作流与运维能力。
- **双向分片、身份保持与严格确认闭环：** 支持超大信封按完整 XML `<archived_reference_...>` 围栏与 `<handoff_chunk>` 分片输出，并支持模型回传**对象级多片 JSON**（强制每片携带 `part_group_id`、`part_index`、`is_final`，内置 64 片 / 4 MiB 上限）；`reconcile commit` 强制绑定 `packReconcile` 发布的不可变来源快照（`--pack`），已发布流上的结构化 `ingest` 强制核对 `base_anchor_id` 防过期基线覆盖；`init` / `adopt` / `ingest` / `reconcile` / `fork` / `mailbox` 默认不授予真人授权等级，`human_authority` 与冲突确认（`human_confirmed`）必须经过本机 Ed25519 一次性挑战防重放签发，绝不采信模型自声明、调用参数或未验证定位符路径。
- **设备级一次性确认凭证防重放：** 已消费确认凭证记录于本机 Thread 库 `consumed_confirmation_nonces` 与设备级 `confirmation-nonces.sqlite`（`BEGIN IMMEDIATE` 原子占位，防并发与跨 Thread / Project 重放）。恢复提示：`fsck --rebuild` 按单个 `thread_id` 执行，若设备级 `confirmation-nonces.sqlite` 丢失，须对相关各个 `thread_id` 分别执行 `fsck --rebuild` 以回填全部历史凭证。

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
# 1. 初始化项目与 Main（可选 --authorize 需在交互式终端手动确认目标）
node dist/src/shell/cli.js init --project ThreadPass --thread main_dev --workspace .

# 2. 复制提取模板并收编外部 AI（亦支持直接粘贴原始对话兜底）
node dist/src/shell/cli.js prompt adopt-extract --source ai_1
node dist/src/shell/cli.js adopt --project ThreadPass --thread main_dev --source ai_1 --file ai_1.txt --transcript-file /path/to/log.jsonl

# 3. 多源 Reconcile 打包与回执落盘（--pack 绑定 Pack 信封；--confirm-topics 可按主题确认冲突）
node dist/src/shell/cli.js reconcile pack --project ThreadPass --thread main_dev --attachment
node dist/src/shell/cli.js reconcile commit --project ThreadPass --thread main_dev --pack <env_id> --file receipt.json --authorize

# 4. 阶段备份回 Main（结构化 checkpoint 强制携带 --base-anchor 或 payload.base_anchor_id）
node dist/src/shell/cli.js prompt checkpoint --project ThreadPass --thread main_dev
node dist/src/shell/cli.js ingest --project ThreadPass --thread main_dev --stream <main_stream_id> --base-anchor <anchor_id> --file ckpt.json --authorize --workspace .

# 5. 原子派发独立 Subagent 与 Escrow 授权管理
node dist/src/shell/cli.js fork --project ThreadPass --thread main_dev --worker sub_a --task "Subtask A" --workspace ./wt-a
node dist/src/shell/cli.js grant issue --project ThreadPass --thread main_dev --grantee sub_a --max-spawns 2

# 6. 只读查阅、设备密钥管理、清理与四步校验重建
node dist/src/shell/cli.js peek --project ThreadPass --thread main_dev --stream <main_stream_id>
node dist/src/shell/cli.js device show
node dist/src/shell/cli.js gc --project ThreadPass --thread main_dev --grace-ms 0
node dist/src/shell/cli.js fsck --project ThreadPass --thread main_dev --rebuild
```
