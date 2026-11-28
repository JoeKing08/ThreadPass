# ThreadPass 架构基线

状态：面向长期维护的规范性架构基线。
范围：本文档定义「业务无关的通用分布式状态与协调内核」及其首个参考应用薄壳「ThreadPass 跨宿主会话协同层（Reference Shell）」的架构权威、通用推演闭环、跨路径接缝原语与状态契约、双域事务与通用谱系 DAG 协议、信任来源模型、统一粘贴 Prompt 交互契约、确定性故障仿真门禁及系统不变量；并在内核内部显式划分**无偏向纯核心（Pure Core）**与**有偏向可插拔物理配置档（Pluggable Profile，首个默认落地为 `Local-Offline Profile`）**。各子系统的精确 Schema、状态机与契约测试由对应子系统规范定义。

文档仅陈述必需的架构、权威、接口、状态转换、资源边界、故障行为与验收条件；现有代码盘点、宿主格式逆向笔记、分阶段实施里程碑与测试结果归入独立的路线图与验证记录。

ThreadPass 架构宪章分为三个规范性设计层级：

1. 本基线定义**三层架构定位（`Pure Core ← Pluggable Profile ← Application Shell`）**、通用分布式架构元模型、统一目标架构、语义权威以及各类决策的归属组件。
2. `threadpass-refactor-roadmap.md` 定义内核优先与确定性故障仿真门禁、应用薄壳 MVP 及可选外挂的分阶段实现依赖顺序、验收门禁及架构变更管理要求。
3. 子系统规范定义精确的数据结构、序列化版本迁移、统一粘贴交接模板、预算与归并算法、事务崩溃恢复协议与可执行契约测试。

对对应核心路径进行实质性重写前，以下子系统规范须按三层归属就绪：

**纯抽象分布式内核规范（业务与具体物理部署均无关，不感知 Prompt / LLM / Agent 语义）：**
- `docs/specs/universal-distributed-kernel.md`

**内核抽象与默认 `Local-Offline Profile` / 会话域实例化协同规范（各自明确区分通用内核原语、默认配置档实现及领域映射接口）：**
- `docs/specs/lineage-dag-transactions-and-mailbox.md`
- `docs/specs/redaction-acl-and-blob-store.md`
- `docs/specs/session-lifecycle-and-disaster-recovery.md`

**ThreadPass 应用薄壳规范（AI 跨宿主协同参考应用）：**
- `docs/specs/canonical-session-ir.md`
- `docs/specs/completeness-floor-and-host-budget.md`
- `docs/specs/prompt-handoff-trust-and-harvest.md`
- `docs/specs/host-descriptors-and-adapters.md`

现有代码仅反映已观测行为，不构成目标架构的规范定义。规范中缺失的细节须先在归属规范中明确决策再变更边界，不得照搬历史点对点转换器的偶发约束或引入第二套会话语义路径。

## 0. 架构权威与维护契约

### 0.1 目标权威与设计变更

架构由跨平台会话状态管理与多分支因果协同需求、上下文有效性与安全信任不变量、多进程并发一致性要求、经验证的模型窗口与宿主边界约束以及可复现证据共同决定。历史目录布局、既有单向脚本边界或单一宿主客户端的私有存储假设不构成架构依据；外部多智能体框架仅提供参考，不凌驾于本项目的自身边界之上。

当代码与规范契约不一致时，按以下规则处理：

| 情形 | 要求响应 |
| --- | --- |
| 必需行为缺失或错误 | 将实现修正至符合规范。 |
| 历史两两直转或兼容路径引入妥协 | 移除绕过规范 IR 的直转或冗余兼容路径；仅在存在明确数据恢复义务时保留。 |
| 某宿主或场景无明确规范 | 在编写适配器或落地语义前，先于归属规范中补充决策。 |
| 某项要求与经验证的宿主约束冲突 | 基于实测证据重新审视架构决策，并同步更新受影响的契约。 |
| 代码采用了不同设计 | 按需求与不变量评估该设计，而非直接默认其合法性。 |

核心设计变更须说明触发需求或约束、支撑证据、备选方案与权衡、受影响的权威与消费方，以及正确性与安全性验证方式。实现便利性不足以作为放宽脱敏与信任来源验证、破坏双事务域原子性、伪造不可观测状态或静默丢弃会话数据的理由。

### 0.1.1 通用分布式架构模型、六步推演闭环与跨路径接缝契约

为避免将“通用底层规律”混同为“脱离业务与物理约束的单一万能实现架构”，本基线明确以下**三层严格划分与命名层次**：

1. **`Universal Distributed Systems Model`（通用分布式系统理论与架构推演模型）：** 跨领域通用，刻画任何分布式系统必然面对的八类基本约束与规律（计算实体、状态持有与复制、异步消息通信、时间与因果偏序、故障假设、一致性与协调边界、持久化与崩溃恢复、权限与信任来源），以及下文不预设 AP/CP/Raft/CRDT/单写者的六步可回退推演闭环。
2. **`Pure Core`（事件型通用分布式状态与协调内核 / Event-Oriented Distributed State & Coordination Kernel）：** 提供可组合状态原语（不可变事件、三关系图、CALM/`I-Confluence` 守卫、`Capability + Escrow` 授权、三态信箱、跨路径接缝契约），适用于事件型、历史可追溯且允许异步分叉的状态协同系统（如会话协同、分布式工作流与事件协调、离线优先协作存储）；微秒级共享内存目录等紧耦合系统不共用此代码运行时。
3. **具体实现架构与参考应用：** 由特定业务语义、延迟预算、连通前提与故障模型推导出的配置档（Profile）与具体应用实例（如 `ThreadPass` 参考应用薄壳，或采用独立紧耦合数据面的低延迟内存目录系统）。

**六步可回退迭代推演闭环：** 基于通用架构模型推演或演化任一分布式系统时，必须按以下六步闭环验证；若后一步选型违背第 1 步不变量，必须显式回退重选，不得以削弱安全性换取实现便利：

1. **定义语义与正确性：** 明确状态不变量、操作语义、Safety（绝不违背的边界）、Liveness（最终推进条件）以及消费方允许观察到的中间/终结状态。
2. **建立物理与故障模型：** 明确节点规模、网络拓扑、延迟与带宽预算、时钟可信度、网络分区、崩溃失联及恶意/不可信主体边界。
3. **分析状态与操作性质：** 识别单调性、可交换性、幂等性、集合可合并性、不变量合流性（`I-Confluence`）、热点竞争度与因果依赖；是否可免协调取决于具体业务不变量与争用分布，须按实际操作性质分析，不得凭经验比例预设。
4. **分配所有权与协调责任：** 决定哪些状态可无协调单调累积，哪些由单写者所有权局部加速，哪些通过 `Escrow` 预切额度在分区内安全运行，哪些互斥核必须经过同步共识或事后显式裁决。**事后显式裁决（Post-hoc Reconciliation）适用铁律：** 仅当离线操作保持 `provisional` 待定状态、不产生受保护的不可逆外部副作用，或其副作用具备经验证的幂等性或确定性补偿机制时，方允许离线推进并留待事后裁决；对无法补偿的唯一性、全局资源守恒与不可逆外部副作用，**必须在副作用执行前**通过同步协调、`Escrow` 预切额度或目标资源自身的原子条件检查守住不变量。
5. **选择物理底盘与多路径执行架构：** 同一架构可依据差异可达十余个数量级的时间预算，显式分层承载**快速执行路径**（微秒~毫秒级本地或低延迟执行）、**持久化与复制路径**（异步日志、提交意图与独立副本校验）与**恢复与协调路径**（离线重构、并发分叉检测、授权核验与语义裁决）。
6. **推演故障、恢复与重配置：** 检验半写崩溃、网络分区、并发头、数据丢失、权限撤销与成员变更时是否仍死守第 1 步全部 Safety 不变量，否则回退调整前序步骤。

**跨路径接缝原语与契约（Cross-Path Seams & Contracts）：** 多时间预算执行路径并行存在时，致命正确性缺陷通常源于路径接缝。`docs/specs/universal-distributed-kernel.md` 必须为下列每项定义精确的前置条件、保证、失效行为与契约测试，严禁混淆不同隔离强度的机制：

1. **可见性与释放栅栏（Visibility & Release Barrier）：** 明确何时候选写操作才对活跃读路径或外部执行端可见，防止未完成持久化或未经独立证书校验的中间状态提前外泄（对应 `committed_pending_publish →` 独立可验证的 `Transaction Manifest` + `Transaction Finalization Certificate`〔含 `commit_intent_proof` 与 `publication_closure_proof`〕及外部投喂门禁 `Release Gate`）。
2. **单调代次栅栏（Fencing Token）：** 单调递增代次凭证（如 `fencing_epoch`、局部 `coordinator_epoch`）。**铁律：Fencing Token 仅在被保护的目标资源实际校验并拒绝小于当前代次的写入时才提供硬隔离**，绝不单独跨无共识离线多副本提供全局防分叉保证；若底层介质本身不校验代次（如普通操作系统文件系统创建/重命名操作不识别 `fencing_epoch`），Profile 必须以**内容寻址/事务作用域的不可变首次创建独占写 + 可变权威库内 Fencing CAS 校验**闭合隔离，而不得假定数据库代次自动保护了文件创建。
3. **有界租约（Lease）：** 带有效期的临时所有权（如流写入预留、信箱重投租约），**显式依赖单调时钟有界假设、续租机制与过期安全回收规则**；契约须显式携带 **`clock_domain_id`**（所属时钟域）与 **`boot_epoch`**（用以识别系统重启导致的单调时钟基准重置）。系统挂起/休眠不改变 `boot_epoch` 且各操作系统单调时钟在挂起期间行为不一（部分暂停、部分继续推进），因此挂起恢复后时钟连续性无法判定时须将旧租约保守降级为 `unknown`，要求原所有者回权威库复验 `fencing_epoch`（严禁跨重启直接比较新旧单调时钟与遗留 `lease_until`，亦不依赖无守护进程跨平台不可得的虚构挂起计数器）。**核心铁律：`Lease` 只能在具有共同可信时钟域与明确仲裁权威的范围内提供过期判定，绝不能升级为跨离线设备的全局租约**；跨设备离线授权继续使用 `Escrow` 切分、不可变事实与冲突对账。
4. **建议性写者提示（Advisory Hint）：** 如 `active_writer_hint` / `baton_handoff`，明确属于 UX 防呆线索而非硬性隔离原语，自身不构成分布式锁或 Fencing Token。
5. **跨路径失步诚实状态契约（Honest State & Degradation Contract）：** 协议层状态类型必须显式区分未决、降级、并发与未校验态，不得将中间态呈现为已终结一致（如 `local_committed` vs `disaster_redundant`、`incomplete_unverified_txn` / `untrusted_unverified_txn` / `broken_dependency_closure` / `partial_blobs_local_only` vs 完整已认证 `finalized`、`Lifecycle Observations` vs 可含 `concurrent`/`unknown` 的 `Lifecycle Projection`、`human_authority` vs `unverified_import`）。

**CAP / CALM / I-Confluence / PACELC 术语铁律：**

- **CAP** 仅刻画网络发生分区（`P`）那一刻在**线性一致性（Linearizability）**与**可用性（Availability）**之间的刚性边界；无分区正常连通时性能延迟（`L`）与一致性（`C`）的权衡须使用 **PACELC** 精确表述，不得混入 CAP。
- **CALM 定理边界：** 纯单调逻辑可在满足消息传播等模型前提时实现协调自由的**汇合计算（Confluent Computation）**；若具体数据类型采用 CRDT 实现，须另行验证其**强最终一致（Strong Eventual Consistency, SEC）**契约与副本传播前提。上述保证均**绝不等同于 CAP 线性一致性（Linearizability）**（分区期间节点 A 仍无法当场读到孤岛 B 的未同步写入）。
- **非单调不变量不可靠单纯追加满足：** 仅靠只追加不可变日志，无法自动满足全局唯一性（如同 `thread_id` 下双端同时指定不同唯一协调权威）或有界资源总量守恒（如超额扣减配额）。一切非单调全局不变量必须经由 **`I-Confluence` 验证、可切分的 `Escrow` 有界配额，或显式同步协调 / 事后 `fail-closed` 对账**加以约束。

**无偏向核心与有偏向配置档的显式边界（Profile Spectrum）：** 任何落地运行时都会基于物理约束做出取舍；默认离线优先实现不影响内核端口向其他部署形态演进。本架构严格区分**“同构事件型可插拔 Profile”**与**“跨领域方法论同源方向”**：

- **同构事件型可插拔物理配置档（共享 `Pure Core` 的 `StoragePort`、`Sync/TransportPort`、`Coordination/ConsensusPort` 代码级端口）：**
  - **`Local-Offline Profile`（当前默认首个落地配置档）：** 本地事务库 + 不可变事务分片 + 独立 `Transaction Manifest` / `Finalization Certificate` + 内容寻址 Blob + 介质无关离线同步 + `thp fsck` 多头对账（显式偏向零守护进程、极简运维与断网可用）。
  - **`Shared-Log / Cloud-Object Profile`（可选演进方向）：** 无状态计算 + 共享分布式日志或云端对象存储条件写（CAS）作为主持久化与定序后端，适合弹性云原生协同与跨端实时同步。
  - **`Online-Consensus Profile`（可选演进方向）：** 常驻多节点控制面 + 共识组 + 活跃租约自动选主，面向在线低延迟强定序与自动容错切换服务。
- **跨领域方法论同源（不强行共享事件型代码端口，使用独立紧耦合数据面）：**
  - **`Low-Latency Directory` 范式：** 面向微秒级高局部性内存目录/硬件共享状态系统。它与上述 Profile **共享同一套 `Universal Distributed Systems Model`（六步推演闭环、多路径分层及接缝契约方法论）**，但在代码层面采用内存页目录与异步 Epoch 检查点，不强行拟合进日志/DAG 型事件流 `Pure Core` 端口。

### 0.2 逻辑模块边界、三层单向解耦与运行时基线

每个子系统须声明其职责、非职责、公开接口、允许的实现依赖以及私有状态。**内核与上层应用强制实行严格的三层单向解耦（`Pure Core ← Pluggable Profile ← Application Shell`）：**
- **`Pure Core`（事件型通用分布式状态与协调内核）：** 零业务领域耦合，同时零具体磁盘、网络拓扑或常驻守护进程假设；仅面向与其抽象契约相容的事件型、历史可追溯、允许异步分叉系统，定义不可变事件与三关系正交图、CALM/`I-Confluence` 守卫、Capability + `Escrow` 授权链、三态信箱状态机、接缝原语与状态契约（Visibility Barrier / Fencing Token / Lease / Advisory Hint / Honest State Contract）及 `StoragePort` / `Sync/TransportPort` / `Coordination/ConsensusPort` 抽象端口。
- **`Pluggable Profile`（有偏向可插拔物理配置档）：** 依据目标场景实现上述抽象端口并显式声明自身物理偏向、端口契约满足边界与并发/持久化约束，禁止将任一具体 Profile 的部署限制（如单机 SQLite 或离线手工对账）反向写死进 `Pure Core`。
- **上层 ThreadPass 应用薄壳（`Application Shell`）：** 首个挂载在 `Pure Core + Local-Offline Profile` 之上的参考应用薄壳，仅通过公开契约接口交互，负责将 AI 协同领域概念（Canonical IR、统一粘贴 Prompt 往返模板、`Current Role Briefing` 认知简报、信任围栏与宿主预算）映射为内核状态操作；所有源适配器与目标适配器之间通过 Canonical Session IR 及显式宿主描述符交互，禁止新增绕过规范 IR 的私有直转路径或模块间依赖环。

**三层验证与确定性仿真门禁（Core DST / Profile Conformance / Fault Integration）：** 仿真测试与真实物理存储/进程的失效模式不同，内核与配置档须依次通过三层验证后方可放行：
1. **Core DST + 关键协议状态空间探索：** `Pure Core` 配套单机可重放随机种子的确定性故障仿真（DST）及关键状态机协议的模型检查/状态空间探索，在给定端口契约下证明核心状态机在分区、并发头、乱序、撤销与权威重建下的 Safety 不变量。
2. **Profile Conformance（端口一致性契约测试）：** 针对每个真实 Profile（如 `Local-Offline`、`Shared-Log`、`Online-Consensus`）的可执行契约测试集，直接验证其 `StoragePort` / `Sync/TransportPort` / `Coordination/ConsensusPort` 实现确实兑现 `Pure Core` 所依赖的原子性、Fencing Token 检查、Lease 语义与 `Transaction Finalization Certificate` 校验契约。
3. **Profile Integration & Fault Injection（真实端到端故障测试）：** 在真实进程中断、半写落盘、文件缺片拷贝与跨机同步下验证端到端崩溃前滚与隔离恢复行为。

默认 `Local-Offline Profile` 运行时基线统一为**零第三方运行时依赖的纯 Node.js 单包 CLI/库**（CLI 主命令统一为 `thp`，配套按需唤起、用完即退的临时本机 Web 交互入口 `thp web`，基于内置 `node:http` 与内嵌静态单页实现、绝不作为后台常驻守护进程，并与 CLI 保持**100% 全量功能齐平**，完整覆盖 `init` / `status` / `adopt` / `reconcile` / `handoff` / `ingest` / `fork` / `mailbox` / `grant` / `peek` / `gc` / `device` / `fsck`；包名与本机存储命名空间保持 `threadpass` / `~/.threadpass/`）：
- **支持与验证矩阵：** 最低支持版本为 **`Node.js 22.18.0`**；受支持 LTS 主线覆盖 **`22.x`、`24.x`、`26.x`**（优先围绕当前 **`Node.js 26 Active LTS`** 验证）；CI 须同时覆盖最低支持版本（`22.18.0`）与全部受支持 LTS 主线（`22.x`、`24.x`、`26.x`）的最新版本。源码使用 TypeScript 编写，发布前必须通过完整静态类型检查（`tsc --noEmit`）并交付标准编译产物。
- **实验/演进 API 隔离：** 内置事务存储（`node:sqlite`）与内置压缩编解码（`node:zlib` 含 Zstandard 流原语）在部分受支持 Node 版本中仍存在实验或演进差异，二者必须封装在独立的基础设施抽象接口（`StateStore` / `CodecProvider`）之后，并配套跨上述全部受支持 LTS 版本的兼容性回归测试。持久化数据库表结构与历史归档格式严格独立于运行时内部实现细节，保证跨 Node 大版本长期可读。
- **默认配置档架构禁区与可选外部工具：** 在当前默认 `Local-Offline Profile` 中，不得引入外部解释器形成双运行时栈，不得引入常驻后台守护进程、实时网络消息代理、外部数据库服务或自建模型推理运行时（该限制属于默认离线配置档边界，而非对 `Pure Core` 理论或其他可选 `Online-Consensus` / `Shared-Log` Profile 的封闭）。**Git 仅为可选外部增强工具，而非核心运行时前提**：在未安装 Git 或无代码仓库的场景（普通目录或纯对话）下，核心会话管理、事务归档、协调日志、脱敏与分片投喂须可完整运行。

### 0.3 状态权威、双存储分工与宿主副本

*作用域声明：本节定义默认 `Local-Offline Profile` 对 `Pure Core` 抽象端口的具体落地方案；其余演进 Profile 可选用共享分布式日志或在线共识存储实现等价的权威语义契约。*

对每项关键状态，须明确其作用域、语义权威、并发仲裁方式、派生副本与恢复规则：

- **本地事务控制库、不可变会话归档与协调日志共同构成跨平台语义权威**，默认位于 `~/.threadpass/`，作用域精确到 `{project_id, workspace_id, thread_id, stream_id, anchor_id, event_id, invocation_id, dispatch_id, grant_id, journal_event_id, input_dependency_id, provenance_ref, reconciliation_id, work_item_id, recovery_id, coordinator_epoch, txn_id}`；当前协作空间的 `main_stream_id` 是指向其中一个 `stream_id` 的角色指定，不构成独立于会话流的第二套身份，也不存在跨所有协作空间的全局唯一 Main：
  - **`state.sqlite`（开启 `WAL`、`busy_timeout` 与 `BEGIN IMMEDIATE` 排他写事务）** 是当前本地规范存储内所有**可变控制状态与并发仲裁**的权威（已发布流头指针、谱系边索引、CAS 乐观锁版本、事务提交意图与流写入租约 `txn_log`、当前 Main 的本地协调写入世代、派发/授权/任务/恢复关系索引、信箱队列状态、祖先可达缓存及 Blob 引用索引）。其中协调日志投影、账本索引与 `lineage.json` 均须可由不可变记录重建；它们不得取代不可变事件作为历史事实依据。
  - **`history/committed/` 与 `blobs/objects/`** 是已提交会话事件及内容负载的不可变归档权威。`blobs/staging/` 与 `history/staging/` 均属于事务暂存区；未被已提交事务引用的暂存对象不得进入归档检索面。Blob 在提交意图落账、对象发布完成且引用登记可验证后，才成为已提交归档对象；只有关联事务完成 `Finalize`、锚点进入 `published` 后，该对象才可作为活跃会话上下文被消费。
  - **`coordination/journal/committed/` 是当前 `thread_id` 的协调事实日志权威（Coordinator Journal）**，与 Main 的对话/模型交互历史分离，采用不可变追加记录，专门保存派发、授权授予/撤销、成果回执与采纳、分支放弃、恢复、收编及归并裁决等改变协作空间控制状态的事实。它不是第二份 Main，也不保存完整会话对话；`state.sqlite` 中的协调账本表是其可重建的并发控制投影。Journal 可在 Main 会话流丢失时重建协调视图，但不能恢复 Journal 从未记录的 Main 对话或决策。
  - **独立灾备副本**须包含已提交 Coordinator Journal 事件及其依赖的获准 Blob、派发胶囊和校验清单，并放置在与主存储不同的故障域。只有在副本内容及引用完整性得到验证后，相关状态才可标记为 `disaster_redundant`；否则只能诚实标记为 `local_committed` / `local_only`。是否要求关键操作在投喂前取得独立副本确认，由明确的持久性策略决定，系统不得把单机落盘描述成独立灾备。
- **各原厂 Agent 客户端内部的本地或远端会话存储**仅构成特定会话流在当前执行阶段的**宿主工作副本（Host Working Replica）**，绝不因持有最新交互字节而自动获得全局谱系权威。
- **核心概念三分法（逻辑主会话 / 宿主原生会话 / 当前主 Agent）：**
  1. **逻辑主会话（Canonical Main Stream / `thp` Main）：** ThreadPass 内核里由 `main_designated` 锚定、指向某条规范 `stream_id` 的唯一权威逻辑主线。**铁律：任何时候、任何外部 IDE/CLI/Agent 框架或网页客户端里的原生会话记录/日志文件，都不是主会话。**
  2. **宿主原生会话（Host Session / Ephemeral Carrier）：** 外部工具或网页端里的单个对话窗口及其原生历史文件（无论本地 `.jsonl`/`.json` 还是云端会话），仅为某一工作阶段临时承载逻辑流的工作副本，可随时替换、分流或收编。
  3. **当前主 Agent（Active Main Agent）：** 此时此刻正在替逻辑主会话值班执行、统筹决策或对接派发的那**一个具体 AI 模型实例**（即便当前只有一个 Agent 在独立工作，它也只是“当前值班的主 Agent”，而不是逻辑主会话本体）。当更换模型、耗尽上下文重开窗口或从云端切换到本地时，替换的是“主 Agent”躯壳，背后延续的仍是同一条“逻辑主会话（thp Main）”。
- **宿主会话文件定位符、可选上传副本与可达性契约（Host Transcript Locators & Reachability）：**
  - 定位符严格区分四档路径来源（及历史归档兼容别名 `model_reported`）：**`operator_verified`**（本机操作者在 CLI/Web 明确提供的本机路径、机器归属与 `host_session_id`）、**`system_prompt_provided`**（宿主系统提示词直接提供的会话 ID/路径）、**`model_probed`**（模型在本地经只读核验后上报的路径）与 **`unknown_null`**（纯云端/沙箱等确实无路径可读时诚实留 `null`）；**仅 `operator_verified`** 可直接触发本机 `existsSync` 可达性判定与 `host_bindings` 绑定，模型禁止自声明 `operator_verified`（越权声明一律降级为 `model_reported` 并告警），非操作者来源路径统一标为 `SYSTEM_PROMPT_PATH_UNVERIFIED` / `MODEL_PROBED_PATH_UNVERIFIED` / `MODEL_REPORTED_PATH_UNVERIFIED`，严禁冒充本机已验证文件或劫持绑定。（注：自动脱敏针对已知凭证模式，不代表完整 PII/商业隐私脱敏，Web 上传单次 JSON 请求上限 4 MiB。）
  - 当工作流跨越多个同机 AI 工具（共用本机文件系统、需要 `grep` 原始历史明细）、跨设备离线同步、Web 上传导出记录及仅存于云端沙箱的 Agent（如无本机盘会话文件的云端 Agent）时，收编、检查点备份、合并提交与派发均可附带 `HostTranscriptLocator` 控制记录并持久化进不可变分片、`state.sqlite` 及派生只读索引 `transcript-index.json`（与 `lineage.json` 同级，支持 `thp fsck --rebuild` 完整重建）。
  - 定位符须**强制携带可靠机器标记与机器归属**：`LocalOfflineEngine` 基于 OS 安装级唯一标识单向 SHA-256 截断派生稳定硬件/系统指纹 **`machine_fingerprint`**（`mach_<12hex>`，Windows 取 `MachineGuid`、Linux 取 `/etc/machine-id`、macOS 取 `IOPlatformUUID`），确保同一台物理机在不同 `THREADPASS_HOME` 目录下保持一致、且不随客户端应用缓存漂移；每条定位符入库均强制打上 `recorded_by_machine_fingerprint` + `recorded_by_hostname` + `recorded_by_device_id`，本地文件路径同时绑定 `machine_fingerprint` + `machine_hostname` + `machine_device_id` + `file_path`（同物理机跨隔离目录通过 `machine_fingerprint` 仍可识别为 `LOCAL_GREPPABLE`，异机则明确标 `OTHER_MACHINE`）。支持 Web 会话**可选上传原始会话文件文本**（先全量流式脱敏再内容寻址存入 `blobs/objects/`）；对纯云端或缺失本地文件的来源，保留 `cloud_remote`（记录 `cloud_provider` / `host_session_id` / 脱敏 `remote_uri`）或 `placeholder_unavailable`（区分 `UNKNOWN_NULL` 与 `PLACEHOLDER`），严禁静默伪造本地路径。
  - 生成 Reconcile / Handoff / Fork 信封时，系统按当前宿主机器环境动态标注每条定位符的**实际可达性**（`LOCAL_GREPPABLE` / `LOCAL_FILE_MISSING` / `OTHER_MACHINE` / `UPLOADED_TRANSCRIPT_BLOB` / `CLOUD_ONLY / NO_LOCAL_FILE` / `NO_TRANSCRIPT_FILE / PLACEHOLDER`），并明确提醒接手 AI 其对全量原始会话日志的可见性是否完整（例如本地接手 AI 可 `grep` 全部同机本地日志，但无法触及 `CLOUD_ONLY` 云端副会话；反之兼容云端宿主若可访问对应提供商 `host_session_id` 则可自行反查），同时按主机预算对信封内联条目数设硬上限（默认 12 条，溢出指向完整 `transcript-index.json`），防止定位符清单随会话增长撑爆上下文窗口。
- 宿主工作副本产生的新增交互，只有在来源验证、凭证脱敏、事务级分片暂存刷盘、SQLite 提交意图登记、Blob/历史分片发布及最终头指针激活全部完成后，方可成为规范谱系中的可见新状态；协调操作还须在同一提交协议中落入 Coordinator Journal。

### 0.4 跨子系统交互契约

每条跨领域规则仅有一个归属规范及稳定章节引用，其他规范直接引用而不重复定义：

| 交互契约 | 归属定义 | 必需消费方 |
| --- | --- | --- |
| 通用分布式系统模型（`Universal Distributed Systems Model`）、事件型 `Pure Core` 适用范围、六步推演闭环、接缝原语与状态契约（可见性栅栏 / `Fencing Token` / `Lease` / `Advisory Hint` / `Honest State Contract`）、CAP/CALM/SEC/`I-Confluence`/PACELC 术语边界与三层验证契约（Core DST / `Profile Conformance` / Integration Fault） | `docs/specs/universal-distributed-kernel.md` | 全部内核子系统、Profile 实现、ThreadPass 应用薄壳、架构变更评审与测试套件。 |
| 事件、模型调用及输入投影语义、工作区清单、版本演进、规范事件注册表与未知块契约 | `docs/specs/canonical-session-ir.md` | 全部源适配器、打包器、归档与 Blob 存储、原生目标写出器。 |
| Lineage DAG、Input Dependency、Provenance、事务级分片发布、`Transaction Manifest` 与 `Transaction Finalization Certificate`、读写屏障、Blob 孤儿回收、崩溃恢复、归并/重协调协议及信箱 | `docs/specs/lineage-dag-transactions-and-mailbox.md` | 协调层、收编/恢复器、多路重协调器、信箱状态机与故障恢复器。 |
| 信息完整性下限、三尺度计量、预算置信度与宿主自适应上限 | `docs/specs/completeness-floor-and-host-budget.md` | 交棒打包器、归并配额分配器、原生目标预检器。 |
| 跨块滑动窗口流式脱敏、跨平台独占 ACL、Blob 寻址/回收与介质无关离线同步白名单 | `docs/specs/redaction-acl-and-blob-store.md` | 初始化器、收编/回收器、归档与信箱存储、备份同步与 `fsck`。 |
| 信任来源认证链、哈希绑定的人类确认凭证、双态交接单、统一粘贴 Prompt 往返闭环、认知角色简报与身份重框定（`Current Role Briefing`）、三级连续性等级与非指令围栏 | `docs/specs/prompt-handoff-trust-and-harvest.md` | 交互协议模板、导入验证器、协调层、打包器、CLI。 |
| 五维结构化宿主描述符、能力路由（以通用粘贴 Prompt 宿主为一等默认路径，本地/旁路采集与原生写回为可选外挂）与运行态锁门禁 | `docs/specs/host-descriptors-and-adapters.md` | 全部源/目标适配器、CLI 调度器、隔离环境导入导出桥。 |
| Main 角色指定、Coordinator Journal、派发授权委派、派发/恢复关系、野生来源固定收编、多 Main 独立性与灾难恢复 | `docs/specs/session-lifecycle-and-disaster-recovery.md` | 协调层、`fork`/`adopt`/`recover`、授权校验、归并器、`fsck`、全部会话与宿主交互入口。 |

### 0.4.1 四项核心协调与关系约束

以下四项均为规范性核心约束；具体 Schema、签名/授权凭证格式、存储布局和状态机由对应子系统规范定义，不在本基线提前锁定实现细节：

1. **派发权限委派：** Main 的协调权不自动传递给子会话。任何子会话创建正式子流的权限必须来自可验证、范围受限且可撤销的显式授权；再委派必须被明确允许，并且只能缩小不能扩大原授权范围。
2. **Coordinator Journal 独立灾备：** Main 对话历史与协调事实日志分离。派发、授权、回执、采纳/拒绝、放弃、恢复、收编和归并裁决等关键事实必须有不可变日志记录；SQLite 账本为可重建投影。只有经验证的独立故障域副本才构成独立灾备保证。
3. **关系语义分离：** Lineage、Input Dependency、Provenance 分属不同关系域。因果谱系 DAG 只表达规范会话状态的继承；输入依赖记录实际消费的规范化输入；来源追溯记录证据出处。三者可互相引用，但不可互相冒充。
4. **野生收编后的独立性：** 收编必须固定到可验证检查点并物化所需内容；新 Main 取得的是自己协作空间的独立协调权，而非对来源会话的所有权或旧 Main 的继任权。旧 Main 复活不自动改变新 Main。


### 0.5 有界资源、预算优先级与安全门禁

系统须保证超大宿主会话下的流式有界内存开销、异构目标窗口下的自适应有界投喂体积，以及可验证的脱敏与权限闭环：

- **流式读取与单记录体积上限：** 源读取器须逐条流式解析宿主记录，严禁一次性载入完整历史文件。单条事件的内联文本设硬性体积上限；超出上限的超长工具输出或二进制/多模态负载须流式分流至 `blobs/` 内容寻址存储，并在事件内仅留结构化摘要与哈希引用，防止单条巨型记录破坏有界内存保证。
- **信息完整性下限与宿主有效预算上限（Completeness Floor & Effective Host Ceiling）：**
  - 系统不以固定最小 Token 数冒充信息完整性，而是定义**五要素结构完整性下限**：① 交接单（模型撰写或确定性客观现场包）；② 最近 $K$ 轮原始真实交互；③ 最近关键工具调用、执行结果及未决错误输出；④ 显式未完成任务与阻塞项；⑤ 工作区状态清单及未提交变更警示。活跃内容低于默认短会话免裁剪阈值且未超出目标有效预算时，须 **100% 原文保留**。
  - 预算引擎同时追踪**实际 UTF-8 字节数**、**源端 Token 估算**与**目标宿主双层预算上限**：① **总有效可用上下文上限（`effective_context_budget`）**（扣除目标宿主自身系统提示词、工具定义、平台注入预留，并计入异构分词器膨胀安全系数与分片协议轮次开销）；② **单次输入载荷上限（`per_turn_input_limit`）与附件规格限制（`attachment_limits`）**。对于不提供精确剩余容量接口的隔离环境，明确标定 `context_budget_confidence = heuristic`，保守预算上限仅作防溢出估算而非容量承诺。
  - **刚性裁决优先级与分片边界：`目标宿主有效可用总预算上限 > 信息完整性下限`**。当内容未超出总有效上下文上限、但超出单次输入或单附件上限时，允许按分片投喂协议拆分交付；但**分片只解决单次输入上限，绝不得借分片绕过或撑爆总有效上下文上限**。总预算装不下完整保底要素时，按确定性优先级阶梯折叠次要历史输出并保留历史/Blob 指针；若目标窗口窄至无法容纳最核心交接头部与工作区状态警示，须闭环拒收报错（fail-closed），严禁强行溢出或静默丢弃核心状态。
- **双端脱敏、不可扫描负载隔离与显式清理边界（Fail-Closed Redaction）：**
  - **入站流式脱敏：** 流式脱敏管道须采用跨缓冲区重叠滑动窗口以识别跨 Chunk 边界凭证及多行密钥块，所有入库文本与结构化字段须在内存中完成已知机密模式与用户自定义机密表清理并通过二次复核扫描；对于无法执行文本扫描的不透明二进制负载，默认标记为 `unscannable_binary` 仅限本机隔离存储或按策略拒绝入库，严禁未经显式放行随交棒包或外部仓库外发。未脱敏中间明文严禁落盘。
  - **出站复核门禁（Outbound Egress Gate）：** 向外部宿主组装交棒信封/分片或导出离线灾备包前，须按当前最新的脱敏规则表与 `sync_policy` 对拟外发内容再执行一次出站复核，拦截因历史入库后新增自定义机密或标记为 `local_only` 而产生的外泄风险。
  - **事后漏脱敏清理边界：** 不可变归档一旦在事后被发现漏脱敏，禁止静默原地篡改已提交分片或 Blob；须通过显式可审计的墓碑化（Tombstone）或重打包清理流程断开/替换敏感负载，并诚实声明此前已外发至外部宿主或远端副本的明文无法由本地清理自动撤回。

## 1. 项目目标、三层定位与连续性边界

本项目采用明确的**「`Pure Core` 无偏向内核 + 可插拔有偏向物理 `Profile` + 极简应用薄壳」三层定位**：

- **`Pure Core`（事件型通用分布式状态与协调内核）：** 面向事件型、历史可追溯、允许异步分叉系统的纯状态机与协调原语核心。内核不感知 Prompt、Token 或模型角色，只维护原子提交/可见性契约、内容寻址对象、三关系正交图（Lineage / Input Dependency / Provenance）、CALM/`I-Confluence` 分层协调日志、Capability + `Escrow` 有界授权链、三态异步信箱及接缝原语/状态契约，通过抽象端口向外部暴露配置档无关的可组合能力（其适用范围边界见 `0.1.1` 节）。
- **`Default Local-Offline Profile`（首个显式有偏向的物理配置档，同时开放 Profile 谱系扩展）：** 将抽象端口首先落地为无守护进程、离线优先的本地事务控制库与不可变分片实现（默认位于 `~/.threadpass/`，支持 `THREADPASS_HOME` 覆盖），既以最低运维成本在最恶劣的离线/异构环境中验证 `Pure Core` 完备性，又明确保留向 `Shared-Log / Cloud-Object`、`Online-Consensus` 两个**同构代码可插拔 Profile** 及 `Low-Latency Directory` **跨领域方法论同源范式**演进的开放路径。
- **上层 ThreadPass（`thp` 参考应用薄壳）：** 挂载在 `Pure Core + Local-Offline Profile` 之上的首个极简参考应用，以**“统一用户粘贴 Prompt 往返闭环”**作为一等交互基线，支持无共同祖先多源收编、任意层级并发裂变、跨辈分局部重协调及平行流异步协作；本地日志抓取、旁路事件采集器、Skill 与原生存储写回均仅为可插拔外挂增强。

### 1.1 渐进心智原则（Progressive Disclosure）

通用分布式内核及会话 DAG 不应成为日常使用的前置认知门槛。在默认单流与常规协同场景下，用户日常界面收口为**“领取标准 Prompt → 粘贴进任意目标宿主 → 将输出粘贴回存”**的极简单一闭环；多源收编、并行裂变、多路重协调、跨流发信及多设备离线对账能力按需显现，不向普通工作流暴露底层图谱与日志复杂度。

### 1.2 连续性承诺等级

系统迁移的是**可观测的会话上下文、因果历史与任务工作区状态**，而非源模型内部不可导出的隐式推理状态或专有运行态。所有跨宿主接力须显式标定其连续性等级，不得混淆等级或静默降级：

| 连续性等级 | 语义边界 |
| --- | --- |
| `Native Resume` | 经已验证目标适配器将活跃上下文投影至本地宿主原生存储，由宿主按原生机制恢复会话（可选增强）。 |
| `Prompt Reconstruction` | 基于规范历史与 `model_authored` 交接包生成标准注入信封（经统一粘贴闭环或 CLI 加载），在目标宿主发起具备完整上下文连续性的新一轮模型调用。 |
| `Evidence Recovery` | 源宿主因额度耗尽、限流或崩溃未能完成语义收尾，由系统零模型提取客观历史/原生日志与工作区状态清单构成 `evidence_based` 现场恢复包。 |

### 1.3 Main 规范会话、协作空间与最高协调权威

**Main 首先是一份具体的 ThreadPass 规范会话（Canonical Session），而不是脱离会话文件而长期存在的抽象逻辑身份，也不是 Agent、模型进程或原厂宿主。** 在一个协作空间（`thread_id`）内，被明确指定为 Main 的规范会话流（`main_stream_id`，本质上也是普通 `stream_id`）是该空间的最高协调权威。Main 的会话流保存自身的规范交互历史；该协作空间的 Coordinator Journal 则独立保存派发、授权、回执、采纳和恢复等协调事实。Main 的权威来自 Main 指定记录及可验证的协调日志，而不是来自某个模型是否在线。**模型会话窗口内的“认知角色认同”由应用壳在信封顶部显式注入（`Current Role Briefing`），而协作空间内的“权威身份与派发裁决权”永远归属底层规范流与协调日志，两者严格分离。**

- **会话文件优先：** 本架构中 Main、A、B、C 等名称首先指采用 Canonical Session IR 的规范会话流/会话归档。本地 CLI/IDE 客户端、云端隔离沙盒、网页及移动端聊天客户端等均只是这些规范会话当前可连接的宿主或工作副本；Agent 只是读取、生成或执行交互的运行时主体，不是 Main/A/B/C 的身份本身。宿主迁移或原厂会话文件丢失，不会自动改变仍然可用的规范会话身份。
- **Main 是协作空间内的权威，不是全局唯一身份：** 每个 Main 只对其自身协作空间及其明确纳入的分支、派发和协调记录拥有最高协调权。ThreadPass 可以同时存在任意多个 Main；它们可以完全无关，也可以通过显式来源引用、消息、收编或归并建立联系，但不会因此形成天然的上下级关系，也不会自动共享或转移协调权。不同 Main 必须具有不同的 `thread_id` / `main_stream_id`。
- **不强制 Main 宿主常驻：** Main 规范会话可以由不同宿主读取和继续，也可以暂时没有在线模型执行它。用户可通过受信任 CLI 对该 Main 已授权的协调记录执行派发、核对、采纳和归并；这不意味着 CLI 本身成为另一份 Main 会话。
- **结构平权、角色有界：** 所有 `stream_id` 在因果 DAG 中仍然遵循同一套锚点和父边规则。Main 的最高协调权不使其自动成为每个子流的因果父节点，也不意味着 Main 拥有子流未提交或未回传的全部原始历史。`anchor.parents[]` 只表达实际继承的已提交历史。
- **Main 丢失不自动产生继任者：** 若 Main 的规范会话及其权威归档不可恢复，幸存子流不会自动继承旧 Main 的身份或协调权；它们相对于旧协作空间成为待重新收编的野生会话。用户或受信任 CLI 可以将一个或多个幸存会话作为野生来源显式 `adopt`，创建具有全新 `thread_id` / `main_stream_id` 的新 Main。新 Main 是新的独立权威，不是旧 Main 的无损复活或自动继任者。
- **旧 Main 迟到恢复时多 Main 并存：** 若旧 Main 后来重新找回其规范会话和历史，它恢复的是原来的 Main；此前创建的新 Main 不会因此被撤销、降级或自动合并。新旧 Main 各自在自己的协作空间中独立有效。可以保留 `reconstructed_from` / `origin_main_ref` 等非权威来源关联，也可以在用户明确操作下互相发信、收编或归并，但来源关联本身既不是因果父边，也不赋予任何一方对另一方的协调权。
- **授权派发：** Main 可在其协作空间策略范围内授予派发权限；子流只有持有仍有效且范围匹配的显式授权，才可进一步创建正式子流。授权、再委派与撤销均记录在 Coordinator Journal；创建子流须在同一规范事务中提交派发事实并生成 Dispatch Recovery Capsule。此处不要求 Main 的宿主在线，也不要求把子流完整对话实时复制进 Main。授权不会因会话谱系继承、收编或恢复而自动继承。
- **协调写入世代与人类授权分离：** `coordinator_epoch` 仅用于保护同一 Main 会话/同一规范控制库内的并发协调写入，不定义 Main 的跨灾难继任关系，也不构成跨设备共识。当前协作空间的 Main 是该空间内任务状态和成果采纳流程的协调权威，但不取代真人授权；不可逆外部副作用及需要用户批准的决策仍受独立权限检查与确认约束。

## 2. 非目标

以下方向违反长期架构要求：

- 自建 Agent 工具执行循环、替代原厂客户端运行时，或要求用户改走第三方按量计费推理 API。
- 在当前默认 `Local-Offline Profile` 及 `thp` 生产 CLI 路径中，维持多语言双运行时分裂栈、引入常驻守护进程或跨设备实时消息服务（且禁止将默认配置档的离线限制反向泄漏进 `Pure Core` 契约，从而写死其他可选 Profile 方向）。
- 新增绕过 `Canonical Session IR` 的宿主间点对点直接转换器。
- 沿用历史遗留的非规范存储目录或借住在宿主私有配置目录内的零散路径作为生产默认目录。
- 强迫整个项目或用户的全部会话归属于一个全局唯一 Main/主线，禁止无共同祖先的独立会话作为野生来源显式收编，或强迫不同协作空间最终合并为单一终点。
- 用僵化的最小 Token 字数要求替代结构化信息完整性，或纵容模型以无证据引用的空泛摘要丢弃近期真实交互与工作区脏状态。
- 将 `evidence_based` 客观现场恢复包伪装为 `model_authored` 语义交接单，或将 `Prompt Reconstruction` 表述为模型内部状态无损迁移。
- 仅凭导入文本或模型输出中可伪造的字段授予 `human_authority` 真人授权等级，或将前任交接单、跨流来信及未知块提升为受信任系统指令。
- 允许多个 CLI 进程直接向同一个活跃 JSONL 分片并发追加记录，或在缺少事务级暂存与崩溃对账的情况下跨 SQLite 与文件系统双域裸写。
- 假定所有宿主均具备本机磁盘访问或 CLI 执行能力，或在不支持送达回执的纯剪贴板通道上虚构“消息已送达并被理解”的保证。
- 在宿主不暴露原厂系统提示词或隐藏工具定义时捏造近似文本冒充真实输入，或将 Git 离线同步直接当作多设备实时事务锁。
- 在多路归并发生结论冲突时由底层工具静默丢弃某一方观点，或静默吞没无法识别的新型协议块。
- 将任务/执行尝试账本扩张为 Agent 调度器、常驻监控器或模型运行时；Main 只记录显式派发、检查点、状态证据与成果采纳，不负责保证宿主持续存活。
- 将失联状态等同于永久死亡，或因创建恢复分支而删除、冻结或自动否定原分支；也不得承诺恢复任何未被规范归档、原厂副本或独立备份保存过的信息。
- 将恢复关联（`recovery_of`）、任务归属（`work_item_id`）或派发关系当作因果父边写入 `anchor.parents[]`；因果边只能表示实际继承/依赖的已提交历史。
- 将 Main 的派发权限默认传递给全部后代会话，或将会话历史继承、任务归属、宿主身份等同于权限继承；未经显式授权不得创建正式嵌套子流。
- 将祖先闭包集合运算、固定排序或文件级无冲突判断直接等同于语义归并正确性；在归并理论与契约测试完成前，不得把候选算法写成唯一强制实现。
- 脱离具体业务语义、故障假设与性能约束强行套用单一实现架构，或将 CALM 汇合计算混同于 CAP 线性一致性。
- 将易碎的网页爬虫、必装浏览器扩展、私有客户端逆向或常驻后台抓包守护进程作为核心主路径的强制前提（非侵入只读采集外挂仅允许作为解耦的可选扩展）。

## 3. 规范架构与存储目录布局

### 3.1 三层架构分离：应用薄壳、无偏向 Pure Core 与有偏向可插拔 Profile

```text
+---------------------------------------------------------------------------------------------------+
|  上层：ThreadPass 应用薄壳（Application Shell —— 极简跨宿主协同参考应用）                         |
|  |-- 默认一等主路径：统一用户粘贴 Prompt 往返闭环（adopt / fork / reconcile / handoff / mailbox）  |
|  |-- 身份与安全适配：Current Role Briefing 认知简报与身份重框定 + 信任来源围栏 + AI 脱敏规则       |
|  |-- 领域模型与预算：Canonical Session IR（event / invocation / control）+ 双层有效预算与分片      |
|  |-- 工作区契约：独立 workspace_id + git_repo / plain_dir / none 清单 + 脏状态告警                |
|  +-- 可选解耦外挂：本地 CLI load | 零模型 harvest 兜底 | 只读旁路事件采集 | 门禁内 Native Resume   |
+---------------------------------------------------------------------------------------------------+
         ||                                                                             /\
         ||  === Domain-Agnostic Core API（以下层完全业务无关，不感知 Prompt / LLM / Agent）===
         \/                                                                             ||
+---------------------------------------------------------------------------------------------------+
|  中层：Pure Core（事件型通用分布式状态与协调内核，面向与本契约相容的事件/历史可追溯系统）         |
|  |-- 接缝原语与契约：可见性与释放栅栏 | Fencing Token | Lease | Advisory Hint | 诚实状态契约       |
|  |-- 无偏向核心引擎：                                                                              |
|  |     |-- 原子提交与可见性栅栏契约（提交意图、独立事务证明接口、确定性恢复、抽象副本绑定与快照）    |
|  |     |-- 三关系正交图（Lineage 继承 / Input Dependency / Provenance 溯源严格分离）                |
|  |     |-- CALM/`I-Confluence` 分层协调日志（纯单调汇合投影 vs 非单调互斥阻断；Observations!=Proj）|
|  |     |-- Capability + Escrow 有界配额切分守恒与不可逆撤销 + 多权威独立并存与固定检查点重建        |
|  |     +-- 三态异步信箱状态机（pending -> offered -> committed_receipt）与并发头对账引擎            |
|  |-- 验证门禁：Core DST+状态空间探索 | Profile Conformance 契约测试 | 真实集成故障测试           |
|  +-- 抽象端口：StoragePort | Sync/TransportPort | Coordination/ConsensusPort                       |
+---------------------------------------------------------------------------------------------------+
         ||                                                                             /\
         ||  === Pluggable Profile Ports（物理偏向严格封装于以下配置档内，不污染 Pure Core）===
         \/                                                                             ||
+--------------------------------------------------+  +---------------------------------------------+
|  [默认首个落地] Local-Offline Profile             |  |  [同构代码可插拔 Profile（共享 Pure Core 端口）]
|  · 纯 Node.js（>=22.18.0；LTS 22/24/26），零守护进程| · Shared-Log / Cloud-Object Profile：      |
|  · StateStore (state.sqlite WAL+CAS) + 独占 ACL  |  |    共享分布式日志 / 对象存储 CAS 后端         |
|  · 双域 5 步发布与 history/ / journal/committed/  |  |  · Online-Consensus Profile：               |
|  · transactions/committed/{manifest,finalized}   |  |    常驻共识控制面 + 在线租约选主 + 自动切换   |
|  · blobs/objects/ + 介质无关离线同步 & thp fsck  |  |  -----------------------------------------  |
|  · 可选建议性 baton_handoff / 外部 CAS 适配器     |  |  [跨领域方法论同源（独立紧耦合数据面）]      |
|  · 显式偏向：零运维、断网不断活、个人/跨端协同    |  |  · Low-Latency Directory 范式：             |
+--------------------------------------------------+  |    共享六步闭环与接缝原语（内存目录数据面）  |
                                                      +---------------------------------------------+
```

### 3.2 规范主目录（`~/.threadpass/`）、事务分片布局与权限契约

*作用域声明：本节定义默认 `Local-Offline Profile` 的本机目录与事务分片布局实现。*

系统默认使用 `~/.threadpass/`（支持 `THREADPASS_HOME` 覆盖）。初始化与启动时强制校验目录独占访问权限：POSIX 系统强制目录 `0700` / 文件 `0600`；Windows 系统不依赖无效的 `fs.chmod()`，而是在初始化时调用系统内置 `icacls` 移除继承权限并仅授予当前用户独占 DACL（校验失败则闭环中止）。

为彻底消除多进程并发追加同一文件导致的记录交错与幽灵事件，历史归档采用**事务级不可变分片 + 暂存发布分离布局**：

```text
~/.threadpass/
├── confirmation-nonces.sqlite                  # 设备级已消费 ConfirmationProof nonce 台账（BEGIN IMMEDIATE + 主键全局防重放）
└── projects/
    └── <project_id>/                           # 按规范项目身份隔离
        ├── project.json                        # 可跨设备共享的规范项目身份与已脱敏远端标识
        ├── device-local.json                   # 本机绝对路径、workspace 路径映射等设备私有信息（Git 排除）
        └── threads/
            └── <thread_id>/
                ├── state.sqlite                # 本地事务/并发仲裁权威：DAG、CAS 版本、txn_log、Journal 投影、信箱状态机、可达索引、Blob 索引
                ├── lineage.json                # 自 state.sqlite 原子导出的只读谱系快照（非权威，可随时重建）
                ├── coordination/
                │   └── journal/
                │       ├── staging/             # 与协调事务绑定的 Journal 暂存分片 (<txn_id>.jsonl)，离线同步与检索严格排除
                │       └── committed/           # 不可变事务分片式协调事实日志 (<seq>_<txn_id>.jsonl)，禁止单文件并发追加；不是 Main 对话历史
                ├── history/
                │   ├── staging/                # 未提交事务暂存目录 (<txn_id>/<stream_id>.jsonl)，支持单流与多流原子事务，全文检索严格排除
                │   └── committed/              # 已提交不可变只读分片，全文检索仅扫描此目录；仅存本 thread_id 原生流，不混入跨空间收编原件
                │       └── <stream_id>/
                │           └── <seq>_<txn_id>.jsonl
                ├── blobs/
                │   ├── staging/                # 未提交事务 Blob 暂存区 (<txn_id>/<sha256>)
                │   └── objects/                # 已发布内容寻址对象 (<sha256前2位>/<sha256>)，含大负载、协调快照及跨空间收编物化快照包
                ├── transactions/
                │   ├── staging/                # 未提交事务清单暂存 (<txn_id>.manifest.json)，离线同步与检索严格排除
                │   └── committed/              # 独立可验证事务证明：<txn_id>.manifest.json + <txn_id>.finalized.json
                └── bundles/                    # 可重建交棒包 (<stream_id>.<anchor_id>.md)
```

`history/committed/` 与 `coordination/journal/committed/` 文件名中的 `<seq>` 仅为本地单调可读排序提示，**既不保证跨设备唯一，也不构成因果全序**；多设备离线并发提交在同步后并存相同 `<seq>` 但不同 `<txn_id>` 的分片完全合法，真实因果顺序一律以分片内部记录的父锚点/因果前驱引用及 `txn_id` 为唯一依据。跨空间收编所需的外部不可变事件与快照包须物化存入目标 `thread_id` 自己的 `blobs/objects/`（配套持久化 `blob_manifest` 并纳入目标 `state.sqlite` 的独立 Blob 引用计数），严禁直接混入目标 `history/committed/` 冒充本空间原生流，亦不得仅留跨 `thread_id` 脆弱指针导致原空间清理或单空间迁移时产生悬空引用。

派发、授权、回执、采纳/拒绝、放弃、恢复、收编及归并裁决等协调事实必须以脱敏后的不可变事件写入 `coordination/journal/committed/`；相关会话流可保存指向 `journal_event_id` 的引用或面向模型的交接摘要，但不得成为协调事实的唯一留存位置。`state.sqlite` 中的协调账本表仅作为并发仲裁与可重建索引。胶囊所引用的版本化协调快照须以内容寻址对象存入 `blobs/objects/`（配套不可变 `blob_manifest`），避免多层嵌套派发重复内联膨胀；导出或离线同步独立子会话灾备包时须连同其引用的已获批快照 Blob 一并打包。

**Coordinator Journal 的持久性与灾备：** Journal 事件须与触发它的控制状态变更处于同一逻辑事务，避免出现“派发已发布但没有派发日志”或“日志宣称已派发但子流初始锚点不存在”。Journal 必须采用事务级不可变分片（`<seq>_<txn_id>.jsonl`）并支持独立于 Main 会话流的导出、完整性校验和重建。独立副本须位于不同故障域，并携带事件清单、哈希校验和必要 Blob；未验证副本不能算灾备副本。若部署策略要求关键派发在独立副本确认后才能投喂，该检查属于本地事务 `Finalize` 之后的**外部投喂门禁（Release Gate）**，通过 `dispatch_record` / Journal 投影上的 `durability_state`（`local_committed` / `disaster_redundant`）独立追踪：未获独立副本确认前阻止释放正式派发 Prompt，但不得把外部副本 I/O 塞进本地 SQLite 写锁或长期阻塞本地流写入屏障；若采用本地提交优先策略，则须明确标记 `local_committed`，不得对外声称已具备独立灾备。

**离线备份、介质无关同步白名单与独立事务完整证明（证书即提交 / Certificate-as-Commit）：** 离线备份与跨机冷迁移契约**不绑定特定传输介质**——Git 仓库仅是推荐载体之一，任何对象存储、rsync、文件同步工具或自包含归档包均遵循同一显式白名单与 `thp fsck` 重建协议，且均不充当多机实时事务锁。由于 `state.sqlite` 不同步到远端设备，`commit_txn_id` 与单纯可重算哈希的 JSON 清单均不足以单独证明权威提交曾在源设备发生；系统正式采用**证书即提交**语义——在 SQLite 之外独立持久化不可变 **`Transaction Manifest`**（`transactions/committed/<txn_id>.manifest.json`，记录 `txn_id`、全部受影响流与预期父头、预期 Journal 头、各会话/Journal 分片清单、全部 Blob 路径与 SHA-256 哈希、输入/依赖关系及 `manifest_hash`）与 **`Transaction Finalization Certificate`**（`transactions/committed/<txn_id>.finalized.json`），并正式定义其三项核心规则：

1. **证明内容：** `.finalized.json` 必须同时封装 **`commit_intent_proof`**（在 `Commit Intent` 阶段通过 SQLite 排他锁与预期父头/Journal 头 CAS 校验时产生，是**受信任设备对本机 Commit Intent CAS 已通过的密码学签名声明（Signed Attestation）**，而非远端对源设备 SQLite 内部锁执行过程的零信任密码学证明，绑定 `txn_id`、`manifest_hash`、`expected_parent_heads`、`expected_journal_head`、`fencing_epoch` 与 `signer_device_id`）及 **`publication_closure_proof`**（全部对象发布并校验 SHA-256 闭合、且 `manifest.json` 经 `PublishImmutable` 落盘完成后生成）。
2. **证明产生时机与提交权威：** 仅当 `.finalized.json` 在 `Commit Intent` 校验保护下经 `PublishImmutable` 完成文件与目录持久化后，该事务即在不可变归档层与跨设备恢复语义下**正式成立为最终提交事实**；各设备的 `state.sqlite` 流头指针与 `txn_log = 'published'` 均为可由不可变归档与该证书确定性重建的**本地并发控制投影**。
3. **极简本地信任根（`trusted_device_keys`）：** 跨设备/多机恢复场景下，`commit_intent_proof` 须携带源设备本机 Ed25519 私钥（内置 `node:crypto`）签发的签名声明；远端 `thp fsck` 校验其签名归属本机 `trusted_device_keys`（peer 设备公钥仅由用户带外显式钉入，绝不从同步包自动信任；默认按 `project_id` 隔离作用域，可按需细化至 `thread_id`）。密钥生命周期仅分 **`active` / `retired` / `revoked`**：正常轮换标 `retired`（已导入历史检查点继续有效，新提交拒收）；私钥泄露标 `revoked`（新提交与未钉住事务隔离为 `untrusted_unverified_txn`；因离线无在线可信时间戳权威 TSA 时无法仅凭时间戳区分泄露前正常签名与泄露后倒填伪造，历史安全边界依赖本机已验证检查点钉死及人工审计）。单机无跨机签名密钥模式下须诚实标注 `local_hash_integrity`，严禁在跨设备同步中仅凭无认证明文哈希认定权威提交发生。

- **允许同步（已终结事务包门禁）：** **仅已生成经校验的 `transactions/committed/<txn_id>.finalized.json` 与对应 `<txn_id>.manifest.json` 的完全 Finalize 事务包**才允许进入跨设备同步白名单——包括 `transactions/committed/<txn_id>.manifest.json`、`<txn_id>.finalized.json`、其清单引用的 `history/committed/` 与 `coordination/journal/committed/` 不可变分片，以及通过脱敏复核并在不可变 `blob_manifest` 中明确标记为 `sync_policy = approved` 的引用 `blobs/objects/` 对象（含跨空间收编物化快照包）。禁止将缺少 `.finalized.json` 证书的半发布事务分片纳入主动同步包；未引用对象不得仅因存在于对象目录而自动外发。
- **禁止同步：** `state.sqlite` 及其 `-wal` / `-shm` 文件、`transactions/staging/`、`coordination/journal/staging/`、`history/staging/`、`blobs/staging/`、临时文件、运行锁、本机私钥材料与缓存；`lineage.json` 和 `bundles/` 属于本机可重建派生物，默认排除。标记为 `unscannable_binary`、`local_only` 或未完成脱敏/授权复核的 Blob 一律禁止外发。
- **机器本地信息：** `project.json` 仅保存可跨设备共享的规范项目身份与已脱敏远端标识；本机绝对路径、凭证及 workspace 路径映射保存在 `device-local.json` 或等价设备私有元数据中，并明确加入同步排除规则。
- **恢复与冲突：** 跨设备拉取合并后，须运行 `thp fsck` 按四步顺序独立验证：① 校验 `.finalized.json` 内的 `commit_intent_proof` 签名属于 `trusted_device_keys` 有效授权集合（未登记、已撤销或签名无效隔离为 **`untrusted_unverified_txn`**）；② 校验 `.finalized.json` 绑定的 `manifest_hash` 与 `.manifest.json` 一致；③ 校验 Manifest 内全部会话分片、Journal 分片与必需 `sync_policy = approved` Blob 齐全且 SHA-256 匹配（缺失或残缺一律隔离为 **`incomplete_unverified_txn`**）；④ **跨事务历史依赖闭包校验**：检查活跃头可达的全部 `anchor.parents[]`（直至创世锚点或显式固定收编检查点）、Coordinator Journal 因果前驱、有效派发胶囊及跨空间收编物化快照包是否全部来自已验证通过的事务且无断链（断链或指向已隔离事务的头一律隔离为 **`broken_dependency_closure`**，严禁激活流头）；若缺失对象在 `blob_manifest` 中已显式标为 `local_only` 或 `unscannable_binary`，不破坏硬因果闭包，允许恢复文本与协调历史，但投影须诚实标注 **`partial_blobs_local_only`** 且对依赖原始字节的操作显式阻断提示。最后据已验证通过的会话分片、Coordinator Journal、目标流中的 `notify_receipt` 控制记录与 `Lifecycle Observations` 重建本地 `state.sqlite` 投影、索引及派生视图。Mailbox 与协调事实均须存在于不可变记录中，SQLite 仅保存可重建的队列/游标/账本投影。若多设备离线期间各自追加了同一协调日志或推进同一 `stream_id`，`fsck` 须保留全部原始不可变分片并识别并发头（Divergent Heads）：对于满足 7.5.8 节定义的纯单调 Journal 事实集合，可在派生投影层按确定性单调规则无冲突并合（不可变日志层仍保留真实双前驱，不伪造先后）；对于同一 `stream_id` 的历史分叉、`main_designated` 冲突或非单调互斥协调裁决，须阻断并要求显式对账，严禁按墙钟时间、文件名 `<seq>`、单纯 epoch 大小或合并顺序静默覆盖历史；对于 `Lifecycle Observations` 中的离线矛盾状态观测，须按因果偏序投影为 `concurrent` / `unknown` 而非按墙钟覆盖。
- **可选单写者接力棒防呆与外部 CAS 增强：** 为在无守护进程前提下减少多设备人为误写分叉，可在同步元数据与 Journal 中记录可选的建议性活跃写者提示（`active_writer_hint` / `baton_handoff`）：非当前持笔设备默认进入只读提示状态，引导用户先同步接棒或显式声明离线分叉（该提示仅作防呆护栏，本身不构成分布式强锁）。若进一步外挂支持原子条件写的远端同步适配器（如对象存储 `If-Match` / `If-None-Match` 条件写或远端 Git 引用 CAS），可在联网时对头指针推进或接棒执行原子 CAS；远端不可达时须按配置策略显式闭环拦截或诚实降级为 `local_committed`，绝不以可选外部适配器取代底层离线多头对账安全网。

## 4. 核心组件

### 4.1 事务与通用谱系 DAG 协调层

*作用域声明：本节职责由 `Pure Core` 抽象协调状态机与默认 `Local-Offline Profile` 双域物理存储实现共同承担；其中 `state.sqlite` 与磁盘分片布局属于默认离线配置档职责。*

职责：
- **双域一致性与事务发布协议（`Local-Offline` 不可变发布 + 证书即提交）：** 面向 `blobs/objects/`、`history/committed/`、`coordination/journal/committed/` 与 `transactions/committed/` 的全部正式落盘统一封装为底层 **`PublishImmutable`** 原语：在同文件系统写唯一临时文件 → 写完整内容 → `fsync(tmp)` → 以原子 `no-replace` 方式通过 **`fs.link(tmp, target)`** 发布至正式路径（目标已存在则原子报 `EEXIST`，绝不覆写）→ `fs.unlink(tmp)` → POSIX 对目标父目录 `fsync`（Windows 下 `node:fs` 不支持目录句柄 `fsync`，由文件级刷盘与 NTFS 日志保障）。遇 `EEXIST` 时须核验既有目标哈希/内容一致性（完全一致则幂等成功；若目标内容损坏或不匹配，严禁覆盖，立即 `fail-closed` 交由 `fsck` 隔离）。同时，签发 `.finalized.json` 与变更 SQLite 活跃头/租约须在同一 SQLite 排他写锁下复验 `owner_token` 与 `fencing_epoch` CAS，过期进程绝不能在失去所有权后继续签发证书或激活流头。任何事务均不得直接向活跃 JSONL 分片追加：
  1. **Reserve：** 分配唯一 `txn_id`，在 `state.sqlite` 登记 `txn_log(state='preparing')`、`owner_token`、绑定 **`clock_domain_id`** 与 **`boot_epoch`** 的本地有界 `lease_until`、单调递增的 `fencing_epoch`、预期父头及全部受影响流的写入预留。常规短事务在单次租约窗口内完成，长耗时准备阶段（如大体积流式导入）由活动进程续租；多流预留按稳定 ID 顺序获取以避免死锁。后续状态变更必须校验当前 `owner_token` 与 `fencing_epoch`；遇 `clock_domain_id` 不匹配或 `boot_epoch` 变更（系统重启），严禁拿新单调时钟直接比较遗留 `lease_until`；若发生挂起/休眠恢复且单调时钟推进不可确定，将旧租约状态保守降级为 `unknown`，原所有者继续写入前须回 SQLite 复验 `owner_token` 与 `fencing_epoch`，恢复器结合本机进程存活检查安全接管（无需跨平台虚构挂起计数器）。租约未过期且同重启代次所有者仍存活时，恢复器与 GC 不得清理其暂存数据。
  2. **Prepare：** 将完成脱敏的 Blob 写入 `blobs/staging/<txn_id>/`，将各受影响流的事件分片分别写入 `history/staging/<txn_id>/<stream_id>.jsonl`，将本次事务的协调事件写入 `coordination/journal/staging/<txn_id>.jsonl`（单流普通事务可不产生协调事件；派发、授权、撤销、成果采纳、恢复、收编与重协调等协调事务必须产生），并同步生成暂存的 **`Transaction Manifest`**（`transactions/staging/<txn_id>.manifest.json`，含全部对象哈希与预期父/Journal 头）。逐项完成文件写入、文件 `fsync` 与必要的目录持久化。所有暂存内容均不进入已提交检索面或跨设备同步面。
  3. **Commit Intent：** 在 `state.sqlite` 的 `BEGIN IMMEDIATE` 事务中重新验证 `owner_token`、`fencing_epoch`、全部受影响流的预期父头、Coordinator Journal 预期头与写入预留，生成并登记 **`commit_intent_proof`**（由源设备在本机 Commit Intent CAS 通过后出具的签名声明 Signed Attestation，绑定 `txn_id`、`manifest_hash`、`expected_parent_heads`、`expected_journal_head`、`fencing_epoch`、`signer_device_id` 及源设备签名或本机单机模式标记 `local_hash_integrity`）、候选 `anchor` 集合、各流事件清单、Journal 事件清单（`staged_journal_events[]`）、Blob 引用、信箱变更与多流分片清单（`staged_shards[]`），将 `txn_log` 推进为 `committed_pending_publish`。此阶段**不推进任何受影响流的对外可见活跃头**；全部受影响流均保持写入屏障，普通读取继续使用各自最近一个活跃头，不得读取候选锚点。
  4. **Publish：** 提交意图落账后，通过 **`PublishImmutable`** 原语将 `blobs/staging/<txn_id>/` 中的暂存对象按内容哈希校验并发布至 `blobs/objects/<prefix>/<sha256>`，再以同一 `PublishImmutable` 语义逐项发布会话分片至 `history/committed/`、Journal 暂存事件至 `coordination/journal/committed/`，以及 `transactions/staging/<txn_id>.manifest.json` 至 `transactions/committed/<txn_id>.manifest.json`。已进入 `committed_pending_publish` 的事务只能前滚完成，不得因后续错误回滚或删除其已提交意图引用的对象。
  5. **Finalize（证书即提交语义与锁边界）：** 首先在 **SQLite 写锁外**完成全部已发布对象与 `.manifest.json` 的全量 SHA-256 闭包核验（`committed_pending_publish` 已阻断 GC，不可变对象安全），并在 `transactions/staging/` 写好并 `fsync` 待发布的临时证书 `.finalized.tmp`（含 `commit_intent_proof` 与 `publication_closure_proof`）。随后进入 `state.sqlite` **`BEGIN IMMEDIATE` 排他写锁**并持续持有至事务提交：① 复验 `txn_log.state = 'committed_pending_publish'`、`owner_token` 与 `fencing_epoch` 仍然有效且未被接管；② 在写锁保护下执行亚毫秒级 `PublishImmutable` 链接发布（`fs.link(tmp, transactions/committed/<txn_id>.finalized.json)` 及 POSIX 父目录 `fsync`）——**自 `.finalized.json` 链接持久化完成的那一刻起，该事务在不可变归档层与跨设备恢复语义下即已正式最终提交**；③ 在同一 SQLite 事务内原子更新可重建的本地流头指针与协调日志头投影、将 `txn_log` 标记为 `published`、统一释放所有流及 Journal 写入预留并 `COMMIT`。最后在锁外清理临时文件与空暂存目录，并原子刷新 `lineage.json` 与 `bundles/`（外部灾备确认仍由 `Release Gate` 在锁外异步核验，派生物刷新失败不回滚已发布事务）。只有具备合法且闭合验证通过的 `.finalized.json` 且依赖闭包完整的锚点才可被 `thp load`、后续写事务、跨设备同步或 Prompt 打包作为已提交活跃头消费。
  6. **确定性崩溃恢复：** 任一 CLI 启动或访问受影响流时，均须对照 `txn_log`、`transactions/committed/` 与文件系统执行恢复；恢复器须通过 SQLite CAS 租约与递增 `fencing_epoch` 独占同一事务的恢复/发布权，避免多个进程并发完成同一事务；不得把启动时发现的所有 `preparing` 事务一概视为失效：

| 故障位置 | 确定性恢复行为 |
| --- | --- |
| `preparing` 阶段进程中断、挂起恢复或跨 `boot_epoch` 重启 | 若 `clock_domain_id` 与 `boot_epoch` 一致、时钟未受挂起影响且租约未过期、所有者仍存活，保留全部暂存目录；若 `boot_epoch` 已变（重启）则直接递升 `fencing_epoch` 清理；若发生挂起/休眠恢复且租约状态保守降级为 `unknown`，或同代次租约已过期且确认原所有者失效，则递升 `fencing_epoch` 标记 `aborted` 并清理 Blob、会话分片、`coordination/journal/staging/<txn_id>.jsonl` 与 `transactions/staging/<txn_id>.manifest.json`。 |
| `state.sqlite` CAS / `fencing_epoch` / 预留校验失败 | 提交意图事务回滚，标记本次操作中止并清理其暂存目录；不得发布任何流的历史分片、Manifest、证书或推进流头。 |
| 已登记 `committed_pending_publish` 后崩溃（尚未写出 `.finalized.json`） | 事务尚未形成不可变最终提交，跨设备同步白名单严格排除；全部受影响流与 Journal 保留写入屏障；启动恢复器获取新代次锁校验 `commit_intent_proof` 后，幂等前滚补完剩余不可变 Blob、分片与 `manifest.json`，校验闭包后写出并 `fsync` 临时证书再于写锁内原子链接 `.finalized.json` 并刷新 SQLite 投影头，且中途绝不得 GC。 |
| 已完成 `transactions/committed/<txn_id>.finalized.json` 链接发布，但 SQLite `COMMIT` 尚未完成 | 按证书即提交语义，该事务在不可变归档层**已经正式最终提交**；恢复器核验 Manifest、`commit_intent_proof` 与 `publication_closure_proof` 闭合后，直接幂等刷新 SQLite 全部受影响流头投影与 Journal 头投影、置 `txn_log = 'published'` 并释放写预留。 |
| 跨设备 `thp fsck` 遇无有效签名、缺证残缺或跨事务断链 | 签名不归属 `trusted_device_keys` 有效授权集合的隔离为 **`untrusted_unverified_txn`**；缺证或 `approved` 清单对象残缺的隔离为 **`incomplete_unverified_txn`**；可达 `anchor.parents[]`、Journal 前驱或必需依赖断链的隔离为 **`broken_dependency_closure`**（均严禁激活流头）；预期未同步的 `local_only` / `unscannable_binary` 缺失不阻断文本恢复，但投影诚实标注 **`partial_blobs_local_only`**。 |
| `lineage.json` 或 `bundles/` 刷新中断 | 由已发布的 `state.sqlite`、`transactions/committed/`、`history/committed/` 与 Coordinator Journal 确定性幂等重建。 |

  **读取与写入可见性：** 活跃状态读取只暴露已具备有效 `.finalized.json` 且刷新进本地投影头的锚点及其已发布依赖；普通读者在某流存在待发布事务时可继续读取旧头，但不得读取候选锚点。该流上的新写入须等待恢复完成或明确失败，不得基于待发布头继续推进。原始全文检索可以发现已发布到 `history/committed/` 的记录，但必须保留 `txn_id` / `anchor_id`，不得将尚未生成 `.finalized.json` 的记录误认为已终结活跃上下文。`committed/` 仅表示不可变分片已落地，并不单独等同于事务已具备完整 `Transaction Finalization Certificate`。

  **Blob 孤儿回收：** Blob 采用内容寻址不可变对象；重复内容可复用。GC 必须在 SQLite 协调保护下，以已发布引用、`committed_pending_publish` 事务引用及仍有效的 `preparing` 租约为保留依据；只有超过安全宽限期、且经再次确认无任何有效引用的对象方可回收。禁止仅凭文件系统扫描结果删除对象，也禁止回收活动事务暂存区中的对象。

- **Coordinator Journal 与 Main 协调账本：** 以 `thread_id` 为作用域维护独立于 Main 对话流的不可变协调事实日志。`state.sqlite` 中的派发/授权/回执等索引是 Journal 的可重建投影。每次 `fork`/嵌套派发须在同一逻辑事务中登记派发事实、授权凭证引用、源检查点、任务边界、目标会话身份及 Dispatch Recovery Capsule；只有 Journal 事件、目标子流初始锚点和胶囊全部提交并发布后，才可输出正式投喂包。Main 账本不复制子流全部原始交互；灾备承诺必须由独立副本确认支持。
- **子会话生命周期与恢复：** 区分规范会话是否可读取、其宿主工作副本是否可用，以及相关任务/成果是否已解决；支持从最后可靠已发布锚点创建独立恢复会话，保留原会话并允许迟到复活；禁止仅因超时或失联标记就删除原会话或断言其永久死亡。
- **Main 丢失后的独立重建：** 若 Main 规范会话及其权威归档不可恢复，幸存子会话不得继承旧 Main 身份或协调权；须按固定检查点的野生来源显式重新 `adopt`，创建新的 `thread_id` / `main_stream_id` 与独立 Main。来源原件不移动，所需内容须物化或打包；若旧 Main 后来恢复，新旧 Main 各自独立存在，其来源关联不产生继任、覆盖或自动合并。
- **历史重协调与工作区绑定：** 维护 Lineage DAG 可达关系与来源依赖索引；对多流成果采纳/归并生成可审计的 `reconciliation_record`，但不得将祖先集合运算直接当作语义合并算法。每次锚点均关联独立 `workspace_id` 与工作区状态清单。
- **信箱状态机推进：** 管理信箱队列的 `pending → offered → committed_receipt` 状态转换、投递批次租约与按 `message_id` 去重。

非职责：
- 不解析特定宿主的私有存储格式，在默认 `Local-Offline Profile` 下不驻留后台守护进程，亦不自动执行有冲突风险的代码合并写盘。

### 4.2 规范 IR、信任核验与宿主预算引擎

职责：
- 校验带 `schema_version` 与确定性 `event_id` 的 `event / invocation / control` 三层记录及跨版本兼容迁移；核验 Invocation 的事件覆盖范围、输入投影精度和可观测请求摘要。
- 执行**信任来源核验**，依据适配器提交的来源证据及哈希绑定的人类确认凭证，而非自声明字段计算 `trust_tier`，并将一切非 `human_authority` 内容封装于显式非指令隔离围栏内。
- 校验 `model_authored`（检查必填槽位及其引用的历史 `anchor_id / event_id` 存在性）与 `evidence_based` 两类交接包头，按三尺度计量与宿主有效预算上限执行确定性降级折叠或闭环拒收。

非职责：
- 不篡改 `history/committed/` 与 `blobs/` 中的已归档原件。
- 不在多路归并出现方案分歧时自行裁决或静默删减某一方证据。

### 4.3 脱敏、独占权限与不可变归档存储层

职责：
- 执行跨平台目录独占 ACL 初始化与运行时复验（POSIX `0700` / Windows `icacls`）。
- 运行跨缓冲区重叠滑动窗口内存脱敏与二次复核扫描，将大文本与多模态/二进制对象分流至 Blob 暂存区及内容寻址对象库，对不可扫描二进制块打标隔离。
- 保障 `history/committed/` 与 `blobs/objects/` 仅通过已登记提交意图的幂等发布流程进入归档；Blob GC 必须服从 SQLite 引用、待发布事务和有效租约保护，确保外部全文检索不触碰半写记录或未提交暂存内容。

非职责：
- 不向目标模型的首轮上下文窗口直接倾倒全量已提交历史或原始二进制大块。
- 不在工作区或系统共享临时目录落地未脱敏明文文件。

### 4.4 平台适配器与五维宿主描述符（Host Descriptor）

适配器不得使用单一布尔表掩盖源端、目标端与产品形态的差异；每个适配器实例须在运行时暴露结构化的五维 **`Host Descriptor`**：

| 维度 | 必需声明内容与约束作用 |
| --- | --- |
| **`Identity`** | 平台类别、具体产品形态（CLI / 桌面端 / 远端沙盒 / 网页端等）与已探测版本号。 |
| **`Source Capabilities`** | 原生导出方式、是否支持按 `anchor_id` 增量回收（分页源须声明空洞检测能力）、可观测事件类型集合及原生角色验证能力。 |
| **`Target Capabilities`** | Prompt 文本/分片注入能力、附件上传能力与规格上限（`attachment_limits`）、是否通过 `native_write` 版本门禁与运行态宿主文件/进程锁冲突检测、是否支持 `native_resume`。 |
| **`Execution Environment`** | 本地工作区或隔离远端工作区属性、本机只读归档权限、本机 CLI 调用权限。 |
| **`Constraints`** | 文件/命令授权范围、总有效上下文预算上限、单次输入载荷上限（`per_turn_input_limit`）、预算估算置信度（`context_budget_confidence`：`exact` / `estimated` / `heuristic`）与导出完整度说明。 |

## 5. 交棒交互契约、统一粘贴闭环与身份重框定

### 5.1 统一粘贴 Prompt 往返主路径、按能力分派与双态诚实归档

- **默认一等主路径（统一用户粘贴 Prompt 往返闭环）：** 无论源宿主或目标宿主是网页端、移动端、云端隔离沙盒还是本地客户端，`adopt`（收编）、`fork`（分开派活）、`reconcile`（多路合并裁决）、`handoff`（跨宿主续聊）与 `mailbox`（跨流消息捎带）均以**“生成结构化标准 Prompt → 用户粘贴入目标宿主 → 将宿主输出粘贴回 `thp` 完成脱敏解析与事务提交”**为默认通用一等闭环：
  - **按需本机浏览器图形薄壳（`thp web` —— 解决大段剪贴板与交互体验痛点）：** 为彻底规避各操作系统终端对长中文 Markdown 粘贴、多片分片接力与换行符处理的限制，默认 `Local-Offline Profile` 提供随用随开、零第三方运行时依赖的本机临时 Web 交互层（`thp web`）：
    - 基于浏览器原生大文本框、`navigator.clipboard` 一键剪贴板、分片步进复制（`Part 1/N`）、`.md` 附件拖拽导出、DAG/分叉冲突对比视图，以及 6.2 节哈希绑定人类确认凭证的一键原文审阅签发，并与 CLI 保持全量功能齐平（完整覆盖 `init` / `status` / `adopt` / `reconcile` / `handoff` / `ingest` / `fork` / `mailbox` / `grant` / `peek` / `gc` / `device` / `fsck`）；
    - **强制安全与非守护进程边界：** ① 仅绑定 `127.0.0.1` 随机空闲端口（严禁监听 `0.0.0.0`）；② 每次启动生成高熵一次性会话 Token 并严格校验 `Host` / `Origin` 请求头（防跨站请求与 DNS Rebinding）；③ 随终端关闭或页面空闲自动退出，绝不作为后台常驻守护进程；④ 零独立状态权威，全部读写 100% 经由 `Pure Core + Local-Offline Profile` 公开接口执行。
  - 当目标内容在 `effective_context_budget` 内、但超出 `per_turn_input_limit` 或单附件上限时，按**优先附件打包、退化为剪贴板分片接力**的梯度启用分片投喂协议：每片携带 `envelope_id`、`part_index / total_parts`、`part_hash`、参考围栏及“等待全部分片收齐后再开始执行”的协作约定（属面向异构/隔离宿主的交互协议约定，不替代第 6.2 节外部执行权限层对副作用的约束），终片携带整包清单哈希 `manifest_hash` 与任务触发指令；缺片或乱序须显式报错请求补发，且**半截中断的分片投喂不得伪装成已完成交接或收编的新规范状态**；
  - 对具备本机 CLI/Skill 执行能力的宿主，可作为可选增强通过 `thp load`（或分片 `peek`）自动读取当前流头 `anchor_id` 的交棒包与信箱批次，无需改变底层状态机或产生第二套语义路径。
- **结束归档与可选旁路增强（Outbound —— 双态诚实区分）：**
  - **常态路径（`model_authored`）：** 源模型存活且有可用额度时，由收尾/派发/合并 Prompt 产出带历史证据引用的结构化交接或回执块，经用户粘贴回存（或本地受控提交）推进新锚点；其中**已纳管流续跑或派发回执**须引用信封中已提供的 `anchor_id / event_id`，而**首次野生外部粘贴收编（`unmanaged_external`）**因源对话尚未入库不存在既有 `event_id`，严禁要求模型捏造 ID，应改用**原文关键片段摘录与轮次定位（及可选原始对话全文粘贴）**作为证据，粘贴回 `thp` 入库时由系统分配确定性 `event_id` 并按 6.2 节默认定级为 `untrusted_peer` / `unverified_import`（不自动授予 `human_authority`）；
  - **Reconcile Pack 快照绑定与 Checkpoint 基线守卫：** `reconcile pack` 必须生成并不可变发布 `ReconcilePackManifest`（锁定 `envelope_id`、`main_base_anchor_id` 与每个来源的 `stream_id / anchor_id / anchor_sha256`），`reconcile commit` 强制核对清单哈希且来源与主线零漂移，任何漂移或缺失均抛出 `ReconcileDriftError` 拒绝提交；已有活跃锚点的流在接收结构化 JSON Checkpoint（`ingest`）时**强制要求**校验 `base_anchor_id`，若缺失或目标流在模型分析期间已推进则以 `StaleCheckpointBaselineError` 拒绝直接追加过期上下文。多源归并遇到同一来源流混合不同 `trust_tier` 时必须标为 `mixed_origin`，严禁以单条真人消息抬高整条流的整体来源可信度。
  - **模型输出分片归并协议（Multipart Output Assembly）：** 当单次收编（`adopt`）、检查点回存（`ingest`）或大合并裁决（`reconcile commit`）因信息量大而接近模型单轮输出上限时，允许模型按**对象级多片有效 RFC-8259 JSON** 分多次输出（严禁将单个 JSON 字符串拦腰截断）：每片须**逐片**携带非空 `part_group_id`、`part_index`（上限 `MAX_MULTIPART_PARTS = 64`）、可选 `total_parts` 与显式布尔 `is_final`（固定 `total_parts` 下终片必须明确 `is_final: true`）；多个不带分片字段的互异 JSON 对象直接拒绝，严禁静默丢弃前序对象。齐备后按 `part_index` 顺序确定性去重归并字段，最终仍通过**单次原子事务**发布锚点与历史，不留半写入孤片；入站投喂 `splitIntoFenceAwareChunks` 同样确保每片内的 `<archived_reference_...>` 标签成对闭合。
  - **中断/限流兜底路径（`evidence_based`）：** 源宿主因额度耗尽、限流锁死或崩溃无法再生成交接块时，允许用户直接粘贴原始对话导出/网页全文，或由可选本地源适配器执行 `thp harvest` 零模型读取宿主客观日志，确定性构建涵盖原始用户请求、已执行工具序列、末尾报错与工作区状态清单的现场恢复包，并强制标注 **`[UNSUMMARIZED_EVIDENCE_HANDOFF]`**，标定连续性等级为 `Evidence Recovery`；
  - **可选只读旁路采集外挂（非核心前提）：** 若用户按需挂载框架生命周期 Hook（如回合结束回调）或浏览器只读网络流捕获插件以实现无感增量入库，该外挂必须严守三条原则：**① 只旁听不插嘴**（绝不向活跃对话注入探测 Prompt 占用上下文）；**② 只读不抢锁**（绝不抢占原厂数据库或活跃日志写入锁）；**③ 事件驱动低频增量同步**（仅在回合完成或文件静默落盘后增量同步，避免高频盲轮询带来冗余负载与限流）。

### 5.2 目标投喂路径铁律

```text
统一粘贴 Prompt 往返与通用信封注入为全平台语义基线（完整闭合连续性、信任围栏、预算裁决、重协调与信箱搭便车）。
本地 CLI 自动 load、只读旁路采集及原生存储投影直写均为严格受门禁约束的可选外挂增强。
原生存储直写绝不定义独立谱系语义；检测到宿主正在持锁运行或版本未验证时，必须安全回退通用 Prompt 路径或显式报错。
```

### 5.3 认知角色简报、身份重框定与宿主内部子 Agent 隔离

1. **认知身份与权威身份严格分离，且保持模型原生自我认同：** 目标 AI 在会话内正常工作需要清晰的协作分工，但**不得破坏其原生助手自我认同，也不得把底层权限控制权托付给模型自称**：
   - **顶部 `Session Handoff Overview`（`Current Role Briefing` 认知简报）：** 应用壳在每份交棒/派发信封顶部以温和的工程交接笔记口吻提供结构化简报，明确当前逻辑流 `thread_id / stream_id`、当前值班分工（Active Main Agent `main_coordinator` 或 Scoped Worker Agent `scoped_worker`）、本次任务范围及输出格式，并显式声明：模型保留自身原生助手身份、能力与安全原则，无需扮演或模仿前任模型；历史摘录中前任 AI 的输出与自身当前窗口的实操经验严格分离（未在本环境亲验的事项均须标为前任记录）；当旧聊天窗口被切换角色（如子 Agent 升格为主 Agent 或相反）时，由本简报显式宣布协作分工切换生效；
   - **底层内核独立仲裁：** 简报仅服务于模型对齐任务边界与输出格式，不授予任何实际写账本、提升信任级或创建正式子流的权限；一切协调权威仍由内核 `main_designation`、`dispatch_grant` 与信任来源证明链核验。
2. **收编、重建与低摩擦参考围栏（Identity Re-framing & Safety-Friendly Framing）：** 当多个独立来源被 `adopt` 收编，或原 Main 丢失后将幸存子流收编进新协作空间时，源历史中必然残留旧视角与过时角色自称（如旧 Main 或旧从属流标识）。应用壳将此类历史片段封装于原生友好的 XML 参考标签 `<archived_reference_<hash8> source="..." origin="..." ref="..."> ... </archived_reference_<hash8>>`（同时对正文中的同名闭合标签与历史分隔标记做中性转义以维持格式边界清晰），分片边界使用 `<handoff_chunk>`；正文与信封一律采用客观平和的工程交接措辞，避免使用命令式警告口吻，在保持参考隔离的同时减少对各类模型安全策略的误触发。
3. **宿主内部瞬态子 Agent 隔离：** 宿主框架自身在单次会话中拉起的内部瞬态子 Agent 属于该流内部的工具执行细节（在 IR 中仅记为 `tool_call` / `tool_result`），不自动在 Coordinator Journal 中登记为正式跨流派发；**严禁**将 ThreadPass 的 `main_coordinator` 会话强行挂载至带有强制只读或下属限制隐藏系统提示词的宿主原生子 Agent 实例内执行。

## 6. 规范中间会话表示（Canonical Session IR）与信任来源模型

每条规范记录均须携带 `schema_version`、全局确定性 `event_id`、`stream_id` 及单调局部序号；每条已归档记录及分片清单还须携带其 `commit_txn_id`，以便将不可变内容与 SQLite 提交意图及恢复状态关联。为彻底解耦“客观事件由谁产生”与“单次模型调用实际收发了什么”，IR 顶层严格划分为三类正交记录：

### 6.1 三类顶层记录

1. **`event`（不可变客观事实事件 —— 仅描述发生了什么及其产生主体，不写死跨轮输入/输出方向）**
   - `human_message`：真人输入文本与外部附件引用；
   - `assistant_part`：模型产出的有序分片（`thinking` / `text` / `tool_call`，区分明文推理、摘要推理与不可跨平台伪造的原厂不透明签名块）；
   - `tool_result`：工具执行观察结果，携带关联 `call_id`、执行状态（`succeeded` / `failed` / `partial` / `interrupted`）、`is_error` 及正文或 `blobs/` 引用，完整覆盖并行工具调用、部分返回与中断恢复；
   - `coordinator_inject`：协调层注入的交棒信封（标明 `handoff_mode` 与连续性等级）及多路归并冲突对齐清单；
   - `cross_stream_message`：源流提交至信箱的不可变消息事件（含 `message_id`、来源流/锚点、目标流及负载引用）；它表示消息已进入规范归档，不代表目标宿主已经收到、读取或理解。提呈批次与目标侧回执由独立控制记录表达。
2. **`invocation`（单次模型调用记录 —— 表达单步推理边界的精确或重建输入/输出视图）**
   - `invocation_id` 与指向环境配置的 `invocation_env_ref`；
   - `input_refs: event_id[]`：有序指向本次模型调用实际纳入上下文的既有事件（**仅按引用指向，严禁按轮次重复复制历史事件原文**）；`input_scope` 仅描述可观测事件覆盖完整度：`exact`（事件引用集合完整）/ `inferred`（由压缩边界或宿主行为推导）/ `partial` / `unobservable`，**不得据此单独推断模型实际收到的序列化请求已被精确记录**；
   - `input_projection`：有序的实际输入投影清单，逐段引用 `source_event_id` 及可选 part/range 选择器，记录投影后的角色/模态、纳入顺序、截断/过滤/摘要/角色映射/附件转换等变换，以及被排除内容与原因；不得复制历史事件全文来表示引用关系；
   - `projection_fidelity`：描述输入投影本身的证据精度，取值为 `exact_observable_payload`（本次请求中全部可观测序列化载荷均被准确捕获并以可验证引用持久保存）、`structured_projection`（精确记录结构、顺序与已知变换，但未持久保存完整序列化载荷）、`reconstructed` 或 `unobservable`。可选记录 `observable_payload_hash`；仅有哈希而无可验证载荷引用时，不得宣称可精确重建请求。若因脱敏、隐私或授权策略不能持久保存原始可观测载荷，须降级标注为 `structured_projection` 或更低等级。隐藏 System Prompt、平台注入、不可见工具定义等不得因此被推定为已捕获。
   - `output_refs: event_id[]`：有序指向本次调用产出的 `assistant_part` 事件及对应工具调用批次关联。
3. **`control`（环境配置、工作区清单、谱系锚点、回执与未知扩展）**
   - `invocation_env`：按内容哈希去重引用的调用环境配置记录（可观测的 `model_id`、推理/采样参数、输出格式约束、可见工具定义摘要及系统指令指纹）；**宿主不暴露或无法导出的原厂系统提示词、隐藏工具与内部状态必须显式标为 `unobservable`，严禁伪造近似值**；
   - `blob_manifest`：为每个外部 Blob 保存不可变的内容哈希、字节长度、扫描/脱敏状态、隔离级别及 `sync_policy`（如 `approved` / `local_only` / `unscannable_binary`）；该元数据须随引用它的会话分片或 Coordinator Journal 分片一同持久化，确保仅被 Journal 或胶囊引用的快照 Blob 也能独立完成同步策略与完整性校验，不能仅保存在可重建的 SQLite 索引中；
   - `workspace_manifest`：绑定独立 `workspace_id` 的工作区状态清单（详见 7.1 节）；
   - `compact_boundary`：标记上下文压缩分界点、交接包类型及覆盖事件范围；
   - `anchor`：不可变规范检查点，记录 `anchor_id`、`stream_id`、Lineage 父锚点数组 `parents: anchor_id[]`、关联 `workspace_manifest` 与可选 `reconciliation_record_ref`。父节点数量只描述规范状态继承结构，不推断操作类型；根收编、单流推进及多源重协调的具体语义须由对应控制记录说明；
   - `main_designation`：不可变记录某个 `thread_id` 将哪一个既有 `stream_id` 指定为该协作空间的 Main（`main_stream_id`）；它只授予该会话在本协作空间内的协调角色，不创建脱离 `stream_id` 的抽象身份，也不影响其他 `thread_id` 中的 Main；
   - `adopted_source_manifest`：记录一个新 Main 通过野生 `adopt` 收编的外部来源会话，逐项 pin 到完整来源身份（`project_id` / `thread_id` / `stream_id` / `anchor_id`）、祖先清单、数据完整性等级及不可变内容包/Blob 清单哈希；来源关系不移动、不改写原会话，不跟随原会话未来的新头，也不自动成为 `anchor.parents[]` 或授予新 Main 对原会话的控制权；
   - `cross_main_ref`：可选记录两个独立 Main 之间经显式操作建立的来源、消息、借阅或归并关联；该引用本身不是因果父边、Main 继任关系或协调权委派；
   - `coordinator_journal_ref`：指向某个协作空间 Coordinator Journal 中的不可变事件及其校验信息；会话流可引用协调事实，但不复制并取代其唯一日志权威；
   - `dispatch_record`：不可变记录一次显式派发（`dispatch_id`、发起 Main 的 `main_stream_id` / `thread_id`、派发时的局部 `coordinator_epoch`、发起流/源锚点、目标 `stream_id`、可选 `work_item_id` 任务分组、任务边界、授权凭证引用与预期产物）；它是协调/来源关系，不自动成为子流的因果父边；
   - `dispatch_grant` / `grant_revocation`：记录派发授权的授予、受限再委派、撤销及其因果来源；授权不由 `anchor.parents[]`、任务归属或会话恢复关系推导；
   - `input_dependency_manifest`：记录某次规范状态形成或模型调用实际消费的外部输入（来源锚点、Journal 事件、Artifact、消息或事件范围），包含固定身份、哈希、选择范围与可观测性等级；它不自动生成 Lineage 父边；
   - `provenance_ref`：记录结论、摘要或 Artifact 的证据出处；单纯引用不代表实际输入依赖，也不生成 Lineage 父边；
   - `reconciliation_record`：不可变记录多来源重协调的输入锚点、实际采用/排除的内容或变化、冲突与裁决依据、输出锚点及验证结果；其格式独立于最终采用何种 Patch/merge 算法；
   - `dispatch_recovery_capsule`：与派发记录绑定、可由子会话独立携带的恢复胶囊，包含原始 Main 的 `main_stream_id` / `thread_id`、Main 源锚点、派发记录引用与哈希、派发时局部 `coordinator_epoch`、以内容寻址 `blobs/objects/` 存储的版本化 `coordinator_snapshot_ref`（派发时 Main 已知的目标/约束、已知子会话与活动派发、已采纳决策和未决事项）、任务说明、初始上下文引用、工作区身份、权限/能力边界、预期产物及完整性声明；仅携带经过脱敏且允许同步的内容或引用，且不得宣称覆盖快照之后 Main 未传播的新状态。胶囊归档用于灾备重建，实际投喂给子流宿主的 Prompt 须按最小必要任务范围与权限边界裁剪，严禁无差别将全局协调快照外泄给低权限或外部宿主；
   - `session_lifecycle`：追加记录不可变可用性观测事件（`Lifecycle Observations`，包含来源、宿主可用性观察值与因果前驱），**唯一权威留存于目标流 `history/committed/<target>/`**，供集合并集保存；Coordinator Journal 不双写第二份原始观测权威，仅可保存引用（若记录独立协调派生事件须有独立事件 ID 与来源引用，不得冒充原始观测）；当前可用性状态（`Lifecycle Projection`）须按因果偏序重建并允许投影为 `concurrent` / `unknown`，宿主观察值不等同于规范会话永久丢失的证明；
   - `recovery_record`：记录恢复目标（原 `stream_id` / `anchor_id` 或原 Main）、新建规范会话的 `stream_id`、实际恢复来源锚点集合、证据引用、数据缺口和 `recovery_of` / `reconstructed_from` 来源关联；该关联不取代 `anchor.parents[]`，也不自动建立 Main 继任关系；
   - `result_receipt`：记录子会话成果、Artifact、工作区状态与所属 Main 的接收/采纳/拒绝/待核对状态（对应 Journal 中的 `result_receipt_recorded`）；接收回执不自动等同于成果已被采纳；
   - `coordinator_epoch`：记录某一具体 Main 会话在其当前规范控制库内的协调写入世代及其本地 fencing 依据；不得跨新旧 Main 继承或用于推断不同 Main 的权威高低；
   - `notify_receipt`：**跨流信箱投递提案与回执的唯一权威记录**，不可变写入目标流已提交分片，记录 `offered` 批次提案（高熵 `batch_id`）与 `committed_receipt` 消息级送达确认；Coordinator Journal 不维护第二份信箱回执权威，SQLite 队列/游标与 `fsck` 仅据其投影重建；
   - `transaction_manifest` / `transaction_finalization_cert`：分别对应独立持久化在 `transactions/committed/` 的 `Transaction Manifest`（事务全量对象清单与哈希闭包）与 `Transaction Finalization Certificate`（包含 `commit_intent_proof` 签名声明 `Signed Attestation` 与 `publication_closure_proof` 的不可变提交证明——按证书即提交语义，经 `PublishImmutable` 落盘即构成跨设备可验证的事务最终提交事实，SQLite 活跃头仅为本地可重建投影）；
   - `unknown`：封装暂未建模的宿主协议或高版本扩展字段（大块或二进制转存 `blobs/`），默认 `trust_tier = untrusted_unknown`，确保零静默丢失且严禁自动作为可信指令投喂模型。

#### 6.1.1 规范记录、Journal 事件与投影权威映射表（Canonical Event Registry）

为避免跨子系统命名混淆，下表锁定控制记录、Coordinator Journal 事实、唯一权威位置与单调性/投影规则的严格对应关系：

| 控制结构 / 记录类型 | 对应 Coordinator Journal 事件 | 唯一权威位置 | 语义归属与投影规则 |
| --- | --- | --- | --- |
| `transaction_manifest` / `transaction_finalization_cert` | —（跨流事务层证明） | `transactions/committed/<txn_id>.{manifest,finalized}.json` | 按证书即提交语义封装 `commit_intent_proof`（`trusted_device_keys` 验签）与 `publication_closure_proof`，构成最终提交事实；SQLite 头为可重建本地投影；异常分别隔离为 `untrusted_unverified_txn` / `incomplete_unverified_txn` / `broken_dependency_closure`（预期缺 `local_only` 负载标 `partial_blobs_local_only`）。 |
| `main_designation` | `main_designated` | `coordination/journal/committed/` | 同 `thread_id` 内不可变创世唯一性约束；并发指定不同 `main_stream_id` 阻断对账。 |
| `dispatch_record` | `dispatch_committed` | `coordination/journal/committed/` | 显式派发事实；与子流初始锚点及胶囊同事务提交。 |
| `dispatch_grant` | `grant_issued` | `coordination/journal/committed/` | 有界授权与 `Escrow` 配额授出/切分；非单调配额扣减按 `fail-closed` 校验。 |
| `grant_revocation` | `grant_revoked` | `coordination/journal/committed/` | 永久不可逆撤销墓碑（单调并集）；恢复须签新 `grant_id`。 |
| `result_receipt` | `result_receipt_recorded` / `result_accepted` / `result_rejected` | `coordination/journal/committed/` | 任务成果回执事实可按唯一 ID 单调入集；同一产物采纳/拒绝裁决属非单调互斥核。 |
| `notify_receipt` | `—`（可选引用目标流，不双写权威） | 目标流 `history/committed/<target>/` | 跨流信箱 `offered → committed_receipt` 唯一权威；单调状态跃迁，`state.sqlite` 仅作投影。 |
| `session_lifecycle` | `—`（仅保存指向目标流观测的引用，不双写原始观测权威） | 目标流 `history/committed/<target>/` | 拆分两层：`Lifecycle Observations` 单调并集；`Lifecycle Projection` 非单调，无序矛盾显式投影 `concurrent` / `unknown`。 |
| `recovery_record` | `recovery_recorded` | `coordination/journal/committed/` + 恢复流锚点 | 记录 `recovery_of` 来源，不代替 `anchor.parents[]`。 |
| `adopted_source_manifest` | `adoption_committed` | 新 Main 流 + `coordination/journal/committed/` | 固定来源检查点快照包，不改变原流。 |
| `reconciliation_record` | `reconciliation_committed` | `coordination/journal/committed/` + 输出流锚点 | 非单调互斥与多来源重协调裁决，强制留证。 |
| `baton_handoff`（`active_writer_hint`） | `baton_handoff` | 同步元数据 / Journal 建议性记录 | **Advisory Hint**：仅作防误写建议提示，不构成硬 Fencing Token 或锁。 |

### 6.2 信任来源认证链（Trust Provenance Chain）

**`human_authority` 不得由导入内容或模型输出自行声明**，而必须由本地受信任适配器依据可验证来源证明统一计算并落盘：

| 来源证明字段 | 约束含义 |
| --- | --- |
| `capture_origin` | 采集通道与宿主身份（绑定对应 `Host Descriptor`）。 |
| `verification_method` | 角色核验方式：`native_structured_role`（经验证适配器从可信原厂结构来源解析角色）、`local_cli_direct`（由 ThreadPass 在交互式受信终端中直接采集的真人输入；非交互 stdin/管道、命令参数、环境变量及 Agent 工具代调均不符合此定义）、`unverified_text_import`（无结构保证的外部导入）、`model_reported`（模型转述或摘要）。 |
| `source_event_ref` | 指向原厂记录标识或本机交互审计依据的溯源指针。 |
| `adapter_version` | 执行本次解析与核验的适配器版本及其受信任验证状态。 |
| `confirmation_proof` | 可选的结构化一次性确认凭证，至少绑定被确认内容的规范化摘要/哈希、确认目的、交互通道、随机 nonce 与确认时间；不得以布尔值或未绑定内容的通用确认标志代替。确认目的至少区分 `accept_import`（仅接受归档该导入材料）与 `authorize_instruction`（用户审阅具体内容后，明确将其作为当前授权指令）。 |

信任分级铁律：
- `native_structured_role` 仅在对应宿主版本、采集通道及适配器解析规则均经过验证，且角色元数据来自可信原厂结构来源时成立；从模型生成文本或不可信导出片段中解析出的 `user` 字样不得满足此条件。
- `local_cli_direct` 仅适用于用户在受信任 CLI 的交互式终端输入内容；由 Agent 工具调用、非交互管道、脚本参数或环境变量提供的文本不得自动获得真人来源证明。
- 外部导入及 `init` / `fork` / `mailbox` / `ingest` 等流程中未经受控真人确认的文本（`initThread --summary`、`forkDispatch --task`、`sendMailboxMessage --text` 及无签发凭证的 `ingest` / `adopt` 均默认归为 `unverified_import` 或 `untrusted_peer`，且服务层 `ingestReply` 严禁接受调用方自报 `verificationMethod`），只有在用户通过本机受信任交互通道（CLI 仅允许真实 TTY 终端核对，`thp web` 需先经 `/api/confirm/prepare` 生成绑定哈希的一次性挑战并在弹窗核验后签发）审阅**确切内容**并生成**由本地设备 Ed25519 私钥或共享 HMAC 密钥签发且未消费**的 `authorize_instruction` 确认凭证后，才可针对该确切内容授予 `human_authority`。已消费的 `nonce` 同时记入 Thread 级 `consumed_confirmation_nonces` 与设备级 `<homeDir>/confirmation-nonces.sqlite`（`BEGIN IMMEDIATE` + 主键原子占位），杜绝并发竞态及跨 Thread / Project 重放（注意恢复边界：`fsck --rebuild` 按单个 `thread_id` 执行，若全局 `confirmation-nonces.sqlite` 丢失，须对涉及的各个 `thread_id` 分别执行 `fsck --rebuild` 以回填全部历史凭证）。`ReconciliationConflict.human_confirmed` 仅由系统根据上述已核验授权凭证衍生并记录 `confirmation_proof_nonce`，模型自声明一律置 `false`。`accept_import` 只表示允许保存材料，不授予指令权威。凭证不得跨内容修改、跨目的或跨对象复用。
- 来源角色不可独立验证且未取得上述明确授权的外部导入历史降级为 **`unverified_import`**，不得自动继承当前系统的指令授权；
- 模型撰写的交接单、模型转述的用户要求及跨流来信一律归为 **`untrusted_peer`**；工具返回与外部抓取内容归为 **`untrusted_observation`**；未知块归为 **`untrusted_unknown`**。除 `human_authority` 外的全部层级在注入目标 Prompt 时均须封装于显式非指令参考围栏内，严禁提升为系统指令或越权触发操作。
- **参考围栏的职责边界：** 非指令参考围栏与信任分级标签仅用于模型上下文层的来源区分与参考隔离，绝不构成执行安全边界或沙箱；一切文件读写、凭证访问、网络外发与不可逆副作用权限**必须由模型之外的受控执行权限层 / 最小权限沙箱强制实施**，严禁仅依赖 Prompt 信任分级机制替代底层权限控制。

## 7. 标识、工作区、通用 DAG 归并与跨流信箱契约

### 7.1 标识域、独立工作区与去重职责隔离

以下标识与哈希严格属于不同语义域，严禁混用：

- **`project_id` vs `workspace_id`：** 同一规范项目（`project_id`）下可同时并存多个独立的物理或远端工作区（`workspace_id`，如多个本地独立工作树、独立克隆副本或远端隔离沙盒工作区）。所有工作区状态均绑定具体 `workspace_id`，并严格区分两类制品：
  - **`Workspace State Manifest`（状态清单）：** 显式声明工作区形态 **`workspace_kind = git_repo | plain_dir | none`**。在 `git_repo` 下记录仓库基准身份（规范根路径、远端 URL、分支、HEAD 提交）、本地暂存/未暂存/未跟踪文件清单、综合状态哈希及远端分支/PR 持久化状态；在 `plain_dir`（无 Git 或非 Git 目录）下记录目录身份与文件清单/修改摘要并标注 `vcs = none`；在 `none`（纯无工作区对话）下显式留空。三类形态均须**显式声明采集完整性等级与已知盲区**（如被忽略文件、子模块内部修改、外部大对象及扫描期文件并发变动警告）；
  - **`Workspace Snapshot Artifact`（恢复制品）：** 可选归档于 `blobs/` 的未提交差异补丁包，用于跨工作区人工核查或辅助恢复；清单不保证能无损重建未纳入版本控制的外部环境。
- **`thread_id` / `stream_id` / `host_session_id`：** 分别标识一个独立协作空间、其中的规范会话流及特定原厂客户端内部会话号。Main 本身就是被指定为该协作空间最高协调权威的一个规范会话流，不是脱离会话文件的抽象 Agent 身份。**`host_session_id` 活跃归属单一化：** 同一原厂 `host_session_id` 在同一时刻仅允许作为一个规范 `stream_id` 的活跃工作副本绑定（用于增量 `harvest` 或 `native_write`）；其他协作空间收编该会话当时的内容时，只能以只读固定快照接入，严禁同时抢占同一原厂窗口的活跃绑定。
- **Main 身份与多 Main 隔离：** `main_stream_id` 标识某个协作空间当前指定的 Main 规范会话流，且其值就是一个具体 `stream_id`；它不是旧 Main 丢失后由新 Main 继承的稳定逻辑身份。不同独立 Main 必须使用不同 `thread_id` / `main_stream_id`，可通过显式来源/跨 Main 引用建立非权威联系。`coordinator_epoch` 仅标识某个 Main 在特定规范控制库内的协调写入 fencing 世代。
- **派发与恢复标识：** `dispatch_id` 标识一次派发操作；可选 `work_item_id` 用于把多个相关会话归入同一逻辑任务，但不取代会话身份；`recovery_id` 标识一次恢复/重新收编操作；`recovery_of` / `reconstructed_from` 指向原会话或原 Main 来源。每份规范会话仍以自己的 `stream_id` 和锚点谱系为身份基础；任务分组与恢复来源只是附加关联，不替代会话身份。
- **关系类型隔离（Lineage / Input Dependency / Provenance）：** 三类关系必须分别建模、分别校验，可互相引用但不得互相冒充：
  1. **`Lineage`（规范状态继承）：** `anchor.parents[]` 只指向实际作为新规范状态之历史基础、且内容可验证的已发布锚点。它回答“这个规范状态从哪些规范状态继承而来”，用于祖先闭包、共同历史分析与谱系恢复。Main 身份、派发、任务、恢复关联、消息通知或单纯阅读来源均不自动构成 Lineage 父边。
  2. **`Input Dependency`（实际输入依赖）：** 记录某次模型调用或规范状态转换实际消费的输入，包括被投喂的来源锚点/事件、Coordinator Journal 事件、跨流消息、Artifact、外部文档片段及其选择范围。依赖必须 pin 到稳定身份与哈希，并标明可观测性/投影精度；“模型可访问”或“后来声称看过”不等于实际消费。输入依赖不自动扩展 Lineage DAG。
  3. **`Provenance`（证据来源追溯）：** 记录一项结论、摘要、成果或 Artifact 的出处与支持证据。仅作为出处引用而未被实际输入/纳入状态的材料只属于 Provenance；它不自动成为 Input Dependency，更不成为 Lineage 父边。
- **判定规则：** 若新锚点完整或部分继承某个来源锚点的规范状态作为其状态基础，应以 Lineage 和/或显式可验证的变化/贡献引用表示；若仅把来源中的选定内容作为输入，则记录 Input Dependency，不得把整个来源检查点伪装成父节点；若仅标注证据出处，则只记录 Provenance。具体“变化/补丁贡献”是否需要独立的一等对象，留待 Patch Theory 等模型研究和契约测试确定，不得提前把 Snapshot DAG 等同于变化依赖图。
- **其他关联隔离：** `dispatch_id` / `work_item_id` 表示派发与任务关联；`recovery_of` / `reconstructed_from` 表示恢复来源；`cross_main_ref` 表示跨 Main 非权威联系。上述关系均不得伪装成 Lineage 父边、权限继承或 Main 继任关系。
- **三层去重职责隔离：**
  1. **`event_id`（事件级幂等）：** 由确定性来源指纹生成，防止同一原始事件因重试或重叠导出被重复入库；
  2. **`anchor_id` + 祖先闭包（谱系级去重）：** 仅负责消除同源谱系中共享祖先与部分共享中间子图的重复注入，绝不跨无共同祖先的独立根流做语义删除；
  3. **`content_hash`（负载级去重与展示折叠）：** 用于 `blobs/` 内容寻址去重，以及在多根独立收编归并中将完全相同的重复大文本块折叠为指向首次出现位置的引用提示，同时保留各源流独立的事件语义。

### 7.2 多源收编（`adopt`）、来源身份反查、隐式分叉保护与未提交告警

- **收编来源二分法：** `adopt` 显式区分两类外部来源，均**严禁依赖不同宿主的墙钟时间戳推断因果先后**：
  1. **`unmanaged_external`（未纳管外部会话）：** 从未进入 ThreadPass 的原厂对话或文本导入。允许在原子事务中先落为本协作空间内 `parents = []` 的独立根流锚点（保留各自 `workspace_id`、`host_session_id` 及后续增量/隐式分叉回收能力），再由新 Main 通过 `adopted_source_manifest` 固定引用；或按用户选择以只读快照包形式收编。
  2. **`foreign_canonical`（跨空间已有规范会话）：** 属于另一个 `thread_id` 的既有规范会话（无论其原 Main 仍在正常运行还是已丢失）。新空间绝不移动、克隆或改写原流，亦不抢占其活跃 `host_session_id` 绑定；而是保留完整原标识（`{project_id, thread_id, stream_id, anchor_id}`）与既有可验证信任级，将所需不可变分片/Blob 物化存入新空间的 `blobs/objects/` 并在 `adopted_source_manifest` 中固定引用。
- **入库前三层反查与防误收编门禁：** 执行 `adopt` 前，CLI 须自动反查本机所有项目的 `state.sqlite` 与导入内容：
  1. 查本机 `host_session_id` 活跃绑定表与导入事件的确定性 `event_id` 指纹；
  2. 扫描外部文本中是否包含疑似 ThreadPass 信封/回执标识（仅作提醒线索，**绝不直接采信其自称身份**）。
  命中结果按三条规则分流，禁止静默降级为陌生野生文本：① 命中**同一 `thread_id` 内既有流** → 拒绝按野生重复 `adopt`，引导改用 `save / harvest / fork / reconciliation`；② 命中**本机另一 `thread_id` 的正常规范流** → 提示升级为正规 `foreign_canonical` 跨空间固定收编（保留谱系与信任级，不抢原流活跃宿主绑定）；③ 文本扫出本机查不到的 ThreadPass 标记（疑似跨设备未同步）→ 显式告警提示先同步，若用户强制继续则标定为 `unverified_import` 且不采信其自称身份。
- **惰性历史边界与工作区漂移告警：** 交棒包强制声明历史事件仅为已完成事实（严禁重做历史指令），并比对源锚点与目标端的 `workspace_id` 及 `Workspace State Manifest`；若源端存在未提交脏改动且目标端位于不同工作区或隔离远端，须在首屏高亮告警未同步的变更清单与补丁引用。
- **老窗口续聊的隐式分叉保护：** 锚点不可变；若某已分叉锚点在旧宿主窗口继续产生新交互并触发回收，CAS 事务将其安全延伸为该锚点下的新兄弟子锚点，绝不破坏已派生子流。

### 7.3 任意层级裂变、并发变化与显式重协调（Reconciliation）

通用 Lineage DAG 支持从任意已发布锚点进行 `fork`，支持任意数量的平行流长期并跑，也支持显式多来源重协调。**本节规定的是历史语义与正确性边界，不预先锁定最终的 Patch、merge 或重协调算法。** `squash-merge` 可以作为面向用户的操作名称保留，但不能据此假定其内部等同于 Git 式快照合并或某一种集合算法。

#### 7.3.1 Lineage DAG 记录什么

- Lineage DAG 记录规范会话状态之间可验证的继承关系；它不是“模型曾经看过什么”的总关系网，也不是所有协调、任务、引用和证据的汇总图。
- `anchor.parents[]` 指向构成该锚点规范状态基础的直接已发布父锚点。父边须由实际状态继承关系与可验证内容支持，不得依据墙钟时间、创建先后、Main 身份、任务归属或恢复标签臆造。
- 多源收编可以从无共同祖先的来源开始，但外部来源默认通过固定的 `adopted_source_manifest` 与 Input Dependency 引用，不得为制造共同祖先而伪造父边。
- Lineage DAG 与潜在的 Patch/Change Dependency Graph 不是同一个概念：前者以规范检查点/状态为节点，后者若被采用则描述可独立应用的变化及变化间依赖。是否需要第二种图、两者如何映射，属于待验证的核心架构问题。

#### 7.3.2 重协调是一个显式、可审计的操作

任何多来源重协调都必须生成不可变 `reconciliation_record`，至少能够追溯：

1. 输入来源的固定身份与锚点，以及各来源的完整性/可观测性状态；
2. 实际被纳入、部分纳入、排除或仅作为证据引用的内容/变化及其来源；
3. 已知的因果依赖、可交换性判断、语义冲突与未知/不可观测部分；
4. 冲突裁决、人工确认或确定性策略的依据；
5. 输出锚点、输出工作区状态、所用算法/策略标识与版本，以及可重复验证所需的输入清单和哈希。

输入会话原件与历史永远保留。重协调不等于删除、改写或压平来源历史；接收某个成果也不等于自动接受来源会话的全部状态。若只采纳某来源的一部分内容，记录必须精确到可验证的选择范围或 Artifact；不得用整个来源锚点冒充“全部内容已合并”。

#### 7.3.3 祖先集合分析的地位

现有祖先闭包集合运算可保留为**谱系预分析工具**。令 $A(p)$ 表示锚点 $p$ 的可验证 Lineage 祖先闭包，则可计算来源集合的可达并集、公共祖先交集及极大公共祖先，用于识别共享历史、避免重复展示共同祖先，并帮助生成待审查的来源计划。

但以下限制是强制的：

- 公共祖先集合、部分共享子图和单父独有增量只是**谱系拓扑事实**，不能单独证明相应内容变化可安全合并，也不能决定来源内容应全部纳入。
- 覆盖父集签名等集合标签只能描述某个祖先被哪些输入锚点包含，不能替代变化依赖、内容选择或语义冲突判断。
- 不得把所有不可比较节点按 `anchor_id` 字典序等人为规则线性化后，宣称该顺序具有因果或语义意义。必要的展示排序必须标记为展示顺序；实际处理必须尊重已知因果偏序，不能给未知关系补造先后。
- 部分共享历史去重只消除同一可验证来源节点的重复呈现，不得跨无共同祖先的独立根流按文本相似度或内容哈希合并事件身份。内容哈希只负责负载去重，不证明语义相同。

#### 7.3.4 冲突与自动化边界

至少区分以下情况，并保留其证据：

- **有因果依赖：** 一个变化必须在另一个变化之后解释或应用；不得任意交换。
- **可交换变化：** 在明确的数据/变化模型及其验证规则下，两个变化的应用顺序可交换且结果等价。不能仅凭“不同文件”或“没有文本重叠”作此判断。
- **结构冲突：** 无法同时满足数据结构不变量、引用完整性或工作区约束。
- **语义冲突：** 内容层面可能可以拼接，但决策、接口、架构意图或结论互相矛盾。
- **未知/不可观测：** 缺少足够输入、隐藏上下文或来源数据，无法可靠判定是否兼容。

系统可以自动完成经验证的确定性转换、重复内容折叠与非冲突数据组合；无法证明安全的部分必须显式保留并进入重协调任务，不能静默选边。技术事实冲突不得由底层存储工具替用户裁决。

#### 7.3.5 候选理论模型、单调性三层边界与决策门禁

按 **CALM 单调性原则与不变量合流性（`I-Confluence`）**，本系统的数据与操作严格分为三层，不得试图用单一归并算法抹平三层差异：**第 1 层基于 CALM 单调性所获得的是在满足消息传播前提下协调自由的“汇合计算（Confluent Computation）”；若具体数据类型采用 CRDT 实现，则须另行验证其强最终一致（Strong Eventual Consistency, SEC）契约与传播前提，且绝不等同于 CAP 线性一致性（Linearizability）**；三层之间的操作分布取决于具体业务不变量与争用热点；跨层与跨执行路径组合须严格遵守 `0.1.1` 节定义的接缝原语与状态契约：

1. **纯单调不可变事实与对象层：** 内容寻址 Blob、不可变 `event`、目标流 `notify_receipt` 状态跃迁集、任务回执入集 `result_receipt_recorded`、不可变观测集 `Lifecycle Observations`、证据来源集（`provenance_ref`）及不可逆撤销墓碑集（`grant_revoked`）——满足确定性集合/半格并合，可在派生视图层无冲突自动收敛；**但仅靠追加日志不能自动满足唯一性（如 `main_designated`）或有界配额守恒等非单调不变量，观测集单调亦不代表当前 `Lifecycle Projection` 状态单调**。
2. **结构化状态与操作日志层：** 授权与 `Escrow` 配额账本、信箱队列投影、工作区清单与拓扑控制记录——适用操作日志、不变量切分检查与偏序/冲突检测，对非单调互斥及越界变更显式暴露冲突或 `fail-closed` 阻断。
3. **非单调语义与意图层：** 自然语言结论、架构方案决策与代码语义分歧——本质上不具备通用代数交换律，**严禁交由 CRDT 静默自动合并**，必须保留冲突证据进入显式 `reconciliation_record` 交由人或受控模型裁决。

在此三层边界内，Patch Theory（主要用于研究部分采纳与显式变更块的依赖表达）、操作日志模型及适用范围明确受限于第一层的 Causal CRDT 均可作为候选理论来源；任何具体候选模型都必须通过 ThreadPass 场景验证：普通分叉、双分支独立演进、部分采纳、交叉派生、恢复分支、野生收编、多 Main 关联、旧 Main 复活，以及含有语义冲突的多路重协调。

在上述验证完成前，基线只强制上述三层边界、Lineage / Input Dependency / Provenance 的关系分离、来源固定、原件保留、冲突可见和重协调可审计；不强制使用某个 VCS 的内部对象模型，也不强制采用某个祖先集合算法作为唯一归并算法。

#### 7.3.6 工作区约束

每次重协调须汇总所有来源对应的 `workspace_id`、分支/提交、远端持久化状态及未提交脏文件清单。会话历史合流不表示代码工作区已合流；工作区文件写入必须由独立工作区快照、补丁、Git worktree 或写租约处理，不能用会话 DAG 的 CAS 替代文件级并发保护。

### 7.4 平行流只读借阅与跨隔离端异步信箱协议

并发流无需以最终合并为终点；长期并跑的平行流之间在默认 `Local-Offline Profile` 下通过两种无守护进程机制协作（在 `Online-Consensus` 等在线 Profile 中亦可无缝映射为等价异步通知协议）：

- **只读借阅（`peek`）：** 具备本机只读权限的宿主可按需查阅指定平行流的已提交交棒包或检索 `history/committed/<target>/`，不修改任何谱系指针或信箱状态。
- **至少一次投递、幂等回执异步信箱（`mailbox`）：** 面向本地与隔离远端/网页端统一采用 **`pending → offered → committed_receipt`** 三态协议（明确不向纯剪贴板通道虚构“对方已读理解”承诺）：
  1. **入队（`pending`）：** 源流提交消息，经脱敏扫描与 `message_id` 幂等去重，将消息正文/负载引用作为不可变 `cross_stream_message` 事件写入源流 `history/committed/`（大负载引用已提交 Blob）；同时在 `state.sqlite` 建立可重建的待投递队列索引与非合并性质的 `notify` 引用边。消息归档与队列登记必须处于同一事务发布协议中，不能只存在于本机 SQLite。
  2. **打包提呈（`offered`）：** 目标流加载或本机为其打包下一轮投喂信封时，系统按批次提取 `pending`（及租约超时的 `offered`）消息，赋予高熵不可预测的唯一 `batch_id`（绑定 `target_stream_id`、目标基准锚点与 `message_ids[]`）及重投租约，包裹于 `untrusted_peer` 非指令围栏内输出，并将该批次的提呈事实作为不可变 `notify_receipt` 控制记录写入目标流已提交分片。`offered` 只表示已生成并提交投递提案，不证明剪贴板内容已被复制、外部通道已发送或目标宿主已收到。
  3. **锚点回执落账（`committed_receipt` —— 单一权威定义）：** 仅当目标流后续经 `save / harvest` 或回执导入成功提交包含对应 `batch_id` 及消息级确认的新锚点，且核验该 `batch_id` 确实匹配本目标流已提呈的批次（而非源自非授信工具输出中的伪造回显）后，方可将消息级确认作为不可变 **`notify_receipt`** 控制记录写入**目标流已提交分片（跨流信箱投递与回执的唯一权威）**，并在 `state.sqlite` 更新可重建的 `committed_receipt` 队列/游标投影（支持部分确认、对重复提交的 `message_id` 幂等忽略，以及针对已归档/失效目标流的过期策略）。Coordinator Journal 不保存第二份竞争性的信箱回执权威，仅可按需记录指向目标流 `notify_receipt` 的引用。该状态只证明目标流提交了对应回执，不证明模型已理解消息或同意其内容。

### 7.5 Main 会话权威、子会话派发、失联恢复与多 Main 重建

本节定义 Main 作为规范会话文件的权威边界、子会话派发与恢复关系，以及 Main 规范会话丢失后重新建立独立 Main 的规则。精确 Schema、状态机、事务组合与契约测试由 `docs/specs/session-lifecycle-and-disaster-recovery.md` 定义。

#### 7.5.1 Main 是会话文件，不是 Agent 身份

- **Main 的定义：** Main 是一个具体的 Canonical Session 会话流（`main_stream_id`，其身份仍由普通 `stream_id` 承载），被指定为一个 `thread_id` 协作空间内的最高协调权威。它保存自己已提交的会话演变历史，并负责记录该空间内已知的派发、关键分支操作、子会话回执、成果采纳与协调状态。Main 不等于执行它的 Agent、模型进程或原厂客户端会话文件；这些仅是 Main 规范会话的宿主工作副本/执行入口。
- **一个协作空间一个当前 Main，整个 ThreadPass 可以有多个 Main：** 同一 `thread_id` 内只能有一个当前指定的 `main_stream_id`；不同 Main 使用不同 `thread_id`，彼此独立。不同 Main 可以在显式操作下互相发信、借阅、收编固定来源快照或归并，但默认不存在全局唯一 Main、天然上下级关系或协调权继承；跨 Main 收编不会把来源会话移动到目标 Main 的协作空间。
- **Main 的权威来自规范会话与已提交账本：** Main 的宿主可以离线或更换。受信任 CLI 可代为提交该 Main 已授权的派发、回执与协调控制记录，但这不产生第二份 Main，也不意味着 CLI 自身成为会话。Main 不要求实时复制子会话全文；Main 未收到的信息仍可能只存在于子会话自己的已提交历史中。
- **因果平权：** Main 的协调权不改变通用 DAG 的父边规则。Main 不会仅因其身份而自动成为所有子会话的因果父节点；子会话的父锚点由实际继承的上下文决定。
- **任务标识不是会话身份：** 可选 `work_item_id` 可用于关联围绕同一任务产生的多个会话；每个会话始终有独立 `stream_id`。不要求将每个规范会话解释为某个 Agent attempt，也不要求建立常驻任务调度器。

#### 7.5.2 显式派发与 Dispatch Recovery Capsule

每次由 Main 或获准委派的子会话派生一个或多个子会话，必须先建立可恢复的派发检查点和派发记录。派发按以下逻辑顺序执行：

1. **确定源检查点：** 派发发起会话必须指定一个已发布的 `source_anchor_id`；若需将当前尚未提交的交互纳入派发，可将该检查点作为同一派发事务中的待提交源锚点，但不得引用事务外尚未提交的候选锚点。若无法回收当前宿主增量，只能使用最近一个可靠 `published` 锚点，并在任务信封中声明该检查点之后可能存在未归档交互。
2. **登记派发事实：** 协调层生成唯一 `dispatch_id`，在发起 Main 的协调账本中记录 `main_stream_id`、`thread_id`、派发时局部 `coordinator_epoch`、发起流与源锚点、派发者身份、任务说明、可选 `work_item_id`、每个目标 `stream_id`、目标 `workspace_id`、预期产物及权限/能力约束。子会话发起的嵌套派发必须经过同一协调事务，不得只在 Prompt 中声明而不入账。
3. **生成恢复胶囊：** 为每个目标子会话生成 **Dispatch Recovery Capsule**。胶囊至少包含原始 Main 的 `main_stream_id` / `thread_id`（仅标识这份原 Main 会话，不表示可由新 Main 继承）、派发记录引用与哈希、派发时局部 `coordinator_epoch`、指向 `blobs/objects/` 内容寻址快照的 `coordinator_snapshot_ref`（派发时 Main 已知的全局目标/约束、已知子会话及派发清单、已采纳决策和未决事项的版本化快照）、任务说明及其来源、实际源锚点、必要上下文引用、目标工作区身份、预期产物、允许的工具/权限边界、初始连续性等级和数据完整性声明。协调快照只描述派发时已知状态，须保留来源证据和采集范围，不得伪装成 Main 全部历史。胶囊必须完成脱敏，不允许同步的负载仅保留本机受控引用并标明 `local_only`，严禁把不可扫描二进制或秘密带入子会话；且**归档灾备面与宿主投喂面严格分离**——实际投喂给外部或低权限宿主的 Prompt 仅按本次任务最小必要权限与范围裁剪上下文，不得无差别暴露整份全局协调快照。
4. **建立子会话初始状态：** 每个子会话的初始锚点以其实际继承的源锚点为因果父节点，并把胶囊控制记录写入子会话首个已提交分片。若多个子会话均由同一发起流的同一检查点派生，则它们均以该发起流检查点为因果父节点；Main 的派发记录用于协调恢复，不因此自动成为这些子会话的因果父节点。
5. **多流原子提交后再投喂：** 发起流源检查点（如本次一并提交）、Main 协调账本派发记录、快照 Blob 以及各目标子会话初始锚点与胶囊，须在受 4.1 节多流事务暂存与发布协议保护的同一次提交中全部登记、发布并 `Finalize`，之后才允许输出可执行 Prompt/附件。若任一步失败，不得向目标宿主宣称派发已成立。

派发记录与胶囊是恢复元数据，不要求 Main 实时接收子会话全文。子会话归档及其独立导出灾备包必须携带胶囊与所引用的已获批协调快照 Blob，确保原 Main 本地控制库丢失后不产生悬空引用，并足以证明自身原始派发来源及派发时 Main 已知的必要协调上下文；但它只代表派发时快照，不保证包含此后 Main 新增且未再次传播的状态。

#### 7.5.3 子会话失联、恢复分支与原分支复活

- **先区分宿主失联与规范会话丢失：** 原厂宿主账号、虚拟机或会话文件不可用，不等于 ThreadPass 规范会话丢失。若规范会话及其已提交归档仍在，直接更换宿主或从最后可靠锚点恢复该会话即可；只有规范会话本身不可读取/不可恢复时，才需要创建新的恢复会话。
- **失联不是死亡证明（观测集合与当前状态投影严格分离）：** 将 `session_lifecycle` 观测拆分为两层：① **`Lifecycle Observations`**（不可变观测事件集，含来源证据与因果前驱，满足单调并集保留全部观测历史）；② **`Lifecycle Projection`**（当前宿主可用性状态投影，取值含 `active`、`suspected_unavailable`、`unavailable`、`recovered` 及冲突态 **`concurrent` / `unknown`**）。当前状态本身非单调（可反复往返），必须按可验证因果偏序投影：超时、无响应或到期观察只能更新投影状态，不能自动删除规范会话或封存其历史；当两个离线副本并发写入无因果先后关系的矛盾观测（如同时记录 `active` 与 `unavailable`）时，严禁按墙钟时间选边，必须确定性投影为 `concurrent` / `unknown` 并保留双方观测记录。
- **恢复分支始终新建：** 当 Main 决定从一个失联/不可恢复子会话继续工作时，创建新的 `stream_id`，并记录 `recovery_of = old_stream_id` 及独立 `recovery_id`。不得覆写原会话或复用其原厂宿主会话身份。是否附带 `work_item_id` 仅用于说明任务关联，不是创建恢复分支的前提。
- **因果父节点与恢复输入分离：** 恢复会话的 `anchor.parents[]` 严格遵循 7.1 节关系分离规则，仅指向其真正继承规范状态基础的可靠已发布锚点——若基于原会话最近一个可靠已发布锚点恢复，则指向该已发布锚点；若原会话无可用中间检查点而仅能回退至派发源锚点，则指向该派发源锚点。若恢复时还额外消费了 Main 中新产生的协调记录、Journal 快照或其他分支的摘要/Artifact，这些实际消费的外部输入必须通过 `input_dependency_manifest` 固定记录，而不得自动塞入 `anchor.parents[]` 伪装成多父 Lineage 继承；仅作背景出处引用的材料只记入 `provenance_ref`。`recovery_of` 仅说明恢复目标，不自动决定父节点。
- **部分汇报不能冒充完整检查点：** Main 曾收到的原会话进度摘要、结果或 Artifact 可以作为恢复证据；若恢复宿主实际消费了 Main 中的新信息，该信息必须保留可追溯来源。简短汇报不能证明 Main 已拥有原会话的完整历史，也不能补齐从未归档的宿主尾部。
- **原分支迟到复活允许并存：** 若恢复会话创建后原会话再次上线，原会话与恢复会话均保留为独立规范会话，可从各自最后可靠锚点继续提交；不因恢复会话存在自动冻结原会话，也不因原会话复活自动撤销恢复会话。二者可以保留共同任务或恢复来源关联，但创建时间、恢复代数或展示名称不构成成果优先级。由其所属 Main 通过回执与成果核对决定继续、停止、采纳或归并哪些成果。
- **工作区隔离：** 并存会话不得无锁地并发写入同一可变工作区。应使用独立 `workspace_id` / Git worktree，或通过受控的工作区写租约串行化；会话流的 CAS 不能替代文件工作区的并发保护。
- **外部副作用核对：** 恢复执行前必须盘点可观察的 Commit、PR、上传物、数据库变更及其他外部副作用，避免原会话已完成操作但未回执时被恢复会话重复执行。系统应优先核对已有成果，再决定补交回执、继续执行或重新执行；不能承诺对不可观测副作用实现 exactly-once。

#### 7.5.4 Main 规范会话丢失、野生收编与新 Main 建立

必须严格区分“Main 宿主副本丢失”和“Main 规范会话丢失”。只有后者才触发建立新 Main 的流程：

1. **仅 Main 宿主副本丢失：** 若原 Main 的规范会话流及其权威归档仍可访问，换用新的宿主继续同一 `main_stream_id`。这不是新建 Main，也不改变该协作空间的权威身份。
2. **Main 规范会话/本地控制库暂时不可用，但可从原样备份验证恢复：** 先通过 `fsck` 验证和重建原归档/索引，恢复原 `thread_id` 与 `main_stream_id`。只有能够验证恢复数据确实属于原 Main，才视为原 Main 恢复；不能仅凭名称、摘要或未经验证的副本认领原 Main 身份。
3. **原 Main 规范会话及其权威归档无法恢复，但一个或多个子会话仍存活：** 幸存子会话相对于新协作空间一律按**野生来源**处理。它们保留自己的原始 `thread_id`、`stream_id`、历史和来源证明，不自动继承旧 Main 的最高协调权，也不因旧 Main 丢失而被改写为新 Main 的子会话。用户或受信任 CLI 显式创建一个全新的 `thread_id` 与 `main_stream_id`，将选定的幸存子会话/锚点作为野生来源执行 `adopt`，建立新的 Main 规范会话及新的协调账本。新 Main 以自己的独立根锚点开始，并将被收编来源固定写入 `adopted_source_manifest`；新 Main 仅对新协作空间及其之后明确派生/纳入的分支拥有权威，不直接取得原来源会话的写入控制权。
4. **野生收编的谱系边界：** 新 Main 必须保留每个被收编来源的原始 `project_id` / `thread_id` / `stream_id` / `anchor_id`、可验证祖先清单、事件/Blob 引用和数据完整性等级。来源之间是否存在共同祖先，只能依据实际可验证的 DAG 证据判断；不能因为它们曾经隶属于同一个旧 Main，就假定旧 Main 的完整历史仍然存在。若祖先内容缺失，必须标记 `ancestry_completeness = partial` / `unknown`，不得伪造 LCA 或完整祖先闭包。新 Main 以自己的独立 `adopt` 根锚点开始（不凭旧 Main 的缺失记录伪造 `anchor.parents[]`），并在不可变 `adopted_source_manifest` 中记录被收编来源的完整标识（`project_id` / `thread_id` / `stream_id` / `anchor_id`）、固定锚点引用与内容清单哈希；这些来源引用是溯源/初始上下文关系，不是新 Main 对原来源会话的所有权，也不自动构成因果父边。新 Main 的后续子会话可以在明确引用这些已收编快照后继续工作，但原来源会话仍留在其原协作空间。野生 `adopt` 本身不表示新 Main 继承了旧 Main 的权威。
5. **收编来源必须固定到检查点：** `adopted_source_manifest` 必须 pin 到明确的 `source_anchor_id` 与内容清单/哈希，不得使用“跟随源会话最新头”的可变引用。为使新 Main 可独立恢复，所需的已获准不可变事件/Blob 内容必须物化到新 Main 可访问的内容寻址归档，或包含在可验证的自包含灾备包中；不得只保存指向可能消失的旧设备路径。原来源会话此后继续推进，不会静默改变新 Main 已收编的上下文；若新 Main 需要纳入后续成果，必须显式再次 `adopt`、导入或执行经验证的归并，并产生新的审计记录。原来源会话保持原 `thread_id` / `stream_id`，不因被收编而移动、改名或改写历史。若要在新 Main 的协作空间继续开展工作，应从新 Main 已提交的收编上下文派生新的子会话，而不是直接把原来源会话改挂到新 Main 名下。
6. **旧 Main 后来恢复：** 若原 Main 的规范会话及可验证归档后来重新出现，它恢复的是原 `thread_id` / `main_stream_id` 对应的旧 Main。先前新建的 Main 保持自己的新身份与独立权威；新旧 Main 各算各的，不发生自动继任、撤销、覆盖、合并或协调权争夺。双方可以记录 `reconstructed_from` / `origin_main_ref` 等非权威来源关系，也可以在用户明确操作下通过跨 Main 消息、只读借阅、再次 `adopt` 或显式重协调（如用户层的 `squash-merge`）建立联系；这些关系不会自动把一个 Main 变成另一个 Main 的上级或后继。
7. **多个 Main 并存是正式支持的状态：** 同一 `project_id` 下可以存在多个互相独立的 `thread_id` / Main。它们可以有共同历史来源，也可以完全无关；每个 Main 独立维护自己的规范历史、派发账本和协调状态。跨 Main 操作必须显式指定来源与目标，不得因历史关联自动共享控制状态或提升权限。
8. **不可恢复边界：** 若 Main 在失联前没有把某项指令、决策或任务分配传播到任何幸存子会话、不可变归档或独立备份，则新 Main 无法推导该信息。Dispatch Recovery Capsule 仅能证明其生成时的派发来源与协调快照，不能恢复此后从未留存的 Main 新状态。若所有规范归档、宿主副本、子会话副本和独立备份均已丢失，则不存在可保证的恢复路径。

#### 7.5.5 Main 身份边界验收场景

以下场景是生命周期子系统必须通过的契约测试，不得仅以实现说明替代：

| 场景 | 必须结果 |
| --- | --- |
| Main 原厂宿主副本丢失，但规范 Main 会话及归档仍可验证 | 沿用原 `thread_id` / `main_stream_id`；不得创建新 Main。 |
| Main 规范归档暂时丢失，但从可信备份完整恢复 | 恢复原 Main 身份与历史；不得将其视为新 Main。 |
| Main 规范会话不可恢复，只有一个或多个子会话幸存 | 幸存会话作为固定检查点的野生来源显式 `adopt`；创建新 `thread_id` / `main_stream_id`，不继承旧 Main 身份或协调权。 |
| 新 Main 建立后，旧 Main 的规范归档又被找回 | 旧 Main 与新 Main 同时存在，各自独立；不得自动撤销、覆盖、继任或合并。 |
| 被新 Main 收编的来源会话后来产生新锚点 | 新 Main 的既有收编快照不自动变化；必须显式再次收编/导入/归并。 |
| 同一 Main 的数据目录被复制到两台设备并离线产生分歧 | 识别为同一 Main 的分歧副本；不得仅因 epoch 相同或不同就自动认定为两个独立 Main。 |
| 用户明确从现有会话另建一个独立 Main | 创建新的 `thread_id` / `main_stream_id`，固定记录来源引用；两个 Main 不共享协调账本。 |
| 未获派发授权的子会话试图创建正式子流 | 拒绝正式派发；不得仅凭其 Lineage 后代身份授权。 |
| 真人用户通过受信任 CLI 以当前 Main 协调者身份从子流检查点发起分叉 | 按 Main 自身协调权威记账并提交派发，无需先向该子流签发 `dispatch_grant`。 |
| 获准再委派的子会话试图扩大权限范围或超额复制配额 | 拒绝超出上游授权范围或剩余配额余额的子授权（再委派配额须从父余额划拨扣减而非复制放大），并记录审计事件；离线多设备超额派生在对账时保守拒绝超额部分自动升格。 |
| 授权撤销后，在线子会话再次派发 | Journal 校验发现不可逆撤销后拒绝新正式派发（重新授权须签发新 `grant_id`）；已有子流与既有历史保留。 |
| 子会话离线期间未获知撤销，且与在线撤销并发产生派发 | 不宣称撤销即时全局可见，亦不得按墙钟判定先后；对账时保守拒绝自动升格为正式 Main 派发（fail-closed），保留产物为待核对/野生工作直至 Main 或真人显式追认/收编。 |
| 同一 `thread_id` 内试图把既有活流再次当野生来源 `adopt` | 拒绝重复野生收编，引导改用 `save / harvest / fork / reconciliation`，防止 `host_session_id` 双绑与拓扑污染。 |
| 跨空间以原厂导出/宿主方式误选本机另一 `thread_id` 的正常会话 | 入库前反查命中后提示升级为 `foreign_canonical` 固定收编，保留原身份与信任级，绝不抢占原流活跃 `host_session_id` 绑定。 |
| Main 对话流丢失但 Coordinator Journal 与独立副本完整 | 可重建只读历史协调视图与灾备恢复输入（派发/授权/采纳事实），原 `thread_id` 不再以原 Main 名义继续签发新派发；继续协同须建新 Main 并显式重建授权，且不得声称恢复 Journal 未记录的 Main 对话。 |
| 只有本机 Journal，没有独立故障域副本 | 状态只能标记 `local_committed` / `local_only`，不得标记 `disaster_redundant`。 |
| 两个离线设备并发追加纯单调 Journal 事件（如互不相交的 `result_receipt_recorded` / `Lifecycle Observations` 追加 / 不可逆 `grant_revoked`） | 不可变日志保留全部原始分片与真实多前驱，观测集与纯单调事实投影按单调半格规则确定性并合，不误报人工假冲突。 |
| 两个离线设备对同一产物或同一协调目标提交非单调互斥裁决（如同时 `accepted` 与 `rejected`/`abandoned`） | 保留全部并发日志头并强制阻断，要求显式人工/Main 对账裁决；不得用最大 epoch 或墙钟时间自动裁决。 |
| 同一 `thread_id` 内出现离线并发提交指向不同 `main_stream_id` 的 `main_designated` 记录 | 判定违反创世唯一性不变量并强制阻断 `thp fsck` 对账，禁止按墙钟或 epoch 覆盖既有 Main。 |
| 两个离线设备并发记录无因果先后关系的矛盾 `Lifecycle Observations`（如同时写 `active` 与 `unavailable`） | 保留全部原始观测事件，`Lifecycle Projection` 确定性投影为 `concurrent` / `unknown`，严禁按墙钟时间选边。 |
| 跨设备 `thp fsck` 遇无有效签名、缺证残缺、跨事务断链或仅缺预期未同步的 `local_only` 负载 | 签名未认证/已撤销隔离为 `untrusted_unverified_txn`，缺证或 `approved` 对象残缺隔离为 `incomplete_unverified_txn`，跨事务祖先/前驱/必需依赖断链隔离为 `broken_dependency_closure`（均严禁激活流头）；预期缺 `local_only` / `unscannable_binary` 允许文本恢复但投影标注 `partial_blobs_local_only`。 |
| 两个来源锚点有共同祖先，但各自语义变化冲突 | 祖先集合分析不能直接宣布可合并；生成带冲突证据的 `reconciliation_record`。 |
| 新会话只引用来源作为证据而没有消费来源内容 | 只记录 Provenance；不得把来源伪装为 Input Dependency 或 Lineage 父节点。 |
| 新会话实际消费外部来源内容但未继承整个来源状态 | 记录固定 Input Dependency；不得自动把整个来源锚点添加为 Lineage 父节点。 |

#### 7.5.6 Main 局部协调写入、离线副本分歧与跨 Main 独立性

- `coordinator_epoch` 只在某一个具体 `main_stream_id` 对应的规范控制库内使用，用于 CAS/fencing，拒绝同一 Main 的陈旧进程或过期租约继续写入；它不是跨 Main 的继任序号，不得从旧 Main 复制到新 Main 后据此推断权威继承。
- 同一 Main 的数据目录被复制到多个设备并离线推进时，仍属于同一个 Main 身份的分歧副本，而不是自动产生多个独立 Main。离线同步后的 `fsck` 必须识别并存协调头、DAG 头和控制事件分歧；没有在线共识服务时不得仅凭 epoch 数值或墙钟时间裁决。用户明确决定分裂为多个独立 Main 时，须为每个新 Main 创建新的 `thread_id` / `main_stream_id`，并通过非权威来源引用保留共同来源。
- 不同 Main 各自拥有独立的协调权与账本。某个 Main 的 epoch、派发状态、成果采纳或权限不能自动写入或约束另一个 Main。跨 Main 的消息、来源引用、adopt 和 merge 均属于显式跨空间操作。
- Main 的最高协调权不表示它可以静默裁定技术事实。成果采纳须保留来源和冲突证据；涉及用户权限或不可逆副作用时仍须遵循人类授权门禁。


#### 7.5.7 派发权限委派、再委派与撤销

派发权限是独立于会话谱系的控制授权，不因 `fork`、Lineage 父边、`recovery_of`、`adopted_source_manifest`、任务归属或宿主迁移而自动继承。

- **发起主体边界：** 必须区分“子会话自身（或其宿主 Agent）发起的嵌套派发”与“真人用户经受信任 CLI 直接以当前 Main 协调者身份从子流已发布检查点发起的分叉”。前者必须持有仍有效且范围匹配的 `dispatch_grant`；后者行使的是 Main 本身在当前协作空间内的协调权威，在 Journal 中明确记为 Main 发起的派发，不需要先给该子流签发 `dispatch_grant`，也不会由此把派发权授予该子流。
- **初始授权与 Escrow 有界配额：** Main 依据协作空间策略向指定 `stream_id` 授予明确的派发权限。授权必须以不可变 `dispatch_grant` 事件写入 Coordinator Journal，并绑定 `grant_id`、签发者、被授权会话、允许的操作类型、任务/项目/工作区范围、有效条件、有界派生配额上限（如 `max_total_spawns`）、最大再委派深度及是否允许再委派。授权范围与配额不得写成无边界的“可任意派发”。
- **权限最小化与配额切分守恒（Escrow Split）：** 被授权子会话只能在授权范围与剩余配额内执行正式派发。若允许再委派，它只能签发不超过自身剩余权限与剩余配额的子授权：操作集合、范围、有效期和深度均不得扩大，且**划拨给下游的派生配额必须从自身剩余余额中实扣（Transfer/Split），严禁按节点数复制放大以防多层再委派引发子流爆炸（Fork Bomb）**；子授权必须引用上游授权形成可验证授权链。默认 `can_redelegate = false`。若同一授权持有人在离线多设备分叉副本上并发透支同一份配额，同步对账时须识别超额并按 fail-closed 拒绝超额派发自动升格。
- **权限不随历史继承：** 从 A 的检查点派生 B，不代表 B 继承 A 的派发权限；B 恢复为 B′、B 被新 Main 收编或 B 迁移宿主，也不自动转移/复制原授权。需要继续派发时，必须由当前有效授权链明确覆盖 B′ 或新会话身份。
- **不可逆撤销语义：** 撤销必须追加 `grant_revocation` Journal 事件，指向被撤销授权及其影响范围；**撤销一经提交永久不可逆（不得对同一 `grant_id` 执行反撤销，恢复权限必须签发全新 `grant_id`）**，从而使撤销集合天然满足单调半格语义。撤销阻止其后在权威协调日志中提交的新正式派发；仅依赖被撤销上游授权的下游子授权同时失效，拥有独立有效授权来源的主体不受该链路撤销影响。已经提交的子流、已执行工作和历史回执不因撤销自动删除。对离线主体不存在瞬时全局撤销保证：主体只有在获知并验证撤销事件后才能确定授权已失效。
- **离线与失联边界：** 无法确认当前权威 Journal 头或授权链状态时，不得把基于陈旧快照产生的派发宣称为已获 Main 正式提交。可在本地保存为待核对/野生工作，但在协调日志对账前不得自动升级为正式派发，也不得据此继续向下游授予新的 Main 权限。当离线派发与在线 `grant_revocation` 并发且因缺少全局可信墙钟而无法判定绝对先后时，对账须采取保守闭环原则（fail-closed）：拒绝自动将其升格为正式派发，但完整保留其产物为待核对/野生工作，仅在当前 Main 或真人显式事后追认或收编后方可纳入正式协调状态。
- **授权与真人权限分离：** Main 的协调委派仅决定谁能在协作空间内创建/登记子流，不替代用户对外部副作用、凭证访问、文件写入或不可逆操作的授权。宿主工具权限仍须按最小权限独立检查。

#### 7.5.8 Coordinator Journal 与 Main 协调状态灾难恢复

Coordinator Journal 是协作空间级的不可变协调事实日志，与 Main 的对话历史、子会话历史和 SQLite 可变索引分离。它不是另一份 Main，也不是 Agent 调度器。

- **日志范围：** 至少记录 `main_designated`（协作空间不可变创世指定唯一 `main_stream_id`）、`dispatch_committed`、`grant_issued`、`grant_revoked`、`result_receipt_recorded`、`result_accepted` / `result_rejected`、`branch_abandoned`、`recovery_recorded`、`adoption_committed`、`reconciliation_committed` 以及可选建议性 `baton_handoff`（写者接力棒 `Advisory Hint`）与指向目标流观测的引用等改变协作控制状态的事实；`notify_receipt` 与原始 `session_lifecycle`（`Lifecycle Observations`）唯一权威均留存于目标流 `history/committed/<target>/`，Journal 只保存引用而不双写原始权威。普通对话、模型思考和未影响协作状态的进度文本不必进入 Journal。
- **单调与非单调事件分层（CALM + 唯一性分类）：** Journal 事件与关联状态在语义上明确区分三类：① **逻辑单调/可交换事实集合**（按唯一 ID 入集的 `result_receipt_recorded`、目标流不可变观测事实集 `Lifecycle Observations`、目标流内按 `(batch_id, message_id)` 单调跃迁的 `notify_receipt`，以及不可逆墓碑 `grant_revoked`）；② **非单调状态视图投影**（如 `Lifecycle Projection`：观测集单调并合，但当前状态非单调，无因果先后的并发矛盾观测投影为 `concurrent` / `unknown`，严禁按墙钟选边）；③ **非单调唯一性与互斥裁决**（同一 `thread_id` 内 `main_designated` 创世唯一性——正式成立后禁止在原 `thread_id` 内更换 `main_stream_id`，并发冲突必阻断；同一产物/任务的 `result_accepted` / `result_rejected` / `branch_abandoned`；配额扣减/再委派；并发撤销与离线派发；`reconciliation_committed`）。
- **事件结构：** 每条记录须具备稳定 `journal_event_id`、所属 `thread_id` / `main_stream_id`、事件类型、发起主体与授权链引用、涉及的会话/锚点/任务标识、因果前驱事件引用、必要负载或 Blob 哈希、事务提交引用及 Schema 版本。Journal 采用事务级不可变分片（`<seq>_<txn_id>.jsonl`）持久化而非单文件追加；离线副本可产生分叉日志头，因果前驱不可验证时不得用墙钟时间补造顺序。
- **事务闭环：** 派发、授权、撤销、采纳、放弃、恢复、收编和重协调等关键操作，须将 Journal 事件、所需负载/Blob、受影响会话事件、控制索引及经 `PublishImmutable` 落盘的 `Transaction Manifest` / `Finalization Certificate`（含 `commit_intent_proof` 签名声明与 `publication_closure_proof`）置于同一证书即提交可恢复协议。任何目标 Prompt 释放前，正式派发事实及其 Dispatch Recovery Capsule 必须已由 `.finalized.json` 在本地权威日志中提交并可验证。
- **双层持久性声明与投喂门禁：** `local_committed` 表示本机事务域内已持久化；`disaster_redundant` 表示 Journal 事件及其必要引用已在独立故障域副本完成校验。若策略要求独立副本确认后才允许正式派发，该门禁须作为 `Finalize` 之后的外部投喂门禁（Release Gate）依据 `durability_state` 拦截 Prompt 释放，不得将外部副本校验塞入 SQLite 写锁或阻塞本地流写入屏障；若允许本地提交后先行工作，界面与导出清单必须保留 `local_only` 状态，不能承诺设备丢失后仍可恢复。
- **重建边界：** Journal 可重建只读历史协调视图、派发关系、授权状态、回执/采纳状态及已记录的决策事实；它不能重建从未写入 Journal 的指令，也不能替代 Main 的会话对话历史。若 Main 会话流不可恢复但 Journal 完整，原 `thread_id` 不得继续以原 Main 名义签发新派发；新 Main 可将已验证的 Journal 快照作为固定恢复输入，按新 Main 身份规则建立新的 `thread_id` / `main_stream_id` 并显式重建所需授权，这不构成旧 Main 会话的无损复活或协调权继承。
- **多副本冲突与单调投影并合：** 多设备离线追加造成多个 Journal 头时，**不可变日志层永远保留全部原始分片与真实多前驱**，不得改写或丢弃任一分支。若并发分支仅包含互不冲突的纯单调事件集，`thp fsck` 可在 SQLite 派生投影层按确定性单调半格规则无冲突并合事实集，并将矛盾生命周期观测投影为 `concurrent` / `unknown`（后续新 Journal 事件显式链接全部并列头作为因果前驱）；只要涉及同一 `thread_id` 的冲突 `main_designated`、非单调互斥裁决、配额超发或并发撤销撞车，必须保留冲突并强制阻断，要求显式人工/Main 对账，严禁按墙钟时间、文件名 `<seq>`、最大 epoch、Git 冲突解决结果或文件覆盖先后自动选出唯一全局胜者。`coordinator_epoch` 只在单个规范控制库内提供局部 `Fencing Token`，不是跨设备共识协议。
- **独立灾备：** Journal 与其必要 Blob/胶囊须纳入独立灾备包和显式同步白名单。灾备副本必须通过清单、哈希、引用闭包与恢复演练验证；只复制 SQLite 数据库或只保存 Main 的摘要，不满足该要求。

## 8. 系统不变量

后续演进须始终满足以下不变量：

- `Pure Core ← Pluggable Profile ← Application Shell` 强制保持三层单向解耦：`Pure Core` 严格限定为业务无关、面向事件型历史可追溯协同的状态与协调内核（不写死物理部署偏向，微秒级共享内存目录等紧耦合系统不共用此代码运行时）；默认 `Local-Offline Profile` 的无守护进程与离线约束封装在 Profile 端口实现内部，同时开放 `Shared-Log` / `Online-Consensus` 同构可插拔 Profile 与 `Universal Distributed Systems Model` 同源的 `Low-Latency Directory` 演进方向。
- 严守分布式理论术语与副作用约束边界：CAP 仅针对分区期线性一致性与可用性权衡，正常连通期延迟与一致性权衡归属 PACELC；CALM 单调逻辑可在满足前提时实现协调自由的汇合计算（Confluent Computation），采用 CRDT 实现时须另行验证强最终一致（SEC）契约，均不等同于 CAP 线性一致性；仅靠追加日志无法自动满足 `main_designated` 唯一性或配额守恒等非单调不变量。仅当离线操作维持 `provisional` 待定状态、不产生不可逆副作用或具备经验证的幂等/补偿机制时才允许事后显式裁决；不可补偿的唯一性、资源守恒与不可逆副作用须在执行前经事前协调、`Escrow` 或目标资源原子条件检查守住不变量。
- 多时间预算执行路径的组合必须遵守**可见性与释放栅栏**、**`Fencing Token`**（仅在被保护资源实际校验时生效；`Local-Offline Profile` 须以 `PublishImmutable` 与 SQLite CAS 共同闭合文件系统防覆写）、**`Lease`**（携带 `clock_domain_id` 与 `boot_epoch`；挂起恢复时钟连续性不可判定时保守降级为 `unknown` 并回库复验，严禁跨重启比较或升级为跨离线设备全局租约）、**`Advisory Hint`**（`baton_handoff` 仅作防呆提示）以及**跨路径失步诚实状态契约**；且须通过**Core DST + 状态空间探索**、**`Profile Conformance` 端口一致性契约测试**与**真实集成故障注入**三层验证门禁。
- ThreadPass 默认以**统一用户粘贴 Prompt 往返闭环**作为跨异构/隔离宿主的通用一等交互基线（分片投喂中的“等待全部分片收齐后再开始执行”属交互协议约定，不替代模型外部执行权限边界），并可配套按需唤起、用完即退的本机临时 Web 壳 **`thp web`**（强制仅监听 `127.0.0.1`、一次性高熵会话 Token、严格校验 `Host`/`Origin`、绝不驻留守护进程且无独立权威，与 CLI 保持全量功能齐平）；信封顶部 `Current Role Briefing` 负责向模型重框定当前认知角色并覆盖历史过时自称，但模型认知身份不授予任何内核权威或派发权；宿主内部瞬态子 Agent 仅属本流工具细节而非跨流正式派发；可选只读旁路采集外挂须严守“只旁听不插嘴、只读不抢锁、事件驱动低频增量同步”三原则，且与 `Native Resume` 同属非核心前提的可选增强。
- 在默认 `Local-Offline Profile` 中，采用**证书即提交（Certificate-as-Commit）**事务语义：`history/committed/ + blobs/objects/ + transactions/committed/ + coordination/journal/committed/` 连同经 `PublishImmutable` 落盘的 `Transaction Finalization Certificate`（含经 `trusted_device_keys` 验签或单机标注的 `commit_intent_proof` 签名声明 `Signed Attestation` 与 `publication_closure_proof`）构成不可变归档与跨设备最终提交权威；`trusted_device_keys` 采用 Ed25519 带外显式钉入、按 `project_id` 隔离作用域并区分 `active / retired / revoked`（历史防线依赖已验证检查点钉死）；`state.sqlite` 活跃头、`lineage.json` 与 `bundles/` 均属可由上述不可变记录与证书确定性重建的本地并发控制/视图投影，原厂客户端会话仅作工作副本。
- 禁止多进程向同一活跃 JSONL 分片并发追加；默认 `Local-Offline Profile` 下所有 `Publish` 写入均须执行 **`PublishImmutable`**（同文件系统临时文件 + `fsync` + `no-replace` `fs.link` + 遇 `EEXIST` 内容不匹配 `fail-closed` 隔离），且完整事务遵循「`Reserve`（带 `clock_domain_id`/`boot_epoch` 与 `fencing_epoch`）→ `staging` 暂存（含 `transactions/staging/<txn_id>.manifest.json`）→ `Commit Intent` 生成 `commit_intent_proof` → `PublishImmutable` 发布对象与 `.manifest.json` → SQLite 写锁内复验 `owner_token`/`fencing_epoch` 并 `fs.link` 发布 `.finalized.json` 成为最终提交事实 → 同事务原子更新 SQLite 投影头与 `txn_log = 'published'`」协议。`thp fsck` 须完成四步验证与跨事务依赖闭包校验，异常分别隔离为 `untrusted_unverified_txn`、`incomplete_unverified_txn` 或 `broken_dependency_closure`（均严禁激活流头），预期缺 `local_only` / `unscannable_binary` 负载诚实标注 `partial_blobs_local_only`。
- Blob 孤儿回收必须同时检查已发布引用、待发布事务引用与有效准备租约；只有超过安全宽限期且经 SQLite 协调确认无引用的对象方可回收，严禁按文件系统扫描结果直接删除。
- IR 严格分离不可变客观事件（`event`）与单次模型调用视图（`invocation`）；输入上下文按 `event_id` 引用，并分别声明事件覆盖完整度（`input_scope`）与输入投影精度（`projection_fidelity`），不得把精确事件列表等同于精确模型请求重建；不可观测的原厂系统提示词、隐藏工具、截断与投影变换必须诚实标注，严禁伪造。
- 信任分级不得信任导入内容或模型输出的自声明字段；`init` / `adopt` / `ingest` / `reconcile` / `fork` / `mailbox` 未经确切内容确认凭证均不授予真人权威，`ingestReply` 不接受调用方自报 `verificationMethod`；真人授权须由受控来源证明或与确切内容哈希绑定的一次性人类确认凭证（同时经 Thread 级与设备级 `confirmation-nonces.sqlite` 原子防重放）核实。`human_authority` 只表示来源/授权指令的可验证等级，不替代具体副作用操作所需的权限检查与二次确认；**非指令参考围栏与信任分级标签仅用于上下文层的来源区分与参考隔离，绝不构成执行安全边界或沙箱，文件读写、凭证访问、网络外发与不可逆操作权限必须由模型之外的受控执行权限层强制实施**。
- 交接包必须诚实标定连续性等级（`Native Resume` / `Prompt Reconstruction` / `Evidence Recovery`）与交接单类型（`model_authored` 在已纳管流须引用信封中已提供的 `anchor_id / event_id`，首次野生 `unmanaged_external` 粘贴收编则改用原文关键片段摘录与轮次定位证据且不得要求模型捏造 ID，回存入库默认定级 `untrusted_peer` / `unverified_import`；`evidence_based` 须显式标注未总结现场恢复）。
- 预算裁决严守**「目标宿主有效可用总预算上限优先于五要素信息完整性下限」**铁律，显式区分总上下文上限与单次输入/附件上限，标注预算置信度；分片投喂仅解单次输入限制，不得借分片突破总有效预算，半截中断的分片投喂不得伪装成完整交接或收编状态，超出核心容忍极限时闭环拒收。
- Lineage、Input Dependency 与 Provenance 必须采用独立关系语义；`anchor.parents[]` 只表示规范状态继承。祖先集合运算只能用于可验证的谱系预分析，不自动构成语义归并算法；所有多来源重协调必须有不可变 `reconciliation_record`，精确保留输入、采纳/排除范围、因果依赖、冲突、裁决依据与输出锚点。不得凭墙钟时间、文件名 `<seq>` 或任意确定性排序臆造因果。
- `project_id` 与 `workspace_id` 解耦分离；Git 为可选增强工具，锚点关联区分 `git_repo / plain_dir / none` 并诚实声明采集边界的 `Workspace State Manifest` 及可选恢复补丁 `Artifact`，跨工作区存在未提交脏改动时强制首屏告警。同一原厂 `host_session_id` 同一时刻仅允许作为单一规范 `stream_id` 的活跃工作副本绑定；`adopt` 区分 `unmanaged_external` 与 `foreign_canonical` 并在入库前执行三层反查，防止同空间重复野生收编与跨空间误降级。
- 跨流信箱遵循 `pending → offered → committed_receipt` 至少一次投递与幂等回执契约；`batch_id` 须高熵且绑定目标流与消息批次以防伪造回显；消息正文归源流 `cross_stream_message`，批次与回执唯一权威为目标流不可变 `notify_receipt` 控制记录，Coordinator Journal 不双写竞争权威，SQLite 队列状态仅据其投影重建；在默认 `Local-Offline Profile` 下严禁引入常驻后台守护进程，且全配置档均禁止共享可变宿主文件。
- 跨平台独占目录权限（POSIX `0700` / Windows `icacls`）、入站跨缓冲区滑动窗口内存脱敏与出站复核门禁（Outbound Egress Gate）闭环生效；超长负载与跨空间收编物化快照包存入目标 `blobs/objects/`，不可扫描二进制默认隔离禁外发；事后漏脱敏须走显式审计墓碑化/重打包清理，禁止静默篡改不可变归档。
- 通用 Prompt 注入为全平台语义基线；所有适配器暴露五维 `Host Descriptor`，原生写回必须通过版本、预算与运行态锁冲突检测门禁，否则安全回退。
- 离线备份与同步协议介质无关（Git 仅是推荐载体之一）并采用显式白名单：允许同步 **`history/committed/` + `coordination/journal/committed/` + `transactions/committed/`（经校验的 `.manifest.json` 与 `.finalized.json`）+ 经脱敏/授权且被已终结清单引用的 `blobs/objects/`**；明确排除 SQLite/WAL/SHM、`transactions/staging/`、`coordination/journal/staging/`、`history/staging/`、`blobs/staging/`、派生视图、运行锁、本机签名私钥与本机路径映射。`thp fsck` 须完成四步验证（`trusted_device_keys` 验签、清单匹配、对象完整性及跨事务历史依赖闭包），据已验证分片和获准 Blob 重建本地状态并识别并发头；可选建议性写者接力棒提示（`active_writer_hint` / `baton_handoff`）与可选外部原子 CAS 适配器仅用于防误写和联网期减少分叉，绝不取代底层离线多头对账安全网。
- Main 是某个 `thread_id` 内被指定的具体规范会话流（`main_stream_id`），不是跨灾难恢复永久不变的抽象 Agent 身份；每个 Main 仅对自己的协作空间拥有最高协调权，ThreadPass 支持多个彼此独立但可显式关联的 Main。
- Main 规范会话不可恢复时，幸存子会话按野生来源显式 `adopt`，以新 `thread_id` / `main_stream_id` 创建独立 Main，并将来源 pin 到不可变检查点与内容清单；收编来源不移动、不改写、不跟随更新，来源内容须在新 Main 的存储或自包含灾备包中可验证可用。旧 Main 若后来恢复，则与新 Main 独立并存，只有显式跨 Main 操作才能建立新联系。
- Coordinator Journal 独立于 Main 对话历史、采用事务级不可变分片保存关键协调事实（含 `main_designated`），并按 CALM 原则区分纯单调事实与非单调互斥裁决；SQLite 协调索引须可由 Journal 重建。离线多头下不可变分片与多前驱永远保留，仅纯单调事实允许在派生投影层无冲突并合，非单调冲突须显式对账。每次正式派发须先在同一事务中完成源检查点、Journal 派发事件、目标子会话身份、内容寻址快照 Blob（附带持久化 `blob_manifest`）及 Dispatch Recovery Capsule 的提交、发布与 `Finalize`，且胶囊归档面与宿主 Prompt 投喂面严格分离。独立灾备状态（`disaster_redundant`）须经异地/异介质副本校验，作为 `Finalize` 之后的投喂门禁（Release Gate）而非本地 SQLite 写锁的一部分；Dispatch Recovery Capsule 只覆盖派发时快照，全部恢复来源消失时不得承诺恢复。`coordinator_epoch` 仅在单一 Main 的本地规范控制库内提供 fencing，不得以 epoch 数值冒充跨设备共识或跨 Main 继任关系。
- 每个规范会话以自己的 `stream_id` 和锚点历史为身份基础；可选 `work_item_id` 只用于关联相关会话。恢复会话通过 `recovery_of` / `recovery_id` 关联来源会话，该关系不等于因果父边或身份继任。`anchor.parents[]` 只表示实际继承的已提交状态基础；恢复时额外消费的 Main 摘要、Journal 快照或 Artifact 须通过 `input_dependency_manifest` 明确记录，未归档信息不得声称可恢复。
- 子会话宿主失联仅更新宿主可用性观察，不证明规范会话永久丢失；规范会话不可恢复时创建新的恢复会话并保留原分支。原分支迟到复活时允许与恢复分支并存，成果由其所属 Main 核对/采纳，不按代际或时间自动择优；并存会话的可变工作区必须隔离或受写租约保护。
- 子流自身发起的嵌套派发必须通过显式、范围与有界配额受限、不可逆可撤销的授权链授予（默认禁止再委派；再委派配额须从父余额实扣切分，防止子流爆炸），区别于真人通过受信任 CLI 直接以 Main 协调者身份从子流检查点发起的分叉；历史继承、宿主迁移、恢复和收编均不自动传递权限，离线撤销与离线超额派生在对账时遵循 fail-closed 保守裁决。
- Main 的协调权威不等于真人操作授权；Journal 中的协调委派不得替代外部副作用、凭证与工作区访问的独立权限检查。
- 所有持久化记录携带 `schema_version`，未知扩展经 `unknown` 保真留存且默认非授信。

## 9. 与外部参考设计的关系

### 9.1 与早期无损侧车（Sidecar）直转架构的关系
早期点对点迁移方案强调字节级原样保留源端全部原始帧并直接伪造目标本地存储。该思路贡献了严格的路径身份校验、覆盖保护门禁与原子落盘纪律；但其全量搬运行为易诱发目标窗口溢出，缺乏未提交工作区状态感知与来源信任核验，难以覆盖隔离远端沙盒与网页/移动端，亦不支持多分支因果协同。本基线保留其路径身份与覆盖保护纪律，同时将内核升级为独立于 Agent Runtime 的事务型会话状态与因果历史管理层。

### 9.2 与隔离执行（Router / Worker）参考架构的关系
隔离执行架构证明了将「全局协调上下文」与「隔离试错执行会话」分离可有效消除底层工具噪音对核心决策上下文的污染，但该范式通常绑定单一自研运行时且受限于固定两层主从树。本基线吸收其「主干结论与底层试错噪音隔离」的工程纪律，同时解除单一运行时与固定主从层级限制，泛化为支持无共同祖先多源收编、任意层级裂变、显式多来源重协调及三态异步信箱的通用协同架构，并在单流日常使用中隐藏底层图复杂度。


### 9.3 因果历史、补丁演算与操作日志的候选参考（非规范性）

以下资料只作为理论与工程参照，不构成 ThreadPass 的运行时依赖、数据格式承诺或最终归并算法选择：

- **Pijul Theory：** <https://pijul.org/manual/theory.html>。重点研究变化依赖如何由编辑上下文产生，以及变化标识如何与文本结构关联。可借鉴的问题是“变化依赖”和“变化应用顺序”如何被显式表达；其文本/文件数据模型不能直接证明会话语义兼容。
- **Pijul Conflicts / CRDT 讨论：** <https://pijul.org/manual/theory>。重点研究冲突如何作为数据结构中的可见状态保留，而不是在合并时静默选边。其 CRDT 性质依赖具体数据结构与操作定义，不能推广为任意自然语言结论都可自动合并。
- **Darcs Patch Mergers：** <https://darcs.net/Theory/MergersDocumentation>。重点研究 Patch Commutation 与 Merge 的关系；该文档也明确指出“同一变化”的语义定义存在含糊之处，因此应同时研究其理论边界，而非仅借用术语。
- **Jujutsu Operation Log：** <https://www.jj-vcs.dev/v0.26.0/operation-log/>。重点参考内容状态视图与操作历史分离、操作前驱记录和可恢复操作历史。它是版本控制仓库操作日志的工程参照，不等同于 Main 的协调事实日志。
- **Event Sourcing / Causal CRDT / CALM 定理：** 作为 Coordinator Journal 分层投影与受限自动收敛的理论参照——依据 CALM 定理将逻辑单调不可变事实（如回执集、证据集与不可逆撤销墓碑）与非单调互斥裁决显式分层；须分别验证事件重建保证、并发分支处理、数据类型前提与冲突可解释性，不得把 Event Sourcing 的状态重放能力误当作因果归并理论，也不得把 CRDT 对单调数据类型的收敛保证推广到非单调的会话决策语义。

候选模型的评估必须使用同一组 ThreadPass 场景与反例：全量继承、部分采纳、相同来源重复输入、部分共享祖先、交叉派生、恢复分支、野生收编、多个 Main、旧 Main 复活、因果依赖与语义冲突并存。只有当模型能明确指出哪些关系可交换、哪些必须有序、哪些不能自动合并，以及信息缺失时如何拒绝做出强结论，才可进入规范设计候选。

## 10. 实现依赖顺序

具体阶段里程碑划分与验收排期由 `threadpass-refactor-roadmap.md` 统一管理。底层工程实现遵循**「`Pure Core` 与默认 `Local-Offline Profile` 内核优先 + DST 仿真与 `Profile Independence` 门禁 → ThreadPass 统一粘贴应用薄壳 MVP → 可选外挂与扩展 Profile」**的依赖顺序；基础契约与仿真门禁未通过前不得跨步堆砌上层特性：

1. **【`Pure Core` 端口与默认 `Local-Offline Profile` 事务底座】** 定义 `Pure Core` 的 `StoragePort` / `Sync/TransportPort` / `Coordination/ConsensusPort` 抽象端口、接缝原语与状态契约（可见性与释放栅栏 / `Fencing Token` / 含 `clock_domain_id`、`boot_epoch` 与挂起降级语义的 `Lease` / `Advisory Hint` / 诚实状态契约）及通用脱敏策略回调钩子，并在默认 `Local-Offline Profile` 中落地 `StateStore`（`node:sqlite` WAL + CAS + 租约 + `fencing_epoch` + `txn_log`）与 `CodecProvider` 抽象、跨平台目录独占 ACL、跨缓冲区重叠滑动窗口流式脱敏管道与出站复核门禁、`PublishImmutable`（`tmp + fsync + fs.link`）原语、SQLite 写锁内复验链接的证书即提交 `.finalized.json`（含 `commit_intent_proof` 签名声明 `Signed Attestation` 与 `publication_closure_proof`）事务证明、Ed25519 `trusted_device_keys`（带外钉入、`project_id` 作用域、`active / retired / revoked`）管理、崩溃前滚及受引用/租约保护的 Blob 安全 GC。
2. **【`Pure Core` 业务无关状态机与默认 Profile 持久化】** 落地 `Pure Core` 的 Lineage / Input Dependency / Provenance 严格分离关系模型、CALM/`I-Confluence` 分层协调状态机（区分 `main_designated` 创世不可变唯一性、`result_receipt_recorded` 与目标流唯一权威 `Lifecycle Observations` vs `Lifecycle Projection`）、Capability + `Escrow` 配额切分与不可逆撤销链、抽象派发恢复快照接口（由应用壳实例化为 `Dispatch Recovery Capsule`）、固定检查点外部来源 `adopt` 与防重入反查接口、抽象外部工作副本单一活跃绑定约束（由应用壳映射为 `host_session_id`）、多权威独立并存、不可恢复态恢复分支与迟到复活核对，以及默认 Profile 下的分片式不可变 Coordinator Journal（含局部 `coordinator_epoch`）、独立灾备包、Release Gate 与 `thp fsck` 四步验证及跨事务依赖闭包重建（含 `untrusted_unverified_txn` / `incomplete_unverified_txn` / `broken_dependency_closure` 隔离、`partial_blobs_local_only` 标注与冲突对账）。
3. **【三层验证门禁】Core DST + 状态空间探索 / `Profile Conformance` / 真实故障集成：** 建立①可重放随机种子的 Core DST 与关键协议状态空间探索，②针对全部真实 `StoragePort` / `Sync/TransportPort` / `Coordination/ConsensusPort` 实现的 `Profile Conformance` 契约测试（验证不可变发布、Fencing 检查、时钟域/重启边界下的 Lease 与外部 `commit_intent_proof` 校验），以及③真实进程崩溃、缺片同步与全 Supported LTS（Node.js `22.18.0`、`22.x`、`24.x`、`26.x`）端到端故障集成测试；三层门禁全部达标前不得跨越至上层特性堆砌。
4. **【内核】平行流只读借阅、三态异步信箱与介质无关离线同步：** 落地本机 `peek` 只读借阅、目标流唯一权威 `notify_receipt` 驱动的 `pending → offered → committed_receipt`（含高熵 `batch_id` 绑定防伪造）不可变异步信箱，以及基于已 Finalize `.manifest.json` + `.finalized.json` 白名单的介质无关离线备份同步（含 Git、可选建议性 `baton_handoff` 提示与可选外部原子 CAS 适配器）和 `blob_manifest.sync_policy` 门禁。
5. **【应用薄壳 MVP】ThreadPass 统一粘贴 Prompt 往返闭环与 `thp web` 本机临时图形薄壳：** 落地带 `schema_version` 的 `event / invocation / control` 三层 IR 编解码、`input_scope` / `projection_fidelity` / `input_projection` 契约、来源证明链与哈希绑定的人类确认凭证、双层宿主有效预算与分片投喂协议、完整性下限与非指令隔离围栏（配合外部执行权限边界）、`Current Role Briefing` 认知角色简报与身份重框定、可选三态 `Workspace State Manifest`（`git_repo / plain_dir / none`），正式打通覆盖 `adopt / fork / reconcile / handoff / mailbox` 的统一粘贴闭环、直接粘贴原始对话触发的零模型 `Evidence Recovery` 兜底路径，以及零依赖按需唤起/用完即退、并与 CLI 保持全量功能齐平的 `thp web` 本机临时浏览器交互层（`127.0.0.1` + 一次性 Token + 大文本框/一键剪贴板/附件拖拽/DAG 冲突对比/原文审阅签发/`grant`/`peek`/`gc`/`device` 运维面板）。
6. **【应用与内核协同】多来源重协调模型实证与契约固化：** 依据 `7.3.5` 与 `9.3` 节候选模型与场景反例矩阵（普通分叉、部分采纳、部分共享祖先、野生收编、含语义冲突的多路归并等），完成理论比较与场景契约测试并固化最终 `reconciliation_record` 归并契约。
7. **【可选外挂】五维宿主描述符、本地原生日志 `harvest` 与只读旁路采集触手：** 落地结构化五维 `Host Descriptor` 路由框架、面向受支持本地客户端的增量日志 `harvest`（含分页空洞检测）与 CLI/Skill `load` 快捷入口，以及严守“只旁听不插嘴、只读不抢锁、事件驱动低频增量同步”红线的框架 Hook / 浏览器旁路增量采集外挂。
8. **【可选外挂】本地原生投影直写与辅助交互层：** 在严格通过版本校验、CAS 事务、宿主预算预检与运行态锁冲突门禁的前提下，按需接入本地宿主 `Native Resume` 投影写入与轻量辅助交互界面。

规范约束每个实现步骤；撰写规范、盘点历史脚本或搭建空接口骨架均不构成运行时阶段完成证据。
