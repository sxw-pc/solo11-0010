# citefuse 冷热分层归档方案（设计稿）

> 范围：只新增归档/恢复能力与配置，**不改任何算法代码、不改现有读写路径的行为**。
> 默认不开启归档时，系统行为与现在逐字节一致。本稿基于当前 `src/citefuse` 链路编写，
> 所有表名、读点均指现状代码。

## 0. 现状盘点（方案的事实基础）

### 0.1 只增不删的五张表及其读点

| 表 | 写入点 | 热读点（归档后必须保住的能力） |
|---|---|---|
| `runs` | `pipeline.save_run`（幂等 upsert，按 run_id 去重，重跑不新增行） | `get_run / list_runs`、`/runs`、`/metrics/report` 的 run 解析、`_resolve_run` |
| `run_attempts` | 每次调用 `run_pipeline` **追加一行**（幂等重跑也追加，是重跑膨胀主源） | `latest_run_id`、`/review/next` 选最新批次、`run_public` 的 attempt_count/last_attempt、`get_run_summary` |
| `pairs` | `replace_pairs`（同 run_id 先删后插，重跑不净增；新 run 才净增） | ① `metrics._threshold_groups` 阈值扫描/报告读全量 `score`；② `review.next_review_pairs` 读最新 run 的 `zone='review'` 行及 `strategies_json/details_json`；③ `submit_decision` 用 `get_pair` 校验 pair 属于最新 run |
| `edges` | `replace_edges` | 仅 `cluster_detail`（`/clusters/{id}`）读，用于展示簇内边及分项 |
| `clusters` | `replace_clusters` | `cluster_summaries`（`/clusters`）、`cluster_detail`、`export_golden`（`/golden/export`），全部直接依赖 `member_keys_json/member_pks_json/golden_json` |

关键事实（决定方案形状）：

1. **`run_id` 是确定性派生的**（`pipeline._run_id` = 批次 input_hash + config_hash + 裁定历史指纹），相同输入重跑复用同一 run_id，pairs/edges/clusters 按 run_id 覆盖。膨胀来自「新批次 + 新裁定产生的新 run_id」与「每次调用追加的 `run_attempts` / `audit_events`」。
2. **复核只认最新 run**：`latest_run_id` 从 `run_attempts` 取全局/批次最新；`submit_decision` 校验 pair 必须在最新 run 的 `pairs` 里。冷 run 天然不在复核路径上。
3. **指标口径绑定 `pairs.score` 全量**：`build_report` 与 `scan_thresholds` 不重算分，只按阈值重放 `list_pairs` 的 score + `cluster_records`。冷 run 若把 pairs 挪走，这两个接口对冷 run 会失去原始重放能力——这是本方案唯一需要显式声明的能力降级（见 §8）。
4. `cluster_detail` 还要回 `records` 表取成员、回 `edges` 取边；`records` 是热库基础数据，**本方案不动 `records/batches/dead_letters/decisions`**，成员详情永远可查。
5. 存储是单连接 + `RLock` + WAL 的单进程 SQLite（`storage.py`），没有多进程写者；归档器与服务共享同一进程时锁语义天然串行，离线工具则靠 SQLite 文件锁 + 状态表互斥。
6. `DELETE /batches/{id}` 靠 `ON DELETE CASCADE` 级联清掉 runs/pairs/edges/clusters/decisions/audit。归档表在**另一个库文件**里，不受级联影响，需要单独的归档 GC（见 §6.4）。

### 0.2 设计目标与硬约束

- G1 热库体积可控：重跑不再线性拖慢 `/clusters`、`/metrics/report`、`/golden/export`、复核队列与备份。
- G2 冷运行可审计：冷 run 的簇、金记录、人工决策、审计事件、运行计数一个都不丢，且可逐行校验完整性。
- G3 历史查询不中断：归档后历史簇查询、金记录导出、run 详情/审计仍可用（部分接口接受显式降级）。
- G4 复核队列零影响：最新 run 永不归档，队列与裁定接口行为不变。
- G5 全程可续做、可回滚、可重入；归档进行中被杀不产生半删状态。
- G6 旧库直接启动；配置默认关闭时与现状完全一致；迁移只做加法。

---

## 1. 总体架构

```
热库 citefuse.sqlite3（现有，WAL，服务直连）
├── 不动：batches / records / dead_letters / decisions
├── 不动（全量保留）：audit_events
├── runs：行保留，新增 lifecycle 标记（hot/archived/...）
├── run_attempts：冷 run 仅保留最新一行（或冷存全量，见 §3.2）
├── pairs：冷 run 替换为 pairs_slim（去 details_json 的瘦行）
├── edges：冷 run 删除（冷库存全量，按需 restore）
└── clusters：冷 run 保留全量快照（决策见 §3.3）

冷库 archive/citefuse-archive.sqlite3（新增，独立文件，只读挂载/按需 ATTACH）
├── archive_runs            每个归档 run 一份清单 + 行数 + 校验哈希
├── archive_run_attempts    run_attempts 全量
├── archive_pairs           pairs 全量（含 details_json 分项明细）
├── archive_edges           edges 全量
├── archive_clusters        clusters 全量（热库的异地冗余副本）
├── archive_jobs            归档作业状态机（可续做的核心元数据）
└── archive_manifest        作业级清单与 SHA-256，配合文件侧校验
```

冷库选 SQLite 单文件而不是自研 JSONL 打包，理由：

- 与现有技术栈一致，参数化 SQL、事务、`ATTACH DATABASE` 都是现成的，备份/拷贝语义就是一个文件。
- 按需恢复（restore 单个 run 的 edges/pairs）可以直接 `INSERT ... SELECT`，不需要解析器。
- JSONL 导出作为**附加的可移植副本**由独立命令生成（见 §6.5），不作为主冷存，避免自造格式的审计风险。

冷热两个库文件放在同一数据目录（`$CITEFUSE_DATA_DIR/archive/`），备份时两个文件 + manifest 一起带走即可。

### 1.1 归档单元与资格（按批次 + 运行时间）

- **归档单元 = 一个 run_id**（作业可一次选多个 run，但状态机、校验、删除都按 run 独立推进，保证中断粒度小、可续）。
- 冷/热判定只看两个维度：**批次归属** 与 **运行时间/新旧次序**。资格规则全部 AND：
  1. `runs.status = 'completed'`（failed run 不归档，避免把不完整明细当冷证据；它们很小，留热库）。
  2. 该 run **不是其批次的最新 run**：`run_id != latest_run_id(batch_id)`。最新 run 承载复核队列与裁定校验，永久热留（G4）。
  3. 批次当前没有正在进行的 run（互斥，见 §5）。
  4. 年龄超过保留窗：`runs.created_at < now - archive.after`（默认如 90 天），或显式 `citefuse-archive run <run_id>` 手动指定（手动仍受 2、3 约束）。
  5. 该 run 未处于 `archiving/restoring` 中间态。
- 保留策略配置（见 §7）支持按批次豁免（`archive.protect_batches`）与全局保留热 run 数（`archive.keep_latest_runs_per_batch`，默认 ≥1）。

---

## 2. 热库元数据变更（只加不删的 DDL）

全部用 `CREATE TABLE/INDEX IF NOT EXISTS` 与 `ALTER TABLE ... ADD COLUMN` 包裹，启动时幂等执行；列均带默认值，旧行自动取默认，**旧库零数据搬迁即可启动**。

```sql
-- 2.1 runs 增加生命周期标记（不动 status 原值，completed/failed 语义不变）
ALTER TABLE runs ADD COLUMN lifecycle TEXT NOT NULL DEFAULT 'hot';
  -- hot | eligible | archiving | archived | restoring
CREATE INDEX IF NOT EXISTS idx_runs_lifecycle ON runs(lifecycle);

-- 2.2 瘦身后的 pairs：结构与 pairs 对齐，仅去掉 details_json
CREATE TABLE IF NOT EXISTS pairs_slim (
  run_id TEXT NOT NULL,
  pair_key TEXT NOT NULL,
  rec_a_pk INTEGER NOT NULL, rec_b_pk INTEGER NOT NULL,
  score REAL NOT NULL, zone TEXT NOT NULL,
  strategies_json TEXT NOT NULL,
  PRIMARY KEY(run_id, pair_key)
);
CREATE INDEX IF NOT EXISTS idx_pairs_slim_zone ON pairs_slim(run_id, zone);

-- 2.3 批次级归档锁/水位（互斥与续做都靠它）
CREATE TABLE IF NOT EXISTS archive_locks (
  batch_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  locked_at TEXT NOT NULL,
  expires_at TEXT NOT NULL          -- 租约，防进程被杀后死锁
);

-- 2.4 可选：记录每个 run 的归档指纹，供审计/巡检对账
CREATE TABLE IF NOT EXISTS run_archive_refs (
  run_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  archive_db_path TEXT NOT NULL,
  archived_at TEXT NOT NULL,
  pair_rows INTEGER NOT NULL, edge_rows INTEGER NOT NULL,
  cluster_rows INTEGER NOT NULL, attempt_rows INTEGER NOT NULL,
  content_sha256 TEXT NOT NULL      -- 对冷库该 run 明细的规范哈希，见 §4.4
);
```

说明：

- `pairs`、`edges`、`clusters` 原表**不改名、不删表**。冷 run 的处理是「行级删除/迁移」，热 run 的读写路径完全不感知新表。
- 不引入外键指向冷库（跨库 FK 不可靠）；`run_archive_refs` 只是指针与对账元数据。
- `run_attempts` 不建新表，冷热策略是「留最新一行 + 全量进冷库」（§3.2）。

---

## 3. 四个关键取舍（明确结论）

### 3.1 取舍一：`pairs` 分项明细是否进冷存 —— 全量进冷存，热库留瘦身行

`pairs` 的体积大头是 `details_json`（六字段 `raw_score/score/weight/contribution/status` 的完整嵌套 JSON，见 `scoring.score_pair`），而各读点真正需要的列不同：

| 读点 | 实际依赖的列 |
|---|---|
| 阈值扫描/指标报告 `metrics._threshold_groups` | 只要 `score`（+pair_key/rec pks） |
| 复核队列 `next_review_pairs` | 最新 run（永不归档），需要 `strategies_json + details_json` |
| 裁定校验 `get_pair` | 最新 run（永不归档），只看行存在性 |

**结论（分层而非二选一）：**

- 冷库存 **`archive_pairs` 全量**（含 `details_json`、`strategies_json`），分项明细一条不丢，满足审计与「按需完整恢复」。
- 热库把冷 run 的 pairs 迁到 **`pairs_slim`**：保留 `pair_key/rec_a_pk/rec_b_pk/score/zone/strategies_json`，**只剥离 `details_json`**。瘦行通常能去掉该行 70–90% 的字节，且：
  - 冷 run 的阈值重放仍可在热库完成（score 还在），指标口径不被迫改变（见 §8）；
  - 行存在性、zone 分布、策略统计仍可查；
  - 复核路径不碰冷 run，瘦身在功能上无损失。
- 若运维追求极限压缩，可配置 `archive.slim_pairs: full`（热库连瘦行也删，全靠冷库），但**默认 `slim`**，因为它在「缩体积」和「指标接口不降级」之间最稳。这是配置项而非硬编码，见 §7。

### 3.2 取舍二：`clusters` 保留快照还是从归档重建 —— 热库保留全量快照

两个候选：

- 方案 A（重建）：冷 run 删 `clusters`，查 `/clusters`、`/golden/export` 时从冷库拉 edges/pairs，结合热库 `records` 重跑 `cluster_records` + `build_golden` 现场重建。
- 方案 B（快照）：冷 run 的 `clusters` 行原样留在热库，冷库再存一份异地副本。

**结论：选 B，热库保留簇快照。** 理由：

1. `clusters` 本身很省：每簇一行，载荷是成员 key/pk 列表 + 一条 golden JSON，体积远小于 pairs（pairs 是 O(n²) 量级，clusters 是 O(n)）。它不是膨胀主因，删掉省不下多少，却让三个核心接口全部依赖冷库在线。
2. 重建路径要复现 `cluster_records` 的并查集 + 人工约束 + `build_golden` 的幸存规则，**任何算法演进都会让「重建结果」与「历史快照」产生差异**，破坏审计的第一原则——历史结果应是当时落库的原样证据，而不是现代码重算的产物。用户已明确「不动去重算法」，更不应在查询链路上引入隐式重算。
3. `/golden/export` 的确定性（README §7 承诺逐行一致）依赖快照字节；重建无法保证跨版本字节一致。
4. 冷库的 `archive_clusters` 作为异地冗余：热库文件损坏/误删时可整 run 恢复，校验哈希保证两份一致。

代价：热库 clusters 表持续增长（但增速 = 簇数，线性且小量）。可接受，并在文档中写明这是有意识的成本交换。

### 3.3 取舍三：清理与正在运行的批次如何互斥 —— 批次租约锁 + 资格闸门，双保险

互斥要挡住两类冲突：(a) 归档正在删/迁某 run 的行时，`POST /runs` 或裁定自动重跑在写同一 run；(b) 归档把某 run 判冷后，它又因新裁定变成「最新 run」。

**结论：三层防护，全部基于数据库状态，不依赖内存标志。**

1. **资格闸门（逻辑层）**：归档候选必须满足 `run_id != latest_run_id(batch_id)` 且批次无未完成 attempt。最新 run 永远不入选，从源头消除与复核/裁定的冲突。
2. **批次租约锁（`archive_locks`，持久层）**：归档作业开始前对涉及的每个批次 `INSERT` 一行锁（带 `job_id` 与 `expires_at`）。`run_pipeline` 在写库前调用一个轻量 `assert_batch_not_archiving(batch_id)`：检测到未过期锁则快速失败（建议返回 503 + `retry_after`，不排队阻塞 HTTP 线程）。锁是租约制：进程被杀后超过 `expires_at` 可被新作业接管，杜绝死锁。
3. **状态机 + 事务边界（行层）**：`runs.lifecycle` 先置 `archiving` 再搬运，删除明细在同一事务内提交；任何读路径看到 `archiving/restoring` 中间态按「暂时不可用，重试」处理（409/503），不会读到半删数据。
4. 单进程部署下服务与归档器共享 `RLock`，天然串行；离线归档工具（独立进程）靠 SQLite 写锁 + `archive_locks` 表协同（WAL 允许并发读，但 DDL/写事务互斥，`BEGIN IMMEDIATE` 抢写锁）。

裁定自动重跑（`/review/decisions` 内联 `run_pipeline`）同样走第 2 层闸门，因此不会与归档并发。

### 3.4 取舍四：归档失败中断怎么续做 —— 每 run 独立状态机 + 幂等阶段 + 租约接管

作业以 run 为最小推进单元，状态落在冷库 `archive_jobs` 与热库 `runs.lifecycle` 两处，阶段全部幂等，重跑归档命令时从断点继续而不是重来：

```
hot
  └─(select eligible)→ eligible
eligible
  └─(acquire batch lease, set archiving)→ archiving
archiving
  ├─ stage 1 COPY    明细写入冷库（INSERT OR IGNORE，按 run_id+主键去重）
  ├─ stage 2 VERIFY  冷库行数/哈希 == 热库源表（§4.4）
  ├─ stage 3 PRUNE   热库：pairs→pairs_slim、删 edges、（attempts 收敛）
  ├─ stage 4 COMMIT  runs.lifecycle='archived'，写 run_archive_refs，释放租约
  └─ 任一 stage 失败 → 保持 archiving，记录 error/step，事务回滚不留半行
archived
  └─(restore)→ restoring →（冷库回灌 + 校验）→ hot
```

续做规则：

- 重新执行归档时，先扫 `archive_jobs` 中 `status IN ('running','interrupted')` 的作业：租约未过期则拒绝并发启动；已过期则**接管**（更新 job_id/心跳），从 `last_completed_step` 的下一步继续。
- stage 1 用 `INSERT OR IGNORE` + 内容哈希，重复执行不产生重复行；stage 2 校验不过绝不进 stage 3（**先落冷、校验通过才删热**，保证任何时刻热数据要么完整在热库、要么已在冷库核验）。
- stage 3 的 prune 在**单事务**内完成（一个 run 的 pairs 迁移 + edges 删除 + attempts 收敛），被杀则整笔回滚，下次重试该 run 即可，不会出现「瘦行建了一半、原行已删」。
- 进程被 `kill -9`：WAL 保证未提交事务回滚；遗留的 `archiving` 标记与租约由下次启动的巡检命令 `citefuse-archive recover` 统一清理（接管或回退为 `hot`）。

---

## 4. 冷库表结构与归档/恢复流程

### 4.1 冷库 DDL（`archive/citefuse-archive.sqlite3`）

```sql
CREATE TABLE IF NOT EXISTS archive_jobs (
  job_id TEXT PRIMARY KEY,
  triggered_at TEXT NOT NULL, finished_at TEXT,
  status TEXT NOT NULL,            -- running | interrupted | completed | failed
  last_completed_step TEXT,        -- copy | verify | prune | commit
  selection_json TEXT NOT NULL,    -- 当时的选入条件（after/批次/run 列表），可审计
  error_text TEXT
);
CREATE TABLE IF NOT EXISTS archive_runs (
  run_id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  batch_id TEXT NOT NULL, config_hash TEXT NOT NULL,
  run_snapshot_json TEXT NOT NULL, -- runs 行的完整 JSON 快照
  pair_rows INTEGER NOT NULL, edge_rows INTEGER NOT NULL,
  cluster_rows INTEGER NOT NULL, attempt_rows INTEGER NOT NULL,
  content_sha256 TEXT NOT NULL,
  archived_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS archive_run_attempts (
  run_id TEXT NOT NULL, attempt_id INTEGER NOT NULL,
  payload_json TEXT NOT NULL,                 -- 原行完整 JSON
  PRIMARY KEY(run_id, attempt_id)
);
CREATE TABLE IF NOT EXISTS archive_pairs (
  run_id TEXT NOT NULL, pair_key TEXT NOT NULL,
  payload_json TEXT NOT NULL,                 -- 含 details_json 全量分项
  PRIMARY KEY(run_id, pair_key)
);
CREATE TABLE IF NOT EXISTS archive_edges (
  run_id TEXT NOT NULL, pair_key TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  PRIMARY KEY(run_id, pair_key)
);
CREATE TABLE IF NOT EXISTS archive_clusters (
  run_id TEXT NOT NULL, cluster_id TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  PRIMARY KEY(run_id, cluster_id)
);
CREATE TABLE IF NOT EXISTS archive_manifest (
  job_id TEXT NOT NULL, run_id TEXT NOT NULL,
  table_name TEXT NOT NULL, row_count INTEGER NOT NULL,
  sha256 TEXT NOT NULL,                       -- 规范序列化后逐行哈希的聚合
  PRIMARY KEY(job_id, run_id, table_name)
);
```

冷表用 `payload_json` 整行存放而不是逐列拆，是为了**与热库表结构解耦**：将来热表加列，冷库的读写与旧归档不受影响；恢复时按 JSON 映射回列。

### 4.2 归档单 run 的事务时序

1. `BEGIN IMMEDIATE`（热库）抢写锁；复核资格 + 抢 `archive_locks` 租约；`runs.lifecycle='archiving'`；提交短事务（缩短锁持有）。
2. 冷库 `BEGIN`；`INSERT OR IGNORE` 写五张 archive 表（runs 快照、attempts、pairs、edges、clusters）；提交。
3. 校验阶段（§4.4）：重算冷库各表行数与 `content_sha256`，与热库源表对比；不符则标记 failed 并中止，**不动热库**。
4. 热库单事务 prune：
   - `pairs` 冷 run 行 → 插入 `pairs_slim`（无 details 列）→ 删 `pairs` 原行；
   - 删 `edges` 该 run 行；
   - `run_attempts` 该 run 仅保留 `attempt_id` 最大一行，其余删除（全量已在冷库）；
   - `clusters` 不动；`audit_events` 不动；`decisions` 不动。
5. 写 `run_archive_refs`、`archive_manifest`，`runs.lifecycle='archived'`，释放租约，`archive_jobs.status='completed'`。

### 4.3 恢复（restore）流程

按需把冷 run 回灌为热 run（例如要重新复核、或离线冷库不可用前的预置）：

1. 校验冷库文件存在且 `content_sha256` 匹配；置 `runs.lifecycle='restoring'`。
2. 冷库 `archive_pairs → pairs`（从 payload_json 还原 details_json）、`archive_edges → edges`、`archive_run_attempts → run_attempts`；`pairs_slim` 中该 run 瘦行删除（被全量行取代）。
3. 行数校验通过后 `lifecycle='hot'`、删 `run_archive_refs`（冷库副本保留，由 GC 按策略回收）。
4. 恢复是可选运维操作，不改变 run_id 与任何结果字节。

### 4.4 完整性校验（可审计的最小集）

- **行级哈希**：对每个 run、每张表，把行按主键排序后做规范 JSON 序列化（`sort_keys=True`，与现有 `json_dumps` 同风格），逐行 `sha256` 再聚合，写入 `archive_manifest` 与 `run_archive_refs.content_sha256`。
- **文件级校验**：归档命令结束后对冷库文件算整体 SHA-256，写入同目录 `citefuse-archive.sqlite3.sha256`；备份恢复后可一键 `verify`。
- **对账命令** `citefuse-archive verify [--run ID]`：重算哈希比对 manifest，并核对热库 `run_archive_refs` 指针；任何不一致非零退出，供巡检/CI 调用。
- 「可审计最小集」定义：冷 run 保留 = runs 计数快照 + 全量 attempts + 全量 pairs（含分项）+ 全量 edges + 全量 clusters/golden + audit_events（热库不动所以天然在）+ decisions（不动）+ 双份哈希。凭此集可离线重放整个 run 的证据链，无需重跑算法。

---

## 5. 与运行批次互斥的落地细节（对接现有代码）

- 新增一个存储方法 `Storage.acquire_archive_lease(batch_id, job_id, ttl)` / `release_archive_lease`，用 `INSERT ... WHERE NOT EXISTS (未过期租约)` 实现抢锁。
- 在 `pipeline.run_pipeline` 入口（取 batch 之后、重算之前）调 `assert_batch_not_archiving`；这是**唯一需要碰到现有流程的代码点**，且内部实现为「无锁直接返回」——当 `archive.enabled=false`（默认）时该方法是空操作，保证默认行为零变化。
- HTTP 表现：归档窗口内 `POST /runs`、`/review/decisions` 收到 503 + `Retry-After`；归档为离线批处理工具，默认在服务低峰由人手/计划任务触发，窗口通常秒级到分钟级。
- 归档器提供两种运行形态：① 同进程 CLI 子命令（推荐，复用 RLock）；② 独立进程 `python -m citefuse.archive`（靠文件锁 + 租约表）。两者状态机一致。

---

## 6. 组件与接口清单（新增，不改旧接口签名）

### 6.1 新增模块（建议）

- `src/citefuse/archive.py`：选入、状态机、copy/verify/prune/commit/restore、租约。
- `src/citefuse/archive_store.py`：冷库连接、ATTACH、payload_json 序列化与哈希。
- `src/citefuse/cli_archive.py`：`python -m citefuse.cli_archive {run|prune|recover|verify|restore|export}`。
- 迁移在 `Storage.__init__` 的幂等 DDL 段追加（与现有 `SCHEMA` 同风格）。

### 6.2 新增 HTTP 接口（可选，默认关闭时不注册或返回 404/410）

| 方法 | 路径 | 行为 |
|---|---|---|
| GET | `/admin/archive/runs` | 列出 eligible/archived run 与体积、保留窗 |
| POST | `/admin/archive/jobs` | 触发一次归档（body 限定批次/run/年龄） |
| GET | `/admin/archive/jobs/{id}` | 作业状态与 last_completed_step（续做可见） |
| POST | `/admin/archive/restore` | 按需恢复冷 run |
| GET | `/admin/archive/verify` | 对账结果 |

这些接口与现有业务接口路径不重叠；不开归档时不影响路由表行为。

### 6.3 现有接口的兼容边界（逐条）

| 接口 | 归档后行为 | 是否变化 |
|---|---|---|
| `POST /batches`、`GET /batches`、`GET /batches/{id}` | 不涉及冷表 | 无变化 |
| `DELETE /batches/{id}` | 级联照删热库；冷库副本成为孤儿，由 §6.4 GC 清理 | 热行为无变化；需补 GC |
| `POST /runs` | 批次被归档租约锁定时 503，否则正常 | 仅锁窗口内新增 503 |
| `GET /runs`、`GET /runs/{id}` | run 行保留，计数/timings/blocking_stats 全在；attempt_count 反映热库保留行数（见 §8 声明） | 字段在，attempt 口径变化需声明 |
| `/review/next`、`/review/decisions` | 只认最新 run（永不归档），pairs 全量热留 | **无变化（G4）** |
| `GET /clusters` | 读热库 clusters 快照 | **无变化、字节一致** |
| `GET /clusters/{id}` | 簇/成员/golden 正常；冷 run 的 `edges` 已移冷库：自动按需 restore 后返回，或冷库离线时 503 并提示 | 增加一条慢路径/一个错误码，正常在线时响应体不变 |
| `GET /golden/export` | 读热库 clusters 快照，jsonl/csv 逐行一致 | **无变化** |
| `GET /metrics/report`、`/metrics/thresholds` | 默认 slim 模式 score 在热库，重放口径不变；full 模式冷 run 返回 409 并提示先 restore | 默认无变化；full 模式显式降级 |
| `/health` | 增加冷库可达性字段（可选），原有字段不动 | 仅增字段 |

`cluster_detail` 的 edges 按需恢复采用「同步小事务回灌该 run 的 edges（平均远小于 pairs）→ 正常返回」，对调用方透明；为避免抖动，也可配置为归档时保留 edges（`archive.keep_edges_hot: true`，体积换便利，默认 false）。

### 6.4 批次删除后的冷库 GC

`DELETE /batches` 不碰冷库。新增 `archive gc`：对比冷库 `archive_runs.batch_id` 与热库 `batches`，删除已不存在批次的全部 archive_* 行与 manifest，输出回收量。GC 同样走作业状态机、可续、默认只 dry-run，需 `--apply` 才真删，防止误删审计证据。

### 6.5 可移植导出

`archive export --run ID --out dir/` 把冷 run 五张表导成 JSONL + `manifest.sha256`，用于脱离 SQLite 的长期留存或外部审计；导入侧 `archive import` 校验哈希后写回冷库。该格式仅作交换用途，主冷存仍是 SQLite。

---

## 7. 配置变更（新增节，默认关闭，旧配置无需修改）

`configs/default.yaml` 与包内 `resources/default.yaml` 同步新增（`AppConfig` 增加 `ArchiveConfig`，`extra="forbid"` 下旧配置文件不含此节时取默认值）：

```yaml
archive:
  enabled: false              # 总开关；false 时所有归档代码路径不激活，行为同现状
  mode: offline               # offline=手动/计划任务 CLI；service=开放 /admin/archive/*
  after_days: 90              # created_at 早于 now-90d 才 eligible
  keep_latest_runs_per_batch: 1   # 每批至少热留的最新 run 数（复核依赖，必须 >=1）
  protect_batches: []         # 永不归档的批次白名单
  slim_pairs: slim            # slim=热库留去 details 的瘦行（默认）；full=热库 pairs 全删
  keep_edges_hot: false       # true 时 edges 不挪冷库（簇详情零慢路径，体积换便利）
  archive_dir: archive        # 相对 data_dir 的冷库目录
  lease_ttl_seconds: 1800     # 批次租约超时，防死锁
  batch_size_runs: 25         # 单作业最多处理多少 run，控制单次事务/时长
  auto_gc_deleted_batches: false
```

兼容性要点：

- `enabled:false` 时：不建冷库连接、不注册 admin 路由、`assert_batch_not_archiving` 为空操作、迁移 DDL 仍幂等执行（只加空表/带默认值列，不影响任何现有 SQL）。
- 旧 YAML 没有 `archive` 节 → pydantic 默认值生效，等价 `enabled:false`。
- 旧 SQLite 没有新列/新表 → 启动幂等 DDL 自动补齐，`lifecycle` 默认 `hot`，所有现有查询谓词不受影响。

---

## 8. 哪些统计会因冷存不可得，以及如何防止指标口径悄悄变化

### 8.1 明确的能力降级清单（默认 slim 配置下）

| 统计/能力 | 冷 run 是否可得 | 说明 |
|---|---|---|
| run 级计数（input_rows、candidate_pairs、matched_edges、cluster_count…） | 完整可得 | 在 `runs` 行内，热留 |
| 阻塞策略统计 `blocking_stats_json`、阶段 timings | 完整可得 | 在 `runs` 行内 |
| 簇、金记录、成员、provenance | 完整可得 | clusters 快照热留 |
| 人工决策历史与冲突审计 | 完整可得 | decisions/audit_events 不动 |
| 阈值重放、pairwise P/R/F1、exact_rate | **默认 slim 下完整可得**（score 在瘦行里）；full 模式需先 restore | 见下 |
| 单 pair 的六字段分项 `details_json` | 热库不可得，冷库全量；按需 restore 或查冷库 | review 只用最新 run，不受影响 |
| 簇内 edges 明细 | 默认在冷库，按需回灌；`keep_edges_hot:true` 则热留 | 仅影响 `/clusters/{id}` 的 edges 字段 |
| `attempt_count` / `last_attempt` 的全量历史 | 热库仅最新 attempt，全量在冷库 | 见 8.3，接口需显式标注口径 |
| 基于 `pairs.details_json` 的临时分析（如字段贡献度分布） | 冷 run 需冷库 | 现网接口本就不暴露该分析 |

### 8.2 防止指标口径漂移的四道闸

1. **score 不出热库（默认 slim）**：`build_report/scan_thresholds` 的 SQL 改为 `pairs UNION ALL pairs_slim` 的仓储层视图（见 8.4），读到的仍是落库原始 score，**不重算、不归一化、不套新阈值逻辑**，P/R/F1/exact_rate 分子分母与归档前同值。加测试：同一 run 归档前后两次 `/metrics/report` 与 `/metrics/thresholds` 响应 `dict` 完全相等。
2. **配置哈希绑定**：报告里已有 `config_hash`；归档 manifest 额外记录归档时 config_hash 与代码版本（`archive_schema_version`）。若未来算法变更，旧冷 run 的报告仍标注其原始 config_hash，不会把新口径结果挂到旧 run 上。
3. **拒绝隐式重算**：任何接口都不用「现代码重跑」来弥补缺失明细（§3.2 已排除重建方案）；缺数据时显式 409/503 + 提示 restore，而不是返回一个看似正常但口径已变的结果。
4. **回归基线**：验收脚本归档前对每个冷 run 落一份 `/metrics/report`、`/metrics/thresholds`、`/golden/export`（jsonl+csv）、`/clusters` 的响应哈希；归档后重算比对，全部一致才允许 prune（并入 stage 2 校验）。

### 8.3 attempt 口径的显式处理

`run_public/get_run_summary` 当前返回 `attempt_count`（热库 `COUNT(*)`）与 `last_attempt`。收敛后热库 count 会变小，这是最容易「悄悄变」的统计。处理：

- 热库保留每个冷 run 最新 attempt，`last_attempt` 字段不变；
- 响应新增 `attempt_count_hot` 与 `attempt_count_archived`（后者来自 `run_archive_refs.attempt_rows - 1` 或冷库 count），保留原 `attempt_count` 字段名但定义为**热库可见数**并在文档/变更日志注明；需要全量审计数时走冷库/新字段。默认关闭归档时两字段行为与现状一致（archived=0）。

### 8.4 仓储层兼容视图（不散落改 SQL）

在 repository 层增加统一读方法，业务代码不直接拼 `pairs`/`pairs_slim`：

```sql
-- 阈值重放专用视图：score/zone/keys 列对齐，不含 details
CREATE VIEW IF NOT EXISTS pairs_for_replay AS
SELECT run_id, pair_key, rec_a_pk, rec_b_pk, score, zone FROM pairs
UNION ALL
SELECT run_id, pair_key, rec_a_pk, rec_b_pk, score, zone FROM pairs_slim;
```

`metrics._threshold_groups` 改读该视图（这是归档功能带来的**唯一一处现有查询改写**，且 `enabled:false` 时 `pairs_slim` 为空，结果集与今天逐行相同，由测试锁死）。`review.py` 继续读原 `pairs` 表（最新 run 数据只在那里）。

---

## 9. 迁移与回滚路径

### 9.1 迁移（旧库 → 支持归档的版本）

1. 部署新代码，保持 `archive.enabled=false`。启动时幂等 DDL：加 `runs.lifecycle` 列（默认 hot）、建 `pairs_slim/archive_locks/run_archive_refs` 与视图。**不搬任何数据**。
2. 验收：现有 `pytest` 全绿 + 在旧库副本上跑一遍只读冒烟（`/runs`、`/clusters`、`/golden/export`、`/review/next`）确认响应不变。
3. 需要启用时改 `enabled:true` 并配置保留窗；首次归档建议先 `--dry-run` 看选入清单与预估体积，再正式执行。
4. 迁移版本号写入新表 `schema_migrations(version, applied_at)`（顺手补上项目目前缺失的迁移记录），每次 DDL 变更一行。

### 9.2 回滚

- **配置级回滚（首选，零数据风险）**：`enabled:false` 重启。新表/新列留着无害，所有旧接口走原路径；已归档 run 的 edges/details 在冷库，热库瘦行仍支撑 metrics/clusters/export，仅「冷 run 分项明细/edges 在线查看」不可用（可先批量 restore 再关）。
- **数据级回滚（恢复到归档前热库形态）**：`citefuse-archive restore --all` 把所有 archived run 从冷库回灌（pairs 全量、edges、attempts），校验哈希后清掉瘦行与 lifecycle 标记，热库即还原为归档前内容；冷库文件保留直至人工确认后删除。
- **代码级回滚（退回旧版本二进制）**：旧版本不认识新列/新表，但 SQLite 对「多出的列/表」宽容——旧代码的 `SELECT *` 会多拿一列 `lifecycle`（现有代码均按列名取值而非按位置，已核对 repository/api_views 全部用命名访问），不影响运行；稳妥做法仍是回滚前先 `restore --all`。回滚步骤写入运维手册并在测试中用「旧版本 schema + 新库文件」冒烟验证。
- 冷库文件损坏：热库不受影响（无跨库外键、无 ATTACH 依赖）；受影响的仅是冷 run 的明细回灌能力，manifest 哈希能定位损坏范围，可从备份冷库恢复。

---

## 10. 验收场景对照（用户提的五个场景如何被满足）

| 验收场景 | 方案保障 | 关键观察点 |
|---|---|---|
| 拿旧库直接启动 | 幂等加法 DDL、列默认值、`enabled:false` 空操作路径 | 启动无报错；`/runs /clusters /golden/export /review/next` 响应与升级前一致；pytest 全绿 |
| 连续重跑再归档 | 同 run_id 重跑只追加 attempt、覆盖明细；归档选入排除最新 run，按 created_at/保留窗挑老 run | 重跑后 latest run 永远 hot；`--dry-run` 清单不含任何批次的最新 run；归档后热库体积下降、冷库行数+manifest 匹配 |
| 归档后历史簇与导出一致性 | clusters 快照热留 + 归档前后响应哈希比对（stage 2） | `/clusters`、`/clusters/{id}`（edges 自动回灌）、jsonl/csv 导出与归档前哈希一致；`verify` 命令通过 |
| 复核队列不受影响 | 最新 run 不归档 + 复核只读原 `pairs` 表 + 裁定校验不变 | 归档进行中与完成后，`/review/next` 列表、提交裁定、自动重跑行为不变；锁窗口内收到明确 503 而非脏数据 |
| 清理中途杀掉再恢复 | 每 run 状态机 + 租约 TTL + 单事务 prune + `recover` 命令 | 在 copy/verify/prune 各阶段 `kill -9` 后重启：无半删行、无死锁；重跑归档从 `last_completed_step` 续做；completed 的 run 不重复处理 |

建议新增测试矩阵（`tests/test_archive.py`，归档功能独立测试，默认配置套件不引入冷库依赖）：

1. 旧 schema 库文件直接 `Storage()` 打开并完成一次完整管道（兼容性）。
2. 多批次多 run 重跑 → dry-run 选入正确性（最新 run 排除、保护批次、年龄窗）。
3. 归档全流程后：metrics 报告/阈值扫描、clusters、golden 导出、review 队列的响应与归档前深比较相等。
4. prune 阶段注入异常（mock 抛错）→ 热库明细行数不变、lifecycle 回退/可续。
5. 租约过期接管：伪造 `expires_at` 已过的锁，新作业可接管；未过期时并发启动被拒。
6. `restore --all` 后热库行数与内容哈希回到归档前基线。
7. `DELETE /batches` 后 `archive gc --dry-run/--apply` 的孤儿回收正确性。
8. `enabled:false` 下不产生冷库文件、不注册 admin 路由、所有现有测试零修改通过。

---

## 11. 体积与备份影响（预期收益）

- 热库增长项从五表降为：`run_attempts`（每冷 run 留 1 行）、`pairs_slim`（剥 details，约为原 pairs 的 10–30%）、`clusters`（线性小表）、`audit_events`（只增但单行小，且是审计刚需）。最大的 O(n²) `details_json` 与 edges 整体移出热库。
- 备份：热库文件变小，常规备份更快；冷库为追加型冷文件，变化频率低，可按更慢节奏备份；两文件 + `*.sha256` 构成完整备份单元。
- `VACUUM` 建议：大批量 prune 后由归档命令在事务外按需触发（或提示运维手动），把删除空间还给文件系统；不在 prune 事务内做，避免长锁。

---

## 12. 明确不做的事（边界）

- 不改 blocking/scoring/clustering/survivorship 任何算法与确定性约定，不改 run_id/cluster_id 派生规则。
- 不归档/不删除 `records`、`batches`、`dead_letters`、`decisions`、`audit_events`。
- 不做跨进程分布式锁（单进程本地服务定位，SQLite 锁 + 租约足够）。
- 不做实时增量归档：归档是离线、批式、可计划的维护动作。
- 不在本项目内实现冷库加密/云上传（本地威胁模型无密钥管理需求；manifest 哈希保证完整性，加密留给文件系统/备份层）。
