# 跨运行周期复核决策复用与冲突仲裁设计方案

> 状态：设计稿（未实施）。基于 citefuse 现有代码（`src/citefuse/`）的契约设计。
> 目标：重复人工复核量降低 ≥30%，实体解析准确率不下降，不破坏运行可复现性。
> 约束：纯 Python 技术栈、完全离线、完全确定性、不侵入核心熔合链路主流程。

---

## 1. 问题定位（基于代码事实）

| 事实 | 位置 | 后果 |
|---|---|---|
| 人工裁定按 `batch_id` 隔离存储与读取 | `repository.py:160` `decision_history(batch_id)`、`review.py:12` | 新批次完全看不到历史批次的裁定，灰区对全部重新进人工队列 |
| `pair_key = SOURCE::RECORD_ID‖SOURCE::RECORD_ID` 是批次内标识 | `models.py:141` | 同一实体在新批次换了 record_id 后，pair_key 不同，即使能跨批读取也匹配不上 |
| `run_id = sha256(batch_id ‖ input_hash ‖ config_hash ‖ 裁定指纹)` | `pipeline.py:20-31` | 复用决策必须进入该指纹，否则不同复用状态下的运行会撞同一个 run_id，破坏可复现性 |
| 聚类只认"裁定字典列表"（must-link / cannot-link） | `clustering.py:70` | 复用决策只要适配成同一字典形状即可零改动进入聚类 |
| 复核队列按"本批次人工裁定"过滤 | `review.py:33-56` | 复用命中的对必须从队列过滤，否则省不下人力 |
| 单进程 SQLite + WAL + 全局 RLock | `storage.py:108-114` | 并发模型是进程内多线程，锁方案必须在此框架内，不引入新锁 |

**根因**：裁定是"批次私有、键值批次相关"的。方案的核心是把裁定翻译成"内容寻址、跨批全局"的形式沉淀下来，并在新运行中以确定性规则回放。

## 2. 设计目标与约束对照

| 需求 | 对应设计 |
|---|---|
| 独立方案、不侵入主流程 | sidecar 模块 `citefuse/reuse/`，主链路仅 1 个适配器 hook（§4.5），禁用/空池时行为与现状逐字节一致 |
| 对接四个模块 | §4.1–§4.4 交互契约 |
| 决策沉淀存储结构 | §5 DDL（`decision_pool` 等 5 张新表，幂等建表） |
| 复用命中规则 | §6（pair 级精确命中 + 字段级见证命中） |
| 冲突判定逻辑 | §7 仲裁矩阵 |
| 整条 vs 字段级取舍 | §8（推荐：整条为骨干，字段级默认关闭） |
| 并发锁竞争 | §9（watermark 谓词读 + 单写者 + 幂等 upsert，零新锁） |
| 冷启动 | §10.1（空池 ≡ 原流程） |
| 默认自动降级 | §10.2（复用层任何异常 → 记审计事件 → 空清单 → 走原流程，绝不中断运行） |
| 完全确定性 | §11（无随机、无网络、全序排序、时钟只写元数据） |
| 边界场景 | §12（冲突决策、字段缺失、决策失效） |
| 资源上限 | §13（配置化硬上限，超限确定性截断） |
| ≥30% 且准确率不降 | §14（重放测量法 + 准确率门禁） |

## 3. 总体架构

```
                ┌──────────────────────── 现有主链路（不改动算法）────────────────────────┐
                │                                                                        │
 POST /batches ─▶ ingest ─▶ records ─▶ blocking ─▶ scoring ─▶ clustering ─▶ golden ─▶ runs│
                │               ▲                                   ▲              │     │
                └───────────────┼───────────────────────────────────┼──────────────┼─────┘
                                │ (4.1 只读契约)                     │ (4.2 只读契约)│ (4.4 归档契约)
                        ┌───────┴───────────────────────────────────┴──────────────┴─────┐
                        │                 citefuse/reuse/（新增 sidecar）                  │
                        │  fingerprints.py  pool.py  collect.py  entity.py  backfill.py  │
                        │                                                                │
                        │   decision_pool │ reuse_entities* │ reuse_manifest │ reuse_meta │
                        └───────▲───────────────────────────────────▲────────────────────┘
                                │ (4.3a 队列过滤契约)                │ (4.3b 裁定沉淀契约)
                          GET /review/next                    POST /review/decisions
```

原则：

1. **算法模块零改动**：`blocking.py` / `scoring.py` / `clustering.py` / `similarity.py` / `survivorship.py` / `ingest.py` 不修改一行。
2. **唯一的侵入点**是 `pipeline.py` 中约 10 行的适配器调用 + `review.py` 的队列过滤与沉淀写入，全部走"空实现即原行为"的降级路径。
3. **复用决策不写入 `decisions` 表**：该表保持"纯人工"语义；复用条目以合成裁定字典在内存中并入 `history`，并单独落 `reuse_manifest` 供审计与队列过滤。理由见 §8.3 被否决方案。

## 4. 与四个模块的交互契约

### 4.1 题录摄入校验（ingest）——只读契约

- 复用层通过既有 `Storage.list_records(batch_id)` 读取记录，**不修改** `records` / `dead_letters`，摄入幂等逻辑（`input_hash`）不变。
- 记录指纹在复用层惰性计算（仅对当次候选对涉及的记录），摄入路径零开销。
- 死信记录无 `norm_json`，永不进入决策池——只有校验通过且归一化成功的记录才有指纹。
- 归一化配置指纹 `norm_version = sha256(canonical_json(config.normalization))` 在池写入时记录、命中时校验：归一化配置变更后旧沉淀自动不可见（决策失效，见 §12.3）。

### 4.2 候选对生成与打分（blocking/scoring）——只读契约

- 复用层位于 **scoring 之后、clustering 之前**，消费现有 `scored_pairs` 列表（`pair_key, rec_a_pk, rec_b_pk, score, zone, details`）。
- 候选召回完全不变：复用从不增删候选对，只对灰区/全量对做"重分类"。
- 字段级复用需要的分项证据直接取自 `pairs.details_json` 已有的 `raw_score/score/weight/contribution/status`，不新增打分逻辑。

### 4.3 人工复核流转（review）——双向契约

**(a) 队列过滤**：`next_review_pairs` 在现有人工裁定过滤之外，追加排除"最近一次 run 的 `reuse_manifest` 中 action ∈ {applied_same, applied_different, field_promoted} 的 pair_key"。读的是已落库清单，不重算、无分歧。

**(b) 裁定沉淀**：`submit_decision` 在既有 `add_decision` 成功后，向 `decision_pool` 追加一条内容寻址条目（`INSERT OR IGNORE`，唯一键 `(pair_fp, source_decision_id)`，重试幂等）。沉淀失败只记 `pool_append_failed` 审计事件，**不影响**人工裁定本身的落库与自动重跑——失败方向是"下次少复用一条"，永远不是"错误复用"。

**(c) 裁定历史 API 不变**：`GET /review/decisions` 仍只返回人工裁定；复用轨迹走新增的 `GET /reuse/manifest?run_id=`（纯增量端点）。

### 4.4 运行快照归档（run archiving）——读写契约

- `run_id` 派生材料中的裁定指纹改为对 **effective_history = 人工裁定 + 复用裁定** 计算。复用条目携带确定性的 `decision_id = -pool_id`（负值域与人工自增主键永不冲突）与原始 `decided_at`，因此同一（批次, 配置, 人工裁定, 池水位）输入必得同一 `run_id`、同一批 pairs/edges/clusters/golden 行。
- 每次运行在 persistence 阶段同事务写入 `reuse_manifest`（先删后插，镜像 `replace_pairs` 模式），并向 `audit_events` 写 `reuse_summary` 事件（水位、各类命中/否决计数）。
- `DELETE /batches/{id}` 级联删除该批次的 runs/manifest（FK `ON DELETE CASCADE`）；**`decision_pool` 与实体表是全局资产，不随批次删除**（`source_batch` 仅为溯源字符串，非外键）。
- 复用层在 `run_attempts` 语义之外不新增"尝试"概念；重跑幂等性由既有 `run_id` 机制保证。

### 4.5 pipeline.py 唯一 hook（改动前后）

```python
# 现状（pipeline.py:55-56）
history = decision_dicts(storage, batch_id)
run_id = _run_id(batch, config, history)

# 设计后（伪代码，约 10 行；run_id 派生位置下移到打分之后）
history = decision_dicts(storage, batch_id)
... blocking / scoring 不变 ...
reuse = collect_reuse(storage, config, batch, records, scored_pairs, history, clock)
history = history + reuse.extra_decisions        # 复用裁定并入（decision_id 为负）
model_edges = model_edges + reuse.promoted_edges  # 字段级晋升边（默认关闭时为空）
run_id = _run_id(batch, config, history)          # 同一函数，输入更宽
... clustering / golden 不变 ...
storage.replace_reuse_manifest(run_id, reuse.manifest)   # 与 replace_pairs 同阶段
record_event(..., "reuse_summary", reuse.summary)        # 审计
```

`collect_reuse` 内部整体包裹 fail-safe（§10.2）。`reuse.enabled=false` 或空池时返回空结构，`history` 与现状完全一致 → run_id 与禁用状态一致（同一配置文件下）。

## 5. 决策沉淀存储结构

### 5.1 指纹定义（确定性核心）

```python
rec_fp  = sha256(canonical_json({"source": record.source, "norm": record.norm}))
pair_fp = sha256(f"{min(rec_fp_a, rec_fp_b)}||{max(rec_fp_a, rec_fp_b)}")
norm_version = sha256(canonical_json(config.normalization.model_dump(mode="json")))
```

- `canonical_json` 复用现有 `storage.json_dumps`（`sort_keys, separators, ensure_ascii=False`）。
- 指纹覆盖**归一化后内容 + 来源**，不覆盖 record_id：同一记录换 ID 重导出仍可命中；大小写/标点/全半角噪声被归一化折叠，不影响命中。
- 不含 `raw_json`：打分与裁定证据都基于归一值，raw 差异不构成新证据。
- 含 `source`：保守策略，跨来源的同内容对不自动复用（留给人工；可作为后续可配置放宽项，见 §14）。

### 5.2 DDL（追加进 `storage.SCHEMA`，全部 `IF NOT EXISTS`，对既有库文件幂等）

```sql
-- 全局决策池：内容寻址的人工裁定沉淀（跨批次）
CREATE TABLE IF NOT EXISTS decision_pool (
  pool_id INTEGER PRIMARY KEY AUTOINCREMENT,
  pair_fp TEXT NOT NULL,                -- 对指纹（§5.1）
  rec_a_fp TEXT NOT NULL, rec_b_fp TEXT NOT NULL,
  rec_a_key TEXT NOT NULL, rec_b_key TEXT NOT NULL,   -- 裁定时 source::record_id（溯源展示用）
  decision TEXT NOT NULL CHECK(decision IN ('same','different')),
  reviewer TEXT NOT NULL, note TEXT NOT NULL DEFAULT '',
  source_batch TEXT NOT NULL,           -- 溯源，非外键（批次可删，池保留）
  source_decision_id INTEGER NOT NULL,  -- 对应 decisions.decision_id
  decided_at TEXT NOT NULL,             -- 复制自 decisions.submitted_at
  norm_version TEXT NOT NULL,           -- 归一化配置指纹（失效判定）
  schema_version INTEGER NOT NULL DEFAULT 1,
  UNIQUE(pair_fp, source_decision_id)   -- 回填/重试幂等
);
CREATE INDEX IF NOT EXISTS idx_pool_pair_fp ON decision_pool(pair_fp);
CREATE INDEX IF NOT EXISTS idx_pool_member  ON decision_pool(rec_a_fp, rec_b_fp);

-- 字段级复用：人审确认实体的成员与字段见证值（§6.2）
CREATE TABLE IF NOT EXISTS reuse_entities (
  entity_fp TEXT PRIMARY KEY,           -- sha256(排序后的成员 rec_fp 列表)
  entity_seq INTEGER NOT NULL,          -- 单调序号（reuse_meta 计数器），新旧仲裁依据
  basis_run_id TEXT NOT NULL,           -- 产生该实体的运行
  norm_version TEXT NOT NULL,
  member_count INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reuse_entity_members (
  entity_fp TEXT NOT NULL, member_fp TEXT NOT NULL, entity_seq INTEGER NOT NULL,
  PRIMARY KEY(entity_fp, member_fp)
);
CREATE INDEX IF NOT EXISTS idx_rem_member ON reuse_entity_members(member_fp);
CREATE TABLE IF NOT EXISTS reuse_entity_values (
  entity_fp TEXT NOT NULL, field TEXT NOT NULL,
  value_hash TEXT NOT NULL,             -- sha256(规范化值的规范编码)，规避长标题
  value_preview TEXT NOT NULL,          -- 截断 120 字符，仅供排查
  PRIMARY KEY(entity_fp, field, value_hash)
);

-- 每次运行的复用清单（审计 + 队列过滤的唯一事实源）
CREATE TABLE IF NOT EXISTS reuse_manifest (
  run_id TEXT NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
  pair_key TEXT NOT NULL,
  action TEXT NOT NULL,   -- applied_same|applied_different|field_promoted|overridden_by_human|vetoed
  pool_id INTEGER, entity_fp TEXT,
  detail_json TEXT NOT NULL,            -- 原裁定、原始分/调整后分、被清字段等
  PRIMARY KEY(run_id, pair_key, action)
);

CREATE TABLE IF NOT EXISTS reuse_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
-- 用途：entity_seq 计数器、schema 版本标记
```

容量估算：池行约 300–500 B；10 万条人工裁定 ≈ 50 MB，本地单文件可承受。增长受人工吞吐量自然约束。

## 6. 复用命中规则

### 6.1 整条题录粒度复用（骨干，默认开启）

**触发范围**：当次运行的全部候选对（不止灰区——`same` 必须能拦住 reject 区，`different` 必须能否决 match 区，与人工裁定语义一致）。

**命中规则**（全部确定性）：

1. 读水位 `wm = SELECT COALESCE(MAX(pool_id),0) FROM decision_pool`（§9）。
2. 对每个候选对计算 `pair_fp`，分块（500/批）查询：
   `WHERE pair_fp IN (...) AND pool_id <= wm AND norm_version = ? AND schema_version = 1`。
3. 同一 `pair_fp` 多条沉淀 → 按 `(decided_at, pool_id)` 全序取最新（池内仲裁，§7-R1）。
4. 该对在本批次**已有人工裁定** → 记 `overridden_by_human`，丢弃复用条目（§7-R2）。
5. 否则生成合成裁定字典并入 `history`：
   `{pair_key, rec_a_key, rec_b_key（均为本批次当前记录的键）, decision, reviewer: "reuse:<原reviewer>", note: 原note, submitted_at: 原decided_at, decision_id: -pool_id}`。
6. 写 `reuse_manifest`（applied_same / applied_different）。

关键点：合成裁定的 `rec_a_key/rec_b_key` 用**本批次**记录键重新填充（`clustering.py:107` 靠它定位记录），指纹只负责"找到历史裁定"，落地用当前键——跨批次 record_id 漂移由此解决。

### 6.2 字段级拆分复用（增强，默认关闭，`reuse.field_level.enabled`）

**实体沉淀**（每次运行完成后，post-run 步骤）：对本运行中含 ≥1 条 `human_same` 边（含复用同源裁定）的簇，计算 `entity_fp`，按字段收集簇内全部归一化值写入 `reuse_entity_values`（成员数 >64 或单字段值数 >32 的簇整体跳过并记审计，§13）。`entity_seq` 单调递增，作为新旧实体的仲裁依据。

**命中规则**（仅灰区对、仅晋升方向）：

1. 对无 pair 级命中的灰区对 (X, Y)，查 X 的 `rec_fp` 所属实体；多个实体含 X 时取 `entity_seq` 最大者（§7-R5）。
2. 计算差异字段集 D = {双方均非缺失且归一值不同的字段}（缺失字段不算差异，走 `missing_policy`，§12.2）。
3. **见证检查**：D 中每个字段，Y 的值哈希 ∈ 该实体的字段见证值集。
4. **否决守卫**（任一命中即放弃晋升，记 `vetoed`）：
   - 池中存在 (fp_X, fp_Y) 的 `different`；
   - 池中存在 Y 与该实体**任一成员**的 `different`；
   - 池中存在该实体**任意两成员之间**的 `different`（实体已被拆分，见证集作废，§12.3）。
5. 全部通过 → 按 `adjusted = total + Σ_{f∈D} w_f·(1 − eff_f)/denom` 重算总分（数据取自 `details_json`，纯算术），**仅当** `adjusted ≥ match_threshold` 且原始分 ≥ `review_low` 时晋升：该对作为 `kind=model` 边进入聚类，`details` 标记 `{reused:"field", entity_fp, cleared_fields, original_score, adjusted_score}`。
6. 字段级**永不自动判异**、永不处理 reject 区、每运行晋升数硬上限（§13）。

### 6.3 为什么 pair 级用"完全一致"而不是"相似"

模糊相似命中会把"实体解析"这个待解决问题递归引入复用层自身，且不可解释、无法保证准确率不降。完全一致命中 = "同一个人对同一份证据的裁定原样回放"，复用精度 = 人工精度，这是"准确率不下降"论证的根基（§14.2）。未命中的近重复对只是回到原流程，代价为零。

## 7. 冲突仲裁逻辑（仲裁矩阵）

| # | 冲突场景 | 仲裁规则 | 审计 |
|---|---|---|---|
| R1 | 池内同 `pair_fp` 多条异向裁定 | 按 `(decided_at, pool_id)` 取最新；旧条目保留不删 | manifest 记 `superseded`，运行事件 `reuse_conflict_resolved` |
| R2 | 复用裁定 vs 本批次人工裁定 | **人工永远胜**：适配器在合并前丢弃该 pair_key 的复用条目（不依赖时间戳比较，对注入时钟免疫） | `overridden_by_human` |
| R3 | 复用 `same` vs 复用/人工 `different`（传递性） | 复用条目以裁定字典形态进入 `cluster_records`，完全复用现有 must-link/cannot-link 与"新裁定胜"语义；`decision_id=-pool_id` 保证复用条目间全序 | 复用现有 `review_conflict` 事件 |
| R4 | 字段级晋升 vs cannot-link | 三条否决守卫（§6.2-4），cannot-link 永远优先于晋升 | `vetoed` |
| R5 | 记录同时属于多个历史实体 | 取 `entity_seq` 最大者（最新证据）；实体表只增不删，旧实体被新实体自然遮蔽 | — |
| R6 | 同 `pair_fp` 命中多条且 `decided_at` 相同（注入时钟/回填） | `pool_id` 大者胜（自增主键是全序兜底） | — |
| R7 | 同批次两条记录指纹相同（同 source 同内容不同 record_id） | 查询由"当前候选对"驱动而非由池条目驱动，天然无歧义；实体成员检查按记录键排序取确定性集合 | — |

所有仲裁只依赖存储内容（`decided_at`、`pool_id`、`entity_seq`、自增序），不依赖墙钟、不依赖随机源。

## 8. 架构级取舍：整条复用 vs 字段级拆分复用

### 8.1 对比

| 维度 | 整条题录复用（Route A） | 字段级拆分复用（Route B） |
|---|---|---|
| 复用单元 | `pair_fp`（双记录归一内容+来源） | （实体， 字段， 见证值） 三元组 |
| 命中条件 | 内容完全一致 | 差异字段值均在已确认实体内被见证 |
| 预期复用率 | 中（= 批次间内容级重合率） | 更高（容忍单字段漂移：新导出方补了 DOI、改了页码格式等） |
| 误复用风险 | 极低：同人、同证据、同结论 | 中：实体污染、拆分滞后、值组合外推 |
| 准确率保障 | 结构性保证（§6.3） | 需否决守卫+上限+默认关闭+灰度验证 |
| 存储/计算 | O（裁定数），行小 | O（实体×字段×值），需硬上限 |
| 可解释性 | manifest 一行直指原裁定 | 需展示实体证据链 |
| 冷启动价值 | 第一批裁定即可复用 | 需先积累人审确认实体 |
| 对主链路语义影响 | 等价于"人工提前裁定" | 改变打分结果（晋升），需额外审计面 |

### 8.2 推荐路线

**两阶段混合，A 为骨干、B 为受限增强**：

- **Phase 1（必须）**：仅 Route A。在"批次高度重合"的前提下，仅精确命中就应覆盖大部分重复灰区；先落地、先测量（§14.3 的可行性检查可提前验证 30% 前提）。
- **Phase 2（可选，默认关闭）**：Route B 以 `reuse.field_level.enabled` 灰度开启，仅晋升方向、仅灰区、带三重否决守卫与每运行上限。它直接回应"同一实体同类字段差异的历史裁定自动生效"这一诉求，但只有在 Phase 1 的度量基线建立后才允许开启，且开启前后必须跑 §14.2 的准确率门禁。

接口设计上 A/B 共用 `decision_pool` 与 collect 适配器，B 只是 collect 内部的第二段逻辑与两张实体表——先 A 后 B 不需要返工。

### 8.3 被否决的备选方案

| 方案 | 否决理由 |
|---|---|
| 复用条目物化进 `decisions` 表 | 污染人工裁定语义；AUTOINCREMENT 主键使重物化后 `run_id` 指纹漂移；批次删除级联会抹掉复用历史 |
| 全局字段等价规则（"JFES≡Journal of..." 不分实体） | 脱离实体上下文的等价关系会全局污染打分，误复用风险不可控 |
| 按 record_id 注册跨批身份表 | 新批次换 ID 即失效；被 `rec_fp` 内容寻址完全覆盖 |
| 模糊相似度命中 | 把实体解析问题递归引入复用层，无法论证准确率不降（§6.3） |
| 复用 `same` 只作用于灰区 | 与人工 must-link 语义不一致（人工裁定本就无视分区），且配置漂移时会产生裁定"失效"假象 |

## 9. 并发与锁竞争

现有模型：单进程、单连接、`Storage._lock`（RLock）串行化全部读写、WAL 模式。方案在此框架内，**不引入任何新锁**：

1. **读一致性——watermark 谓词**：collect 先读 `wm = MAX(pool_id)`，随后所有池查询携带 `AND pool_id <= wm`（实体表同理携带 `entity_seq <= seq_wm`）。并发提交的沉淀（pool_id > wm）对本次运行不可见——无需长事务、无需快照隔离，单次运行内部完全确定。
2. **写串行化**：沉淀写入、实体重建、manifest 落库全部走既有 `Storage.execute/transaction`（进程内 RLock 串行）；WAL 下读不阻塞写。
3. **幂等 upsert**：池 `UNIQUE(pair_fp, source_decision_id)` + `INSERT OR IGNORE`；manifest 先删后插；实体表 `INSERT OR REPLACE`。并发重试、重复回填、并发同 run_id 重跑全部幂等收敛。
4. **并发同 run_id**：两个线程同时跑同批次且水位相同 → 算出同一 run_id、写入完全相同的内容（replace 语义），结果正确；水位不同 → 不同 run_id，各自成行，互不覆盖。
5. **锁顺序**：只有 `Storage._lock` 一把锁，无锁序问题、无死锁面。
6. **未来多进程预案**（当前不需要）：`PRAGMA busy_timeout=5000`、写事务 `BEGIN IMMEDIATE`、唯一约束兜底；collect 的 watermark 模式在多进程下依然成立。

## 10. 冷启动与降级策略

### 10.1 冷启动（无历史沉淀）

- 空池：`wm=0`，所有查询返回空 → collect 返回空结构 → `history` 与现状逐字节一致 → **run_id、队列、聚类结果与未启用复用时完全相同**。
- 冷启动即自动降级，无需任何特判分支。
- **历史回填**（可选但强烈建议）：`python -m citefuse.reuse.backfill` 离线重放既有 `decisions` 与各 run 的簇结构，幂等填充 `decision_pool` 与实体表（`INSERT OR IGNORE` / 清空重建实体表），让第一天就有沉淀可用。回填是 §14.3 可行性测量的前置步骤。

### 10.2 异常降级矩阵（默认动作永远是"回退原流程、不中断运行"）

| 异常 | 检测点 | 降级行为 | 审计事件 |
|---|---|---|---|
| 池表缺失/损坏/SQLite 错误 | collect 整体 try/except `Exception` | 返回空结构，运行继续 | `reuse_degraded{error}` |
| 沉淀追加失败（submit 时） | 池写入 try/except | 人工裁定正常落库+重跑，仅损失未来复用 | `pool_append_failed` |
| 查询结果超上限 | 行数检查 | 按 `pair_fp` 升序确定性截断 | `reuse_truncated{cap}` |
| `norm_version` 不匹配 | 查询谓词 | 条目不可见（非错误，配置回滚后自动恢复） | 计数进 `reuse_summary` |
| 实体见证矛盾（拆分后） | 否决守卫 | 放弃晋升，该对回灰区人工 | `vetoed` |
| 复用 vs 人工冲突 | 适配器预过滤 | 人工胜（R2） | `overridden_by_human` |
| 时钟异常（`decided_at` 并列） | 仲裁排序 | `pool_id` 兜底全序（R6） | — |
| `reuse.enabled=false` | 配置 | 适配器不构造，零代码路径 | — |
| 回填于空库 | backfill | 空遍历，成功退出 | — |

## 11. 确定性论证

1. **无随机源**：全部逻辑为哈希、排序、集合运算；不引入 `random`、不读时钟做判定（时钟仅写 `created_at` 元数据，不参与命中/仲裁）。
2. **全序排序**：池仲裁 `(decided_at, pool_id)`；截断按 `pair_fp`；实体仲裁 `entity_seq`；合成裁定 `decision_id=-pool_id` 进入现有 `(submitted_at, decision_id)` 排序——所有比较都有全序兜底。
3. **指纹确定性**：`canonical_json` 与现有 `json_dumps` 同源（sort_keys/separators），sha256 跨进程稳定。
4. **水位确定性**：同一（批次内容， 配置， 人工裁定， 池水位， 实体水位）五元组 → 同一 manifest → 同一 `run_id` → 同一 pairs/edges/clusters/golden。可复现性从"批次+配置+裁定"三元组显式扩展为五元组，清单落库使每次运行的输入可审计、可重放。
5. **离线**：零网络调用，零新依赖（纯 stdlib + 现有 SQLite）。

## 12. 边界场景处理

### 12.1 冲突决策

按 §7 矩阵执行；补充：人工推翻一条曾被复用的裁定时，新裁定经 §4.3(b) 沉淀入池，`decided_at` 更新、`pool_id` 更大 → 此后所有运行按 R1 用新裁定，旧复用自动失效，无需清理作业。

### 12.2 字段缺失

- 缺失字段参与指纹（`null` 是规范 JSON 的一部分）：双方同缺失 → 指纹照常命中；一方新增字段（如补了 DOI）→ 指纹变化 → pair 级不命中，**正确**地回到人工（证据已变，人可能改判）。
- 字段级：缺失不构成"差异"，不参与见证检查，仍走 `missing_policy`（neutral/exclude）——复用层不改变缺失语义。
- 归一化失败（死信）记录无指纹，永不入池。

### 12.3 决策失效

| 失效触发 | 机制 | 恢复 |
|---|---|---|
| 归一化配置变更 | `norm_version` 谓词使旧条目不可见 | 配置回滚即恢复；或重跑回填重建 |
| 打分配置变更（权重/阈值） | pair 级裁定仍有效（人工判断与权重无关）；字段级晋升按新阈值实时重算，自然适配 | 无需处理 |
| 人工反裁定 | R1 最新胜 | 自动 |
| 实体被 `different` 拆分 | 否决守卫 §6.2-4 第三条使旧实体见证集作废；新运行以更高 `entity_seq` 写入新实体遮蔽旧实体（R5） | 自动 |
| 批次删除 | 池与实体表保留（全局资产），manifest 随 run 级联删除 | 设计如此，文档化 |
| 池 schema 升级 | `schema_version` 谓词隔离旧行 | 迁移脚本重建 |

## 13. 资源占用上限（`reuse.limits`，超限一律确定性截断+审计，不报错）

| 配置项 | 默认 | 约束对象 |
|---|---|---|
| `max_pairs_per_collect` | 20 000 | 单次运行参与指纹查询的候选对数 |
| `query_chunk` | 500 | 单条 IN 查询的指纹数（SQLite 变量上限余量） |
| `max_reuse_per_run` | 10 000 | 单运行 pair 级复用条目数 |
| `max_field_promotions_per_run` | 2 000 | 单运行字段级晋升数 |
| `entity_max_members` | 64 | 实体成员数（约束否决守卫 O(k²) 检查 ≤2016 对） |
| `entity_max_values_per_field` | 32 | 单实体单字段见证值数 |
| `entity_rebuild_per_run` | 5 000 | 单运行重建的实体数 |

复杂度：collect 为 O（候选对数） 次索引点查 + O(P log P) 排序，相对打分阶段（标题 JW O(n·m)）可忽略；实体重建仅触及含人工边的簇。池规模受人工吞吐量自然约束；保留策略默认"永久保留"（删除会破坏水位可复现性），如未来需要修剪，须以独立管理操作进行并记录审计。

## 14. 验收与度量

### 14.1 复核量下降度量（≥30% 的判定式）

```
下降率 = 1 − |启用复用后进入人工队列的灰区对| / |不启用复用时进入人工队列的灰区对|
```

分子分母在同一批历史批次、同一人工裁定输入下重放获得；复用命中数直接来自 `reuse_manifest`（applied_same + applied_different + field_promoted），队列数来自 `pairs(zone='review')` 差集，全部可 SQL 复算。

### 14.2 准确率门禁（不下降的判定式）

- 同一重放语料上：`pairwise F1（启用复用) ≥ pairwise F1（不启用）`，零容忍。
- Phase 1（仅 pair 级）结构上满足：复用 = 同人对同证据的裁定回放，唯一变化是"不再重复问"。
- Phase 2 开启字段级前后必须各跑一次门禁；字段级晋升单独出报表（晋升数、被金标证伪率）。

### 14.3 可行性预检（一行 SQL，先于任何实施）

回填后执行：统计最近若干批次灰区对的 `pair_fp` 在池中的命中率。若历史内容级重合度本身 <30%，则 30% 目标在该数据形态下不可达，需先放宽指纹策略（如去掉 `source` 分量，§5.1 已留配置口）——**用数据验证前提，而不是承诺数字**。

### 14.4 测试清单（新增，全部确定性、可用 FixedClock）

- 指纹：规范 JSON 稳定性、source 分量、缺字段、跨进程一致。
- 池仲裁：R1/R6 全序；R2 人工胜（含 FixedClock 等时戳）。
- 端到端：批次 B1 裁定 → 批次 B2（同内容换 record_id）复用命中 → 队列缩短、聚类含 must-link/cannot-link 效果、run_id 稳定、重跑幂等。
- 冷启动：空池 run_id ≡ 禁用复用的 run_id（同配置）。
- 降级：删池表后运行完成且有 `reuse_degraded`；沉淀失败不影响裁定落库。
- 失效：`norm_version` 变更后旧条目不可见、回滚恢复。
- 并发：多线程并发 submit + run，结果收敛、无 SQLITE_BUSY、manifest 一致。
- 字段级：晋升、三类否决守卫、上限截断、实体拆分后见证作废。
- 回填：幂等（两次回填结果一致）、空库空遍历。

## 15. 分阶段落地计划

| 阶段 | 内容 | 验收 |
|---|---|---|
| P0 预检 | 回填脚本 + 命中率测量 SQL | 用真实历史数据验证 30% 前提 |
| P1 骨干 | 5 张表 DDL、指纹、池读写、collect（仅 pair 级）、pipeline/review hook、manifest、审计事件、§14.4 测试（除字段级） | 重放测量下降率 ≥30%、F1 门禁通过、冷启动等价 |
| P2 增强 | 实体表重建、字段级晋升（默认关闭）、否决守卫、门禁复测 | 灰度开启后 F1 不降、晋升可解释 |
| P3 运营化 | `GET /reuse/manifest`、复用统计报表、（可选）跨来源指纹放宽评估 | 复核抽样确认误复用率 |

预计代码量：新增 `citefuse/reuse/` 约 600–800 行；既有文件改动 ≤60 行（`storage.py` DDL、`config.py` 配置节、`pipeline.py` hook、`review.py` 两处、`api.py` 可选端点），算法模块零改动。
