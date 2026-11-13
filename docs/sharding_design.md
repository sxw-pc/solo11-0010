# 大规模题录熔合：内存防控与分片处理方案设计

> 适用对象：`src/citefuse`（citefuse 纯本地离线题录熔合引擎，Python 3.11 / SQLite / Pydantic / FastAPI）
> 约束：全程离线、无外部服务、无新增重依赖；百万级题录运行期内存 ≤ 本机可用内存 70%；千/万级小任务延迟涨幅 ≤ 10%；分片结果与全量一次性处理**逐行一致**；配置驱动、可审计、可断点续跑、临时资源自动回收。
> 默认参数按 8 GB 内存主流设备标定。

---

## 0. 结论速览（先给答案）

| 决策点 | 结论 |
|---|---|
| 分片维度选型 | **阻塞键哈希分片（blocking-key hash）+ 记录复制 + 大桶二级分裂**，淘汰「来源分片」「时间分片」 |
| 分片边界放在哪 | 只切分 **blocking → scoring（候选对生成与打分）** 这一内存热点；归一化、聚类（并查集）、金记录仍走全局确定性流程，改为流式/分页喂数 |
| 小任务保护 | `sharding.mode=auto`：低于内存预算阈值直接走现有 `run_pipeline` 原路径，零分片开销 |
| 确定性如何保证 | 分片只改变"在哪里算"，不改变"算哪些对、按什么顺序聚合"；`run_id` 与 `cluster_id` 派生规则、排序键、阈值、幸存规则全部不动 |
| 断点续跑 | 新增 `shard_manifests` / `shard_tasks` 两张状态表 + 落盘分片文件，按分片幂等续跑，已完成分片不重算 |
| 内存控制 | 离线内存探测 + 采样估算单条/单对占用 → 反算分片数与每片行数上限，运行期 RSS 软熔断；上限默认 = 可用内存 × 70% |
| 新增依赖 | **零新增第三方依赖**（仅用标准库 `hashlib`、`mmap`、`resource`、`tempfile`、`sqlite3`） |

---

## 1. 现状与问题定位

### 1.1 现有管道的内存模型

当前一次 `run_pipeline`（`src/citefuse/pipeline.py:43`）的调用链与内存特征：

| 阶段 | 现有实现 | 内存放大点 |
|---|---|---|
| 加载 | `_load_records` 一次性 `list_records` 全量反序列化（`pipeline.py:39`） | 每条记录同时持有 `raw_json` + `norm_json` 两份 Python dict，十万级即 GB 级常驻 |
| 阻塞 | `generate_candidates`（`blocking.py:71`）全量 `sorted(records)` + 三个 `defaultdict(list)` 桶 + `by_pair` 字典 + `lookup` 字典 | 桶内 `combinations` 最坏 O(n²)，候选对以 Python 对象 `CandidatePair` 全量驻留 |
| 打分 | 对全部候选对串行 `score_pair`，结果追加到 `scored_pairs` 列表（`pipeline.py:71-83`） | 每个 pair 一份含完整分项 `details` 的 dict，候选对百万级时是最大单项开销 |
| 聚类 | `cluster_records`（`clustering.py:70`）并查集 + `by_pk`/`by_key` 双字典 | 并查集本身近线性、很轻；重的是为查 key 又建了两份全量记录字典 |
| 金记录 | 对每个簇 `build_golden(group)`，`groups` 持有全量 Record 引用 | 与前序共享对象，增量不大，但峰值叠加 |
| 落库 | `replace_pairs/edges/clusters`（`repository.py:90/114/129`）先 `DELETE` 再 `executemany` 整列表 | 入参本身就是全量 list，落库前后峰值不释放 |

**根因**：管道是"全量加载 → 全量候选 → 全量打分 → 一次性落库"的单进程驻留模型，内存与候选对数量（而非记录数）近似线性甚至平方增长。十万级记录若阻塞区分辨力不足，候选对可达千万级，进程被 OOM killer 杀掉即表现为"异常退出"。

### 1.2 不可破坏的现有契约（方案的硬边界）

通读代码后，以下契约必须原样保留，方案围绕它们做"外挂式"编排：

1. **确定性身份**
   - `run_id = sha256(batch_id | input_hash | config.fingerprint() | decision_fp)`（`pipeline.py:20-31`），与数据怎么分、分几片**无关**。
   - `cluster_id = sha256(batch_id | 排序后的 member_keys)`（`pipeline.py:34-36`）。
   - pair key 由 `make_pair_key` 按 `(record_id, source)` 字典序规范化（`models.py:141`）。
2. **候选对语义**：三策略（year_venue / title_signature / author_year）各自分桶、取并集、按 `(left_pk,right_pk)` 去重、按 pair_key 排序（`blocking.py:71-119`）。`blocking_stats.strategy_counts` 与 `union_count` 是对外审计字段，分片后必须能还原出与全量完全相同的数值。
3. **打分语义**：`score_pair` 纯函数，输入两条 Record + config，输出 `score/zone/details`（`pipeline.py:73`），无任何全局状态 → **天然可按对独立并行/分片，且结果与执行位置无关**。
4. **聚类语义**：人工 same/different 裁定（最新者胜、禁连边传递校验）+ 模型边按 `(-score, pair_key)` 排序入并查集（`clustering.py:138-151`）。这是**全局算子**，不能按记录分片各聚各的，否则跨片同实体会被拆成两个簇。
5. **金记录语义**：簇内字段级幸存规则，平票按 `source_priority → record key`（`survivorship.py`，README §6）。
6. **配置指纹**：`AppConfig.fingerprint()` 对规范化 JSON 取 SHA-256（`config.py:145`），写入 `runs.config_hash`。
7. **存储契约**：SQLite 单文件、WAL、参数化 SQL、表结构固定（`storage.py:18-96`）；对外 API（`api.py` 路由、`api_views.py` 出参字段）不变。
8. **审计契约**：每次尝试追加 `run_attempts`，事件写 `audit_events`，时间走可注入 `Clock`（测试用 `FixedClock`），JSON 一律 `sort_keys=True`。

> 关键推论：**打分是无状态纯函数、聚类是必须全局的轻量算子**。因此正确的分片切面是"候选对生成 + 打分"，而不是"记录各管一片、各聚各的"。这一判断直接决定了第 2 节的选型。

---

## 2. 三种分片维度对比与选型

### 2.1 统一评估口径

- **实现成本**：改动面、是否需要复制记录、是否要改现有核心函数签名。
- **负载均衡**：数据倾斜（某来源/某年/某热门标题占比过高）时各分片体量是否均匀，决定 OOM 风险是否被真正消除。
- **跨分片匹配逻辑复杂度**：同一条真实实体的多条重复记录若落在不同分片，是否还能被候选对生成"碰到一起"。这是实体解析分片与普通 ETL 分片的本质区别。
- **异常恢复效率**：单分片损坏/失败时的重放粒度、是否牵连全量。
- **确定性影响**：是否容易保持与全量处理逐行一致。

### 2.2 三方案对比

| 维度 | 哈希分片（记录键 hash） | 来源分片（按 source） | 时间分片（按 year） |
|---|---|---|---|
| 切分依据 | `hash(record_key) % N` | `record.source` | `record.year` |
| 实现成本 | 低：一次取模即可分文件 | 最低：来源本就是字段，零计算 | 低：按年份分桶 |
| 负载均衡 | **好**：键空间均匀，片大小近似相等 | **差**：各来源体量天然不均，大来源单独成片仍是 OOM 点；且片数受来源数硬上限 | **差**：年份分布严重倾斜（近年文献占绝大多数），热门年份单桶爆炸；缺年份记录无处可去 |
| 跨分片匹配 | 直接按记录键切会把**同一实体的重复记录切到不同片**，片内 blocking 永远碰不到它们 → 必须额外做"按 blocking key 重分发（shuffle）"，退化为 2.3 的阻塞键哈希方案 | **致命**：citefuse 的核心场景就是跨来源去重（同一篇文献被 ORBIT/LANTERN/MEADOW/COMPASS 同时收录）。来源分片后重复对 100% 跨片，片内 blocking 召回率直接塌陷，需要全量跨片两两探测，等于没分 | **致命**：同一文献不同来源年份常差 0–2 年（打分模型本就容忍 1–2 年差，见 `year_diff_scores`），time 分片会把它们切开；且 year_venue / author_year 阻塞键都含 year，恢复需跨邻近年份多片复制，边界复杂且不确定 |
| 异常恢复 | 片与来源/年份解耦，单片重放不影响其他片，粒度好 | 重放某来源即可，粒度尚可，但失败的往往正是最大那个来源 | 重放某年即可，但热门年份分片本身就是失败高发区 |
| 确定性 | 哈希函数固定、取模固定，分配可复现 | 来源枚举可复现 | 年份可复现，但边界复制规则易引入不确定性 |
| 适配本引擎 | 需升级为阻塞键哈希才正确（见 2.3） | **不适用**：与"跨来源熔合"的产品目标正面冲突 | **不适用**：与"容忍年份差异"的打分/阻塞模型冲突 |

### 2.3 选型：阻塞键哈希分片（blocking-key hash，含记录复制与大桶分裂）

直接按记录键哈希会漏掉跨片重复对。正确做法是**沿着引擎已有的 blocking 语义切**，让"可能成为候选对的记录"必然落在同一片：

**核心思想（两阶段）**

1. **规划阶段（plan，流式、低内存）**：流式扫描 `records` 表，对每条记录用现有三个阻塞函数（`blocking.py` 的 `_year_venue / _title_signature / _author_year`）算出它产生的 0..3 个阻塞键；对每个阻塞键 `bk` 计算 `shard = int(sha256(bk)[:8],16) % N`，产出"记录 → 它需要被复制到哪些分片"的分配清单（manifest）。
2. **执行阶段（worker，逐片）**：每个分片拿到它覆盖的全部阻塞键及其成员记录（同一条记录可因多个键被复制到多个分片），**在片内原样调用现有 `_group_pairs` + 三策略逻辑**生成候选对并立即 `score_pair` 打分，结果写分片级临时落库/落盘文件后释放内存。

**为什么结果与全量完全一致（等价性论证）**

- 现有 `generate_candidates` 的本质是：对每个策略，按阻塞键把记录分桶，桶内做 `combinations(members,2)`，最后三策略并集去重。
- 分片后：每个阻塞键桶**整体**（含其全部成员）被哈希到且只被分配到一个分片，桶内 `combinations` 在该片内完整执行 → **没有任何一个桶被拆散，候选对集合天然不重不漏**。
- 同一对记录可能因多个策略/多个键在不同分片重复出现，这与全量模式下"三策略并集、按 `(left_pk,right_pk)` 去重"是同一种重复；在合并阶段用与 `blocking.py:94-96` 完全相同的 `by_pair` 去重键（或直接靠 `pairs` 表 `PRIMARY KEY(run_id,pair_key)` 幂等 upsert）消重，得到的并集与全量逐对相同。
- `score_pair` 是纯函数，同一 pair 无论在哪个分片算，`score/zone/details` 字节级一致；合并时无需重算。
- 聚类仍在全局对"去重后的完整边集"执行，输入边集与全量模式相同 → 簇相同 → `cluster_id` 相同 → 金记录相同。

**倾斜处理（大桶二级分裂，保证 70% 内存红线）**

阻塞键哈希解决了片间均衡，但解决不了**单个阻塞键桶自身过大**（例如某年某大刊前缀命中数万条，桶内 pairs 是平方级）。规划阶段统计每个键的桶大小 `b`，对桶内候选对数 `b*(b-1)/2` 超阈值的键，不再整体投放，而是：

- 对该桶成员先按 `record.key` 排序（与 `_group_pairs` 内排序规则一致）；
- 用**条带化（striping）**枚举 pair：把成员按块切，枚举所有"块对 (i,j)（i≤j）"作为子任务，每个子任务只含两块成员及其 `combinations`，子任务再哈希到分片。因为只枚举成员对、不重排成员，覆盖的仍是同一组 `(left,right)` 对，等价性保持。
- 这样单个超级桶的平方爆炸被摊到多个子任务，任何单个任务的驻留内存都有上界。

**记录复制的代价与可接受性**

- 每条记录最多复制到 3 个分片（三个策略各一键，缺失字段的策略产键为 `None` 则不复制），平均远低于 3。复制的是**分片文件里的引用/紧凑行**，不是同时驻留内存的 Python 对象；磁盘占用有上界（约为 records 表的 ≤3 倍临时空间），且任务结束即删。
- 换取的是"不改 `score_pair`、不改三策略函数、不引入跨片两两探测"，实现成本和正确性风险都最低。

### 2.4 选型依据小结

> 选**阻塞键哈希分片**，因为它是唯一同时满足"候选对不重不漏（正确性）+ 片间天然均衡（哈希）+ 单桶可二次分裂（内存上界）+ 纯函数打分可独立执行（确定性）+ 失败重放粒度为片/子任务（恢复快）"的方案。
> 来源分片与跨来源去重目标冲突、时间分片与年份容忍模型冲突，二者均淘汰，仅可作为规划阶段的统计/诊断维度，不作为计算分片维度。

---

## 3. 总体架构与模块划分

### 3.1 分层

```
                         配置层 (config.py 新增 sharding 段，纳入校验)
                                      │
            ┌─────────────────────────┴──────────────────────────┐
            │              ShardOrchestrator（新增，编排器）        │
            │  plan → dispatch → worker(逐片) → merge → finalize   │
            └──────────────────────────┬──────────────────────────┘
        ┌──────────────┬───────────────┼───────────────┬──────────────┐
   membudget.py   planner.py        worker.py        merger.py     cleanup.py
   内存探测/预算   流式规划/ manifest 片内 blocking+    去重/全局DSU/   临时资源
   RSS 软熔断      大桶分裂/状态表     score/片级落盘   金记录/落库      回收/孤儿清扫
        └──────────────┴───────────────┴───────────────┴──────────────┘
                                      │
   现有核心（契约不动）：normalize.Normalizer · blocking 三策略 · scoring.score_pair
                        clustering.cluster_records · survivorship.build_golden · storage.Storage
```

新增文件（均在 `src/citefuse/sharding/` 包内）：

| 文件 | 职责 |
|---|---|
| `config.py`（在现有文件**新增段**，不改既有字段） | `ShardingConfig` Pydantic 模型 + 挂载到 `AppConfig` |
| `sharding/__init__.py` | 导出编排入口 `run_sharded_pipeline` |
| `sharding/membudget.py` | 离线可用内存探测、70% 预算、单条/单对内存采样估算、分片数反算、运行期 RSS 软熔断 |
| `sharding/planner.py` | 流式扫描产阻塞键、哈希分配、大桶分裂、写 manifest 与状态表 |
| `sharding/spill.py` | 分片临时文件的原子写、校验和、紧凑行序列化（JSONL，sort_keys） |
| `sharding/worker.py` | 单分片执行：复用现有 blocking/scoring，片级结果落盘，幂等 |
| `sharding/merger.py` | 片结果去重合并、阻塞统计汇总、喂给全局并查集、流式金记录、统一落库 |
| `sharding/cleanup.py` | 正常/异常临时资源回收、启动时孤儿清扫、磁盘预检 |
| `sharding/state.py` | 两张新表的 DDL 与状态机迁移（幂等建表，挂到现有 SCHEMA 之后） |
| `pipeline.py`（**仅加分支，不改原函数体**） | `run_pipeline` 入口按 `sharding.mode` 决定走原路径还是编排器 |

### 3.2 与现有三个核心环节的对接（不改核心处理契约）

#### 环节 A：输入解析（ingest）

- **HTTP 小负载路径不变**：`POST /batches` 仍走 `ingest_jsonl`（`ingest.py:43`），逐行校验、死信、`input_hash`、幂等全部保留。
- **新增离线大文件流式入口**（CLI/内部调用，不改变 HTTP 契约）：现有 `ingest_jsonl` 接收的是已读入内存的 `payload: str|bytes`，百万级时 `text.splitlines()` 会先制造一份全量字符串副本。新增 `ingest_jsonl_stream(storage, config, clock, path, ...)`：
  - 用 `hashlib.sha256` **增量哈希**逐行喂入，得到与一次性 `sha256(raw_bytes)` 完全相同的 `input_hash`（哈希是流式等价的，保证批次幂等身份不变）；
  - 逐行 `json.loads → RawRecord → Normalizer.normalize → insert_record`，每行处理完即释放，死信逻辑与 `_validation_reason` 原样复用；
  - 归一化在导入期完成这一既有约定不变，因此分片阶段读到的 `records` 与现在完全同构。
- 分片**不直接读原始 JSONL**，只读已归一化落库的 `records` 表（分页 `ORDER BY record_pk LIMIT/OFFSET` 或 keyset 分页），从而与输入层彻底解耦。

#### 环节 B：匹配计算（blocking + scoring + clustering）

- blocking：把 `blocking.py` 的三个键函数 `_year_venue/_title_signature/_author_year` 与 `_group_pairs` **原样复用**。为支持"只处理指定分片的桶"，对 `generate_candidates` 做一处**向后兼容的可选参数**增强（见 §5.1 接口缝），默认行为字节不变，现有测试零改动。
- scoring：worker 对片内每个候选对直接调用现有 `score_pair(left, right, config)`，**函数一行不改**；其无状态纯函数特性保证跨片结果一致。
- clustering：**不切片**。merger 把去重后的完整模型边按现有 `(-score, pair_key)` 顺序、人工裁定按现有时间序，喂给一个内存友好版的全局并查集（见 §4.4），聚类规则、禁连边传递校验、冲突审计与 `cluster_records` 完全等价（通过同一套 `resolve_decisions` + 排序键实现，抽公共逻辑复用而非另写规则）。

#### 环节 C：结果输出（pairs/edges/clusters/golden + API/导出）

- 落库仍用现有 `replace_pairs / replace_edges / replace_clusters`（`repository.py`），但入参改为**分批 `executemany`**（见 §5.2），避免整列表驻留；表结构、字段、`PRIMARY KEY(run_id,pair_key)` 不变，因此 `api_views.py` 的所有查询与出参零改动。
- `run_id`、`cluster_id` 派生公式不变 → 同输入重跑仍命中同一 `run_id`，幂等重建语义不变。
- `GET /golden/export` 在大数据量下增加**流式分页导出**可选路径（按 `cluster_id` keyset 分页读、逐行 yield），JSONL/CSV 列与排序规则不变；小数据量仍走现有 `export_golden` 全量路径。

---

## 4. 关键机制设计

### 4.1 内存预算与分片数反算（membudget.py）

**（1）离线可用内存探测（无第三方依赖、不联网）**

按优先级回退，全部基于标准库/系统文件：

1. Linux 读 `/proc/meminfo` 的 `MemAvailable`（容器/WSL2 均有此文件，最贴近真实可用量）；
2. cgroup v1/v2 内存上限：读 `/sys/fs/cgroup/memory.max`（v2）或 `memory/memory.limit_in_bytes`（v1），取与 `MemAvailable` 的较小值，适配容器配额；
3. 配置显式 `sharding.available_memory_mb` 覆盖（离线设备/受限环境的兜底）；
4. 以上都不可用时回退 `resource.getrlimit(RLIMIT_AS)`，再不行用保守默认 `available_memory_mb=2048`。

**预算上限**：`budget_bytes = min(探测可用, 配置覆盖) × memory_fraction`，`memory_fraction` 默认 **0.70**（即红线 70%）。

**（2）单条记录 / 单个候选对的占用估算（采样，不跑全量）**

- 规划阶段对 `records` 做 keyset 分页流式扫描，前 `sample_size`（默认 1000）条用 `sys.getsizeof` 递归估算归一化后 Record 实际占用，外推 `bytes_per_record`；
- 每处理一个阻塞桶即可统计桶大小，结合三策略命中数估算候选对总量 `P_est`；单对占用按"两条 Record 引用 + pair dict + 完整 details dict"实测采样得 `bytes_per_pair`（details 是最大头，必须实测而非拍常数）。

**（3）分片数与每片上限**

设单 worker 峰值 ≈ `片内记录副本字节 + 片内候选对字节 + 并查集/杂项固定开销 C`，要求 ≤ 预算给单 worker 的配额 `budget_bytes / max(1, worker_processes)`：

```
N = clamp( ceil( (R*bpr + P_est*bpp + C) / worker_budget ),
           min_shards, max_shards )
```

- `min_shards` 默认 1，`max_shards` 默认 256（防止片过多导致调度/小文件开销反噬小任务延迟）。
- 同时给出每片候选对上界 `max_pairs_per_shard`（默认按 worker 配额反算，8 GB 设备默认标定约 20 万对/片），超限触发大桶分裂或增片。
- N 与每条记录的片分配都写入 manifest 并参与 plan 哈希，**重跑时 N 确定性复现**（同输入同配置同采样 → 同 N；采样取固定前 K 条、排序固定，无随机）。

**（4）运行期 RSS 软熔断**

- worker 周期性（每处理 N 对或每若干秒）读 `resource.getrusage(RUSAGE_SELF).ru_maxrss` 得到峰值 RSS；
- 超过 `soft_trigger_fraction`（默认 0.85 × 预算，即预算的 85%，留裕量）时，worker **主动落盘当前片结果并提前切出当前桶**，把剩余桶交还调度器重排，而不是等 OOM；
- 连续熔断说明估算偏低，编排器按 2 倍系数扩片重规划（仅重规划未完成部分，已完成片结果复用）。该机制是估算的安全网，不影响最终结果（落盘的是已算完的纯函数 pair，重放幂等）。

### 4.2 规划器与 manifest（planner.py / state.py / spill.py）

**规划阶段全程流式**：分页扫 `records`，逐条算 0..3 个阻塞键，聚合"键 → 成员 pk 列表（计数，不常驻完整记录）"。为避免规划器自身把超大成员列表撑爆，键成员关系**直接增量追加到分片临时文件**，内存里只保留 `键 → (shard, 计数)` 的紧凑索引（整数 pk，非完整 Record）。

**分片临时文件布局**（位于 `data/shard_work/{run_id}/`，见 §6）：

```
shard_work/{run_id}/
  manifest.json            # 规划结果：N、算法版本、各片负责的键、plan 哈希、校验和
  shard_00000/
    records.jsonl          # 本片需要的记录紧凑行（可能含跨片复制，带 record_pk）
    buckets.index          # 阻塞键 → 成员 pk 列表 / 大桶子任务映射
    pairs.partial.jsonl    # worker 产出的已打分 pair（追加写，原子 rename 收尾）
    status.json            # 片状态、行数、对数、校验和、错误信息
  shard_00001/ ...
```

- 所有 JSONL 用 `json.dumps(..., sort_keys=True, ensure_ascii=False, separators=(",",":"))`，与 `storage.json_dumps` 风格一致，保证可 diff、可复现。
- 每个文件写完生成 SHA-256 侧车校验（`*.sha256`），用于 §7 的损坏检测。
- 写文件走 `tempfile.NamedTemporaryFile` 同目录临时名 + `os.replace` 原子落盘，进程中断不会留下"看似完整"的半文件。

**新增状态表（幂等建表，追加在现有 SCHEMA 之后，不改动既有表）**：

```sql
CREATE TABLE IF NOT EXISTS shard_manifests (
  run_id TEXT NOT NULL,
  plan_hash TEXT NOT NULL,          -- 输入+配置+N+分配规则的确定性哈希
  shard_count INTEGER NOT NULL,
  strategy TEXT NOT NULL,           -- 'blocking_key_hash_v1'
  status TEXT NOT NULL,             -- planning|ready|running|merging|completed|failed
  manifest_json TEXT NOT NULL,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
  PRIMARY KEY(run_id, plan_hash)
);
CREATE TABLE IF NOT EXISTS shard_tasks (
  run_id TEXT NOT NULL, plan_hash TEXT NOT NULL,
  shard_id INTEGER NOT NULL,        -- 大桶子任务用负号或 sub_task_id 扩展
  sub_task_id INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,             -- pending|running|done|failed|corrupt
  attempt INTEGER NOT NULL DEFAULT 0,
  record_count INTEGER NOT NULL, pair_count INTEGER NOT NULL DEFAULT 0,
  file_checksum TEXT, error TEXT,
  started_at TEXT, finished_at TEXT,
  PRIMARY KEY(run_id, plan_hash, shard_id, sub_task_id)
);
```

状态机：`pending → running → done`；异常路径 `running/failed →（重跑）pending`；校验失败置 `corrupt` 并重建该任务。

### 4.3 工作器（worker.py）

单分片执行步骤（幂等，可安全重放）：

1. 读 `shard_xxxxx/records.jsonl` 反序列化本片记录为 `Record`（紧凑行，仅本片所需），校验 `file_checksum`，不符则置 `corrupt` 触发重建。
2. 按 `buckets.index` 对每个阻塞键**原样调用现有 `_group_pairs` 的桶内 `combinations`**（大桶任务则只处理分配到的成员块对），用与 `blocking.py` 相同的排序键（`record.key`）和 pair 规范化 `make_pair_key`。
3. 每个候选对立即 `score_pair`，结果**流式追加**到 `pairs.partial.jsonl`，按批 flush（默认每 5000 对），不在内存累积全片 pair 列表。
4. 片内维护本片的 `strategy_counts`（按策略键计数），完成后写入 `status.json`。
5. 收尾 `os.replace` 为最终片结果文件，`shard_tasks` 置 `done`、写 `pair_count` 与校验和；`finally` 中释放本片 Record 集合，强制峰值不跨片叠加。

**并行度**：默认 `worker_processes=1`（串行逐片，调度开销最小，服务于小任务延迟约束）。配置可调到 >1，用标准库 `concurrent.futures.ProcessPoolExecutor` 离线多进程；每个子进程独立打开**只读** SQLite 连接（WAL 模式支持多读单写），写各自分片文件、不共写主库，规避 SQLite 多写锁竞争。无论并行度取何值，合并阶段统一排序去重，**输出与并行度无关**。

### 4.4 合并、全局聚类与流式金记录（merger.py）

1. **pair 去重与统计汇总**：按 `shard_id` 顺序流式读各片 `pairs.partial.jsonl`，靠 `(run_id,pair_key)` 幂等 upsert 到主库 `pairs` 表（分批事务），天然完成跨片/跨策略去重；同步累加各片 `strategy_counts` 得到与全量模式**数值相同**的 `blocking_stats`（并集 `union_count` 以去重后实际行数为准，与 `blocking.py:117` 口径一致）。
2. **全局聚类（内存友好版并查集）**：
   - 并查集改用**整型数组**实现（`array('i')`，parent/rank 各 4 字节/记录），百万记录约 8 MB 量级，而非现有 `dict[int,int]`（Python int 装箱，大数集膨胀数十倍）；
   - 不重建 `by_pk/by_key` 全量 Record 字典：pk→key 映射用紧凑数组/按需分页查库；
   - 人工裁定走现有 `resolve_decisions`（最新者胜、冲突事件），模型边从主库按 `ORDER BY score DESC, pair_key ASC` 流式拉取（复用现有 `idx_pairs_zone` 索引），逐条入并查集，禁连边传递校验逻辑与 `clustering.py:90-151` 等价；
   - 输出的簇成员排序、`cluster_id` 派生与现规则一致。
3. **流式金记录**：按簇分组（成员 pk 排序）分页加载该簇成员 Record → 调用**现有 `build_golden(group, config)`**（函数不改）→ 立即 `replace_clusters` 分批落库后释放，避免 `groups` 全量驻留。幸存规则与平票处理天然保持。
4. **落库与审计**：沿用 `save_run / add_attempt / record_event`；`timings` 在原阶段键基础上增加 `planning`、`sharding`、`merge` 键（原键 `blocking/scoring/clustering/golden/persistence/total` 保留，数值口径为各片汇总，保证 `GET /runs` 出参结构不破坏）。

### 4.5 确定性保障清单

| 风险点 | 保障手段 |
|---|---|
| 分片数/分配漂移 | 分配仅依赖 `sha256(阻塞键)%N`，N 由固定采样与确定性公式算出；写入 `plan_hash` 并落库，重跑强制复用同 plan，不一致则视为新 plan 全量重规划 |
| 记录复制顺序影响结果 | 片内、桶内一律按 `record.key` 显式排序后再 `combinations`，与 `_group_pairs` 现状一致；pair 用 `make_pair_key` 规范化左右 |
| 并行/串行差异 | worker 只产出无状态 pair；所有排序、去重、聚类统一在 merger 单点按全局键完成，worker 数量不进入任何哈希或排序 |
| 配置指纹 | 见 §8：分片参数**不纳入** `AppConfig.fingerprint()` 的业务指纹（它只影响执行方式不影响结果），另存 `plan_hash` 审计；保证同业务配置下 `run_id` 恒定 |
| 时间/耗时字段 | 继续走可注入 `Clock`，测试用 `FixedClock`；耗时是观测值不参与身份派生 |
| JSON 序列化 | 全部沿用 `sort_keys=True` 的紧凑序列化，分片中间文件可与全量结果逐行 diff |

---

## 5. 对现有代码的最小侵入改动

### 5.1 blocking 的"接口缝"（向后兼容）

不重写三策略，只把"全量"放宽为"可限定桶集合"。`generate_candidates` 增加一个带默认值的可选参数，默认路径行为完全不变：

```python
# blocking.py —— 仅示意接口缝，默认行为与现状逐行一致
def generate_candidates(records, config, bucket_filter=None):
    # bucket_filter: None  => 现有全量行为（现有测试/小任务走这条）
    #               set   => 只处理阻塞键落在该集合内的桶（worker 走这条）
    ...
```

三策略键函数 `_year_venue/_title_signature/_author_year`、`_group_pairs`、`STRATEGY_FUNCS` 均原样复用。worker 内部也可直接调用 `_group_pairs` 而不动 `generate_candidates`，二选一，优先"加可选参数"以便统一审计统计口径。

### 5.2 repository 落库批量化（不改表结构）

`replace_pairs/edges/clusters` 现状是"整 list 一次 executemany"。增加内部按 `batch_size`（默认 1000）分块 executemany，函数签名与最终表内容不变；分片路径与原路径共用同一方法，小数据量下行为无差别。

### 5.3 pipeline 入口分支（原函数体不动）

```python
# pipeline.py
def run_pipeline(storage, config, clock, batch_id):
    if config.sharding.mode == "on" or (
        config.sharding.mode == "auto"
        and _exceeds_inproc_budget(storage, config, batch_id)   # 只读计数+估算，不加载全量
    ):
        from .sharding import run_sharded_pipeline
        return run_sharded_pipeline(storage, config, clock, batch_id)
    return _run_pipeline_inproc(storage, config, clock, batch_id)  # 现有函数体原样改名保留
```

- `_exceeds_inproc_budget` 仅基于 `count_records` + 规划器的轻量桶计数估算，不触发全量加载，开销为一次流式计数（小任务下毫秒级，见 §9 延迟账）。
- `mode=off` 时完全等同今天的行为，便于灰度与回滚。

---

## 6. 临时资源与磁盘生命周期（cleanup.py）

**目录约定**：所有分片临时产物落在 `data/shard_work/{run_id}/`（`data/` 已在 `.gitignore` 中，无需改忽略规则）。主库 `data/citefuse.sqlite3` 与 WAL 不受影响。

**三级回收**：

1. **正常收尾**：merger 成功并 `replace_*` 落库后，编排器在 `finally` 中删除整个 `shard_work/{run_id}/`，并在 `audit_events` 记录 `shard_workspace_cleaned`（含释放字节数）。
2. **异常退出（try/finally 兜底）**：任何阶段抛错，当前 run 的临时目录保留用于续跑（不能删），但**孤儿目录**（对应 run 已 `failed` 且无 running 任务、或目录存在而状态表无记录）在下次启动时由清扫器回收。
3. **启动孤儿清扫**：服务启动 / CLI 入口执行一次 `cleanup_orphans(root="data/shard_work")`：
   - 扫描目录，对照 `shard_manifests`；无 manifest、或 manifest 处于终态（completed/failed 且超过保留期 `retain_failed_hours` 默认 24h）的目录删除；
   - 对带 `*.sha256` 但校验不过的半文件，删除该任务分片并将 `shard_tasks.status` 置 `corrupt`，续跑时重建；
   - 清扫动作写审计，可审计删除了什么。

**磁盘预检**：规划前估算临时空间 ≈ `records 字节 × 平均复制系数(≤3) + P_est × 单对落盘字节`，若 `shutil.disk_usage(data).free < 估算值 × disk_headroom(默认1.3)`，直接 fail-fast 并给出明确错误（不跑到一半写满磁盘），错误进 `run_failed` 审计。

**级联删除对齐**：`DELETE /batches/{id}` 现有外键 `ON DELETE CASCADE` 会清主库表；在批删除路径额外调用 `cleanup.run_dirs_for_batch`，按 manifest 中 run_id 清理对应临时目录，保持"删批次即清干净"。

---

## 7. 异常处理链路与断点续跑

### 7.1 场景覆盖矩阵

| 故障场景 | 检测方式 | 状态变化 | 恢复行为（自动续跑，不重算已完成部分） |
|---|---|---|---|
| **进程中断/被 kill/断电** | 重启后 `shard_tasks` 残留 `running` 但无活进程；临时文件经原子 rename 要么完整要么不存在 | running→（启动清扫）pending | 重新 `POST /runs`（同 run_id）时编排器加载 manifest，跳过所有 `done` 任务，仅把 `running/pending` 重新调度；已落盘的 `done` 片直接复用 |
| **输入分片文件损坏**（位腐/被截断） | 读前比对 `*.sha256`；JSONL 逐行 parse 失败 | 该任务置 `corrupt` | 删除坏分片文件，依据 manifest **从 records 表重新生成该片**（records 是权威来源且未损坏），任务 attempt+1；重建不影响其他片 |
| **单分片处理失败**（代码异常/数据特例） | worker 捕获异常，错误写 `status.json` 与 `shard_tasks.error`，置 `failed` | failed | 按 `max_task_retries`（默认 2）自动重试该片（独立、无副作用、纯函数可安全重放）；超过重试上限则整 run 置 `failed` 并带分片号/堆栈进 `run_failed` 审计，已完成片保留，修复后再跑仍只续未完成片 |
| **规划阶段失败** | manifest 未原子落盘 | 无 ready manifest | 规划是幂等只读扫描，整体重跑规划即可，成本低；不产生需要续跑的半成品 |
| **合并阶段失败** | `pairs/edges/clusters` 落库中途异常 | 事务回滚（现有 `transaction()` 已保证原子）+ run 置 failed | pair 落库靠 `PRIMARY KEY(run_id,pair_key)` 幂等，重跑 merger 先复用 `done` 片、对已 upsert 的 pair 幂等覆盖；不重算 worker |
| **RSS 软熔断** | ru_maxrss 超阈值 | 当前桶提前落盘交还 | 编排器扩片重规划剩余部分，已算 pair 复用（见 §4.1(4)） |
| **磁盘不足** | 规划前预检 / 写入时 ENOSPC | fail-fast 或 failed | 报明确错误；清理孤儿释放空间后续跑，状态表保证可继续 |
| **SQLite 主库损坏** | 打开/查询报 `DatabaseError` | run failed | 不在本方案自动恢复范围（属存储层故障）；给出 `sqlite3 .recover` 指引与审计，分片临时文件保留以便库恢复后续跑 |

### 7.2 续跑入口与幂等保证

- 用户侧入口不变：对同一批次再次 `POST /runs`。因 `run_id` 由 batch+config+decisions 确定性派生，身份一致；编排器用 `(run_id, plan_hash)` 找到历史 manifest 与任务表。
- 幂等三支柱：① 分片任务结果纯函数、可重复计算；② pair/edge/cluster 落库主键/事务幂等；③ manifest 与 plan_hash 固定分配，重跑不会把记录换到不同片而产生重复劳动。
- 每次续跑仍追加一条 `run_attempts`（沿用现有审计约定），并在 `audit_events` 记录 `shard_resume`（复用了多少 done 片、重放了哪些片），做到续跑过程本身可审计。
- `FixedClock` 注入保证测试可对续跑时间线做确定性断言。

---

## 8. 配置项设计（configs/default.yaml 新增段，8 GB 默认标定）

在现有 yaml 追加（既有字段一个不动），并在 `config.py` 新增对应 `ShardingConfig`（`extra="forbid"`、带范围校验，与现有风格一致）：

```yaml
sharding:
  # off：永远走原单进程路径；auto：按内存预算自动决定（默认）；on：强制分片
  mode: auto
  strategy: blocking_key_hash_v1
  # 内存红线：运行期峰值不超过 可用内存 × 0.70
  memory_fraction: 0.70
  # 显式可用内存(MB)；留空则自动探测 /proc/meminfo + cgroup。8GB 设备可留空自动探测
  available_memory_mb: null
  soft_trigger_fraction: 0.85      # 达预算 85% 主动落盘切桶的软熔断
  # 分片粒度
  min_shards: 1
  max_shards: 256
  target_records_per_shard: 50000  # 8GB 标定的每片记录数参考上界
  max_pairs_per_shard: 200000      # 8GB 标定的每片候选对上界，超限触发大桶分裂/增片
  sample_size: 1000                # 内存估算采样条数（固定，保证 N 确定性）
  # 执行与调度
  worker_processes: 1              # 默认串行，调度开销最小；离线多核可调
  spill_batch_size: 5000           # pair 流式落盘每批条数
  db_write_batch_size: 1000        # 主库分批 executemany 大小
  max_task_retries: 2
  # 临时资源
  work_dir: data/shard_work
  disk_headroom: 1.3               # 临时空间预检冗余系数
  retain_failed_hours: 24          # 失败工作目录保留时长，供诊断后续跑
```

**8 GB 设备默认行为推演**：`MemAvailable` 典型约 6–7 GB（系统占用后），预算 ≈ 6.5GB × 0.7 ≈ 4.5GB。十万级记录、候选对百万级时，按采样反算 N 通常落在 8–16 片，每片 5 万记录 / ≤20 万对，单 worker 峰值控制在数百 MB 至 1GB 出头，稳态远低于 4.5GB 红线；百万记录时 N 趋向 max_shards 并配合大桶分裂，单任务驻留仍被 `max_pairs_per_shard` 封顶。

**配置指纹处理（关键）**：`sharding` 段只改变"如何执行/分几片"，不改变业务结果。为遵守"同业务配置 run_id 恒定、结果可复现"的既有契约：

- `AppConfig.fingerprint()` 的业务哈希**默认不纳入** `sharding` 段（调 `model_dump(exclude={"sharding"})`），保证调分片参数不会凭空产生新 run_id、不会破坏现有幂等；
- 分片实际执行的完整可复现性由独立的 `plan_hash`（含 N、分配、算法版本、分片相关参数）承载并落 `shard_manifests`，审计链不丢；
- 提供 `sharding.include_in_fingerprint: false`（默认）开关，若审计口径要求"执行参数也进 run 身份"可置 true，行为显式可选。

---

## 9. 小任务延迟保护（涨幅 ≤ 10% 的工程账）

`mode=auto` 下小批量任务走"快速路径"，分片机制近乎零成本：

1. **判定本身极轻**：`_exceeds_inproc_budget` 只用 `count_records`（一次 `COUNT(*)`，索引覆盖）+ 固定小采样估算，不加载全量记录、不生成 manifest、不建临时目录。千/万级下该判定为亚毫秒~毫秒级。
2. **不越界就零额外调度**：估算内存在预算内 → 直接调用改名后的原 `_run_pipeline_inproc`，**执行路径与今天完全相同**，没有进程池、没有临时文件、没有合并阶段，理论开销仅一次 COUNT + 估算，实测相对增幅预期 <1%，远低于 10% 上限。
3. **仅大任务付分片成本**：只有估算超预算才进入规划/分片；大任务的对照基线本来就是"会 OOM 跑不完"，分片带来的是可用性而非可感知的延迟回退。
4. **防片过多反噬**：`max_shards=256` + `min_shards=1` + 串行默认，避免小任务被切成大量微小分片产生调度/小文件开销；大桶分裂只对真正的超级桶触发，常规桶不分裂。
5. **采样固定且小**：`sample_size=1000` 不随数据量增长，判定成本在万级和百万级都是常数级。

---

## 10. 验证思路（分数据量 + 故障注入 + 兼容性）

### 10.1 正确性/等价性（所有规模通用，最重要）

- **黄金等价测试（golden equivalence）**：对同一批数据分别跑 `mode=off`（全量）与 `mode=on/auto`（分片，且 N 取 1/2/7/16 多值），逐行断言：
  - `runs`：`run_id`、`candidate_pairs`、`matched_edges`、`cluster_count`、`golden_count`、`blocking_stats.strategy_counts/union_count` 全等；
  - `pairs`：按 `pair_key` 排序后每行 `score/zone/details/strategies` 字节一致；
  - `edges`、`clusters`（含 `cluster_id`、成员顺序）、`golden_json`（含每字段 `provenance`）逐行一致；
  - `GET /golden/export` 的 JSONL/CSV 导出做整文件哈希比对。
- **并行不变量**：`worker_processes=1` 与 =2/4 的输出做上述全等比对，证明结果与并行度无关。
- **既有测试不回归**：`pytest -q` 全绿（当前 200 条 fixture、F1=1.0、幂等、复核、API 错误路径全部保留）；`mode=off` 下行为与改造前完全一致。
- **确定性重跑**：同 seed fixture 两次分片跑，`plan_hash`、manifest、所有中间文件、最终结果字节一致（对齐 README §9 的 sha256 验收方式）。

### 10.2 分规模验证

| 规模 | 数据构造（沿用 `scripts/seed_fixtures.py` 确定性生成思路，全虚构离线数据） | 验收重点 |
|---|---|---|
| 千级（~1k） | 现有 fixture 放大参数 | 走快速路径；相对 `mode=off` 端到端延迟增幅 **<10%**（benchmark 计时，pytest-benchmark 或手动计时多轮取中位数）；结果全等 |
| 万级（1–5 万） | 增大重复组数与单例数 | 临界区：验证 auto 判定在预算内仍走原路径或仅少量分片；内存峰值、延迟增幅、结果全等 |
| 十万级（10–50 万） | 高重复 + 高离散混合，人为制造 1–2 个大阻塞桶 | 确认进入分片；RSS 峰值 ≤ 可用内存 70%；大桶分裂触发且结果与全量一致；`blocking_stats` 口径一致 |
| 百万级（100 万+） | 极限压测，构造倾斜来源/热门年份 | 稳定跑完不 OOM；峰值内存红线达标；断点续跑演练；磁盘临时空间与自动回收核对；端到端吞吐/耗时基线 |

### 10.3 内存与性能指标采集（离线、无外部服务）

- 用 `resource.getrusage(RUSAGE_SELF).ru_maxrss` 在各阶段记录峰值 RSS（子进程则 `RUSAGE_CHILDREN`），写入 `timings`/审计，断言 `peak_rss ≤ MemAvailable × 0.70`；
- 记录各阶段耗时（planning/各 shard/merge/total）、分片 N、每片记录数/对数分布（验证负载均衡，给出最大/中位片体量比）；
- 用标准库 `tracemalloc` 在测试环境做快照，定位副本与 pair dict 占用，校准 `bytes_per_record/bytes_per_pair` 估算误差（要求估算偏差可被软熔断覆盖）。

### 10.4 故障注入与续跑演练

- **进程中断**：在 worker 处理到指定分片时 `kill -9`（测试里用 monkeypatch 在第 k 片抛异常模拟），重启后续跑，断言：done 片未重算（看 `shard_resume` 审计与 attempt）、最终结果与一次跑完全等。
- **分片损坏**：人为篡改某 `pairs.partial.jsonl`/删字节，断言校验失败 → 置 `corrupt` → 自动重建 → 结果一致。
- **单分片反复失败**：让某片前两次抛错第三次成功，验证 `max_task_retries` 内自动恢复；超过上限验证 run 置 failed 且错误含分片号、已完成片保留。
- **临时文件回收**：制造孤儿目录（无 manifest / 终态超时），跑启动清扫，断言目录被清且有审计；正常完成后断言 `shard_work/{run_id}` 不存在；`DELETE /batch` 后断言关联临时目录一并清理。
- **磁盘预检**：把 `work_dir` 指向低剩余空间路径，断言 fail-fast 且不产生半成品。
- **幂等重放**：续跑多次，最终 `pairs/edges/clusters` 无重复、行数与单次跑一致（主键约束 + 计数断言）。

### 10.5 兼容性与审计验收

- API 契约：用现有 `tests/test_api.py` 加超大批次用例，路由、出参字段、状态码、死信行为不变；
- 复核链路：分片模式下提交 human same/different，验证禁连边传递、冲突审计、自动重跑分裂/合并仍正确（聚类是全局算子，重点测跨片同实体的人工裁定）；
- 审计可复现：依据 `config_hash + plan_hash + manifest + shard_tasks + audit_events` 能完整还原"分了几片、每片哪些键/记录、算了哪些对、失败重放了什么"，并在干净环境重放得到同结果；
- 离线合规：全流程 `pip install` 后断网运行，不引入新第三方包，不读网络/敏感环境变量，与 README §1 边界一致。

---

## 11. 落地步骤建议（每步可独立验收、可回滚）

1. **第 1 步（零行为变化）**：新增 `ShardingConfig` + 配置段（默认 `mode=off`）、两张状态表幂等 DDL、`cleanup.py` 与磁盘预检；`pytest` 全绿。
2. **第 2 步**：抽 blocking 接口缝（可选参数，默认行为不变）+ repository 分批落库；黄金等价测试在 `mode=off` 下证明零回归。
3. **第 3 步**：实现 membudget + planner + spill + 单进程 worker，先支持 `mode=on`、N 固定，做千/万级等价与内存测试。
4. **第 4 步**：merger（去重、数组并查集、流式金记录、统一落库/审计），打通端到端，黄金等价覆盖十万级。
5. **第 5 步**：大桶二级分裂 + RSS 软熔断 + 自动 N 反算，开启 `mode=auto`，百万级压测达标。
6. **第 6 步**：故障注入、断点续跑、孤儿清扫、流式导入/导出补齐，补全 §10 全部用例。
7. **第 7 步（可选）**：`ProcessPoolExecutor` 多进程并行与并行不变量测试；默认仍 `worker_processes=1`。

每一步都保持 `mode=off` 可立即回滚到今天的单进程行为，且任何阶段 `pytest` 全绿为合并门槛。

---

## 附：与需求条款的对照表

| 需求 | 本方案落点 |
|---|---|
| 纯离线、无外部服务、无联网 | 仅标准库 + 现有 SQLite；内存探测读 `/proc`、cgroup；§3.1、§4.1 |
| 百万级稳定、内存 ≤ 可用 70% | 阻塞键哈希分片 + 大桶分裂 + 70% 预算 + RSS 软熔断；§4.1、§8 |
| 千/万级延迟涨幅 ≤ 10%、调度开销小 | `mode=auto` 快速路径、默认串行、不越界零分片；§9 |
| 分片与全量结果完全一致 | 桶不拆散 + 纯函数打分 + 全局聚类 + 统一排序去重 + 黄金等价测试；§2.3、§4.5、§10.1 |
| 确定性、可审计可复现 | run_id/cluster_id 规则不变、plan_hash、manifest、状态表、FixedClock、sort_keys；§4.5、§7.2 |
| 配置驱动、分片粒度/内存阈值可调、8G 默认 | 新增 `sharding` 配置段与标定默认值；§8 |
| 三方案对比与明确选型 | §2 全表对比，选阻塞键哈希、淘汰来源/时间分片 |
| 对接输入解析/匹配/输出三环节且不改核心契约 | §3.2，仅加可选参数与分支，核心函数与表结构/API 不动 |
| 进程中断/分片损坏/单片失败处理、断点续跑、不重算全量 | 状态机 + 幂等三支柱 + 场景矩阵；§7 |
| 临时资源自动回收、异常无残留 | 三级回收 + 启动孤儿清扫 + 磁盘预检 + 级联删除；§6 |
| 分数据量验证思路 | §10 千/万/十万/百万 + 内存 + 故障注入 + 兼容性 |
