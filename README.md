# citefuse（题录熔合）

**纯本地、可离线运行的题录去重与主记录归一引擎。**  
输入多来源、脏格式的 JSONL 题录，经过校验、归一化、阻塞配对、相似度打分、聚类和字段级幸存规则，输出可审计的簇与「金记录」；不确定的配对进入人工复核队列，人工裁定在后续重跑中优先于模型分。

## 1. 定位与边界

**是：**

- 本地批处理 + HTTP 服务形态的实体解析引擎（单实体类型：题录）。
- 配置驱动（YAML）、算法确定性、结果可复核、可评测、可重跑。
- 批次 → 记录 → 候选对 → 匹配边 → 簇 → 金记录 的完整链路。
- SQLite 单文件存储、手写参数化 SQL、启动时幂等建表。

**不是：**

- 文献管理器、参考文献排版器、图书借阅系统、联网检索工具。
- 通用 ETL 平台、分布式/流式系统、Web 前端或报表看板。
- 机器学习/语义向量/联网 DOI 校验工具。
- 登录、多用户、权限系统。

所有 fixture、人物、刊名、机构名和 DOI 均为脚本生成的**全虚构数据**。运行期和测试期不访问网络，不读取敏感环境变量，不包含任何密钥。

## 2. 快速上手

要求 Python ≥ 3.11。依赖安装完成后可完全断网运行。

```bash
# 1) 安装依赖 + 安装本项目包（提供 citefuse 包）
python -m pip install -r requirements.txt
python -m pip install . --no-deps --no-build-isolation

# 2) 生成确定性虚构 fixture（默认 data/fixtures/）
python -m scripts.seed_fixtures

# 3) 启动服务：固定监听 127.0.0.1:8321
python -m citefuse.app

# 4) 另开终端，跑测试（必须全绿）
python -m pytest -q
```

若不想安装项目包，可在项目根目录用 `PYTHONPATH=src python -m citefuse.app`；验收的标准入口是安装后的 `python -m citefuse.app`。端口可用非敏感环境变量覆盖：`CITEFUSE_PORT=9001`；数据目录可用 `CITEFUSE_DATA_DIR=/tmp/cf` 覆盖；配置用 `CITEFUSE_CONFIG=...` 覆盖。

### 最小完整流程

```bash
curl -s -X POST 'http://127.0.0.1:8321/batches?batch_id=DEMO&name=Demo' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @data/fixtures/records.jsonl

RUN=$(curl -s -X POST 'http://127.0.0.1:8321/runs' \
  -H 'Content-Type: application/json' -d '{"batch_id":"DEMO"}' \
  | python -c 'import json,sys; print(json.load(sys.stdin)["run_id"])')

curl -s "http://127.0.0.1:8321/clusters?run_id=$RUN"
curl -s "http://127.0.0.1:8321/review/next?batch_id=DEMO&n=3"
curl -s "http://127.0.0.1:8321/metrics/report?run_id=$RUN"
curl -s "http://127.0.0.1:8321/metrics/thresholds?run_id=$RUN&grid=0.60,0.70,0.80,0.90"
curl -s "http://127.0.0.1:8321/golden/export?run_id=$RUN&format=jsonl" -o data/golden.jsonl
```

默认配置在 200 条 fixture 上的结果是 **120 个簇、pairwise F1 = 1.0**。fixture 的 40 组重复每组 3 条记录，另有 80 条单例，共 120 个金标准簇。

## 3. 配置说明（`configs/default.yaml`）

配置在启动时加载并校验；其规范化 JSON 的 SHA-256 写入每次运行和报告，便于复现。关键项及默认值取舍：

| 配置项 | 默认值 | 说明与取舍 |
|---|---|---|
| `normalization.unicode_form` | `NFKC` | 将全角字母/标点折叠为半角，覆盖常见导出噪声。 |
| `casefold` / `strip_punctuation` | `true` / `true` | 标题、刊名、作者统一大小写并剥离标点，降低格式差异。 |
| `author_formats` | `family_given, given_family` | 同时接受「姓, 名」与「名 姓」；逗号格式优先。 |
| `title_stopwords` | 一组英文停用词+通用学术词 | 只用于标题分桶，不改变展示/打分文本。 |
| `venue_aliases` | 5 个虚构刊名及别名 | 归一后映射到规范刊名，解决缩写刊名。 |
| `blocking.year_venue` | 开，刊名前缀 5 | 高召回、低桶量；年份相同且刊名前缀相同才进桶。 |
| `blocking.title_signature` | 开，前 5 个 token | 去停用词后取前 N 个 token 再排序拼接，对词序/标点稳健。 |
| `blocking.author_year` | 开 | 第一作者姓 + 年份，补足标题缺失或变化较大的情况。 |
| `scoring.weights` | 标题 .38 / 作者 .18 / 刊名 .10 / 年份 .10 / 页码 .06 / DOI .18 | 标题为主、DOI 强信号，任一分项不足以独立越过阈值。 |
| `missing_policy` | `neutral` | 缺失字段按 0.5 中性分且保留权重；`exclude` 则丢弃该字段并重归一化权重。 |
| `year_diff_scores` | `[1.0, .75, .40]` | 相差 0/1/2 年得分，≥3 年为 0。 |
| `review_low` | `0.68` | 灰区下界；低于此值直接判为不同，不进复核。 |
| `match_threshold` | `0.80` | 自动连边阈值；灰区为 `[0.68, 0.80)`。 |
| `survivorship.*` | 见下节 | 字段级金记录规则。 |
| `source_priority` | ORBIT > LANTERN > MEADOW > COMPASS | 并列时的确定性来源优先级，未列出的来源排最后。 |
| `review.default_page_size` / `max_page_size` | 10 / 100 | 复核队列默认与上限。 |

## 4. 数据模型（SQLite）

| 表 | 核心字段 | 用途 |
|---|---|---|
| `batches` | `batch_id`, `input_hash`, `total_lines`, `valid_lines`, `bad_lines` | 批次及统计；重复内容由 `input_hash` 识别。 |
| `records` | `record_pk`, `(batch_id,source,record_id) UNIQUE`, `raw_json`, `norm_json`, 各归一列 | 原始值与归一值双份保存。 |
| `dead_letters` | `line_no`, `reason`, `raw_text` | 逐行校验失败，不阻断整批。 |
| `runs` | `run_id`, `config_hash`, 各计数, `timings_json`, `blocking_stats_json`, `status` | 一次逻辑运行的可复现快照。 |
| `run_attempts` | 与 `runs` 相同计数 + `started_at`, `finished_at` | 每次实际调用都追加一条审计尝试记录。 |
| `pairs` | `pair_key`, 两记录主键, `score`, `zone`, `details_json` | 所有候选对的总分与分项得分明细。 |
| `edges` | `pair_key`, `kind=model/human_same`, `score` | 实际参与聚类的匹配边。 |
| `clusters` | `cluster_id`, `member_keys_json`, `golden_json` | 簇成员与字段级金记录。 |
| `decisions` | `pair_key`, `decision`, `reviewer`, `note`, `submitted_at`, `scope`, `status` | 批次内人工裁定历史；同 pair 以提交时间/自增序号最新者为准。`scope` 默认 `batch`，历史行无需迁移。 |
| `cross_batch_decisions` | `pair_key`, 两侧业务 key 与内容指纹, `decision`, `source_batch_id`, `status` | 显式标记为跨批次的全局裁定；与批次生命周期解耦，源批次删除后仍存活。部分唯一索引保证每个 `pair_key` 至多一条 `active`。 |
| `decision_inheritances` | `batch_id`, `source_decision_id`, `source_batch_id`, `pair_key`, 指纹, `decision` | 每个批次首次跑管道时冻结的继承快照；重跑只读快照，保证老批次结果不被后续全局裁定污染。 |
| `audit_events` | `run_id`, `event_type`, `detail_json`, `created_at` | 复核冲突、继承命中/身份不匹配、运行完成/失败等审计事件。 |

`run_id` 由批次内容哈希、配置哈希、有效裁定历史（含冻结继承快照）确定性派生；`cluster_id` 由批次与排序后的成员 key 派生。相同输入重跑会复用同一个 `run_id`，并重建相同的 pairs/edges/clusters/golden 行。每次调用仍会追加 `run_attempts` 和审计事件。继承快照只在批次首次运行时冻结，之后新增的跨批次裁定不会改变老批次的 `run_id` 与任何已落库结果。

输入记录还可附带可选的评估字段 `gold_cluster`（fixture 自带）。它只用于 `GET /metrics/report` 与阈值扫描，绝不参与阻塞、打分或聚类。

## 5. API 一览

服务固定绑定 `127.0.0.1`，默认 `8321`。所有响应均为 JSON（金记录导出除外）。逐行坏 JSONL 会进入死信并仍返回 201；空请求、非法 UTF-8、非法 JSON envelope 返回 400。

| 方法 | 路径 | 入参 | 出参/说明 |
|---|---|---|---|
| GET | `/health` | — | 状态与配置哈希。 |
| POST | `/batches` | JSONL 原始体，或 `{"jsonl":"...","batch_id":"...","name":"..."}`；可选 `?batch_id=&name=` | 201，批次统计、`inserted`、`duplicates`、`dead_added`。 |
| GET | `/batches` | — | 批次列表（含最新 run id）。 |
| GET | `/batches/{id}` | — | 批次统计 + 死信样例；不存在 404。 |
| DELETE | `/batches/{id}` | — | 级联删除记录、pairs、edges、clusters、runs、decisions、审计；404 若不存在。 |
| POST | `/runs` | `{"batch_id":"..."}` | 201，跑完整管道并返回 run 摘要、阻塞统计、冲突审计、`inherited_decisions`（含来源批次）与 `inheritance_mismatches`。 |
| GET | `/runs` | `?batch_id=` | 运行列表，每个 run 带 `inheritances` 继承摘要。 |
| GET | `/runs/{run_id}` | — | 运行详情、最近一次尝试耗时、审计事件。 |
| GET | `/review/next` | `?n=10&batch_id=` | 最新 run 中未裁定的灰区 pair，含双记录、总分、分项、策略；身份对不上的历史跨批次裁定以 `historical_decision` 标注并保留在队列。 |
| POST | `/review/decisions` | `{"batch_id","pair_key","decision":"same\|different","reviewer","note","scope":"batch\|cross_batch"}` | 持久化裁定并**自动重跑**；`scope` 省略时默认 `batch`。跨批次互斥并发提交时仅一条生效，落败方收到 409。 |
| GET | `/review/decisions` | `?batch_id=` | 批次内裁定历史，含本批次提交的跨批次裁定。 |
| GET | `/metrics/report` | `?run_id=` | pairwise P/R/F1、簇完全一致率；写 `data/reports/{run_id}.json`。 |
| GET | `/metrics/thresholds` | `?run_id=&grid=0.5,0.6,...` | 复用已落库分数做阈值扫描，不重跑归一化/阻塞；写扫描报告。 |
| GET | `/clusters` | `?run_id=` 或 `?batch_id=` | 簇摘要与金记录。 |
| GET | `/clusters/{cluster_id}` | `?run_id=`/`batch_id=` | 成员原始+归一数据、金记录、边明细与分项得分。 |
| GET | `/golden/export` | `?format=jsonl\|csv&run_id=` | 确定性金记录导出（Content-Disposition 文件下载）。 |

### 响应示例

`POST /runs`：

```json
{"run_id":"run_232907b32f3380aa","batch_id":"DEMO","config_hash":"ffdf...","input_rows":200,
 "bad_rows":0,"candidate_pairs":218,"matched_edges":116,"cluster_count":120,
 "golden_count":120,"timings":{"blocking":0.001,"scoring":0.02,"clustering":0.001,
 "golden":0.001,"persistence":0.003,"total":0.026},"blocking_stats":
 {"strategy_counts":{"year_venue":186,"title_signature":120,"author_year":83},"union_count":218},
 "status":"completed","conflicts":[]}
```

`GET /review/next` 的每个 pair 形如：

```json
{"pair_key":"COMPASS::G030B||ORBIT::G030C","score":0.795455,"zone":"review",
 "strategies":["title_signature","year_venue"],
 "details":{"score":0.795455,"zone":"review","doi_match":false,
 "details":{"title":{"raw_score":1.0,"score":1.0,"weight":0.38,"contribution":0.38,
 "status":"scored"}, "...":"..."}},
 "records":[{"record_key":"COMPASS::G030B","raw":{},"normalized":{},"gold_cluster":"G031"}, "..."]}
```

## 6. 算法与打分模型

### 归一化

- Unicode NFKC → casefold → 标点转空白 → 空白折叠。
- 作者解析为 `{family, given, key}`；`key = family+首字母`，保留顺序用于有序重叠。
- 刊名别名在归一后映射到规范刊名。
- 年份提取四位年份（1500–2100）；页码取数字并展开 `129–35 → 129-135`；DOI 去 `doi:` / `https://doi.org/` 前缀、去空白、小写。
- 可选字段无法解析时置空；标题归一后为空则整行进入死信。记录始终保留 `raw_json` 与 `norm_json`。

### 候选对（Blocking）

三种策略独立开关，结果取并集并按 pair key 去重：

1. `year + venue[:5]` 分桶。
2. 标题去停用词后取前 N 个 token，排序后拼接分桶。
3. 第一作者姓 + 年份分桶。

pair 内记录按 `record_id` 字典序（并列时按 source）排列，pair key 为 `SOURCE::ID||SOURCE::ID`。各策略原始对数与并集对数写入 `runs.blocking_stats_json`。

### 相似度（全部仓库内自实现）

| 字段 | 函数 | 说明 |
|---|---|---|
| 标题 | Jaro-Winkler | 标准匹配窗口 + 最多 4 字符前缀加成；O(n·m)。 |
| 作者 | 有序重叠系数 | 作者 key 序列的 LCS / max(长度)，容忍部分乱序；O(a·b)。 |
| 刊名 | token Jaccard | 词集合交集/并集；O(n+m)。 |
| 年份 | 差分档 | 0 年=1，1 年=.75，2 年=.40，≥3 年=0。 |
| 页码 | 区间重叠度 | 闭区间交集长度/并集长度，支持 `start-end` 和单页。 |
| DOI | 精确匹配 | 双缺失为缺失；一方缺失按缺失策略；双方存在不同=0。 |

总分 = Σ(权重 × 字段分) / 有效权重和。`neutral` 保留缺失字段权重并给 0.5；`exclude` 剔除缺失字段并重新归一化。分项 `raw_score/score/weight/contribution/status` 全部落库。DOI 命中是强信号但不是单独否决/放行条件。

### 聚类与人工约束

- 分数 ≥ `match_threshold` 的 pair 成为模型边；按“分数降序、pair key 升序”处理。
- 人工 `same` 是强制连边，人工 `different` 是禁连边；同一 pair 以最新裁定为准。
- 模型边不能违反禁连边，即使可通过其他记录传递；若人工 `same` 与更新的 `different` 冲突，按提交时间/自增序号由新裁定胜出，并写 `review_conflict` 审计事件。
- 并查集按 rank 合并，最终簇按最小成员 key 排序；输出与输入顺序无关。

### 跨批次裁定作用域与继承

人工裁定有两种作用域，提交时通过 `scope` 显式选择，缺省为 `batch`：

- **`batch`（默认）**：写入 `decisions`，只在提交批次内生效，行为与历史版本完全一致；升级前的旧数据无需迁移，自动按批次作用域继续生效。
- **`cross_batch`**：写入全局表 `cross_batch_decisions`，可被后续批次继承。它与批次生命周期解耦——源批次被删除重建后，裁定行仍然保留。

继承匹配不是只看 `pair_key` 字符串，而是同时校验三重业务身份：

1. 候选对的 `pair_key` 与裁定记录一致；
2. 两侧业务标识（`source::record_id` 规范序）与裁定的 `rec_a_key`/`rec_b_key` 一致；
3. 两侧记录的归一化内容指纹（SHA-256）与裁定保存的指纹一致。

任一条件不满足（典型场景：批次删除后用同名 `record_id` 重建了内容不同的记录，`pair_key` 被复用），都不会盲目继承。该 pair 回落为新的灰区对，在 `/review/next` 中带 `historical_decision`（含 `mismatch_reason` 与来源批次），并写 `inheritance_identity_mismatch` 审计事件。

**冻结与可复现**：批次首次运行管道时，把当时命中的跨批次裁定物化为 `decision_inheritances` 快照。之后该批次重跑只读快照，全局表后续新增或被取代的裁定都无法改写它的 pairs/edges/clusters/golden，`run_id` 保持不变，金记录导出逐字节一致。本批次自己提交的跨批次裁定走独立的 own-cross 通道（不进冻结表），每次运行重新校验身份，因此同名重建后同样受指纹保护。

**优先级**：本地批次裁定 > 本批次提交的跨批次裁定 > 冻结的外来继承裁定。

**并发与互斥**：`cross_batch_decisions` 上建有部分唯一索引 `(pair_key) WHERE status='active'`，数据库层保证同一 `pair_key` 同时只有一条有效跨批次裁定。两个批次近乎同时提交互斥裁定时，一条原子取代旧裁定生效，另一条在唯一索引上失败并回滚到保存点，返回 409 并携带胜出裁定，绝不会出现两个批次各自认为自己的裁定生效。被取代的行保留为 `status='superseded'` 以留存完整审计轨迹。

### 金记录幸存规则

| 规则 | 默认字段 | 行为 |
|---|---|---|
| `longest` | title, pages | 取最长非空值。 |
| `most_frequent` | venue | 取出现次数最多的归一值，平票按来源优先级/记录 key。 |
| `min_year` | year | 取最小年份。 |
| `source_priority` | doi | 按来源优先级取第一个非空值。 |
| `non_null_concat` | authors, volume, issue | 作者取最完整作者表并补充去重作者；字符串去重后以 `; ` 连接。 |

金记录保存 `member_count` 与每字段 `provenance`（入选来源记录 key）。所有并列都用来源优先级 → 记录 key 解决。

## 7. 幂等、确定性与审计

- 同一批次重复导入：内容哈希相同直接返回原批次，`inserted=0`；`(batch_id, source, record_id)` 唯一约束兜底。
- 同一 batch + config + 裁定历史：`run_id` 相同，重跑重建完全一致的 pairs/edges/clusters/golden。
- 每次实际调用追加 `run_attempts`，记录输入/坏行/候选对/边/簇/金记录数、各阶段耗时、状态、起止时间；`audit_events` 记录冲突与完成/失败。
- 时间戳和耗时通过可注入 `Clock` 获取；测试使用 `FixedClock`。所有集合排序显式，JSON 使用排序键。
- 配置 SHA-256 写入 `runs.config_hash` 和指标报告；报告位于 `data/reports/{run_id}.json` 与 `{run_id}_thresholds.json`。
- 指标：pairwise TP/FP/FN 基于金标准簇；簇级完全一致率 = 与某个金标准簇成员集合完全相同的预测簇数 / 有金标签的预测簇数。阈值扫描仅复用 `pairs.score`，不重跑归一化或阻塞。
- 指标报告按**当前有效裁定**（本地 + own-cross + 冻结继承）即时重算聚类，是评估视图，不读写该 run 已冻结的 `clusters`/`edges`；因此在 run 落库后新增的裁定会反映在新报告里，但不会改变该 run 的 `run_id` 与已存储结果。
- 并发：进程内由可重入锁串行化所有读写；跨进程共用同一 SQLite 文件时依赖 WAL 与 `busy_timeout=5000` 等待写锁，跨批次裁定的互斥性最终由部分唯一索引在数据库层保证，锁等待超时也会回落为 409 冲突响应。

## 8. 已知局限

- 归一化在导入时完成；修改 `normalization` 配置后需重新导入批次（幂等以内容哈希为准）。
- 单进程 SQLite，适合本地中小规模批次；阻塞桶内最坏 O(n²)，不做分布式/增量流式处理。
- 不做跨语言、语义 embedding、作者机构消歧、DOI 联网校验。
- 仅接收 JSONL/HTTP 与 JSONL/CSV 导出，不解析 RIS/BibTeX/EndNote。
- 金记录字段为归一化后的值；原始值保留在簇成员的 `raw_json` 中。
- 复核裁定接口同步自动重跑，超大批次时请求耗时随管道耗时增长。

## 9. 测试与验收走查

```bash
# 安装
python -m pip install -r requirements.txt
python -m pip install . --no-deps --no-build-isolation

# 全绿测试（覆盖 F1–F8、幂等、复核、API 错误路径）
python -m pytest -q

# 确定性 fixture：同 seed 两次生成字节一致
python -m scripts.seed_fixtures
sha256sum data/fixtures/records.jsonl data/fixtures/gold.json

# 启动服务（终端 A）
python -m citefuse.app

# 终端 B：导入 → 跑管道 → 查看
curl -s -X POST 'http://127.0.0.1:8321/batches?batch_id=DEMO&name=Demo' \
  -H 'Content-Type: application/x-ndjson' --data-binary @data/fixtures/records.jsonl
RUN=$(curl -s -X POST 'http://127.0.0.1:8321/runs' \
  -H 'Content-Type: application/json' -d '{"batch_id":"DEMO"}' \
  | python -c 'import json,sys; print(json.load(sys.stdin)["run_id"])')
curl -s "http://127.0.0.1:8321/clusters?run_id=$RUN" | head -c 500
curl -s "http://127.0.0.1:8321/review/next?batch_id=DEMO&n=1"

# 复制上一步 pair_key，提交裁定（会自动重跑并生成新 run）
curl -s -X POST 'http://127.0.0.1:8321/review/decisions' \
  -H 'Content-Type: application/json' \
  -d '{"batch_id":"DEMO","pair_key":"PAIR_KEY_HERE","decision":"different","reviewer":"reviewer-1","note":"different trial"}'

# 显式提交一条可跨批次复用的裁定（scope 省略时默认为 batch）
curl -s -X POST 'http://127.0.0.1:8321/review/decisions' \
  -H 'Content-Type: application/json' \
  -d '{"batch_id":"DEMO","pair_key":"PAIR_KEY_HERE","decision":"same","scope":"cross_batch","note":"reusable for later monthly exports"}'
# 后续批次首次跑 /runs 后，该 pair 不再出现在它的 /review/next；
# /runs 的 inheritances 字段可查到来源批次 DEMO。

# 指标、阈值扫描、导出
curl -s "http://127.0.0.1:8321/metrics/report?run_id=$RUN"
curl -s "http://127.0.0.1:8321/metrics/thresholds?run_id=$RUN&grid=0.60,0.70,0.80,0.90"
curl -s "http://127.0.0.1:8321/golden/export?run_id=$RUN&format=jsonl" -o data/golden.jsonl
curl -s "http://127.0.0.1:8321/golden/export?run_id=$RUN&format=csv" -o data/golden.csv

# 级联删除
curl -s -X DELETE 'http://127.0.0.1:8321/batches/DEMO'
```

验收关注点：`pytest` 全绿；fixture 200 条、120 个金标准簇；管道 F1 ≥ 0.90（默认实现为 1.0）；同一批次重复导入 `inserted=0`；同一 run 重跑簇/金记录逐行一致；复核裁定后重跑分裂/合并；阈值扫描与报告落盘；`data/reports/` 和 `data/citefuse.sqlite3` 为运行期产物且已被 `.gitignore` 忽略。
