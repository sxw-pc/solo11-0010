# citefuse（题录熔合）

**纯本地、可离线运行的题录去重与主记录归一引擎。**  
输入多来源、脏格式的 JSONL 题录，经过校验、归一化、阻塞配对、相似度打分、聚类和字段级幸存规则，输出可审计的簇与「金记录」；不确定的配对进入人工复核队列，人工裁定发布为带版本的回流视图，提交即刻从复核队列离线消失，并在下一次运行打分前优先于模型分生效。

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
| `decisions` | `pair_key`, `decision`, `reviewer`, `note`, `submitted_at` | 人工裁定的 append-only 审计历史；同 pair 以提交时间/自增序号最新者为准。字段与语义保持不变。 |
| `decision_view_versions` | `batch_id`, `current_version` | 每个批次当前的裁定视图版本指针；无裁定的批次停在版本 0。 |
| `decision_views` | `(batch_id,view_version,pair_key)` PK, `decision`, `decision_id`, 逻辑 record key | 批次级**裁定即时回流视图**：某版本下每个 pair 至多一行有效裁定，版本不可变；按逻辑 record key 对齐并重算 canonical pair_key，免疫 record 主键漂移。 |
| `audit_events` | `run_id`, `event_type`, `detail_json`, `created_at` | 复核冲突、运行完成/失败等审计事件。 |

`run_id` 由批次内容哈希、配置哈希和裁定历史确定性派生；`cluster_id` 由批次与排序后的成员 key 派生。相同输入重跑会复用同一个 `run_id`，并重建相同的 pairs/edges/clusters/golden 行。每次调用仍会追加 `run_attempts` 和审计事件。

### 裁定即时回流视图（versioned decision view）

人工裁定的真源仍是 append-only 的 `decisions` 表（字段名、`pair_key` 语义、无额外唯一约束全部兼容历史库）。在其之上为每个批次维护一份**带版本号、不可变的有效裁定快照**：

- 提交裁定时，在**同一个 SQLite 事务**内追加 `decisions` 行并发布下一版视图；视图主键 `(batch_id, view_version, pair_key)` 保证每个 pair 在任一版本中只有一条有效裁定——两个复核员并发对同一 pair 提交互斥裁定，两条审计行都保留，但视图里只存活最新一条，绝不出现两条互斥的 same/different。
- 视图版本不可变。`/runs` 启动时 pin 一个版本号，同批次两个并行 run 读到的裁定态必然完全一致。
- 视图行通过逻辑键 `source::record_id` 对齐批次当前 records 并重算 canonical `pair_key`；记录重传导致 `record_pk` 漂移后裁定仍然命中，不再被静默丢弃。
- 升级旧库无需人工重新裁定：任何读路径首次访问时自动把历史 `decisions` 投影为版本 1。
- `/review/next` 基于当前视图版本做**离线过滤**，提交后立即不再返回该 pair，不依赖重跑，也不会因 `pairs` 表重建而漂移。
- 下一次 `/runs` 在**打分之前**应用回流：已判 `different` 的候选对直接出池（不打分、不进 `pairs`），已判 `same` 的候选对跳过打分直接作为 `human_same` 边进入聚类。回流计数写入 `runs.blocking_stats_json.decision_reflux`。

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
| POST | `/runs` | `{"batch_id":"..."}` | 201，跑完整管道并返回 run 摘要、阻塞统计、冲突审计。 |
| GET | `/runs` | `?batch_id=` | 运行列表。 |
| GET | `/runs/{run_id}` | — | 运行详情、最近一次尝试耗时、审计事件。 |
| GET | `/review/next` | `?n=10&batch_id=` | 最新 run 中未裁定的灰区 pair，含双记录、总分、分项、策略。已提交裁定的 pair 由裁定视图即时离线过滤，无需重跑。 |
| POST | `/review/decisions` | `{"batch_id","pair_key","decision":"same\|different","reviewer","note"}` | 持久化裁定并在同一事务发布新版裁定视图；**不再同步重跑**，返回 `decision`（含 `view_version`）。下一次 `/runs` 在打分前应用回流。 |
| GET | `/review/decisions` | `?batch_id=` | 裁定历史（append-only 审计行）。 |
| GET | `/review/view` | `?batch_id=` | 当前裁定视图：`view_version`、有效裁定数、same/different 计数及每 pair 的有效裁定行。 |
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

- 回流发生在**打分之前**：当前裁定视图中判 `different` 的候选对直接出池（不打分、不写入 `pairs`），判 `same` 的候选对跳过打分直接作为 `human_same` 边进入聚类；其余候选对正常打分，分数 ≥ `match_threshold` 者成为模型边，按“分数降序、pair key 升序”处理。
- 人工 `same` 是强制连边，人工 `different` 是禁连边；同一 pair 以最新裁定为准。
- 模型边不能违反禁连边，即使可通过其他记录传递；若人工 `same` 与更新的 `different` 冲突，按提交时间/自增序号由新裁定胜出，并写 `review_conflict` 审计事件。
- 并查集按 rank 合并，最终簇按最小成员 key 排序；输出与输入顺序无关。

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
- 裁定视图版本不可变；run 启动时 pin 版本，同批次并行 run 读到完全一致的裁定态。视图发布与决策追加在同一事务内，并发提交同一 pair 时视图仅保留一条有效裁定。
- 旧库升级：新表由启动时幂等 DDL 创建，历史 `decisions` 在首次读视图时惰性投影为版本 1，无需人工重新裁定。
- 每次实际调用追加 `run_attempts`，记录输入/坏行/候选对/边/簇/金记录数、各阶段耗时、状态、起止时间；`audit_events` 记录冲突与完成/失败。
- 时间戳和耗时通过可注入 `Clock` 获取；测试使用 `FixedClock`。所有集合排序显式，JSON 使用排序键。
- 配置 SHA-256 写入 `runs.config_hash` 和指标报告；报告位于 `data/reports/{run_id}.json` 与 `{run_id}_thresholds.json`。
- 指标：pairwise TP/FP/FN 基于金标准簇；簇级完全一致率 = 与某个金标准簇成员集合完全相同的预测簇数 / 有金标签的预测簇数。阈值扫描仅复用 `pairs.score`，不重跑归一化或阻塞。

## 8. 已知局限

- 归一化在导入时完成；修改 `normalization` 配置后需重新导入批次（幂等以内容哈希为准）。
- 单进程 SQLite，适合本地中小规模批次；阻塞桶内最坏 O(n²)，不做分布式/增量流式处理。
- 不做跨语言、语义 embedding、作者机构消歧、DOI 联网校验。
- 仅接收 JSONL/HTTP 与 JSONL/CSV 导出，不解析 RIS/BibTeX/EndNote。
- 金记录字段为归一化后的值；原始值保留在簇成员的 `raw_json` 中。
- 单进程 SQLite + 进程内锁序列化写事务；裁定视图版本在该模型下保证并发安全，跨进程的外部写者需另行协调。

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

# 复制上一步 pair_key，提交裁定（即时更新回流视图，不触发重跑）
curl -s -X POST 'http://127.0.0.1:8321/review/decisions' \
  -H 'Content-Type: application/json' \
  -d '{"batch_id":"DEMO","pair_key":"PAIR_KEY_HERE","decision":"different","reviewer":"reviewer-1","note":"different trial"}'
# 查看当前裁定视图版本与有效裁定
curl -s "http://127.0.0.1:8321/review/view?batch_id=DEMO"
# 显式触发下一次 run：different 出池、same 直接进边（发生在打分之前）
curl -s -X POST http://127.0.0.1:8321/runs -H 'Content-Type: application/json' -d '{"batch_id":"DEMO"}'

# 指标、阈值扫描、导出
curl -s "http://127.0.0.1:8321/metrics/report?run_id=$RUN"
curl -s "http://127.0.0.1:8321/metrics/thresholds?run_id=$RUN&grid=0.60,0.70,0.80,0.90"
curl -s "http://127.0.0.1:8321/golden/export?run_id=$RUN&format=jsonl" -o data/golden.jsonl
curl -s "http://127.0.0.1:8321/golden/export?run_id=$RUN&format=csv" -o data/golden.csv

# 级联删除
curl -s -X DELETE 'http://127.0.0.1:8321/batches/DEMO'
```

验收关注点：`pytest` 全绿；fixture 200 条、120 个金标准簇；管道 F1 ≥ 0.90（默认实现为 1.0）；同一批次重复导入 `inserted=0`；同一 run 重跑簇/金记录逐行一致；复核裁定后重跑分裂/合并；阈值扫描与报告落盘；`data/reports/` 和 `data/citefuse.sqlite3` 为运行期产物且已被 `.gitignore` 忽略。
