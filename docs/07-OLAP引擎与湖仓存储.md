# 07 · OLAP 引擎与湖仓存储

> 对应排期：W5 D4-D7
> 定位：⭐⭐ 为主，选型对比部分 ⭐⭐⭐。实时数仓的落地终点，Leader 面常问选型理由。

## 一、OLAP 引擎全景 ⭐⭐

| 引擎 | 类型 | 架构 | 强项 | 弱项 |
| --- | --- | --- | --- | --- |
| **Hive / Spark SQL** | 批式 OLAP | 计算存储分离（HDFS） | 海量数据、生态成熟、成本低 | 延迟高（分钟级） |
| **Impala** | MPP 交互式 | 依赖 HDFS + Hive Metastore | 秒级查询 | 内存敏感、稳定性一般 |
| **Presto / Trino** | MPP 联邦查询 | 纯内存计算、无存储 | 跨源联邦查询、秒级 | 大数据量易 OOM，不适合 ETL |
| **ClickHouse** | 单表 OLAP | Shared-Nothing MPP | 单表聚合极快、压缩率高 | Join 弱、高并发差、更新删除弱 |
| **Doris / StarRocks** | MPP 分析型数据库 | FE + BE，存算一体（新版支持存算分离） | 高并发、Join 强、实时写入、MySQL 协议 | 超大规模成本高 |
| **Kylin** | 预计算 Cube | 依赖 Hadoop | 固定维度亚秒响应 | 维度爆炸、灵活性差 |
| **Druid** | 时序 OLAP | 实时摄入 + 预聚合 | 高并发时序查询 | SQL 能力弱、Join 差 |

## 二、ClickHouse ⭐⭐

### 2.1 为什么快

1. **列式存储 + 高效压缩**（LZ4/ZSTD，压缩比常 10:1）
2. **向量化执行引擎**：按列批量处理，充分利用 SIMD 指令
3. **稀疏主键索引**：每 8192 行一个 mark，索引常驻内存
4. **多核并行**：单查询自动拆分到所有 CPU 核
5. **数据分片 + 分布式表**：Shard 并行扫描
6. **多种专用引擎**：MergeTree 家族（ReplacingMergeTree、SummingMergeTree、AggregatingMergeTree、CollapsingMergeTree）

### 2.2 MergeTree 核心

```sql
CREATE TABLE t (
    dt Date, user_id UInt64, amt Decimal(18,2)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(dt)
ORDER BY (dt, user_id)          -- 排序键即主键索引基础
SETTINGS index_granularity = 8192;
```
- [ ] **数据组织**：Partition → Part（目录）→ Column 文件（.bin）+ 主键索引（primary.idx）+ mark 文件
- [ ] **查询流程**：分区裁剪 → 主键索引粗筛（mark range）→ 跳数索引（minmax/set/bloom filter）→ 读列 → 向量化计算
- [ ] **后台 Merge**：Part 不断合并，这也是「更新删除是异步」的根源

### 2.3 更新与删除（弱点）⭐⭐

- [ ] `ALTER TABLE ... UPDATE/DELETE`：**Mutation**，异步重写整个 Part，代价极大，不适合频繁调用
- [ ] `ReplacingMergeTree`：Merge 时按 ORDER BY key 去重保留最新，但 **Merge 时机不确定 → 查询时要用 `FINAL` 或自行去重**
- [ ] `CollapsingMergeTree` / `VersionedCollapsingMergeTree`：靠 Sign 列（1/-1）折叠，需保证写入顺序
- [ ] 轻量删除：`DELETE FROM`（新版本支持，标记式）
- [ ] 结论：**ClickHouse 不适合高频更新场景**，这类需求走 Doris/StarRocks 或 Hudi/Paimon

### 2.4 使用注意

- [ ] Join 能力弱：大表 Join 大表容易 OOM，建议**宽表化**（预先 Join 好）或用字典表
- [ ] 高并发差：官方建议 QPS < 100，需要前置缓存或代理层
- [ ] 分布式表 vs 本地表：写入写本地表，查询查分布式表；避免分布式表写入的不均衡
- [ ] 副本：`ReplicatedMergeTree` 依赖 ZooKeeper（新版可用 ClickHouse Keeper）
- [ ] 数据倾斜：分片键要选好（`rand()` 均匀但同 key 数据分散；`cityHash64(user_id)` 保证同用户在同分片利于 Join）

## 三、Doris / StarRocks ⭐⭐⭐（当前国内主流）

### 3.1 架构

```
FE（Frontend，Java）
   ├── 元数据管理（BDB JE Raft 选主）
   ├── SQL 解析 / 查询规划 / 调度
   └── 分 Master / Follower / Observer
BE（Backend，C++）
   ├── 数据存储（列存，本地磁盘）
   ├── 查询执行（向量化）
   └── 数据导入
```
- [ ] StarRocks 3.x 支持**存算分离**（数据放对象存储 + 本地 Cache），弹性伸缩更好
- [ ] 兼容 **MySQL 协议**，BI 工具直接连

### 3.2 数据模型 ⭐⭐⭐（必考）

| 模型 | 说明 | 适用 |
| --- | --- | --- |
| **Duplicate Key** | 明细模型，不去重，按前缀排序 | 日志、明细事实表 |
| **Aggregate Key** | 预聚合模型，相同 key 按聚合函数合并（SUM/MAX/MIN/REPLACE） | 固定维度报表 |
| **Unique Key** | 唯一主键模型，保证 key 唯一 | 需要 upsert 的维度/事实表 |

**Unique Key 的两种实现** ⭐⭐⭐：
- **Merge-on-Read**：写入时只追加，读取时合并去重 → 写快读慢
- **Merge-on-Write**（Doris 1.2+ / StarRocks 主键模型）：写入时标记删除旧版本 → 读快写稍慢，**支持高并发点查和实时更新**，是当前推荐方案

### 3.3 分区分桶 ⭐⭐

```sql
CREATE TABLE dwd_order (
    dt DATE, order_id BIGINT, user_id BIGINT, amt DECIMAL(18,2)
)
UNIQUE KEY(dt, order_id)
PARTITION BY RANGE(dt) (
    PARTITION p20240101 VALUES LESS THAN ('2024-01-02'),
    ...   -- 或动态分区 dynamic_partition
)
DISTRIBUTED BY HASH(order_id) BUCKETS 32;
```
- [ ] **分区（Partition）**：按时间，支持动态分区自动创建/过期删除，实现分区裁剪
- [ ] **分桶（Bucket）**：分区内按 hash 打散，是**数据分布和并行执行的最小单位**
- [ ] 分桶数经验：单个 Tablet 大小控制在 **1-10GB**（Doris 官方建议 1G 左右），`桶数 ≈ 分区数据量 / 1GB`，且建议为 BE 节点数的整数倍
- [ ] 追问：分桶太多/太少有什么问题？（太多 → 小文件多、元数据压力、查询调度开销；太少 → 单 Tablet 过大、并行度不足、数据倾斜）

### 3.4 数据导入方式 ⭐⭐

| 方式 | 说明 | 场景 |
| --- | --- | --- |
| **Stream Load** | HTTP PUT 同步导入本地文件/流 | 小批量、实时写入 |
| **Routine Load** | 持续消费 Kafka | **实时数仓主流** |
| **Broker Load** | 从 HDFS/S3 异步批量导入 | 离线大批量 |
| **Insert Into** | SQL 写入，支持 `insert into select` | 小量、跨表 |
| **Spark Load** | 通过 Spark 预处理后导入 | 超大批量、需要 ETL |
| **Flink Connector** | Flink Sink 两阶段提交 | 实时 + Exactly-Once |

- [ ] 追问：Routine Load 怎么保证不丢不重？（Kafka offset 由 Doris 内部管理，导入任务失败自动回退 offset 重试；Unique Key 模型保证幂等 upsert）
- [ ] 追问：实时写入为什么会产生大量小版本？怎么解决？（每次导入生成一个 rowset/version；Doris 后台 Compaction 合并；写入端要**攒批**，避免高频单条写入导致 `-235 TOO_MANY_VERSIONS`）

### 3.5 查询优化 ⭐⭐

- [ ] **前缀索引**：表 key 的前 36 字节构成前缀索引，建表时把高频过滤字段放前面
- [ ] **物化视图 / Rollup**：预聚合，查询自动路由
- [ ] **Colocate Join**：两表分桶键相同 + 桶数相同 + 同一 Colocate Group → 本地 Join，避免 Shuffle ⭐⭐⭐
- [ ] **Bucket Shuffle Join**：只 Shuffle 小表到对应桶，减少网络传输
- [ ] **Bitmap / BloomFilter 索引**：高基数等值查询加速；Bitmap 用于精确去重（`bitmap_union`、`bitmap_union_count`）
- [ ] **分区裁剪 + 谓词下推**
- [ ] 追问：如何做精确 UV？（`bitmap_union(to_bitmap(user_id))`，或 HLL 近似 `hll_union_agg`）

### 3.6 Doris vs StarRocks（选型话术）⭐⭐

| 维度 | Doris | StarRocks |
| --- | --- | --- |
| 出身 | 百度 Palo → Apache 顶级项目 | DorisDB → 独立商业化，Fork 自早期 Doris |
| 社区 | Apache 基金会，中立、活跃 | 商业公司主导，迭代快 |
| 存算分离 | 3.0 开始支持 | 3.0 起成熟 |
| 性能 | 优秀 | 多表 Join / CBO 常被认为更强 |
| 生态 | 国内使用最广，文档中文友好 | 亦广泛 |

**回答模板**：「两者同源、能力接近，我们选 X 主要因为（已有集群/社区活跃度/存算分离需求/团队熟悉度/多表 Join 性能）。技术上都能满足实时数仓的 upsert + 高并发查询需求。」

## 四、数据湖格式 ⭐⭐⭐

### 4.1 为什么需要数据湖格式

传统 Hive 表的痛点：
1. 不支持 ACID 更新（只能整分区覆盖）
2. 无版本管理，无法时间旅行
3. Schema 演进受限
4. 小文件问题严重，无自动 Compaction
5. 流批割裂，实时写入不友好
6. 分区列表依赖 Metastore，list 慢

**湖格式（Table Format）在「文件」之上加了一层元数据/事务管理**，解决上述问题。

### 4.2 三大湖格式对比 ⭐⭐⭐（必考）

| 维度 | **Apache Hudi** | **Apache Iceberg** | **Apache Paimon** |
| --- | --- | --- | --- |
| 出身 | Uber（2016） | Netflix → Apache | Flink Table Store 演进而来 |
| 定位 | 增量数据管道、近实时数仓 | 开放表格式标准、引擎中立 | 流式数据湖、流批一体 |
| 表类型 | COW（Copy-On-Write）/ MOR（Merge-On-Read） | 单一（元数据分层） | LSM-Tree 结构 |
| 更新能力 | 强（原生 upsert） | 支持（v2 format，equality delete） | **强，LSM 天然适合高频 upsert** |
| 流式读取 | 支持增量查询 | 支持增量读 | **原生流读流写，changelog 完整** |
| 引擎兼容 | Spark/Flink/Presto/Trino | **最广，几乎所有引擎** | Flink 最佳，Spark 次之 |
| 元数据 | Timeline + .hoodie | Manifest List → Manifest → Data File，纯文件元数据 | Snapshot + Manifest（类 Iceberg） |
| 小文件治理 | Clustering / Compaction | Rewrite + 自动 Compaction | LSM Compaction |
| 社区热度 | 成熟稳定 | 事实标准，增长最快 | 国内新兴，Flink 生态首选 |

**选型话术**：
- 已有 Hive 数仓要平滑升级、多引擎混用 → **Iceberg**（中立标准、隐藏分区、Schema 演进最规范）
- 需要频繁 upsert 的近实时数仓、CDC 入湖 → **Hudi**（COW/MOR 灵活）或 **Paimon**
- 以 Flink 为核心的流式湖仓、需要完整 changelog → **Paimon**
- 追问：为什么 Paimon 适合流？（LSM-Tree 结构天然支持高频写入与 Compaction，能生成完整的 changelog 供下游流读，实现「流式数仓分层」）

### 4.3 核心能力 ⭐⭐⭐

**1. ACID 与快照隔离**
- 写入生成新快照（原子提交），读者始终看到一致的快照版本
- Iceberg 提交冲突通过乐观并发控制（OCC）+ 元数据原子替换解决

**2. Time Travel（时间旅行）**
```sql
SELECT * FROM t FOR VERSION AS OF 1234567890;
SELECT * FROM t FOR TIMESTAMP AS OF '2024-01-01 00:00:00';
```
- 用途：数据回溯、问题排查、审计、回滚

**3. Schema Evolution**
- 加列/删列/改名/改类型/调整顺序，**不需要重写数据**（靠列 ID 而非列名映射）
- Iceberg 做得最规范（保证 exactly-once 的列变更语义）

**4. Hidden Partitioning（隐藏分区）⭐ Iceberg 独有**
```sql
PARTITIONED BY (days(ts))    -- 用户查询写 where ts > '2024-01-01' 即可自动分区裁剪
```
- 传统 Hive 需要用户显式写 `where dt='2024-01-01'`，否则全表扫；Iceberg 把分区转换规则记在元数据里，用户无感

**5. Partition Evolution**
- Iceberg 支持修改分区方案而不重写历史数据（新数据用新方案，元数据分别记录）

**6. 小文件治理**
```sql
-- Iceberg
CALL catalog.system.rewrite_data_files('db.t');
CALL catalog.system.rewrite_manifests('db.t');
CALL catalog.system.expire_snapshots('db.t', TIMESTAMP '...');
-- Hudi
hoodie.cleaner.policy=KEEP_LATEST_BY_HOURS
hoodie.compact.inline=true
```

### 4.4 Hudi 的 COW vs MOR ⭐⭐⭐（高频）

| 维度 | Copy-On-Write（COW） | Merge-On-Read（MOR） |
| --- | --- | --- |
| 写入 | 合并后重写整个 Parquet 文件 | 变更写入 delta log（Avro） |
| 写放大 | 高 | 低 |
| 读性能 | **高**（直接读 Parquet） | 低（需实时合并 base + log） |
| 延迟 | 分钟级 | **秒级** |
| 存储 | 无额外 log | base file + log file |
| 适用 | 读多写少、批处理、报表 | 写多、近实时、CDC 入湖 |

- [ ] 追问：MOR 怎么优化读性能？（定期 Compaction 把 log 合并进 base file；查询时可指定 `read_optimized` 表只读 base）
- [ ] 追问：Hudi 的 Timeline 是什么？（记录所有 action 的时间线：commit、deltacommit、clean、compaction、rollback，是 Hudi 的事务与版本基础）

### 4.5 湖仓一体（Lakehouse）⭐⭐

- [ ] 定义：在数据湖的低成本、灵活存储之上，提供数仓级别的事务、Schema 管理、性能优化能力
- [ ] 典型架构：
  ```
  数据源 → CDC/日志采集 → Kafka
                          ↓
             Flink（流式 ETL）→ Paimon/Hudi/Iceberg（湖存储，分层 ODS/DWD/DWS）
                          ↓                    ↓
              Spark（批处理/修正）      Trino/StarRocks/Doris（查询加速，可用外表直查湖）
                          ↓
                    BI / 数据服务
  ```
- [ ] 优势：一份数据服务批流与多种查询引擎，避免 Lambda 的两套代码/存储；成本低于传统数仓
- [ ] 追问：湖仓一体的挑战？（查询性能仍不及专用 OLAP、小文件治理成本、元数据规模膨胀、生态兼容与运维复杂度、权限治理不成熟）
- [ ] 追问：StarRocks/Doris 和数据湖什么关系？（可作为湖的**查询加速层**，通过 External Catalog 直查 Iceberg/Hudi/Paimon，也可把热数据内置存储 —— 「湖仓 + MPP」混合架构）

## 五、实时数仓完整链路（务必能默画）⭐⭐⭐

```
┌──────────────┐
│ 业务库 MySQL  │──Flink CDC(binlog)──┐
└──────────────┘                     │
┌──────────────┐                     ▼
│ 埋点/日志     │──Flume/Flink SDK──► Kafka（ODS 层，按业务分 Topic）
└──────────────┘                     │
                                     ▼
                        ┌────────────────────────┐
                        │ Flink（实时 ETL）        │
                        │  清洗/打宽/维度关联       │ → Kafka DWD（明细宽表，changelog）
                        │  (Lookup/异步IO 维表)    │
                        └────────────────────────┘
                                     │
                     ┌───────────────┼──────────────────┐
                     ▼               ▼                  ▼
              Flink 轻度汇总    Flink 实时指标       数据湖（Paimon/Hudi）
              → Kafka DWS      （窗口聚合/TopN）     支持流读 + 批修正
                     │               │                  │
                     └───────────────┴──────────────────┘
                                     ▼
                        ┌────────────────────────┐
                        │ OLAP 存储                │
                        │ Doris/StarRocks/CK      │ ← ADS 层，支持高并发查询
                        │ + Redis（热点结果缓存）   │
                        └────────────────────────┘
                                     ▼
                        实时大屏 / BI 报表 / 数据服务 API
                                     ▲
                        离线 T+1（Spark/Hive）修正对账 ──┘
```

**每一跳的关键设计点**：

| 环节 | 关键问题 | 方案 |
| --- | --- | --- |
| 采集 | 全量+增量一致 | Flink CDC 无锁快照 + binlog 位点衔接 |
| Kafka ODS | 分区数、保留时间 | 按峰值吞吐规划；保留 3-7 天用于回溯 |
| Flink ETL | 维表关联 | 异步 IO + 本地缓存 / Lookup Join / 广播维表 |
| 实时分层 | 中间结果复用 | Kafka Topic 分层（ODS/DWD/DWS）或 Paimon 流式湖仓 |
| 精确一次 | 端到端 EOS | Source offset 入 Checkpoint + Sink 两阶段提交/幂等 upsert |
| 迟到数据 | 准确性 | Watermark + allowedLateness + 侧输出 + 离线修正 |
| OLAP | 高并发 + 实时更新 | Doris Unique Key（MOW）+ 攒批写入 + Compaction |
| 数据一致 | 实时 vs 离线 | 每日对账任务，差异告警，以离线为准修正 |

## 六、本章高频面试题速查

1. ClickHouse 为什么快（⭐⭐⭐）
2. ClickHouse 的更新删除机制，ReplacingMergeTree 的坑（⭐⭐）
3. Doris/StarRocks 的三种数据模型，Unique Key 的 MOW vs MOR（⭐⭐⭐）
4. Doris 分区分桶怎么设计，桶数怎么定（⭐⭐⭐）
5. Doris 数据导入方式，Routine Load 怎么保证不丢（⭐⭐）
6. Colocate Join 是什么（⭐⭐）
7. Hudi 的 COW 和 MOR 区别（⭐⭐⭐）
8. Iceberg / Hudi / Paimon 怎么选（⭐⭐⭐ 必考）
9. 什么是湖仓一体，解决什么问题（⭐⭐⭐）
10. Iceberg 的隐藏分区有什么好处（⭐⭐ 加分）
11. 画一下你们实时数仓的架构（⭐⭐⭐ 必考）
12. 实时和离线数据不一致怎么办（⭐⭐⭐）
13. Presto/Trino 和 Spark SQL 的区别（⭐⭐）
