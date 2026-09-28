# 04 · Spark 核心与调优

> 对应排期：W3
> 定位：⭐⭐⭐ 最高优先级。离线方向面试的核心战场，中高级岗位会深挖 Shuffle、内存模型、AQE 与真实调优案例。

## 一、Spark 基础与架构 ⭐⭐

### 1.1 为什么比 MapReduce 快

1. **内存计算**：中间结果不落 HDFS（MR 每个 Stage 都要落盘）
2. **DAG 调度**：多 Stage 统一优化，减少不必要的 Shuffle 和落盘
3. **线程级 Task**：Task 是线程而非进程，启动开销小，可 JVM 复用
4. **算子丰富**：80+ 算子 vs MR 只有 map/reduce
5. **惰性求值 + Catalyst 优化器**（SQL）

- [ ] 追问：Spark 一定比 MR 快吗？（不一定。数据量远超内存、Shuffle 极重时，Spark 也要落盘，优势缩小；MR 在超大规模稳定性上更成熟）

### 1.2 运行架构

```
Driver（SparkContext）
   │  ① 申请资源
   ▼
Cluster Manager（YARN / K8s / Standalone）
   │  ② 分配 Container
   ▼
Executor（多个，每个含多个 Task 线程 + BlockManager + Cache）
   │  ③ 执行 Task，汇报状态
   ▼
Driver
```

- [ ] 部署模式：
  - **client**：Driver 在提交机，日志好看，适合调试；提交机挂了作业就挂
  - **cluster**：Driver 在集群 AM 中，适合生产
- [ ] YARN cluster 模式下 Driver 就是 ApplicationMaster
- [ ] 一个 Application → 一个 Job（由 Action 触发）→ 多个 Stage（由 Shuffle 边界切分）→ 多个 Task（由分区数决定）

### 1.3 作业提交流程（能画）

```
1. spark-submit 启动 Driver，创建 SparkContext
2. SparkContext 向 Cluster Manager 注册并申请 Executor
3. CM 分配资源，启动 Executor（ExecutorRunner → CoarseGrainedExecutorBackend）
4. Executor 反向注册到 Driver
5. Action 触发 → DAGScheduler 划分 Stage → 生成 TaskSet
6. TaskScheduler 分发 Task 到 Executor（考虑数据本地性）
7. Task 执行，通过心跳与 Driver 通信，结果返回 Driver
```

- [ ] 数据本地性等级（`spark.locality.wait` 默认 3s）：`PROCESS_LOCAL > NODE_LOCAL > NO_PREF > RACK_LOCAL > ANY`

## 二、RDD ⭐⭐⭐

### 2.1 五大特性（必背）

1. **分区列表**（A list of partitions）—— 并行度基础
2. **计算函数**（A function for computing each split）
3. **依赖关系**（A list of dependencies on other RDDs）—— 血缘
4. **分区器**（Optionally, a Partitioner for key-value RDDs）
5. **优先位置**（Optionally, a list of preferred locations）—— 数据本地性

### 2.2 特性与算子

- [ ] 特性：不可变、可分区、惰性求值、血缘容错
- [ ] **宽依赖 vs 窄依赖** ⭐⭐⭐：

| 依赖类型 | 说明 | 示例 | 是否 Shuffle | 失败恢复 |
| --- | --- | --- | --- | --- |
| 窄依赖（Narrow） | 父 RDD 每个分区最多被子 RDD 一个分区使用 | map、filter、union、coalesce(减分区)、mapPartitions | 否 | 只重算丢失分区，代价小 |
| 宽依赖（Wide/Shuffle） | 父 RDD 一个分区被子 RDD 多个分区使用 | groupByKey、reduceByKey、join(未分区)、repartition、distinct | 是 | 需重算所有父分区，代价大 |

- [ ] 追问：为什么 Stage 按宽依赖切分？（Shuffle 是同步边界，父 Stage 所有分区必须全部完成才能开始子 Stage 拉数据）
- [ ] 追问：`union` 为什么是窄依赖？（各分区直接拼接，无跨分区数据交换；但两个 RDD 分区数不同时会产生 Shuffle 依赖的处理）

### 2.3 算子分类

| 类别 | 算子 |
| --- | --- |
| Transformation（窄） | map、mapPartitions、flatMap、filter、union、coalesce、mapPartitionsWithIndex |
| Transformation（宽） | groupByKey、reduceByKey、sortByKey、repartition、distinct、join、cogroup |
| Action | collect、count、take、first、reduce、aggregate、saveAsTextFile、foreach、foreachPartition、show |

- [ ] `reduceByKey` vs `groupByKey` ⭐⭐⭐：
  - `reduceByKey` 有 **Map 端预聚合（combine）**，Shuffle 数据量小
  - `groupByKey` 不预聚合，全量 Shuffle，极易 OOM
  - **生产中优先 reduceByKey / aggregateByKey**
- [ ] `repartition` vs `coalesce` ⭐⭐⭐：
  - `coalesce(n)`：默认不 Shuffle（`shuffle=false`），只能减分区（增分区时无效，因为会退化为不重分布）；`coalesce(n, true)` 等价于 repartition
  - `repartition(n)`：一定 Shuffle，可增可减，分区更均匀
  - 选择：减少分区且不需要均衡用 coalesce（省 Shuffle）；需要打散/增加分区用 repartition
- [ ] `cache` / `persist` / `checkpoint` ⭐⭐⭐：

| 机制 | 存储 | 生命周期 | 血缘 |
| --- | --- | --- | --- |
| `cache()` | 内存（MEMORY_ONLY） | Application 结束 | 保留 |
| `persist(level)` | 可选内存/磁盘/序列化/副本 | Application 结束 | 保留 |
| `checkpoint()` | 可靠存储（HDFS） | 永久 | **切断**（重算依赖丢失） |

  - `checkpoint` 会触发一次额外 Job（除非先 cache）；用于长血缘链路截断，防止重算代价过大
  - 存储级别：`MEMORY_ONLY`、`MEMORY_AND_DISK`、`MEMORY_ONLY_SER`、`DISK_ONLY`、`*_2`（副本）
  - 追问：cache 后数据一定在内存吗？（不一定，内存不足会按 LRU 淘汰到磁盘或重算，取决于存储级别）

### 2.4 分区器

- [ ] HashPartitioner（默认，`key.hashCode % numPartitions`，可能负数取模处理）
- [ ] RangePartitioner（水塘抽样确定边界，用于 sortByKey，全局有序）
- [ ] 自定义分区器：继承 `Partitioner`，实现 `numPartitions` 与 `getPartition`
- [ ] 追问：HashPartitioner 会倾斜吗？（会，key 分布不均时；解决见 §六）

## 三、Shuffle 机制 ⭐⭐⭐（必考重点）

### 3.1 Shuffle 演进

| 版本 | 机制 | 问题 |
| --- | --- | --- |
| Spark 0.8 及以前 | **Hash Shuffle**（未优化）：每个 Map Task 为每个 Reduce 生成一个文件 → `M × R` 个文件 | 小文件爆炸 |
| Spark 1.2+ | **Hash Shuffle 优化版**：同一 Executor 内多个 Task 复用文件 → `Cores × R` | 仍有大量文件、无排序 |
| Spark 1.2+ 默认 | **Sort Shuffle**：每个 Map Task 生成一个数据文件 + 一个索引文件 | 主流，文件数 = 2M |
| — | **Bypass Merge Sort Shuffle**：无排序、分区数 ≤ 200（`spark.shuffle.sort.bypassMergeThreshold`）且非 map 类依赖时走，避免排序开销 | 适合分区少的场景 |
| — | **Unsafe/Tungsten Sort Shuffle**：直接操作序列化后的二进制内存（Page），减少反序列化开销 | 现代默认实现 |

### 3.2 Sort Shuffle 流程（能画）

**Write 端**：
```
Map Task 输出 → 写入内存缓冲区（PartitionedAppendOnlyMap / PartitionedPairBuffer）
  → 达到阈值 spill 到磁盘（按 partitionID + key 排序）
  → 多个 spill 文件归并（merge）成一个数据文件 + 一个索引文件
索引文件记录每个 partition 在数据文件中的 offset 和 length
```

**Read 端**：
```
Reduce Task 启动 → 向 Driver/MapOutputTracker 获取 map 输出位置
  → 通过 BlockManager 从各 Executor 拉取（netty）
  → 本地聚合/排序（ExternalSorter / ExternalAppendOnlyMap）→ 交给 Reduce 逻辑
```

- [ ] 追问：Shuffle Write 为什么要排序？（Read 端要按分区连续读取，排序后同分区数据物理相邻，索引可定位；顺序 IO 远快于随机 IO）
- [ ] 追问：Shuffle 一定是排序的吗？（bypass 模式不排序；Hash Shuffle 也不排序）
- [ ] 追问：Shuffle 的瓶颈在哪？（磁盘 IO（spill/merge）+ 网络 IO（拉数据）+ 序列化反序列化 + GC）

### 3.3 Push-based Shuffle（Spark 3.2+）⭐

- [ ] 问题：大规模作业下 Reduce 端要从大量 Executor 拉碎片数据，随机 IO 严重
- [ ] 方案：Map 端输出除了本地保留，还异步 push 到其他 Executor 或外部存储（Magnet），合并成更大的连续块
- [ ] 参数：`spark.shuffle.push.enabled=true`
- [ ] 收益：Shuffle 读取吞吐提升，Stage 完成时间下降

### 3.4 关键 Shuffle 参数

```properties
spark.shuffle.file.buffer=32k              # write 缓冲区，增大减少 spill 次数
spark.shuffle.io.maxRetries=3              # 拉取重试
spark.shuffle.io.retryWait=5s              # 重试等待（GC 长时可调大避免 FetchFailed）
spark.reducer.maxSizeInFlight=48m          # reduce 端一次拉取缓冲
spark.shuffle.sort.bypassMergeThreshold=200
spark.shuffle.compress=true                # shuffle 文件压缩
spark.shuffle.spill.compress=true
spark.shuffle.service.enabled=true         # External Shuffle Service（动态资源分配必需）
```

## 四、内存管理 ⭐⭐⭐

### 4.1 统一内存模型（Spark 1.6+，默认）

```
Executor JVM 堆内存 (spark.executor.memory)
├── Reserved Memory  300MB           （固定，防 OOM）
├── User Memory      (heap - 300MB) × (1 - spark.memory.fraction)
│                                     用户代码、UDF 对象、依赖信息
└── Spark Memory     (heap - 300MB) × spark.memory.fraction (默认 0.6)
    ├── Storage Memory  × spark.memory.storageFraction (默认 0.5)
    │     缓存 RDD、broadcast
    └── Execution Memory
          Shuffle、Join、Sort、Aggregate 的缓冲
```

- [ ] **动态占用机制**：Storage 与 Execution 可互相借用；Execution 可驱逐 Storage（因 Execution 更关键），Storage 不能强制抢回被 Execution 借走的部分
- [ ] `spark.executor.memoryOverhead`（默认 `max(384MB, 0.1 × executorMemory)`）：堆外内存，用于 Netty 缓冲、JVM 本身、Python 进程
- [ ] Container 申请内存 = `executor.memory` + `memoryOverhead`

### 4.2 OOM 分类与排查 ⭐⭐⭐

| 类型 | 现象 | 常见原因 | 对策 |
| --- | --- | --- | --- |
| **Driver OOM** | `java.lang.OutOfMemoryError: Java heap space` in Driver | `collect()` 拉回大量数据、broadcast 表过大、分区元数据过多 | 避免 collect 全量；调大 `spark.driver.memory`；控制 broadcast 阈值 |
| **Executor OOM（堆内）** | Container killed / OOM | 单分区数据过大（倾斜）、cache 过多、UDF 中大对象 | 调大内存、处理倾斜、减少 cache、用 mapPartitions 复用对象 |
| **Executor 超物理内存被 YARN kill** | `Container killed by YARN for exceeding memory limits` | 堆外内存不足（Netty/压缩缓冲/Python） | 调大 `memoryOverhead`；`spark.yarn.executor.memoryOverhead` |
| **GC OOM** | `GC overhead limit exceeded` | 对象创建过多、内存碎片 | 开启 Kryo 序列化、调大内存、减少对象 |

- [ ] 追问：Container 被 kill 但没看到 OOM 日志，怎么排查？（大概率是堆外超限，看 YARN NM 日志中的 `physical memory limits`，调大 overhead）

### 4.3 序列化 ⭐⭐

| 序列化器 | 特点 |
| --- | --- |
| Java 默认（JavaSerializer） | 兼容性好，慢，体积大 |
| **Kryo**（推荐） | 快 ~10x，体积小 ~5x，需注册自定义类 |

```scala
conf.registerKryoClasses(Array(classOf[MyClass]))
spark.serializer = org.apache.spark.serializer.KryoSerializer
```

- [ ] 追问：Kryo 为什么快？（二进制紧凑编码、注册类后用 int ID 代替全类名、无需反射元信息）

## 五、Spark SQL 与 Catalyst ⭐⭐⭐

### 5.1 DataFrame / Dataset / RDD 对比

| 维度 | RDD | DataFrame | Dataset |
| --- | --- | --- | --- |
| Schema | 无 | 有 | 有 |
| 类型安全 | 是 | 否（Row） | 是 |
| 优化器 | 无 | Catalyst | Catalyst |
| 序列化 | Java/Kryo | Tungsten 堆外 | Tungsten |
| 适用 | 底层控制 | SQL/Python | Scala/Java 强类型 |

- [ ] Tungsten：堆外内存管理 + 缓存友好的二进制格式（UnsafeRow）+ 代码生成，减少 GC

### 5.2 Catalyst 优化流程

```
SQL/DataFrame API
  → Unresolved Logical Plan（解析，绑定 Catalog）
  → Resolved Logical Plan（Analyzer：解析表名列名、类型检查）
  → Optimized Logical Plan（Optimizer：谓词下推、列裁剪、常量折叠、Join 重排）
  → Physical Plans（多个候选）
  → Cost Model 选择最优物理计划（如 SortMergeJoin vs BroadcastHashJoin）
  → Executable Plan（Whole-Stage Codegen 生成 Java 代码）
```

- [ ] **谓词下推（Predicate Pushdown）**：把过滤条件下推到数据源，Parquet/ORC 可直接跳过 Row Group
- [ ] **列裁剪（Column Pruning）**：只读需要的列
- [ ] **Whole-Stage Codegen（全阶段代码生成）**：把一个 Stage 内多个算子融合成一个 Java 方法，消除虚函数调用、减少对象创建
- [ ] 查看计划：`df.explain(true)` / SQL `EXPLAIN EXTENDED`

### 5.3 Join 策略 ⭐⭐⭐（高频）

| 策略 | 条件 | 原理 | 性能 |
| --- | --- | --- | --- |
| **Broadcast Nested Loop Join** | 一侧极小（< `spark.sql.autoBroadcastJoinThreshold` 默认 10MB） | 小表广播到所有 Executor，Map 端 Join，无 Shuffle | 最快 |
| **Shuffle Hash Join** | 一侧较小能放进单分区内存 | Shuffle 后小侧建 HashTable | 快 |
| **Sort Merge Join** | 两侧都大（默认） | 两侧按 key Shuffle + 排序，归并 Join | 慢但通用，Spark 2.x 默认 |
| **Cartesian Product** | 无 join 条件 | 笛卡尔积 | 禁用 |
| **Bucket Join** | 两侧都是分桶表 | 桶对桶本地 Join | 快，需预分桶 |

```sql
-- 手动 hint
SELECT /*+ BROADCAST(dim) */ ...
SELECT /*+ MERGEJOIN(t1, t2) */ ...
SELECT /*+ SHUFFLE_HASH(dim) */ ...
```

- [ ] 追问：`spark.sql.autoBroadcastJoinThreshold` 设多大合适？（默认 10MB 偏保守，维度表场景可调到 100-500MB；过大会导致 Driver 收集 + 广播耗时暴增和 Executor OOM）
- [ ] 追问：Broadcast Join 的瓶颈在哪？（Driver 端 collect 小表 → 序列化 → 分块广播 → Executor 反序列化；小表统计信息不准会误判）
- [ ] 追问：为什么 SortMergeJoin 要排序？（归并 Join 需要有序；同时排序后可利用流水线处理，且 Spark 会复用排序结果）

### 5.4 AQE（Adaptive Query Execution，Spark 3.0+）⭐⭐⭐

三大能力：

1. **动态合并小分区** `spark.sql.adaptive.coalescePartitions.enabled`
   - 解决 `spark.sql.shuffle.partitions`（默认 200）固定导致的碎分区问题
   - 基于运行时真实数据量自动合并，减少 Task 数
2. **动态切换 Join 策略** `spark.sql.adaptive.autoBroadcastJoinThreshold`
   - 运行时发现某侧实际很小 → 把 SortMergeJoin 转成 BroadcastJoin，消除 Shuffle
   - 解决统计信息缺失/不准的问题
3. **动态优化数据倾斜** `spark.sql.adaptive.skewJoin.enabled` ⭐⭐⭐
   - 运行时检测倾斜分区（大小 > `skewedPartitionFactor` × 中位数 且 > `skewedPartitionThresholdInBytes` 默认 256MB）
   - 自动把倾斜分区**切分成多个子分区**，另一侧对应数据复制多份分别 Join
   - 无需改 SQL，是目前最省心的倾斜方案

```properties
spark.sql.adaptive.enabled=true                     # Spark 3.2+ 默认开启
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.coalescePartitions.minPartitionSize=1MB
spark.sql.adaptive.advisoryPartitionSizeInBytes=64MB
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB
spark.sql.adaptive.localShuffleReader.enabled=true
```

- [ ] 追问：AQE 的局限？（只能在 Shuffle 边界后基于真实数据做决策，无法优化第一个 Stage；对倾斜 key 极度集中的场景切分后仍可能不均；不解决业务逻辑层面的倾斜）

## 六、Spark 数据倾斜完整解决方案 ⭐⭐⭐（必考，准备真实案例）

### 6.1 定位手段

1. Spark UI → Stages → Task 列表，看 `Shuffle Read Size / Records` 的最大值 vs 中位数（>5x 即倾斜）
2. 看 Task Duration 分布，长尾任务
3. 抽样定位倾斜 key：
   ```sql
   SELECT key, count(1) c FROM t GROUP BY key ORDER BY c DESC LIMIT 50;
   ```
4. 日志中 `FetchFailedException` / Executor OOM 常伴随倾斜

### 6.2 方案矩阵

| 方案 | 适用场景 | 做法 | 优缺点 |
| --- | --- | --- | --- |
| **1. 开 AQE skewJoin** | Spark 3.0+，Join 倾斜 | 见 §5.4 | 最省心；对极端倾斜仍不足 |
| **2. 过滤异常 key** | 倾斜 key 无业务意义（null、-1、脏数据） | `where key is not null` | 最简单；可能丢数据需确认 |
| **3. 提高并行度** | 轻度倾斜 | 调大 `spark.sql.shuffle.partitions` | 治标不治本，增加 Task 开销 |
| **4. MapJoin / Broadcast** | 大表 join 小表 | `/*+ BROADCAST(small) */` | 直接消除 Shuffle；小表受内存限制 |
| **5. 倾斜 key 单独处理** | 少数几个超级热点 key | 拆成两个作业：热点 key 走 broadcast join，其余走普通 join，最后 union | 效果好；SQL 变复杂 |
| **6. 加盐两阶段聚合** | 聚合倾斜（group by） | 加随机前缀局部聚合 → 去前缀全局聚合 | 通用；多一轮 Shuffle |
| **7. 加盐 + 扩容 Join** | 大表 join 大表倾斜 | 大表 key 加 0~N 随机后缀，小表膨胀 N 倍 | 通用；数据膨胀 N 倍 |
| **8. 分桶预聚合** | 稳定的大表 Join | 提前按 join key 分桶落表 | 一次投入长期收益 |
| **9. 自定义分区器** | RDD 层可控 | 按业务规则把热点 key 分散到多分区 | 需要 RDD API |

**方案 6 示例（聚合加盐）**：
```sql
SELECT real_key, SUM(cnt) total
FROM (
    SELECT
        substring(salted_key, 1, length(salted_key) - 4) AS real_key,
        COUNT(1) AS cnt
    FROM (SELECT concat(key, '_', lpad(cast(floor(rand()*100) as string),3,'0')) salted_key FROM t) a
    GROUP BY substring(salted_key, 1, length(salted_key) - 4)
) b GROUP BY real_key;
```

**方案 5 示例（热点 key 拆分）**：
```sql
-- 热点走 broadcast
SELECT /*+ BROADCAST(dim) */ f.*, dim.name
FROM fact f JOIN dim ON f.key = dim.key
WHERE f.key IN ('hot1','hot2')
UNION ALL
-- 其余走普通 join
SELECT f.*, dim.name
FROM fact f JOIN dim ON f.key = dim.key
WHERE f.key NOT IN ('hot1','hot2');
```

## 七、Spark 调优参数体系 ⭐⭐⭐

### 7.1 资源类

```properties
# 并行度（最重要）
spark.default.parallelism                    # RDD 默认并行度，建议 = 总核数 × 2~3
spark.sql.shuffle.partitions=200             # SQL shuffle 分区，按数据量调（每分区 128-256MB 为宜）

# Executor
spark.executor.instances                     # 数量
spark.executor.cores=4                       # 每个 executor 核数（建议 2-5，太多导致 HDFS 并发写下降）
spark.executor.memory=8g                     # 堆内存
spark.executor.memoryOverhead=2g             # 堆外

# Driver
spark.driver.memory=4g
spark.driver.maxResultSize=1g                # collect 结果上限，防止 Driver OOM

# 总核数估算：executor.instances × executor.cores
```

**经验公式**：
- 单个 Task 处理数据量控制在 **100-256MB**
- 分区数 ≈ 总数据量 / 128MB
- Task 执行时间控制在 **几分钟级别**（太短调度开销大，太长容错代价高）

### 7.2 性能类

```properties
# 序列化
spark.serializer=org.apache.spark.serializer.KryoSerializer

# Shuffle
spark.shuffle.file.buffer=64k
spark.reducer.maxSizeInFlight=96m
spark.shuffle.io.maxRetries=5
spark.shuffle.io.retryWait=10s
spark.shuffle.compress=true

# Broadcast
spark.sql.autoBroadcastJoinThreshold=100m

# AQE（3.x）
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true

# Codegen
spark.sql.codegen.wholeStage=true

# 动态资源分配
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=200
spark.dynamicAllocation.executorIdleTimeout=60s
spark.shuffle.service.enabled=true           # 必须开启 External Shuffle Service

# 小文件与输出
spark.sql.files.maxRecordsPerFile=0          # >0 时限制单文件行数
spark.sql.sources.partitionOverwriteMode=dynamic   # 动态分区覆盖，只覆盖有数据的分区
```

### 7.3 容错类

```properties
spark.task.maxFailures=4                     # Task 重试次数
spark.stage.maxConsecutiveAttempts           # Stage 重试
spark.speculation=true                       # 推测执行，处理慢节点（谨慎开，会浪费资源）
spark.speculation.multiplier=1.5
spark.speculation.quantile=0.9
spark.network.timeout=300s                   # 长 GC 时调大避免心跳超时
spark.executor.heartbeatInterval=30s
```

### 7.4 调优通用套路（面试可背）

```
1. 看 Spark UI：哪个 Stage 慢？Task 数多少？Shuffle 量多大？有无长尾？
2. 判断瓶颈类型：
   - Task 太少 → 提高并行度
   - Task 太多且都很短 → 降低并行度 / 开 AQE 合并
   - Shuffle 量大 → 提前过滤、列裁剪、broadcast join、预聚合
   - 长尾 → 数据倾斜处理
   - OOM → 加内存 / 处理倾斜 / 减少 cache / 调 overhead
   - GC 频繁 → Kryo 序列化、减少对象创建、mapPartitions 复用
   - Driver 慢 → 避免 collect、减少分区元数据
3. 单变量调整 → 对比验证 → 固化到作业配置模板
```

## 八、Spark Streaming / Structured Streaming ⭐⭐

### 8.1 Spark Streaming（DStream，微批）

- [ ] 原理：把流切成小批次（batchDuration，如 5s），每批作为一个 RDD 处理
- [ ] 背压：`spark.streaming.backpressure.enabled=true`（PID 控制器动态调节接收速率）
- [ ] 接收端：Receiver（占用一个 core）/ Direct 模式（无 Receiver，直接读 Kafka，靠 offset 管理）
- [ ]  Exactly-Once：Direct 模式 + 手动提交 offset 到外部存储（与输出在同一事务）

### 8.2 Structured Streaming ⭐⭐

- [ ] 核心抽象：无界表（Unbounded Table），流 = 不断追加行的表
- [ ] 输出模式：`append`（只输出新增）、`update`（输出变化的行）、`complete`（输出全量结果，仅聚合场景）
- [ ] 触发方式：ProcessingTime / Once / Continuous（毫秒级，实验性）
- [ ] Exactly-Once：offset log（记录读到的 offset）+ commit log（记录已提交的批次）+ 幂等/事务 Sink
- [ ] 水位线（Watermark）：处理迟到数据，`withWatermark("eventTime", "10 minutes")`
- [ ] 状态管理：`HDFSBackedStateStore`（默认）/ `RocksDBStateStore`（Spark 3.2+，大状态推荐）
- [ ] 与 Flink 对比：

| 维度 | Structured Streaming | Flink |
| --- | --- | --- |
| 模型 | 微批（默认）/ Continuous | 真流（事件驱动） |
| 延迟 | 秒级（微批） | 毫秒级 |
| 状态 | StateStore，能力较弱 | 完整状态后端 + 增量 Checkpoint |
| 窗口 | 滑动/滚动/会话（3.x 支持） | 丰富 + 灵活触发器 |
| Exactly-Once | 支持 | 支持（两阶段提交） |
| 生态/成熟度（实时） | 一般 | 事实标准 |

- [ ] 结论话术：**离线批处理用 Spark，实时流处理用 Flink**，这是业界主流选择。

## 九、Spark 3.x 新特性 ⭐

- [ ] AQE（自适应查询执行）—— 3.0 最重要特性
- [ ] Dynamic Partition Pruning（动态分区裁剪）—— Join 时用小表侧的结果裁剪大表分区，星型模型收益巨大
- [ ] Join Hints 增强（SHUFFLE_HASH、SHUFFLE_REPLICATE_NL）
- [ ] Pandas API on Spark（原 Koalas）
- [ ] RocksDB State Store（3.2+）
- [ ] Push-based Shuffle / Magnet（3.2+）
- [ ] Spark on Kubernetes 成熟（3.x 起成为一等公民）
- [ ] Barrier Execution Mode（深度学习场景）

## 十、本章高频面试题速查

1. Spark 为什么比 MR 快（⭐⭐⭐）
2. 宽依赖窄依赖，为什么这么划分 Stage（⭐⭐⭐ 必考）
3. Shuffle 过程详细讲，Sort Shuffle 和 Hash Shuffle 区别（⭐⭐⭐ 必考）
4. reduceByKey 和 groupByKey 区别（⭐⭐⭐）
5. cache / persist / checkpoint 区别（⭐⭐⭐）
6. repartition 和 coalesce 区别（⭐⭐⭐）
7. Spark 内存模型，OOM 怎么排查（⭐⭐⭐ 必考）
8. Spark SQL Join 有哪几种策略，怎么选（⭐⭐⭐）
9. AQE 是什么，解决什么问题（⭐⭐⭐ 近两三年必考）
10. 数据倾斜怎么解决（⭐⭐⭐ 必考，要有真实案例 + 量化收益）
11. Spark 参数怎么调，你的经验值（⭐⭐⭐）
12. Spark 和 Flink 怎么选（⭐⭐）
13. 一个 Spark 作业跑得很慢，你的排查思路（⭐⭐⭐ 场景题，按 §7.4 回答）
14. Stage 里的 Task 数由什么决定（⭐⭐）
