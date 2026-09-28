# 05 · Flink 与实时计算

> 对应排期：W4
> 定位：⭐⭐⭐ 最高优先级。实时方向的绝对核心，中高级岗位会深挖状态、Checkpoint、时间语义、双流 Join 与反压。

## 一、Flink 架构与基础 ⭐⭐

### 1.1 核心组件

```
Client（提交 JobGraph）
   ▼
JobManager
   ├── Dispatcher      接收作业、启动 JobMaster、提供 Web UI
   ├── ResourceManager  Slot 资源分配与回收
   └── JobMaster(旧版 JobManager)  把 JobGraph 转成 ExecutionGraph，调度 Task，协调 Checkpoint
   ▼
TaskManager（多个）
   └── Task Slot（资源隔离单位，一个 slot 可跑多个 subtask —— slot sharing）
```

- [ ] **JobGraph → ExecutionGraph**：JobGraph 是逻辑图（算子链化后），ExecutionGraph 是物理图（按并行度展开成 subtask）
- [ ] **Slot 与并行度**：作业所需 slot 数 = 所有算子中最大并行度（因为 slot sharing）
- [ ] **Slot Sharing**：同一 slot 可运行来自不同算子的 subtask，提高资源利用率、便于同链路数据本地传递
- [ ] 追问：TaskManager 数量怎么算？（`总并行度 / 每个 TM 的 slot 数`）

### 1.2 部署模式 ⭐⭐

| 模式 | 说明 | 适用 |
| --- | --- | --- |
| Standalone | 自建集群 | 测试 |
| **YARN Session** | 先起一个常驻 Flink 集群，多作业共享 | 小作业多、资源复用 |
| **YARN Per-Job**（已废弃） | 每作业一个集群 | 隔离好但启动慢 |
| **YARN Application** | 每应用一个集群，main() 在 JM 上执行 | 生产推荐，隔离 + 减轻客户端压力 |
| **Kubernetes Application/Session** | 云原生，支持 Native K8s 动态申请资源 | 新集群推荐 |

- [ ] 追问：Application 模式和 Session 模式的区别？（Session 多作业共享 JM/TM，一个作业 OOM 可能影响别人；Application 独享，隔离性强，且 `main()` 方法在集群侧运行，避免客户端成为瓶颈）

### 1.3 算子链（Operator Chain）⭐⭐

- [ ] 条件：上下游算子并行度相同、同一 SlotSharingGroup、连接方式为 **Forward**、都开启了 chaining
- [ ] 作用：把多个算子合并到一个 Task 线程内执行，避免线程间序列化和网络传输
- [ ] 何时要禁用：算子内有阻塞操作、需要独立监控/重启、资源需求差异大 → `disableChaining()` / `startNewChain()`
- [ ] 追问：怎么定位算子链导致的问题？（Web UI 中一个 Task 显示多个算子，若某算子耗时会拖累整条链；可通过 Metrics 拆分）

## 二、时间语义与 Watermark ⭐⭐⭐（必考）

### 2.1 三种时间

| 时间 | 定义 | 特点 |
| --- | --- | --- |
| **Event Time** | 事件实际发生时间（数据自带） | 结果确定、可重放；需处理乱序 → 生产首选 |
| Ingestion Time | 进入 Flink Source 的时间 | 折中 |
| Processing Time | 算子处理时的系统时间 | 延迟最低，但结果不确定、重跑不一致 |

- [ ] 为什么用 Event Time？（业务准确性；数据回溯/重放结果一致；不受采集延迟影响）

### 2.2 Watermark（水位线）⭐⭐⭐

**定义**：一种插入到数据流中的特殊时间戳标记，表示「事件时间小于等于 watermark 的数据（大部分）已经到达」，用于触发窗口计算。

**工作机制**：
```
数据流：e(t=1) e(t=3) W(3) e(t=5) W(5) e(t=2, 迟到) ...
窗口 [0,5) 在收到 W(5) 时触发计算
```

- [ ] 生成策略：
  - 单调递增（有序流，罕见）
  - **周期性 + 最大事件时间减固定延迟**：`forBoundedOutOfOrderness(Duration.ofSeconds(5))` —— 最常用
  - 自定义 `WatermarkGenerator`
- [ ] Watermark 是**多并行度对齐**的：一个算子的 watermark = 所有上游输入通道 watermark 的**最小值**
- [ ] 追问：为什么取最小值？（保证所有通道都推进到该时间点，否则窗口会过早触发漏数据）
- [ ] 追问：某个分区没有数据，watermark 卡住不推进怎么办？（**空闲源**问题 → `withIdleness(Duration.ofMinutes(1))` 标记该通道空闲，不参与对齐）
- [ ] 追问：Watermark 延迟设多大合适？（根据业务对准确性 vs 延迟的容忍度：日志类 5-30s；交易类可能要 1-5min；太短会漏数据，太长会增加延迟和状态大小）

### 2.3 迟到数据三种处理方案 ⭐⭐⭐

| 方案 | API | 说明 |
| --- | --- | --- |
| **1. Watermark 容忍乱序** | `forBoundedOutOfOrderness(n)` | 推迟窗口触发，覆盖大部分乱序 |
| **2. 允许延迟（侧输出前的二次触发）** | `allowedLateness(Duration)` | 窗口触发后不立刻销毁，再等一段时间，期间迟到数据会**增量更新窗口结果**（会多次输出） |
| **3. 侧输出流（Side Output）** | `sideOutputLateData(OutputTag)` | 超出 allowedLateness 的极端迟到数据进侧流，单独落库/告警/离线修正 |

**组合使用模板**：
```java
stream
  .keyBy(...)
  .window(TumblingEventTimeWindows.of(Time.minutes(1)))
  .allowedLateness(Time.minutes(5))
  .sideOutputLateData(lateTag)
  .aggregate(new MyAgg())
  .process(new MyProcess());   // 主输出

resultStream.getSideOutput(lateTag).addSink(lateSink);
```

- [ ] 追问：`allowedLateness` 和 watermark 延迟的区别？（watermark 延迟影响所有下游、推迟首次触发；allowedLateness 只影响当前窗口、允许多次触发更新结果）
- [ ] 追问：如何做到最终准确？（实时给近似结果 + 侧输出/离线 T+1 修正，即 Lambda 思路）

### 2.4 窗口类型 ⭐⭐⭐

| 窗口 | 说明 | 参数 |
| --- | --- | --- |
| **滚动窗口** Tumbling | 固定大小，无重叠 | `size` |
| **滑动窗口** Sliding | 固定大小 + 滑动步长，有重叠 | `size`, `slide` |
| **会话窗口** Session | 按活跃间隙划分 | `gap`（可动态） |
| **全局窗口** Global | 需自定义 Trigger | 自定义 |

- [ ] Time Window vs Count Window（`countWindow(n)` 基于 ProcessingTime 语义）
- [ ] **WindowAll vs KeyedStream Window**：前者全局单并行度（性能差，慎用），后者按 key 分区并行
- [ ] 窗口核心组件：**Assigner**（分配窗口）+ **Trigger**（何时计算）+ **Evictor**（计算前/后剔除元素）+ **Function**（计算逻辑）
- [ ] 追问：滑动窗口为什么容易 OOM？（一条数据会同时属于 `size/slide` 个窗口，如 1h 窗口 1min 滑动 → 60 份状态）

## 三、状态管理 ⭐⭐⭐（必考）

### 3.1 状态分类

| 类型 | 作用域 | 示例 |
| --- | --- | --- |
| **Keyed State**（键控状态） | 每个 key 独立 | `ValueState`、`ListState`、`MapState`、`ReducingState`、`AggregatingState` |
| **Operator State**（算子状态） | 每个算子并行实例独立，与 key 无关 | Kafka Source 的 offset、`ListState`、`BroadcastState`、`UnionListState` |

**Keyed State 类型详解**：
- [ ] `ValueState<T>`：单值
- [ ] `ListState<T>`：列表（append / update）
- [ ] `MapState<K,V>`：Map
- [ ] `ReducingState<T>`：持续 reduce 聚合
- [ ] `AggregatingState<IN,OUT>`：通用聚合（配合 AggregateFunction）

**Operator State 类型**：
- [ ] `ListState`（even-split，重分布时均分）
- [ ] `UnionListState`（重分布时每个 subtask 拿到全量并集 —— 用于需要全局视图的场景）
- [ ] `BroadcastState`（广播流，如动态规则/维度更新）

### 3.2 状态后端（State Backend）⭐⭐⭐

| 后端 | 存储位置 | 序列化 | 容量上限 | 性能 | 适用 |
| --- | --- | --- | --- | --- | --- |
| **HashMapStateBackend** | TaskManager JVM 堆内存 | 对象形式 | 受堆内存限制（GB 级） | 极快 | 小状态、低延迟 |
| **EmbeddedRocksDBStateBackend** | 本地 RocksDB（磁盘）+ 堆外内存 | 序列化字节 | TB 级 | 较慢（读写需序列化） | 大状态、生产主流 |

**RocksDB 关键能力**：
- [ ] **增量 Checkpoint**：基于 SST 文件的 LSM-Tree，只上传变化的文件 → 大状态下 checkpoint 时间可控
- [ ] 状态可超过内存（磁盘 + BlockCache）
- [ ] 支持状态 TTL

**RocksDB 调优要点**：
```properties
state.backend.rocksdb.memory.managed=true          # 托管内存，防 OOM
state.backend.rocksdb.thread.num=4                 # 后台线程（compaction/flush）
state.backend.rocksdb.block.cache-size=256mb
state.backend.rocksdb.writebuffer.size=128mb
state.backend.rocksdb.writebuffer.count=4
taskmanager.memory.managed.fraction=0.4            # 托管内存占比，RocksDB 主要用它
```
- [ ] 追问：RocksDB 为什么比 HashMap 后端慢？（每次状态读写都要序列化/反序列化，HashMap 直接操作 Java 对象）
- [ ] 追问：RocksDB 状态大了以后慢怎么优化？（调大 block cache、增加写缓冲、开启 bloom filter、SSD 本地盘、拆分 key 设计避免热点、开启增量 checkpoint）

### 3.3 状态 TTL ⭐⭐

```java
StateTtlConfig ttl = StateTtlConfig.newBuilder(Time.days(1))
    .setUpdateType(UpdateType.OnCreateAndWrite)      // 或 OnReadAndWrite
    .setStateVisibility(NeverReturnExpired)          // 或 ReturnExpiredIfNotCleanedUp
    .cleanupInRocksdbCompactFilter(1000)             // RocksDB 压缩时清理
    .cleanupIncrementally(10, false)                 // 增量清理
    .build();
descriptor.enableTimeToLive(ttl);
```
- [ ] 为什么必须设 TTL？（防止状态无限膨胀导致 Checkpoint 超时、磁盘打满、恢复缓慢）
- [ ] 追问：TTL 清理是即时的吗？（不是，过期数据在被清理前仍占空间，靠后台 compaction / 增量清理 / 全量快照清理）

### 3.4 Broadcast State 动态规则/维度 ⭐⭐

- [ ] 场景：规则、维度、阈值需要动态更新，且所有并行实例都要看到
- [ ] 实现：规则流 `broadcast(descriptor)` → `connect` 主流 → `BroadcastProcessFunction` 中 `processBroadcastElement` 更新广播状态，`processElement` 读取状态处理主流
- [ ] 注意：广播状态只能在 `processBroadcastElement` 中写，在 `processElement` 中只读（保证一致性）

## 四、Checkpoint 与容错 ⭐⭐⭐（必考）

### 4.1 Chandy-Lamport 分布式快照算法

**Barrier 对齐机制**：
```
JobManager 周期性触发 Checkpoint，向所有 Source 注入 Barrier(id)
Barrier 随数据流向下游传播
算子收到 Barrier：
  - 单输入通道：直接做快照
  - 多输入通道：先到的通道数据被缓存，等所有通道 Barrier 都到齐（对齐）
    → 对齐完成后做状态快照 → 异步上传到持久化存储 → 向下游广播 Barrier
所有 Sink 完成 → CheckpointCoordinator 标记该 Checkpoint 成功
```

- [ ] 追问：为什么要 Barrier 对齐？（保证快照是「全局一致切面」，即快照内容对应同一个逻辑时间点，不混入 barrier 之后的数据）
- [ ] **Unaligned Checkpoint（非对齐检查点，Flink 1.11+）** ⭐⭐：
  - 问题：反压时 barrier 传播被阻塞（barrier 排在大量数据后面），对齐时间极长 → Checkpoint 超时
  - 方案：Barrier 到达即快照，**把 in-flight 数据（输入输出缓冲区）一起存入 Checkpoint**，不做对齐
  - 代价：Checkpoint 体积变大；恢复时需要重放 in-flight 数据
  - 参数：`execution.checkpointing.unaligned.enabled=true`
  - 适用：严重反压、大并行度、Checkpoint 频繁超时的场景
- [ ] `aligned-checkpoint-timeout`：先尝试对齐，超时后自动转非对齐（折中方案）

### 4.2 Checkpoint 核心参数

```properties
execution.checkpointing.interval=60s              # 间隔
execution.checkpointing.timeout=10min             # 超时
execution.checkpointing.min-pause=30s             # 两次 checkpoint 最小间隔（防止连环触发）
execution.checkpointing.max-concurrent-checkpoints=1
execution.checkpointing.mode=EXACTLY_ONCE         # 或 AT_LEAST_ONCE（不对齐 barrier，性能更好）
execution.checkpointing.tolerable-failed-checkpoints=3
execution.checkpointing.externalized-checkpoint-retention=RETAIN_ON_CANCELLATION
state.backend=rocksdb
state.backend.incremental=true
state.checkpoints.dir=hdfs:///flink/checkpoints
state.savepoints.dir=hdfs:///flink/savepoints
```

### 4.3 Checkpoint vs Savepoint ⭐⭐⭐

| 维度 | Checkpoint | Savepoint |
| --- | --- | --- |
| 触发 | 系统自动周期触发 | 用户手动触发 |
| 目的 | 故障自动恢复 | 版本升级、集群迁移、作业改造、A/B |
| 生命周期 | 作业取消可自动清理 | 永久保留，需手动管理 |
| 格式 | 后端相关（可增量） | 标准化格式（可跨版本，全量） |
| 恢复灵活性 | 同作业 | 允许修改并行度、算子（需 uid 稳定） |

### 4.4 端到端 Exactly-Once ⭐⭐⭐（必考）

**三段式保证**：

| 环节 | 机制 |
| --- | --- |
| **Source** | 可重放（Kafka offset 纳入 Checkpoint 状态，恢复时回退到快照点重读） |
| **Flink 内部** | Checkpoint + Barrier 对齐，保证状态一致切面 |
| **Sink** | ① **幂等写入**（如 Redis、HBase、Doris upsert、ES 按 docId 覆盖）② **两阶段提交 2PC**（`TwoPhaseCommitSinkFunction`：预提交随 Checkpoint 完成，Checkpoint 成功后 commit） |

**Kafka Sink 2PC 流程**：
```
1.beginTransaction：创建 Kafka 事务，写数据（读端 read_committed 看不到）
2. 数据写入 Kafka 事务中
3.Checkpoint 触发 → snapshotState：预提交事务（flush + commit transaction）
4.Checkpoint 完成 → notifyCheckpointComplete：真正提交事务
5. 若失败：回滚事务；恢复时从事务状态中恢复未提交的事务
```
- [ ] 关键约束：`transaction.timeout.ms` 必须 > Checkpoint 间隔 + 恢复时间，否则 Kafka 会自动 abort 事务导致数据丢失
- [ ] 追问：Sink 不支持事务怎么办？（用幂等写：主键 upsert、去重表、外部去重）
- [ ] 追问：At-Least-Once 和 Exactly-Once 的取舍？（EOS 有性能损失（事务、对齐、状态）；对准确性要求高的用 EOS，日志统计类可用 ALOS + 离线修正）

### 4.5 Checkpoint 失败排查 SOP ⭐⭐⭐（场景题）

```
1. 看 Flink Web UI → Checkpoints 页：失败原因（超时？对齐超时？异常？）
2. 超时类：
   - 反压导致 barrier 传播慢 → 看 Backpressure 页定位瓶颈算子 → 开 Unaligned Checkpoint
   - 状态过大 → 换 RocksDB + 增量 Checkpoint，加 TTL，减小状态
   - 存储慢 → 检查 HDFS/OSS 负载，调大 timeout
   - 间隔太短 → 加大 interval、设置 min-pause
3. 异常类：
   - 看 JobManager/TaskManager 日志的 CheckpointException 根因
   - 常见：Task 失败、网络抖动、Sink 事务超时、序列化错误
4. 资源类：
   - 托管内存不足 → 调大 taskmanager.memory.managed.fraction
   - 磁盘满 → RocksDB 工作目录清理
5. 兜底：设置 tolerable-failed-checkpoints 避免单次失败即挂作业
```

### 4.6 恢复策略（Restart Strategy）

```properties
restart-strategy=fixed-delay           # fixed-delay / failure-rate / exponential-delay / none
restart-strategy.fixed-delay.attempts=3
restart-strategy.fixed-delay.delay=10s
restart-strategy.failure-rate.max-failures-per-interval=3
restart-strategy.failure-rate.failure-rate-interval=5min
```

### 4.7 反压（Backpressure）⭐⭐⭐

- [ ] **原理**：基于 Credit-Based 流控。下游 Task 通过 Network Buffer 向上游通告可接收的 credit（可用缓冲区数），上游按 credit 发送，无 credit 则阻塞 → 压力逐级传导到 Source，Source 降低拉取速率
- [ ] **危害**：吞吐下降、Checkpoint 超时甚至失败、延迟增大、状态膨胀
- [ ] **定位**：Web UI Backpressure 页（HIGH/OK）；Metrics：`outPoolUsage`、`inPoolUsage`、`isBackPressured`
  - `inPoolUsage` 高 → 下游消费不过来（本算子或被下游反压）
  - `outPoolUsage` 高 → 上游产出太快或下游阻塞
  - 找到**第一个 outPoolUsage 高但 inPoolUsage 正常**的算子 → 它就是瓶颈
- [ ] **解决**：
  1. 提高瓶颈算子并行度
  2. 优化算子逻辑（减少同步 IO → 改 `AsyncFunction` 异步；减少序列化；本地缓存）
  3. 解决数据倾斜（`rebalance()` / `rescale()` / 两阶段聚合 / 打散 key）
  4. 调大 Network Buffer（`taskmanager.memory.network.fraction`）
  5. 开启 Unaligned Checkpoint 缓解 barrier 阻塞
  6. 资源不足 → 加 TaskManager / slot

## 五、Flink SQL 与双流 Join ⭐⭐⭐

### 5.1 动态表与流表转换

- [ ] **Stream → Table**：Append 流直接映射；带更新的流需要主键，产生 changelog（+I/-U/+U/-D）
- [ ] **Table → Stream**：Append 模式 / Retract 模式（撤回）/ Upsert 模式
- [ ] **Changelog 语义**：这是 Flink SQL 的核心，理解 `-U/+U` 才能解释「为什么结果会变化」
- [ ] 追问：为什么有些查询会输出撤回流？（无窗口的聚合结果会随新数据不断更新，必须先撤回旧值再发新值）

### 5.2 Join 类型对比 ⭐⭐⭐（高频）

| Join 类型 | 语法 | 状态 | 特点 |
| --- | --- | --- | --- |
| **Window Join** | `JOIN ... ON a.w = b.w`（同窗口） | 窗口内数据 | 只 Join 同一窗口的数据，窗口结束清空 |
| **Interval Join** | `ON a.t BETWEEN b.t - INTERVAL '10' MINUTE AND b.t` | 时间区间内数据 | 限定时间范围，状态可控，**推荐** |
| **Regular Join（全量 Join）** | `ON a.k = b.k` | 双侧全量数据 | 状态无限增长，必须配 TTL；结果会随两侧更新而回撤 |
| **Temporal Join（时态表 Join）** | `FOR SYSTEM_TIME AS OF a.proctime` | 维表版本 | 关联维表在事件时刻的快照，**维表 Join 首选** |
| **Lookup Join** | `FOR SYSTEM_TIME AS OF proctime` + LookupTableSource | 无状态（外部查询） | 关联外部维表（MySQL/Redis/HBase） |

### 5.3 维表 Join 方案 ⭐⭐⭐（必考）

| 方案 | 原理 | 优点 | 缺点 |
| --- | --- | --- | --- |
| **1. 预加载到内存 Map** | 启动时全量加载，`RichFunction.open()` 中初始化 | 查询最快，无外部依赖 | 维度大时内存不足；更新不及时 |
| **2. 同步查外部存储** | `RichFunction` 中直接 JDBC/Redis 查询 | 数据实时、实现简单 | **阻塞 IO，吞吐极低**，反压 |
| **3. 异步 IO + 缓存** ⭐ | `AsyncDataStream.unorderedWait` + Guava/Caffeine 本地缓存 | 高吞吐、可控并发 | 需处理超时/失败/一致性；缓存有过期风险 |
| **4. Flink SQL Lookup Join** | Connector 实现 `LookupTableSource`，内置缓存 | 声明式、简洁 | 依赖 Connector 实现质量 |
| **5. 广播维表流** | 维度变更走 Kafka 广播流，`BroadcastState` | 维度变更可实时感知 | 只适合小维度、变更频繁场景 |
| **6. 维表入湖/HBase** | 维度存 HBase，RowKey 点查 | 支持大维度、高并发点查 | 需要维护 HBase |

**异步 IO 模板**：
```java
AsyncDataStream.unorderedWait(
    stream, new MyAsyncRichFunction(), 1000, TimeUnit.MILLISECONDS, 100 /*capacity*/);
```
- [ ] `unorderedWait` vs `orderedWait`：无序吞吐高但会打乱顺序（Event Time 下 Flink 仍保证 watermark 前的顺序）；有序延迟高
- [ ] 追问：缓存如何保证维度更新的及时性？（TTL 短一些 + 关键维度走广播更新 + 允许最终一致 + 离线 T+1 修正）

### 5.4 双流 Join 的倾斜与状态问题

- [ ] Regular Join 状态无限增长 → 必须设 `table.exec.state.ttl`
- [ ] 热点 key → 打散 key（加随机后缀）+ 两阶段聚合；或热点走单独链路
- [ ] 追问：双流 Join 数据乱序/迟到导致 Join 不上怎么办？（用 Interval Join 给时间容忍；或状态保留足够长；或补数机制）

### 5.5 常用 Flink SQL 配置

```properties
table.exec.state.ttl=86400000                    # 状态 TTL，必设
table.exec.mini-batch.enabled=true               # 微批聚合，减少状态访问
table.exec.mini-batch.allow-latency=2s
table.exec.mini-batch.size=5000
table.optimizer.agg-phase-strategy=TWO_PHASE     # 两阶段聚合，解决 group by 倾斜
table.optimizer.distinct-agg.split.enabled=true  # count(distinct) 倾斜优化
```

## 六、Flink 精确一次去重与常见业务实现 ⭐⭐

### 6.1 去重方案

| 方案 | 适用 | 说明 |
| --- | --- | --- |
| `ValueState<Boolean>` 标记 | 数据量可控 | KeyedProcessFunction 中判断是否首次 |
| `MapState` 存已见 key | 需保留明细 | 配合 TTL |
| RocksDB State + TTL | 海量去重 | 状态存 key，TTL 控制窗口 |
| Redis Set / Bitmap | 全局去重、跨作业 | 引入外部依赖，需处理一致性与性能 |
| BloomFilter | 允许极低误判 | 内存友好，不能删除 |
| Flink SQL `ROW_NUMBER() ... WHERE rn=1` | 简单去重 | 底层优化为 Deduplication 算子 |

### 6.2 TopN 实现 ⭐⭐

```sql
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_end, category
                                 ORDER BY sales DESC) AS rn
    FROM window_agg
) WHERE rn <= 10;
```
- [ ] Flink 内部优化为 **TopN 算子**，用 `MapState` 维护排序列表
- [ ] 无排名输出模式（`outputRankNumber=false`）可减少下游压力
- [ ] 追问：TopN 结果会不断更新（changelog），下游怎么处理？（Sink 需支持 upsert，如 Doris/MySQL/ES）

### 6.3 CEP ⭐

- [ ] 复杂事件处理：`Pattern.begin("a").where(...).next("b").within(Time.minutes(1))`
- [ ] 场景：风控（连续登录失败）、营销（浏览未下单）、异常检测
- [ ] 底层：NFA（非确定有限自动机）+ 共享缓冲区

## 七、Flink 作业上线与运维 ⭐⭐

- [ ] **UID 稳定性** ⭐⭐⭐：每个有状态算子必须 `uid("xxx")`，否则从 Savepoint 恢复时算子 ID 变化会导致状态无法匹配
- [ ] 修改并行度：Savepoint 恢复时可调整（Keyed State 会重分布；Operator State 看类型）
- [ ] 作业升级流程：`flink stop --savepointPath` → 修改代码 → `flink run -s savepoint`
- [ ] 监控指标：`numRecordsIn/Out`、`currentInputWatermark`、`lastCheckpointDuration/Size`、`numberOfFailedCheckpoints`、`isBackPressured`、TaskManager GC 时间、RocksDB 磁盘使用
- [ ] 告警项：Checkpoint 连续失败、延迟（watermark 落后 wall clock）超阈值、反压 HIGH、作业重启次数、Kafka Lag
- [ ] 追问：如何评估实时作业的延迟？（`currentFetchEventTimeLag`、`currentEmitEventTimeLag`、端到端用埋点时间戳对比落库时间）

## 八、本章高频面试题速查

1. Flink 和 Spark Streaming 的区别（⭐⭐⭐ 必考）
2. Watermark 原理，怎么处理乱序和迟到数据（⭐⭐⭐ 必考）
3. Flink 有哪些状态，State Backend 怎么选（⭐⭐⭐ 必考）
4. Checkpoint 原理（Barrier 对齐），和 Savepoint 区别（⭐⭐⭐ 必考）
5. 端到端 Exactly-Once 怎么实现（⭐⭐⭐ 必考）
6. Checkpoint 一直失败/超时怎么排查（⭐⭐⭐ 场景题）
7. 反压是什么，怎么定位和解决（⭐⭐⭐）
8. 维表 Join 有哪些方案，你们用哪个（⭐⭐⭐ 必考）
9. 双流 Join 有哪几种，状态怎么控制（⭐⭐⭐）
10. Slot 和并行度的关系（⭐⭐）
11. 算子链是什么，什么时候要断开（⭐⭐）
12. Unaligned Checkpoint 解决什么问题（⭐⭐ 加分）
13. Flink 作业怎么平滑升级（⭐⭐）
14. 实时作业数据倾斜怎么处理（⭐⭐⭐）
15. Flink SQL 的 retract 机制（⭐⭐ 加分）
