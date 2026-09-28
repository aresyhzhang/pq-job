# 06 · Kafka 与消息中间件

> 对应排期：W5 D1-D3
> 定位：⭐⭐⭐ 实时链路必考。重点在可靠性投递、Exactly-Once、积压处理、CDC。

## 一、Kafka 架构 ⭐⭐⭐

### 1.1 核心概念

```
Producer → Topic（逻辑） → Partition（物理，有序） → Segment（.log + .index + .timeindex）
                            ↓ 副本
                     Leader / Follower（Follower 只从 Leader 拉数据）
Consumer Group → 每个 Partition 只能被组内一个 Consumer 消费
Coordinator（Group Coordinator）管理消费组与 offset 提交
```

- [ ] **Partition**：并行度单位、有序性单位（分区内有序，分区间无序）
- [ ] **Replica**：Leader 负责读写，Follower 只同步；ISR（In-Sync Replicas）= 与 Leader 保持同步的副本集合
- [ ] **Controller**：集群中某个 Broker 当选，负责分区 Leader 选举、副本状态管理
- [ ] **Consumer Group**：实现负载均衡（分区分配给组内消费者）+ 广播（不同组各自消费全量）
- [ ] **ZooKeeper → KRaft**：Kafka 2.8+ 引入 KRaft 模式（基于 Raft 的元数据管理），3.x 逐步移除 ZK 依赖

### 1.2 为什么 Kafka 快 ⭐⭐⭐（必考）

1. **顺序写磁盘**：追加写 log，避免随机寻道，磁盘顺序写速度接近内存随机写
2. **Page Cache**：读写走操作系统页缓存，避免 JVM 堆内 GC 与数据拷贝
3. **零拷贝（sendfile）**：数据从 PageCache 直接经 DMA 送到网卡，跳过用户态拷贝（传统 4 次拷贝 2 次上下文切换 → 2 次拷贝）
4. **批量与压缩**：Producer 端 batch 发送，支持 gzip/snappy/lz4/zstd
5. **分区并行**：水平扩展读写能力
6. **稀疏索引**：`.index` 文件稀疏存储（offset → 物理位置），二分查找 + 顺序扫描

- [ ] 追问：零拷贝具体省了哪几步？（传统：磁盘→内核缓冲→用户缓冲→socket 缓冲→网卡（4 次拷贝，2 次用户/内核切换）；sendfile：磁盘→内核缓冲→（DMA gather）→网卡，用户态不参与）
- [ ] 追问：Kafka 用了 PageCache，重启会丢数据吗？（未 fsync 的数据可能丢；靠 `acks=all` + 副本复制保证，而非本地 fsync）

### 1.3 存储结构 ⭐⭐

```
/topic-0/
  ├── 00000000000000000000.log      # Segment 数据文件（默认 1GB）
  ├── 00000000000000000000.index    # 稀疏偏移索引（offset → position）
  ├── 00000000000000000000.timeindex# 时间戳索引（用于按时间查 offset）
  └── leader-epoch-checkpoint
```
- [ ] Segment 命名 = 该段第一条消息的 offset
- [ ] 查找过程：二分找到目标 Segment → 在 `.index` 中二分找到 <= offset 的物理位置 → 顺序扫描到目标 offset
- [ ] `log.retention.hours=168`（默认 7 天）、`log.retention.bytes`、`log.segment.bytes=1073741824`
- [ ] 追问：删除策略？（delete 整段删除；compact 日志压缩按 key 保留最新值 —— 用于 `__consumer_offsets`、CDC 场景）

## 二、生产者与可靠性 ⭐⭐⭐

### 2.1 消息发送流程

```
Producer
  → Interceptors（拦截器）
  → Serializer（序列化）
  → Partitioner（分区器：指定 key → hash(key) % 分区数；无 key → 粘性分区/Sticky）
  → RecordAccumulator（缓冲区，batch.size 默认 16KB，linger.ms 默认 0）
  → Sender 线程（IO 线程）按 Broker 分组批量发送
```

- [ ] **分区策略**：
  - 指定 partition：直接写
  - 有 key：`murmur2(key) % numPartitions` —— **保证同 key 有序**
  - 无 key：Kafka 2.4+ 用 **Sticky Partitioner**（粘住一个分区直到 batch 满，提升批量效率）
- [ ] 追问：如何保证消息顺序？（同一业务 key 用相同 partition；分区内有序；单分区单消费者；Producer 开 `max.in.flight.requests.per.connection=1` 或开启幂等）

### 2.2 acks 与可靠性 ⭐⭐⭐

| acks | 含义 | 可靠性 | 性能 |
| --- | --- | --- | --- |
| `0` | 不等待确认 | 可能丢 | 最高 |
| `1` | Leader 写入本地 log 即确认 | Leader 挂且未同步则丢 | 中 |
| `all`/`-1` | Leader + 所有 **ISR** 副本写入才确认 | 最高（除非 ISR 只剩 Leader） | 最低 |

**生产级不丢消息配置组合**：
```properties
# Producer
acks=all
retries=Integer.MAX_VALUE
max.in.flight.requests.per.connection=1      # 或开启 enable.idempotence=true（推荐，可到 5）
enable.idempotence=true                       # 幂等，避免重试导致重复
# Broker
replication.factor=3                          # 3 副本
min.insync.replicas=2                         # ISR 最少 2 个，否则拒绝写入
unclean.leader.election.enable=false          # 禁止非 ISR 副本选主（防丢数据）
auto.create.topics.enable=false
# Consumer
enable.auto.commit=false                      # 手动提交 offset，处理完再提交
```

- [ ] 追问：`min.insync.replicas=2` + `acks=all`，如果 ISR 只剩 1 个会怎样？（Broker 抛 `NotEnoughReplicasException`，Producer 重试 → 表现为不可写，牺牲可用性换一致性）
- [ ] 追问：`unclean.leader.election.enable=true` 的风险？（允许落后很多的副本当 Leader，会丢数据，但提高可用性）

### 2.3 幂等与事务 ⭐⭐⭐

**幂等 Producer（PID + SequenceNumber）**：
- Broker 为每个 `<PID, Partition>` 维护序列号，重复或乱序的消息被丢弃
- 作用域：**单分区、单会话**（Producer 重启或换分区就失效）
- 解决：重试导致的重复

**事务 Producer（Exactly-Once 语义）**：
```java
producer.initTransactions();
try {
    producer.beginTransaction();
    producer.send(record1);
    producer.send(record2);
    producer.sendOffsetsToTransaction(offsets, consumerGroupId);  // 消费位移纳入事务
    producer.commitTransaction();
} catch (Exception e) {
    producer.abortTransaction();
}
```
- 作用域：**跨分区、跨会话**，由 TransactionCoordinator + `__transaction_state` 管理
- 关键能力：把「消费 offset 提交」与「生产消息」放在同一事务 → 实现 consume-process-produce 的 EOS
- 消费端需 `isolation.level=read_committed` 才能只读到已提交事务的消息
- 追问：`read_uncommitted` vs `read_committed`？（前者读到所有消息含未提交事务；后者只读已提交，通过 LSO（Last Stable Offset）控制）

## 三、消费者与 Rebalance ⭐⭐⭐

### 3.1 消费流程与 offset 管理

- [ ] offset 存在内部 topic `__consumer_offsets`（50 个分区，compact 清理策略）
- [ ] 提交方式：
  - 自动提交 `enable.auto.commit=true`（`auto.commit.interval.ms=5000`）—— **可能重复消费或丢数据**
  - 手动同步 `commitSync()` —— 可靠，阻塞
  - 手动异步 `commitAsync()` —— 高吞吐，失败靠重试回调
  - 最佳实践：**同步 + 异步组合**，或处理完一批后同步提交
- [ ] 追问：自动提交为什么会丢数据/重复？（提交在 poll 后立即发生：处理失败但 offset 已提交 → 丢；处理成功但未到提交周期就崩溃 → 重复）

### 3.2 Rebalance（重平衡）⭐⭐⭐

**触发条件**：
1. 消费组成员变化（新加入、崩溃、主动离开、处理超时被踢）
2. 订阅的 Topic 分区数变化
3. 订阅的 Topic 列表变化

**分配策略**：
| 策略 | 说明 | 问题 |
| --- | --- | --- |
| RangeAssignor（默认） | 按 Topic 逐个范围分配 | 多 Topic 时前面的消费者多拿分区，不均 |
| RoundRobinAssignor | 所有分区轮询分配 | 均匀，但订阅不同 Topic 时会有浪费 |
| StickyAssignor | 尽量均匀 + 尽量保留原分配 | 减少 Rebalance 时的分区迁移，推荐 |
| CooperativeStickyAssignor | **增量协作式 Rebalance**，只回收需要变动的分区 | Kafka 2.4+，避免 STW，生产推荐 |

**Rebalance 的危害**：
- **STW（Stop-The-World）**：传统 Rebalance 期间所有消费者停止消费
- 重复消费：offset 未及时提交，分区被重新分配后从头消费
- 状态失效：消费者本地缓存/状态需要重建

**避免 Rebalance 的参数**：
```properties
session.timeout.ms=45000              # Broker 判定消费者死亡的时间
heartbeat.interval.ms=3000            # 心跳间隔（建议为 session.timeout 的 1/3）
max.poll.interval.ms=300000           # 两次 poll 的最大间隔（处理慢就调大）
max.poll.records=500                  # 单次 poll 拉取条数（调小减少单次处理时间）
```

- [ ] 追问：消费者频繁被踢出组怎么排查？（`max.poll.interval.ms` 超时 —— 单批处理太慢；解决：减小 `max.poll.records`、提高处理并发、异步化、调大超时）
- [ ] 追问：Rebalance 一定会重复消费吗？（配合手动提交 + 幂等处理可避免影响；或用事务把 offset 提交与处理绑定）

### 3.3 消息积压（Lag）处理 ⭐⭐⭐（场景必考）

**定位**：
```bash
kafka-consumer-groups.sh --bootstrap-server xxx --describe --group my-group
# 看 LAG 列
```
或看监控（Burrow / Kafka Exporter + Prometheus）。

**处理思路（按顺序讲）**：
1. **判断根因**：
   - 生产突增（活动/异常刷数据）？
   - 消费变慢（下游 DB/接口慢、GC、反压、数据倾斜）？
   - 消费者挂了 / Rebalance 风暴？
   - 分区数不足导致并行度受限？
2. **应急止血**：
   - 扩容消费者实例（**上限 = 分区数**，超出无效）
   - 提高单消费者并发（多线程处理，注意 offset 提交与顺序）
   - **临时扩容方案**：新建一个分区数更多的临时 Topic，用一个「搬运」消费者把积压数据快速转发过去，再起多倍消费者消费 → 突破原分区数限制
   - 降级：跳过非核心逻辑、异步落库改批量、关闭部分校验
3. **根治**：
   - 优化消费逻辑（批量写、异步 IO、加缓存）
   - 提前规划分区数（分区数 = 峰值吞吐 / 单分区吞吐 × 冗余）
   - 数据倾斜：热点 key 打散
   - 建立 Lag 告警与容量水位监控

- [ ] 追问：如果积压的数据已经过期/无价值怎么办？（评估后可以直接重置 offset 到最新 `--reset-offsets --to-latest`，但需要业务确认）
- [ ] 追问：分区数是不是越多越好？（不是。分区多 → 文件句柄多、副本同步开销大、Rebalance 变慢、端到端延迟上升、Controller 压力大）

## 四、CDC 与数据采集 ⭐⭐⭐

### 4.1 CDC 方案对比

| 方案 | 原理 | 优点 | 缺点 |
| --- | --- | --- | --- |
| **基于时间戳/自增 ID 轮询** | `where update_time > last` | 实现简单，对源库压力可控 | 无法捕获物理删除；有时间窗口漏数；压力大 |
| **基于触发器** | DB trigger 写变更表 | 能捕获所有变更 | 侵入性强，性能差，几乎不用 |
| **基于日志（Binlog/WAL）** ⭐ | 解析 MySQL binlog / PG WAL | 无侵入、实时、能捕获删除、对源库压力小 | 需理解日志格式，需处理 DDL 变更 |

### 4.2 Canal vs Debezium vs Flink CDC ⭐⭐

| 工具 | 说明 |
| --- | --- |
| **Canal** | 阿里开源，伪装 MySQL Slave 拉 binlog，输出到 Kafka/RocketMQ；只支持 MySQL 系 |
| **Debezium** | 基于 Kafka Connect，支持 MySQL/PG/Oracle/MongoDB/SQLServer，功能全面 |
| **Flink CDC** | 基于 Debezium 引擎，**无需 Kafka**，直接 Flink Source；支持全量 + 增量一体化、无锁读取、Exactly-Once、Schema Evolution |

**Flink CDC 核心优势（面试话术）**：
1. **全量 + 增量无缝切换**：先做无锁快照读取（增量快照算法，chunk 切分并行读），再从 binlog 位点继续增量
2. **无需中间 Kafka**：链路简化，运维成本低
3. **Exactly-Once**：offset 纳入 Flink Checkpoint
4. **断点续传**：从 Savepoint/Checkpoint 恢复
5. **支持 Schema 变更**（3.x 的 Pipeline 能力）

- [ ] 追问：Flink CDC 全量阶段怎么保证不影响生产库？（无锁快照：不加全局读锁，通过 chunk 切分 + binlog 位点回溯保证一致性；控制并行度与限速）
- [ ] 追问：CDC 数据乱序怎么处理？（同一主键的变更要保证有序 → 按主键 hash 分区；Flink 侧用 upsert 语义 + 版本号/时间戳比较丢弃旧版本）
- [ ] 追问：CDC 全量同步期间发生了删除怎么办？（增量快照算法通过 chunk 级 binlog 位点回溯修正）

## 五、Kafka 运维与调优 ⭐⭐

### 5.1 关键参数

**Broker**：
```properties
num.network.threads=8                  # 网络线程
num.io.threads=16                      # IO 线程（约等于磁盘数 × 2）
socket.send.buffer.bytes / socket.receive.buffer.bytes
log.flush.interval.messages            # 一般不显式 fsync，靠副本保证
num.replica.fetchers=4                 # 副本同步线程
```

**Producer 吞吐调优**：
```properties
batch.size=65536                       # 增大提升批量
linger.ms=5~50                         # 等待聚合，提升吞吐但增加延迟
compression.type=lz4                   # 或 zstd/snappy
buffer.memory=67108864
max.in.flight.requests.per.connection=5   # 开幂等时可到 5
```

**Consumer 吞吐调优**：
```properties
fetch.min.bytes=1048576                # 攒够 1MB 再返回
fetch.max.wait.ms=500
max.poll.records=500
```

### 5.2 分区数规划

- 目标吞吐 = 峰值 TPS × 消息大小 × 冗余系数
- 单分区吞吐经验值：Producer ~10MB/s，Consumer ~30MB/s（取决于硬件）
- 分区数 = max(生产吞吐 / 单分区生产吞吐, 消费吞吐 / 单分区消费吞吐) × 1.5
- 追问：**分区数只能增加不能减少**；增加分区会打乱有 key 的消息分布（同 key 不再落同一分区），需评估影响

### 5.3 常见问题

- [ ] **PageCache 脏页刷盘抖动**：`vm.dirty_background_ratio` 调低，避免集中刷盘
- [ ] **磁盘 IO 瓶颈**：多磁盘 JBOD（Kafka 自身做分区分配），不用 RAID5
- [ ] **副本同步落后（ISR 频繁收缩）**：网络/磁盘瓶颈，调大 `num.replica.fetchers`、`replica.lag.time.max.ms`
- [ ] **消息堆积导致磁盘满**：设置合理的 `log.retention`，监控磁盘水位
- [ ] **Controller 故障切换慢**：KRaft 模式改善明显
- [ ] **JVM 配置**：堆内存不用太大（6-8GB 足够），把内存留给 PageCache；G1 GC

## 六、其他消息中间件对比 ⭐

| 中间件 | 吞吐 | 延迟 | 顺序 | 事务 | 特点 |
| --- | --- | --- | --- | --- | --- |
| **Kafka** | 百万级/s | ms | 分区内有序 | 支持 | 大数据日志/流处理首选 |
| RocketMQ | 十万级/s | ms | 支持严格有序 | 支持 | 阿里系业务消息，支持定时/事务消息 |
| RabbitMQ | 万级/s | us-ms | 队列内有序 | 弱 | 路由灵活（AMQP），业务解耦 |
| Pulsar | 百万级/s | ms | 分区内有序 | 支持 | 存算分离（BookKeeper），多租户，云原生 |

- [ ] 追问：Kafka 和 Pulsar 的区别？（Pulsar 存算分离，Broker 无状态，扩容不需搬数据；支持多租户与跨地域复制；生态成熟度 Kafka 更高）

## 七、本章高频面试题速查

1. Kafka 为什么这么快（⭐⭐⭐ 必考）
2. 零拷贝原理（⭐⭐⭐）
3. Kafka 怎么保证消息不丢失（⭐⭐⭐ 必考，要从 Producer/Broker/Consumer 三段答）
4. Kafka 怎么保证消息不重复（幂等 + 事务）（⭐⭐⭐）
5. Kafka 怎么保证消息顺序（⭐⭐⭐）
6. ISR、HW、LEO 是什么（⭐⭐）
7. Rebalance 什么时候发生，有什么危害，怎么避免（⭐⭐⭐）
8. 消息积压了几千万条怎么处理（⭐⭐⭐ 场景必考）
9. Kafka 和 ZooKeeper 的关系，KRaft 是什么（⭐⭐）
10. CDC 方案对比，Flink CDC 的优势（⭐⭐⭐ 实时方向必考）
11. Kafka 分区数怎么定，能不能减少（⭐⭐）
12. Kafka 和 RocketMQ/Pulsar 怎么选（⭐）

### 补充：HW 与 LEO ⭐⭐

- [ ] **LEO（Log End Offset）**：每个副本自己 log 的下一条待写入 offset
- [ ] **HW（High Watermark）**：ISR 中所有副本都已同步的最小 LEO，**消费者只能读到 HW 之前的消息**
- [ ] 作用：保证消费者读到的数据是已被足够副本确认的，避免读到可能丢失的数据
- [ ] 追问：Leader 挂了会发生什么？（从 ISR 中选新 Leader；HW 之后的未同步数据对消费者不可见，因此不丢；若开启 unclean 选举则可能丢）
