# 01 · 大数据基础与 Hadoop 生态

> 对应排期：W1 D1-D3
> 定位：⭐⭐ 为主。3-5 年经验的岗位不会深挖 MapReduce 手写，但会拿 HDFS/YARN 原理来验证你的底层理解。

## 一、HDFS ⭐⭐⭐

### 1.1 架构与角色

- [ ] NameNode（NN）：管理元数据（文件树、块位置、权限），元数据全部在内存 + 磁盘持久化
- [ ] DataNode（DN）：存实际数据块，默认 128MB，定期向 NN 汇报心跳（3s）与块报告（6h）
- [ ] Secondary NameNode（SNN）：不是热备，负责定期 merge edits log 到 fsimage，减轻 NN 压力
- [ ] HA 架构：Active NN + Standby NN + JournalNode 集群（共享 edits）+ ZKFC（故障转移）
- [ ] Federation：多个 NN 分管不同 namespace，解决单 NN 内存瓶颈

### 1.2 自测

- [ ] 为什么块大小默认 128MB？（减少寻址开销、让寻址时间远小于传输时间；太大影响并行度，太小放大元数据）
- [ ] NN 挂了会发生什么？HA 下 ZKFC 如何做故障切换？（ZK 抢锁 → Standby 追平 edits → 提升为 Active）
- [ ] NN 的元数据能存多少文件？（受内存限制，经验值 1 亿文件 ≈ 20-30GB 堆，超出需 Federation 或小文件治理）
- [ ] JournalNode 为什么是奇数个？（基于多数派 quorum 写入，3 个容忍 1 个挂）

### 1.3 读写流程 ⭐⭐⭐

**写流程**（务必能画出来）：
```
Client → NN 请求上传 → NN 校验权限/路径 → 返回可写
Client 切分 block → 对每个 block 向 NN 申请 DN 列表（3个）
Client 建立 pipeline（DN1→DN2→DN3）→ 以 packet(64KB) 为单位流式发送
每个 packet 走 ack 反向确认 → 全部完成后通知 NN
```

**读流程**：
```
Client → NN 获取 block 位置列表（按网络拓扑距离排序）→ 就近读取
```

- [ ] pipeline 中某个 DN 挂了怎么办？（关闭 pipeline，剔除故障节点，把已确认的 block 标记为 incomplete，剩余数据写到其余健康节点，后续 NN 异步补副本）
- [ ] 副本放置策略？（第 1 个：客户端所在节点或随机；第 2 个：另一机架；第 3 个：与第 2 个同机架不同节点。机架感知降低跨机架带宽同时保证机架故障不丢数据）

### 1.4 小文件问题 ⭐⭐⭐（高频）

**危害**：
1. NN 内存压力（每个文件/块/目录约 150 字节元数据）
2. Map 任务数爆炸，任务启动开销 > 计算开销
3. 随机读性能差

**治理方案**：
- [ ] 源头控制：Hive 开启 `hive.merge.mapfiles` / `hive.merge.mapredfiles`，设置 `hive.merge.smallfiles.avgsize`
- [ ] 采集侧：Flume/DataX 按时间或大小滚动落地，避免每条一个文件
- [ ] Spark：`spark.sql.shuffle.partitions` 配合 `coalesce` / AQE 的 `coalescePartitions`
- [ ] 事后合并：`INSERT OVERWRITE ... SELECT` 重写到目标表；HDFS `getmerge`；Hive `CONCATENATE`（仅 RCFile/ORC）
- [ ] 存储格式：改用 ORC/Parquet + 合理分区，配合 HAR 归档（不推荐，影响性能）
- [ ] 湖仓方案：Iceberg/Hudi 自带小文件合并（`rewriteDataFiles` / compaction）

- [ ] 追问：小文件合并会带来什么新问题？（合并任务本身消耗资源；重写会改变文件时间戳影响下游增量；对分区表要注意分区倾斜）

## 二、YARN ⭐⭐

### 2.1 核心组件与提交流程

- [ ] ResourceManager（RM）：全局资源调度，含 Scheduler 与 ApplicationsManager
- [ ] NodeManager（NM）：单机资源管理，启动/监控 Container
- [ ] ApplicationMaster（AM）：单应用的资源申请与任务协调
- [ ] Container：资源抽象（CPU vcore + 内存）

**作业提交完整流程**（能画出来）：
```
1. client 提交 app 给 RM
2. RM 在某 NM 启动 AM Container
3. AM 启动后向 RM 注册，开始申请 Resource
4. RM 返回 Container 列表给 AM
5. AM 联系对应 NM 启动 Container（跑 task），并心跳同步进度
6. 任务完成，AM 向 RM 注销
```

### 2.2 调度器 ⭐⭐⭐

| 调度器 | 特点 | 适用 |
| --- | --- | --- |
| FIFO | 队列先来先服务 | 测试环境 |
| Capacity | 多队列，每队列内部 FIFO，支持弹性借用、资源保证 | Yahoo 默认，大集群多租户 |
| Fair | 多队列 + 队列内公平分配，支持抢占 | CDH 默认，共享集群 |

- [ ] Capacity vs Fair 的区别？（Capacity 保证队列最小容量、空闲可被借用但可被抢占回收；Fair 追求所有 app 平均分配资源，支持 DRF 主导资源公平）
- [ ] 资源不足时怎么办？（Capacity 按队列权重回收；Fair 支持抢占，`yarn.scheduler.fair.preemption`）
- [ ] YARN 上如何做多租户隔离？（队列 + 标签调度 node label + Linux cgroup 内存/CPU 隔离）

### 2.3 内存模型

- [ ] `yarn.nodemanager.resource.memory-mb`：NM 可分配总内存
- [ ] Container 内存 = JVM Heap + Off-Heap（`yarn.scheduler.capacity.maximum-am-resource-percent` 限制 AM 占比）
- [ ] Container 被 kill 常见原因：物理内存超限（超出 `pmem-check-maximum`）、虚拟内存超限（`vmem-pmem-multiplier` 默认 2.1）
- [ ] 追问：Spark on YARN 的 executor 内存怎么映射到 Container？（`spark.executor.memory` + `spark.executor.memoryOverhead` ≈ Container 请求内存）

## 三、MapReduce ⭐

### 3.1 完整流程（了解即可，仍可能被问）

```
InputFormat → Split → Map → Partition → Sort → Combine → Spill(80%) →
Merge → (Shuffle 网络) → Reduce 端 Merge → Sort → Group → Reduce → OutputFormat
```

- [ ] Split 和 Block 的关系？（Split 是逻辑切分，默认等于 Block 大小；一个 Split 对应一个 Map 任务）
- [ ] 为什么要排序？（Reduce 端需要把相同 key 的数据聚在一起，全局有序使归并代价最低）
- [ ] Combiner 和 Reducer 的区别？（Combiner 是 Map 端本地优化，必须满足结合律+交换律，如 sum/max 可用，avg 不可用）
- [ ] 为什么 avg 不能用 Combiner？（对各分组均值再求均值 ≠ 全局均值）

### 3.2 Shuffle 优化点

- [ ] `mapreduce.task.io.sort.mb`（环形缓冲区大小，默认 100MB）
- [ ] `mapreduce.map.sort.spill.percent`（溢写阈值，默认 0.8）
- [ ] `mapreduce.map.combine.minspills`（合并时最小溢写文件数）
- [ ] 压缩：map 端输出压缩（snappy）减少网络 IO

## 四、ZooKeeper ⭐⭐

- [ ] 核心能力：分布式协调、配置管理、命名服务、分布式锁、Leader 选举
- [ ] 数据模型：树形 znode，节点类型（持久、临时、顺序、容器）
- [ ] ZAB 协议：崩溃恢复 + 消息广播，保证主从数据一致与顺序性
- [ ] 为什么是奇数台？（过半机制，3 台容忍 1 挂，4 台也只容忍 1 挂，浪费）
- [ ] Watcher 机制：一次性、顺序性、轻量；客户端注册 → 服务端触发 → 通知
- [ ] ZK 在大数据生态中的角色：HDFS HA 的 ZKFC、YARN HA、HBase Master 选举、Kafka（旧版本）Controller 选举与元数据

- [ ] 追问：ZK 的写为什么比读慢？（写要走 ZAB 广播到过半节点，读可在任意 follower 本地完成）
- [ ] 追问：ZK 能做分布式锁，有什么坑？（羊群效应 → 用顺序临时节点 + 只监听前一个节点；session 超时导致锁提前释放）

## 五、通用采集与传输工具 ⭐⭐

| 工具 | 用途 | 关键点 |
| --- | --- | --- |
| Flume | 日志采集 | Source/Channel/Memory or File Channel/事务保证；拦截器做初步清洗 |
| DataX | 异构数据源离线同步 | Reader/Writer/Channel，单机多线程，适合批量、无增量 binlog |
| Sqoop | RDBMS ↔ HDFS（已过时） | 底层跑 MR，靠 JDBC，增量靠 `--incremental` |
| Canal / Flink CDC | MySQL binlog 增量采集 | 见 docs/06 §4 |
| SeaTunnel / Spark Connector | 新一代同步 | 支持批流一体 |

- [ ] Flume 如何保证不丢数据？（File Channel + 事务：Source 写入 Channel 事务提交，Sink 从 Channel 取数并确认下游成功后再 commit/删除）
- [ ] Flume 内存 Channel vs 文件 Channel 取舍？（内存快但断电丢；文件慢但可靠，生产用 File Channel 或双 Channel 故障转移）
- [ ] DataX 如何处理增量同步？（基于自增 ID 或时间戳 where 条件；无法捕获物理删除，需配合全量兜底）

## 六、文件格式与压缩 ⭐⭐⭐

### 6.1 格式对比

| 格式 | 类型 | 切分 | 索引 | 适用 |
| --- | --- | --- | --- | --- |
| TextFile | 行存 | 可 | 无 | 调试、原始层 |
| SequenceFile | 行存 KV | 可 | 无 | MR 中间结果 |
| RCFile | 行列混合 | 可 | 弱 | 已被 ORC 取代 |
| **ORC** | 列存 | 可 | 三级索引（File/Stripe/Row）| Hive 生态首选 |
| **Parquet** | 列存 | 可 | Row Group + Page 统计 | Spark/Impala 生态首选 |
| Avro | 行存 | 可 | 无 | Schema 演进、序列化传输 |

### 6.2 必问点

- [ ] 列存为什么适合 OLAP？（只读需要的列，减少 IO；同列数据类型一致压缩率高；支持谓词下推到文件级跳过）
- [ ] ORC 的三级索引怎么用？（File-level statistics → Stripe-level → Row Index（默认每 10000 行），配合谓词下推跳过不匹配数据）
- [ ] Parquet 的 Row Group / Column Chunk / Page 结构？
- [ ] 压缩算法对比：

| 算法 | 压缩率 | 速度 | 可切分 | 备注 |
| --- | --- | --- | --- | --- |
| Gzip | 高 | 慢 | 否（单文件） | 归档用 |
| Bzip2 | 最高 | 最慢 | 是 | 少用 |
| LZO | 中 | 快 | 是（需建索引） | 需装 native |
| **Snappy** | 中低 | 很快 | 否（块内） | 中间数据、Shuffle 首选 |
| Zstd | 高 | 快 | 取决于容器 | 新一代推荐 |

- [ ] 追问：gzip 文件为什么不可切分？（DEFLATE 流式解压，必须从头开始；但 gzip 包裹的 Avro/SequenceFile 内部按块压缩，可切分）
- [ ] 追问：ORC/Parquet + Snappy 是怎么做到可切分的？（每个 Row Group / Stripe 独立压缩，配合文件级索引定位）

## 七、集群运维与容量估算 ⭐（Leader 面常问）

- [ ] 容量估算模板：日增量 10TB，3 副本，保留 2 年 → `10TB × 3 × 730 ≈ 21.9PB`，加 30% 冗余 → 约 28PB
- [ ] 节点规模估算：单机 12×8TB 盘 ≈ 88TB 可用 → 28PB / 88TB ≈ 320 台 DN
- [ ] 常见监控指标：HDFS 容量使用率、缺失块、under-replicated block、NN RPC 延迟、YARN pending container、节点失联
- [ ] 数据均衡：`hdfs balancer`（阈值 -threshold 10）、`hdfs mover`（按存储策略移动）、DN 上下线 decommission 流程
- [ ] 扩容流程：新节点上架 → 配置分发 → 启动 DN/NM → balancer → 观察
- [ ] 追问：一台 DN 磁盘坏了怎么处理？（DN 掉线 → NN 检测到副本缺失 → 自动在其他 DN 补副本 → 修复后重新加入 → balancer）

## 八、本章高频面试题速查

1. HDFS 读写流程详细讲一下（⭐⭐⭐ 必考）
2. NameNode HA 原理，JournalNode 作用（⭐⭐⭐）
3. 小文件问题的危害和解决方案（⭐⭐⭐ 必考）
4. YARN 作业提交流程（⭐⭐）
5. Capacity 和 Fair 调度器区别（⭐⭐）
6. ORC 和 Parquet 区别，为什么列存快（⭐⭐⭐）
7. Snappy 和 Gzip 什么时候用哪个（⭐⭐）
8. ZooKeeper 在集群里干什么，为什么奇数台（⭐⭐）
9. Flume 怎么保证数据不丢不重（⭐⭐）
10. 给你一个日增 5TB 的业务，怎么估算集群规模（⭐ Leader 面）
