# 09 · Java 并发与 JVM 基础

> 对应排期：W7 D1-D4
> 定位：⭐⭐。大数据岗对 Java 的要求低于后端岗，但 Flink/Spark 底层都是 Java，中高级面试仍会问集合、并发、JVM。

## 一、Java 集合 ⭐⭐⭐

### 1.1 HashMap ⭐⭐⭐（必考）

**结构（JDK 8+）**：数组 + 链表 + 红黑树
```
table[] (Node<K,V>[])
  └── 链表长度 >= 8 且 table.length >= 64 → 树化为红黑树
      链表长度 <= 6 → 退化回链表
```

**核心参数**：
- 默认容量 16，负载因子 0.75，扩容阈值 = 容量 × 负载因子 = 12
- 容量永远是 2 的幂 → `index = (n - 1) & hash`（等价于取模但更快）
- 扰动函数：`hash = key.hashCode() ^ (key.hashCode() >>> 16)`（高 16 位参与运算，减少碰撞）

**扩容（resize）流程**：
- 容量翻倍，重新计算 index
- JDK 8 优化：`hash & oldCap == 0` 留在原位，否则移到 `原位置 + oldCap`（避免重新计算 hash）

**为什么线程不安全**：
- JDK 7：并发扩容时链表头插法可能形成**环形链表**，get 时 CPU 100%
- JDK 8：改尾插法解决环，但仍可能**数据覆盖丢失**、size 不准

- [ ] 追问：为什么负载因子是 0.75？（时间空间折中：太大冲突多、太小浪费空间；0.75 下泊松分布冲突概率可接受）
- [ ] 追问：为什么树化阈值是 8？（泊松分布下单桶达到 8 个节点的概率约千万分之一，是极端情况兜底；退化用 6 是为了避免频繁树化/退化抖动）
- [ ] 追问：HashMap 初始容量怎么设置？（`expectedSize / 0.75 + 1`，避免多次扩容）

### 1.2 ConcurrentHashMap ⭐⭐⭐

| 版本 | 实现 | 锁粒度 |
| --- | --- | --- |
| JDK 7 | Segment 分段锁（每个 Segment 是一个小 HashMap，继承 ReentrantLock） | 段级（默认 16 段 → 16 并发） |
| **JDK 8** | 数组 + 链表/红黑树，**CAS + synchronized 锁单个桶头节点** + 多线程协助扩容 | 桶级（并发度 = 桶数） |

**JDK 8 put 流程**：
```
1. 计算 hash，定位桶
2. 桶为空 → CAS 写入（无锁）
3. 桶不为空且是普通节点 → synchronized(头节点) → 遍历插入/覆盖
4. 是红黑树节点 → synchronized → 树插入
5. 链表长度 >= 8 → 树化
6. size 达到阈值 → 多线程协助扩容（transfer，sizeCtl 控制）
```
- [ ] `size()` 用 `baseCount + CounterCell[]` 分段计数（类似 LongAdder），避免竞争
- [ ] 为什么 key/value 不能为 null？（并发下 `get(k)==null` 无法区分「不存在」还是「值就是 null」，二义性）

### 1.3 其他集合

- [ ] ArrayList vs LinkedList：数组随机访问 O(1)、扩容 1.5 倍；链表插入删除 O(1)（已知位置）但随机访问 O(n)
- [ ] HashMap vs Hashtable vs ConcurrentHashMap（Hashtable 全表 synchronized，已过时）
- [ ] TreeMap（红黑树，有序）、LinkedHashMap（双向链表保序，可实现 LRU）、HashSet（内部就是 HashMap）
- [ ] CopyOnWriteArrayList：写时复制，读无锁，适合读多写极少
- [ ] fail-fast vs fail-safe：`modCount` 检测 → `ConcurrentModificationException`；并发容器用弱一致性迭代器

## 二、并发编程 ⭐⭐⭐

### 2.1 JMM 与三大特性

- [ ] **原子性**：synchronized、Lock、CAS（Atomic 类）
- [ ] **可见性**：volatile、synchronized、final
- [ ] **有序性**：volatile（禁止指令重排 —— 内存屏障）、synchronized、happens-before 规则
- [ ] `volatile` 原理：
  1. 写：立即刷回主内存 + 通过 MESI 协议使其他 CPU 缓存行失效
  2. 读：从主内存读
  3. 插入内存屏障禁止重排序（StoreStore / StoreLoad / LoadLoad / LoadStore）
  - **不保证原子性**（如 `i++`）
- [ ] 追问：DCL 单例为什么要加 volatile？（`new` 分三步：分配内存 → 初始化 → 引用赋值；重排后其他线程可能拿到未初始化完成的对象）

### 2.2 synchronized 与锁升级 ⭐⭐⭐

```
无锁 → 偏向锁 → 轻量级锁（CAS 自旋） → 重量级锁（Monitor，OS mutex，用户态/内核态切换）
```
- [ ] 对象头 Mark Word 存锁状态；锁只能升级不能降级（偏向锁在 JDK 15 后默认关闭）
- [ ] Monitor：`monitorenter` / `monitorexit`，依赖对象头的 Monitor 指针，底层是 C++ 的 `ObjectMonitor`（EntryList / WaitSet）
- [ ] `synchronized` vs `ReentrantLock`：

| 维度 | synchronized | ReentrantLock |
| --- | --- | --- |
| 层级 | JVM 关键字，自动释放 | JDK API，必须手动 unlock（finally） |
| 公平性 | 非公平 | 可选公平/非公平 |
| 可中断 | 不支持 | `lockInterruptibly()` |
| 超时尝试 | 不支持 | `tryLock(timeout)` |
| 条件变量 | 单个 wait/notify | 多个 `Condition` |
| 性能 | JDK 6 后优化，基本持平 | 持平 |

### 2.3 AQS ⭐⭐⭐

- [ ] **AbstractQueuedSynchronizer**：`volatile int state` + CLH 变体的 FIFO 双向等待队列
- [ ] 独占模式：`tryAcquire` / `tryRelease`（ReentrantLock、写锁）
- [ ] 共享模式：`tryAcquireShared` / `tryReleaseShared`（Semaphore、CountDownLatch、读锁）
- [ ] 获取失败 → 包装成 Node 入队 → `LockSupport.park()` 挂起 → 前驱释放后 `unpark` 唤醒
- [ ] ReentrantLock 可重入：state 累加，同一线程再次获取时 state+1

### 2.4 线程池 ⭐⭐⭐（必考）

**七大参数**：
```java
new ThreadPoolExecutor(
    corePoolSize,        // 核心线程数
    maximumPoolSize,     // 最大线程数
    keepAliveTime, unit, // 非核心线程空闲存活时间
    workQueue,           // 任务队列
    threadFactory,       // 线程工厂（命名！）
    handler              // 拒绝策略
);
```

**执行流程**：
```
提交任务 → 核心线程未满 → 创建核心线程执行
        → 核心线程满 → 入队列
        → 队列满 → 创建非核心线程（到 max）
        → 达到 max 且队列满 → 执行拒绝策略
```

**四种拒绝策略**：AbortPolicy（抛异常，默认）、CallerRunsPolicy（调用者线程执行，起到降速作用）、DiscardPolicy（静默丢弃）、DiscardOldestPolicy（丢最老的）

**队列选择**：
| 队列 | 特点 |
| --- | --- |
| ArrayBlockingQueue | 有界数组，必须指定容量 —— **推荐** |
| LinkedBlockingQueue | 默认无界（Integer.MAX_VALUE），易 OOM —— `Executors.newFixedThreadPool` 的坑 |
| SynchronousQueue | 不存储，直接交付，配合大 maxPoolSize（`newCachedThreadPool` 的坑） |
| PriorityBlockingQueue | 带优先级 |
| DelayedWorkQueue | 延迟任务（`newScheduledThreadPool`） |

- [ ] 为什么阿里规范禁止用 `Executors` 创建线程池？（FixedThreadPool/SingleThreadPool 用无界 LinkedBlockingQueue → OOM；CachedThreadPool/ScheduledThreadPool 允许 maxPoolSize=Integer.MAX_VALUE → 创建海量线程 OOM）
- [ ] 线程数怎么设置？
  - **CPU 密集型**：`核数 + 1`
  - **IO 密集型**：`核数 × 2` 或 `核数 × (1 + 等待时间/计算时间)`
  - 大数据场景（Shuffle、读写 HDFS）偏 IO 密集，但要注意与 YARN vcore 的映射
- [ ] 追问：线程池里任务抛异常会怎样？（`execute` 提交：线程终止并新建补充；`submit` 提交：异常被封装进 Future，get 时才抛出 —— 容易吞异常，务必处理）

### 2.5 并发工具与其他

- [ ] `CountDownLatch`（一次性，等待 N 个任务完成）vs `CyclicBarrier`（可重用，互相等待）vs `Semaphore`（限流信号量）
- [ ] `ThreadLocal`：每个线程持有副本，`ThreadLocalMap` 的 key 是弱引用、value 是强引用 → **内存泄漏**风险，用完必须 `remove()`（尤其线程池场景线程复用）
- [ ] `CompletableFuture`：异步编排，`thenApply` / `thenCompose` / `allOf`（大数据中常用于并行维表查询）
- [ ] CAS 三大问题：ABA（用 `AtomicStampedReference` 加版本号）、自旋开销大、只能保证单变量原子（用 `AtomicReference` 包装对象或加锁）
- [ ] `LongAdder` vs `AtomicLong`：分段（Cell[]）减少竞争，高并发下更快，但只能求和

## 三、JVM ⭐⭐⭐

### 3.1 内存区域

```
线程私有：程序计数器、虚拟机栈（栈帧：局部变量表、操作数栈、动态链接、返回地址）、本地方法栈
线程共享：堆（新生代 Eden/S0/S1 + 老年代）、方法区（JDK 8+ 为 Metaspace，使用本地内存）
直接内存：NIO DirectByteBuffer（Spark/Flink 大量使用，注意 memoryOverhead）
```
- [ ] JDK 7 → 8 变化：永久代（PermGen）移除，改为 Metaspace（本地内存，`-XX:MaxMetaspaceSize`）；字符串常量池移到堆
- [ ] 追问：为什么大数据作业要关注堆外内存？（Netty 传输、Kryo/Tungsten 序列化、RocksDB BlockCache 都在堆外，YARN Container 限制的是物理内存，堆外超限会被 kill）

### 3.2 对象创建与内存分配

- [ ] 逃逸分析 → 栈上分配 / 标量替换 / 锁消除（减少堆分配压力）
- [ ] TLAB（Thread Local Allocation Buffer）：Eden 中每线程私有分配区，避免分配竞争
- [ ] 大对象直接进老年代（`-XX:PretenureSizeThreshold`）

### 3.3 GC ⭐⭐⭐

**判活算法**：引用计数（无法解决循环引用，不用）→ **可达性分析**（GC Roots：栈中引用、静态变量、常量、JNI 引用、活跃线程）

**引用类型**：强、软（内存不足回收，适合缓存）、弱（下次 GC 必回收，ThreadLocalMap key）、虚（配合 ReferenceQueue 做资源清理）

**分代收集**：
```
Minor GC / Young GC：Eden 满触发，存活对象复制到 S0/S1，年龄+1
年龄达阈值（默认 15）→ 晋升老年代
大对象 / 动态年龄判定（同龄对象总和 > Survivor 一半）→ 直接晋升
Major GC / Old GC：老年代回收
Full GC：整堆 + Metaspace，STW 长，要极力避免
```

**垃圾收集器**：
| 收集器 | 区域 | 算法 | 特点 |
| --- | --- | --- | --- |
| Serial / Serial Old | 新生代/老年代 | 复制/标记整理 | 单线程，Client 模式 |
| ParNew | 新生代 | 复制 | Serial 的多线程版 |
| Parallel Scavenge / Parallel Old | 全堆 | 复制/标记整理 | **吞吐量优先**，JDK 8 默认 |
| **CMS** | 老年代 | 标记清除 | 低延迟，并发收集；有内存碎片、浮动垃圾、CPU 敏感问题；JDK 14 移除 |
| **G1** | 全堆（Region） | 标记整理 + 复制 | 可预测停顿（`-XX:MaxGCPauseMillis`），JDK 9+ 默认，大数据作业常用 |
| **ZGC / Shenandoah** | 全堆 | 染色指针/读屏障 | 亚毫秒停顿，超大堆（TB 级），JDK 15+ 生产可用 |

**CMS 四个阶段**：初始标记（STW）→ 并发标记 → 重新标记（STW）→ 并发清除
**G1 流程**：初始标记（STW，借 Minor GC）→ 并发标记 → 最终标记（STW）→ 筛选回收（STW，按回收价值排序选 Region）

### 3.4 JVM 调优 ⭐⭐⭐

**大数据作业常用配置模板**：
```bash
# Spark Executor（8G 堆为例）
-Xms8g -Xmx8g                              # 堆初始=最大，避免动态调整抖动
-XX:+UseG1GC
-XX:MaxGCPauseMillis=200
-XX:InitiatingHeapOccupancyPercent=35      # IHOP，提前触发并发标记
-XX:G1HeapRegionSize=16m
-XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:/path/gc-%t.log
-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/path/
-XX:MaxDirectMemorySize=2g                 # 堆外上限
```

**调优思路**：
1. **先看 GC 日志**：频率、单次耗时、Full GC 是否频繁、GC 后老年代是否回落
2. **常见病症**：
   - Young GC 频繁 → Eden 太小 / 对象创建过多 → 加大新生代、减少临时对象（用 mapPartitions 复用）
   - 老年代增长快、频繁 Full GC → 对象过早晋升 → 加大 Survivor、调 `-XX:MaxTenuringThreshold`
   - Full GC 后内存不降 → 内存泄漏 → dump 分析（MAT / jvisualvm）
   - GC 时间占比 > 10% → 需扩容或优化数据结构
3. **工具**：`jps`、`jstat -gcutil <pid> 1000`、`jmap -histo` / `-dump`、`jstack`（线程/死锁）、MAT、Arthas、JFR

- [ ] 追问：Executor OOM 了怎么排查？（`-XX:+HeapDumpOnOutOfMemoryError` 拿 dump → MAT 看 dominator tree 找大对象 → 通常是单分区数据过大（倾斜）、collect 拉回过多、cache 过多、UDF 中持有大集合）
- [ ] 追问：Flink 用 RocksDB 时 JVM 堆该怎么配？（堆不要太大，留足 Managed Memory 给 RocksDB；`taskmanager.memory.managed.fraction=0.4`；G1 或 ZGC）

## 四、Java 基础补充 ⭐

- [ ] `==` vs `equals` vs `hashCode`：重写 equals 必须重写 hashCode（HashMap 依赖）
- [ ] String 不可变性（`final char[]` / JDK 9+ `byte[]`）、字符串常量池、`intern()`
- [ ] `StringBuilder`（非线程安全，快）vs `StringBuffer`（synchronized）
- [ ] 异常体系：Error / Exception（Checked vs Unchecked）；`try-with-resources`
- [ ] 反射与动态代理（JDK Proxy 基于接口、CGLIB 基于继承）
- [ ] NIO：Channel + Buffer + Selector；**直接内存 + 零拷贝**（Kafka/Netty 的底层）
- [ ] SPI 机制（`ServiceLoader`）：JDBC Driver、Flink Connector、Hadoop FileSystem 都靠它
- [ ] Java 8+：Lambda、Stream（注意 `parallelStream` 用的是公共 ForkJoinPool，大数据场景慎用）、Optional、新时间 API

## 五、本章高频面试题速查

1. HashMap 原理，JDK 7 和 8 的区别（⭐⭐⭐ 必考）
2. HashMap 为什么线程不安全（⭐⭐⭐）
3. ConcurrentHashMap 怎么保证线程安全（⭐⭐⭐）
4. volatile 原理，和 synchronized 区别（⭐⭐⭐）
5. synchronized 锁升级过程（⭐⭐）
6. AQS 原理（⭐⭐）
7. 线程池七大参数和执行流程（⭐⭐⭐ 必考）
8. 线程数怎么设置（⭐⭐⭐）
9. ThreadLocal 原理和内存泄漏（⭐⭐）
10. JVM 内存区域（⭐⭐⭐）
11. GC 算法和收集器，G1 原理（⭐⭐⭐）
12. 一次 OOM / CPU 100% 的排查过程（⭐⭐⭐ 场景题）
13. JVM 参数你在大数据作业里怎么配的（⭐⭐⭐ 结合项目）
14. 什么是零拷贝（⭐⭐，可结合 Kafka 回答）
