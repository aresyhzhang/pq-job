# 02 · Hive 与 SQL 离线开发

> 对应排期：W1 D3-D7（含周末 SQL 刷题）
> 定位：⭐⭐⭐ 核心。SQL 手写几乎是所有大数据岗的笔试/一面必考项，Hive 优化是中高级的分水岭。

## 一、Hive 架构与执行原理 ⭐⭐

### 1.1 组件

- [ ] Metastore：元数据（表结构、分区、字段、HDFS 路径、SerDe），一般存 MySQL，可独立服务化
- [ ] Driver：解析器（Parser）→ 编译器（Compiler）→ 优化器（Optimizer）→ 执行器（Executor）
- [ ] 执行引擎：MapReduce（慢，已淘汰）/ Tez（DAG，主流）/ Spark（Spark on Hive）
- [ ] 存储：HDFS；表 = 目录，分区 = 子目录，分桶 = 文件

### 1.2 SQL 执行流程（能画）

```
SQL → 词法/语法解析(AST) → 逻辑计划(QueryBlock) → 逻辑优化(谓词下推/列裁剪)
    → 物理计划(MR/Tez/Spark Task DAG) → 物理优化 → 执行
```

- [ ] 追问：Hive 的谓词下推在什么情况下失效？（对分区字段用函数包裹，如 `substr(dt,1,7)='2024-01'`；ORC 文件级下推需要开启 `hive.optimize.ppd`）

### 1.3 内部表 vs 外部表 ⭐⭐⭐

| 维度 | 内部表（Managed） | 外部表（External） |
| --- | --- | --- |
| 数据归属 | Hive 管理 | HDFS 自主管理 |
| drop 表 | 删元数据 + 删数据 | 只删元数据 |
| 适用 | 临时表、中间表 | ODS 层接入原始数据、共享数据 |

- [ ] 生产实践：**ODS 层一律用外部表**（防止误删原始数据），DW/ADS 中间表用内部表。
- [ ] 追问：为什么 ODS 用外部表？（原始数据是唯一真相源，重跑代价高；外部表 drop 不会误删底层文件）

### 1.4 分区与分桶 ⭐⭐⭐

- [ ] 分区（Partition）：目录级隔离，减少扫描数据量；`hive.exec.dynamic.partition` 动态分区
- [ ] 分桶（Bucket）：按字段 hash 分成固定数量文件，利于 Join 优化（SMB Join）与抽样
- [ ] 分区字段选择原则：查询高频过滤条件、基数不宜过高（避免分区爆炸）、通常用日期
- [ ] 动态分区参数：`hive.exec.max.dynamic.partitions`（默认 1000）、`hive.exec.max.dynamic.partitions.pernode`（默认 100）

- [ ] 追问：分区过多有什么问题？（NN 元数据压力、Metastore 查询慢、小文件、任务 list 分区耗时）
- [ ] 追问：分桶表怎么做 SMB Join？（两表按相同 key 分桶且桶数相同，Join 时桶对桶本地合并，避免 Shuffle）

## 二、Hive SQL 优化 ⭐⭐⭐（面试最高频）

### 2.1 优化清单（要求能完整口述）

| 优化点 | 做法 | 原理 |
| --- | --- | --- |
| **列裁剪** | 只 select 需要的列，禁止 `select *` | 列存格式下直接减少 IO |
| **分区裁剪** | where 带上分区字段，避免函数包裹 | 跳过整个目录 |
| **谓词下推** | 过滤条件尽量前置 | 减少参与 Shuffle 的数据量 |
| **Map 端聚合** | `set hive.map.aggr=true` | 相当于 Combiner，减少网络传输 |
| **合理设置 Map/Reduce 数** | `mapreduce.input.fileinputformat.split.maxsize`、`hive.exec.reducers.bytes.per.reducer` | 避免任务过多或单任务过重 |
| **Join 优化** | 小表放左边 / 开启 MapJoin | 见 §2.2 |
| **数据倾斜处理** | 见 §2.3 | 最高频追问点 |
| **并行执行** | `hive.exec.parallel=true` | 无依赖的 Stage 并发跑 |
| **向量化** | `hive.vectorized.execution.enabled=true` | 一次处理 1024 行，减少 CPU 开销 |
| **CBO** | `hive.cbo.enable=true` | 基于统计信息选择最优 Join 顺序 |
| **文件格式** | ORC/Parquet + Snappy/Zstd | 压缩 + 索引下推 |
| **减少 Job 数** | 合并子查询、用 `UNION ALL` 代替多次扫表 | 一次扫描多路聚合（multi-insert） |
| **推测执行** | `mapreduce.map.speculative.execution` | 处理慢节点，但可能浪费资源 |

### 2.2 Join 优化 ⭐⭐⭐

**执行顺序**：Hive 中多个表 Join，最后一个表会作为 reduce 端的驱动表 / 或者 MapJoin 的小表加载到内存。

```sql
-- 小表在前（Hive 会尽量把小表加载到内存）
SELECT /*+ MAPJOIN(b) */ a.*, b.name
FROM big_table a
JOIN small_dim b ON a.id = b.id;
```

- [ ] MapJoin 原理：把小表全量加载到每个 Map 任务的内存 HashTable，Map 端完成 Join，无 Reduce、无 Shuffle
- [ ] 参数：`hive.auto.convert.join=true`、`hive.mapjoin.smalltable.filesize=25000000`（25MB）
- [ ] 限制：MapJoin 不支持 Full Outer Join；小表必须能放进内存
- [ ] Common Join（Reduce 端 Join）：Shuffle 按 key 分区 → Reduce 端 hash join，大表 Join 大表用它
- [ ] SMB Join：分桶表专用，桶对桶合并，比 Common Join 快很多

- [ ] 追问：大表 join 大表怎么办？（1. 先过滤再 join 减少数据量；2. 拆分维度分桶走 SMB；3. 倾斜 key 单独处理；4. 换成 Spark + AQE skew join；5. 考虑预聚合到中间表）

### 2.3 数据倾斜 ⭐⭐⭐（必考，务必背熟）

**现象**：99% 的 Reduce 秒完成，1% 的 Reduce 跑几小时；日志卡在 99.99%。

**根因**：key 分布不均，某些 key 数据量巨大，落到同一个 Reduce。

**常见倾斜场景与对策**：

| 场景 | 方案 |
| --- | --- |
| **空值/默认值倾斜**（如 user_id 为 null） | ① 提前过滤 `where id is not null`；② 打散：`on a.id = CASE WHEN a.id IS NULL THEN concat('hive_', rand()) ELSE a.id END` |
| **热点 key 倾斜**（如爆款商品） | ① 热点 key 单独拆出来跑，最后 union；② 加随机前缀两阶段聚合 |
| **大表 join 小表倾斜** | 开 MapJoin，直接消除 Shuffle |
| **count(distinct) 倾斜** | 改 `group by` 再 `count`：`select count(1) from (select uid from t group by uid) x` |
| **group by 倾斜** | `set hive.groupby.skewindata=true`（自动两阶段聚合）；或手动加盐 |
| **动态分区倾斜** | 分区字段值集中，先按分区排序再写；控制 `hive.optimize.sort.dynamic.partition=true` |

**通用两阶段聚合模板（加盐打散）**：

```sql
-- 阶段1：加随机前缀局部聚合
SELECT
    split_part(salt_key, '_', 1) AS real_key,
    SUM(cnt) AS total
FROM (
    SELECT
        concat(key, '_', cast(floor(rand() * 100) AS string)) AS salt_key,
        count(1) AS cnt
    FROM t
    GROUP BY concat(key, '_', cast(floor(rand() * 100) AS string))
) tmp
GROUP BY split_part(salt_key, '_', 1);
```

**Join 端加盐模板**：

```sql
-- 大表 key 加随机后缀 0~99，小表膨胀 100 倍
SELECT a.*, b.attr
FROM (
    SELECT *, concat(key, '_', cast(floor(rand()*100) as int)) AS join_key FROM big_table
) a
JOIN (
    SELECT *, concat(key, '_', n) AS join_key
    FROM small_table
    LATERAL VIEW explode(array(0,1,2,/*...*/99)) t AS n
) b ON a.join_key = b.join_key;
```

- [ ] 追问：怎么定位是哪个 key 倾斜？
  ```sql
  SELECT key, count(1) c FROM t GROUP BY key ORDER BY c DESC LIMIT 20;
  ```
  或看 YARN 上 Reduce 任务的输入记录数分布、Hive 的 Counter。
- [ ] 追问：`hive.groupby.skewindata=true` 的原理？（生成两个 MR Job：第一个 Map 输出随机分发到 Reduce 做局部聚合，第二个按真实 key 分发做最终聚合）

### 2.4 其他高频 SQL 参数

```properties
# 小文件合并
hive.merge.mapfiles=true
hive.merge.mapredfiles=true
hive.merge.smallfiles.avgsize=16000000
hive.merge.size.per.task=256000000

# 并行
hive.exec.parallel=true
hive.exec.parallel.thread.number=8

# 向量化
hive.vectorized.execution.enabled=true
hive.vectorized.execution.reduce.enabled=true

# MapJoin
hive.auto.convert.join=true
hive.mapjoin.smalltable.filesize=25000000

# 倾斜
hive.groupby.skewindata=true
hive.optimize.skewjoin=true
hive.skewjoin.key=100000

# JVM 重用（MR）
mapreduce.job.jvm.numtasks=10
```

## 三、SQL 手写题清单 ⭐⭐⭐（必须逐题手写通过）

> 建议在本地 Hive/Spark SQL 或 MySQL 8（窗口函数语法兼容）上真实跑一遍。
> 完成一题打一个勾。

### 3.1 基础聚合与去重

- [ ] 求每个部门薪资 Top 3 的员工（含并列）—— 考察 `dense_rank` vs `rank` vs `row_number`
- [ ] 统计每天新增用户数（首次出现日期 = 当天）
- [ ] 统计每个用户的连续登录天数最大值（**高频难题**，见 §3.2）
- [ ] 求每类商品的销量占比（`sum() over(partition by)`）
- [ ] 行转列 / 列转行（`explode` + `lateral view`、`collect_set` + `concat_ws`）

### 3.2 连续登录 N 天（⭐⭐⭐ 必考）

**核心思路**：日期减去行号 → 同一段连续区间的差值相同 → 按差值 group by。

```sql
SELECT user_id, COUNT(1) AS continuous_days, MIN(dt) AS start_dt, MAX(dt) AS end_dt
FROM (
    SELECT
        user_id,
        dt,
        date_sub(dt, row_number() OVER (PARTITION BY user_id ORDER BY dt)) AS grp
    FROM (
        SELECT DISTINCT user_id, dt FROM user_login   -- 必须先去重，一天多次登录算一天
    ) t
) t2
GROUP BY user_id, grp
HAVING COUNT(1) >= 3;
```

- [ ] 变体：允许中断 1 天的「准连续登录」怎么做？（先按 gap 分组，gap<=1 视为同段，再用累加标记法）
- [ ] 变体：求最长连续登录区间的起止日期。

### 3.3 留存率 ⭐⭐⭐

```sql
-- 次日留存率
SELECT
    a.dt,
    COUNT(DISTINCT a.uid)                                        AS new_users,
    COUNT(DISTINCT b.uid)                                        AS retained,
    COUNT(DISTINCT b.uid) / COUNT(DISTINCT a.uid)                AS retention_1d
FROM (SELECT dt, uid FROM dwd_user_first_login) a
LEFT JOIN (SELECT dt, uid FROM dwd_user_login) b
       ON a.uid = b.uid
      AND b.dt = date_add(a.dt, 1)
GROUP BY a.dt;
```

- [ ] 扩展：同时算 1/3/7/30 日留存（用 `datediff` + 条件聚合 `count(distinct case when ...)`）

### 3.4 同环比与累计

- [ ] 日环比、周同比（`lag()` 或自关联）
- [ ] 月度累计销售额（`sum(x) over(partition by month order by dt)`）
- [ ] 滑动 7 日平均（`rows between 6 preceding and current row`）

### 3.5 TopN 与分组极值

- [ ] 每个类目下销量前 3 的商品
- [ ] 每个用户最近一次下单记录（`row_number() desc = 1`，或关联子查询求 max 时间）
- [ ] 每个用户的第 2 次购买与第 1 次购买的间隔天数

### 3.6 行列转换

```sql
-- 行转列
SELECT user_id,
       max(CASE WHEN subject='math'    THEN score END) AS math,
       max(CASE WHEN subject='english' THEN score END) AS english
FROM score GROUP BY user_id;

-- 列转行（Hive）
SELECT user_id, subject, score
FROM score_wide
LATERAL VIEW explode(map('math', math, 'english', english)) t AS subject, score;
```

### 3.7 其他高频

- [ ] 求中位数（`percentile` / `percentile_approx`）
- [ ] 求每个用户购买过的商品品类数（`count(distinct)`）
- [ ] 漏斗分析：曝光 → 点击 → 加购 → 下单 各步转化率（多表关联 + 条件聚合）
- [ ] 求 UV/PV，及去重 UV 的近似算法（HyperLogLog / `bitmap`）
- [ ] 找出「A 买了但 B 没买」的商品（`left join ... where b.x is null` / `not exists` / `except`）
- [ ] 拉链表查询某天的历史快照（`where start_date <= 'x' and end_date > 'x'`）

## 四、窗口函数速查 ⭐⭐⭐

```sql
func() OVER (
    [PARTITION BY col]
    [ORDER BY col [ASC|DESC]]
    [ROWS/RANGE BETWEEN x PRECEDING AND y FOLLOWING]
)
```

| 函数 | 说明 | 并列处理 |
| --- | --- | --- |
| `row_number()` | 连续唯一序号 | 不并列 |
| `rank()` | 跳跃排名（1,2,2,4） | 并列后跳号 |
| `dense_rank()` | 密集排名（1,2,2,3） | 并列不跳号 |
| `ntile(n)` | 均分成 n 组 | — |
| `lag(col,n,default)` | 前 n 行 | — |
| `lead(col,n,default)` | 后 n 行 | — |
| `first_value/last_value` | 窗口内首/末值 | `last_value` 默认窗口到当前行，需注意 |
| `sum/avg/count/max/min over` | 聚合窗口 | 有 order by 时为累计 |

- [ ] 坑点：`last_value()` 默认窗口是 `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`，要取分组最后一个值需显式写 `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
- [ ] 坑点：带 `ORDER BY` 的 `sum() over` 是累计和，不带才是分组总和

## 五、UDF / UDAF / UDTF ⭐⭐

| 类型 | 输入输出 | 示例 |
| --- | --- | --- |
| UDF | 一行进一行出 | 自定义脱敏、加解密、IP 转地域 |
| UDAF | 多行进一行出 | 自定义去重计数、加权平均 |
| UDTF | 一行进多行出 | `explode`、自定义 JSON 展开 |

- [ ] 开发步骤：继承 `org.apache.hadoop.hive.ql.udf.generic.GenericUDF` → 实现 `initialize`/`evaluate` → 打 jar → `ADD JAR` → `CREATE TEMPORARY FUNCTION`
- [ ] `GenericUDF` vs `UDF`：前者支持复杂类型、延迟解析 ObjectInspector，性能好，是推荐方式
- [ ] 追问：什么时候该写 UDF 而不是用内置函数？（业务规则复杂、需复用、涉及外部词典/加解密；能用内置绝不用 UDF，因为 UDF 无法被优化器下推且增加维护成本）

## 六、Hive 事务与数据更新 ⭐

- [ ] Hive ACID 表：需要 ORC + 分桶 + `hive.support.concurrency=true`，底层靠 delta 目录 + compaction
- [ ] 局限：性能差、只支持 ORC、跨引擎兼容差 → 生产一般用 **拉链表** 或 **湖仓格式（Hudi/Iceberg）** 替代
- [ ] 拉链表（Zipper Table）设计：
  ```
  id | name | ... | start_date | end_date
  ```
  - 新增/变更：插入新记录 start_date=今天，end_date='9999-12-31'
  - 原记录：end_date 改为今天
  - 查历史快照：`where start_date <= 'D' and end_date > 'D'`
- [ ] 追问：拉链表怎么做回滚/重跑？（保留每日全量快照分区，或用 `end_date` 边界重算；重跑当天需先删除当天 start_date 的记录并恢复被关链的记录）

## 七、Hive on Spark / Tez 与迁移 ⭐

- [ ] Tez：DAG 执行，避免 MR 每步落盘，Stage 间可 pipeline；`hive.execution.engine=tez`
- [ ] Hive on Spark：复用 Spark 引擎，与 Spark SQL 区别在于 Hive 保留自己的解析优化器
- [ ] 迁移趋势：Hive → Spark SQL / Trino(Presto) / Doris，Hive 逐渐退化为「元数据 + 存储格式标准」

## 八、本章高频面试题速查

1. 内部表和外部表的区别，生产怎么选（⭐⭐⭐）
2. Hive SQL 优化你做过哪些（⭐⭐⭐ 必考，要能讲 8 条以上并举例）
3. 数据倾斜怎么排查、怎么解决（⭐⭐⭐ 必考，至少准备 3 种方案 + 真实案例）
4. MapJoin 原理和限制（⭐⭐⭐）
5. 手写：连续登录 N 天（⭐⭐⭐ 笔试高频）
6. 手写：留存率 / TopN / 行转列（⭐⭐⭐）
7. `count(distinct)` 为什么慢，怎么优化（⭐⭐⭐）
8. 分区和分桶的区别（⭐⭐）
9. order by / sort by / distribute by / cluster by 区别（⭐⭐⭐）
10. 拉链表怎么设计，怎么查历史快照（⭐⭐）

### 补充：四种 by 的区别（高频）

| 语法 | 作用 | 全局有序 |
| --- | --- | --- |
| `order by` | 单 Reduce 全局排序 | 是（数据量大时危险） |
| `sort by` | Reduce 内部排序 | 否 |
| `distribute by` | 按字段 hash 分发到 Reduce | 否 |
| `cluster by` | = distribute by + sort by（同字段，只能升序） | 否 |

- [ ] 追问：生产为什么禁用 `order by`？（单 Reduce 瓶颈，容易 OOM/超时；需配合 `limit` 使用，或 `set hive.mapred.mode=strict`）
