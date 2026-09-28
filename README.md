# pq-job · 大数据开发求职复习计划

> 目标岗位：大数据开发（离线数仓 + 实时计算方向）
> 经验定位：3-5 年中高级
> 复习周期：8 周（2 个月）深度复习，每天 1-2 小时

## 一、这个仓库是什么

一份可直接执行的求职复习作战手册。所有文档以「知识点清单 + 自测问题 + 复习排期」的形式组织，
边看边打勾，看完能讲出来即算通过。

## 二、目录导航

| 文档 | 内容 | 优先级 |
| --- | --- | --- |
| [00-复习总览与8周排期](docs/00-复习总览与8周排期.md) | 整体路线图、每周主题、每日节奏、里程碑验收 | P0 |
| [01-大数据基础与Hadoop生态](docs/01-大数据基础与Hadoop生态.md) | HDFS / YARN / MapReduce / ZooKeeper | P1 |
| [02-Hive与SQL离线开发](docs/02-Hive与SQL离线开发.md) | Hive 架构、SQL 优化、数据倾斜、UDF | P0 |
| [03-数仓建模与数据治理](docs/03-数仓建模与数据治理.md) | 维度建模、分层规范、指标体系、数据质量 | P0 |
| [04-Spark核心与调优](docs/04-Spark核心与调优.md) | RDD/DataFrame、Shuffle、内存模型、AQE、调优 | P0 |
| [05-Flink与实时计算](docs/05-Flink与实时计算.md) | 状态、时间语义、Checkpoint、双流 Join、反压 | P0 |
| [06-Kafka与消息中间件](docs/06-Kafka与消息中间件.md) | 分区副本、Exactly-Once、CDC、积压处理 | P0 |
| [07-OLAP引擎与湖仓存储](docs/07-OLAP引擎与湖仓存储.md) | Doris/StarRocks/ClickHouse、Iceberg/Hudi/Paimon | P1 |
| [08-数据平台调度与工程化](docs/08-数据平台调度与工程化.md) | DolphinScheduler/Airflow、数据血缘、成本治理 | P1 |
| [09-Java并发与JVM基础](docs/09-Java并发与JVM基础.md) | 集合、并发、JVM 调优、GC | P1 |
| [10-分布式理论与中间件](docs/10-分布式理论与中间件.md) | CAP、一致性、Redis、MySQL、分库分表 | P1 |
| [11-项目复盘与自测题库](docs/11-项目复盘与自测题库.md) | 项目话术模板、追问应对、分模块自测题 | P0 |

## 三、快速开始

1. 打开 [00-复习总览与8周排期](docs/00-复习总览与8周排期.md)，确认起始日期。
2. 复制 [templates/daily-checkin.md](templates/daily-checkin.md) 到 `progress/` 下，按 `YYYY-MM-DD.md` 命名，每天填写。
3. 每周日复制 [templates/weekly-review.md](templates/weekly-review.md) 到 `progress/week-XX-review.md`，做周复盘。
4. 进度总表见 [progress/README.md](progress/README.md)。

## 四、复习原则

- **输出优先**：一个知识点只有能对着白板讲清楚、并回答 2 层追问，才算掌握。
- **八股服务于项目**：所有原理最终要能落到「我在项目里怎么用的、遇到什么问题、怎么解决的」。
- **不求全覆盖**：清单中标 ⭐⭐⭐ 的是必须拿下的高频考点，⭐ 属于加分项，时间不够可以跳过。
- **每周一次真题模拟**：按 [11-项目复盘与自测题库](docs/11-项目复盘与自测题库.md) 计时自测，录音回放。

## 五、标记说明

- ⭐⭐⭐ 高频必考，必须能脱口而出
- ⭐⭐ 常考，需要理解原理并能举例
- ⭐ 加分项，有余力再看
- `[ ]` 未复习 / `[x]` 已复习且能自述 / `[!]` 复习过但讲不清，需要二刷
