# AGENTS.md — go-stdx

## 项目定位与边界

Go stdlib 扩展库，长期对标 Java 里 Guava 的位置——项目手写 helper 之前先来这里找。收录的是**基本、常见、三五行就能封装好的数据操作**，免得各服务各写一遍。与现有生态分工：samber/lo 占泛型集合、gods 占数据结构、lancet 是 kitchen-sink；go-stdx 拼的是纪律而非广度。**名字即收录纪律**：stdlib 没有的才进（手搓 max、复刻 slices.Clone、包一层 strconv 都不属于这里）。**不追求零依赖**——底层数据的生成能力（uuid、hash 等）直接依赖成熟基础库、不自己实现，本库只封装调用方要传来传去的那个形态；但也不滥引，只引成熟的基础件。stdlib 补齐等价物后这里的条目废弃删除。符合定位即可收录，不设重复次数门槛。

## 代码地图

子包镜像 stdlib 命名（`slicesx` / `stringsx` / `osx` / `ptrx` / `filepathx` / `tarx` / `shellx` / `randx` / `uuid`），调用点读起来像它扩展的那个标准库。一包一职责，包内保持小。

`timeline` 提供 Stage 纯数据、StageHandle 计时接口与 Record 完整补录，
Manager 管理本地句柄的后台提交与排空，Store 负责持久化和读取。应用安装默认 Manager 后，
业务可通过 `timeline.Start(ctx, id, operation)` / `For(id)` / `Read(ctx, id)` 使用全局入口；实例生命周期仍由应用管理。不同进程通过同一业务 ID
独立记录，由 Store 汇总。`timeline/sqlstore` 复用调用方 SQL 连接池。
`timeline/gospan` 提供进程内记录实现，Registry 仅索引本地活跃实例。
ID 的生成、业务含义和完成决策归调用方，公共接口不暴露
backend 类型。设计与完成边界见 `docs/timeline.md`。

## 关键约定

1. 收录评审问三件事：stdlib 真没有吗（含最新版本）？是真通用还是某项目的业务形状？是薄封装（三五行、不重造底层能力）吗？
2. 语义约定：Uniq 系保留**首次出现**且保序；所有函数 nil-in-nil-out。
3. 消费方（ccr / hostel / …）内部不许再就地手写本库已有的操作——消费仓的 AGENTS.md 应有对应约定。

## References

- `docs/timeline.md`：操作时间线、阶段生命周期与快照一致性

- 首个消费方与孵化史：[case-code-review](https://github.com/qiankunli/case-code-review) `pkg/stdx`（已迁出）
- 第二个消费方：[hostel](https://github.com/qiankunli/hostel)——`osx`/`randx`/`shellx`/`tarx`/`filepathx` 均由其内部手写实现迁入
