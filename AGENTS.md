# AGENTS.md — go-stdx

## 项目定位与边界

Go stdlib 扩展库，长期对标 Java 里 Guava 的位置——项目手写 helper 之前先来这里找。收录的是**基本、常见、三五行就能封装好的数据操作**，免得各服务各写一遍。与现有生态分工：samber/lo 占泛型集合、gods 占数据结构、lancet 是 kitchen-sink；go-stdx 拼的是纪律而非广度。**名字即收录纪律**：stdlib 没有的才进（手搓 max、复刻 slices.Clone、包一层 strconv 都不属于这里）。**不追求零依赖**——底层数据的生成能力（uuid、hash 等）直接依赖成熟基础库、不自己实现，本库只封装调用方要传来传去的那个形态；但也不滥引，只引成熟的基础件。stdlib 补齐等价物后这里的条目废弃删除。符合定位即可收录，不设重复次数门槛。

## 代码地图

子包镜像 stdlib 命名（`slicesx` / `stringsx` / `osx` / `ptrx` / `filepathx` / `tarx` / `shellx` / `randx` / `uuid`），调用点读起来像它扩展的那个标准库。一包一职责，包内保持小。

`timeline` 按 ID 聚合阶段事实，操作级 Start / Finish 可独立省略。

```text
timeline/
├── global.go、manager*.go     # 根包六个 ID 入口与 Manager 编排
├── handle.go                 # 非主要路径的对象式 API
├── recorder.go、writer.go     # 内部事实构造与缓存写入
├── internal/cache/           # 所有原生入口共用的加载、合并与保存实现
├── model/                    # 录制与存储共享的值类型、校验及快照编码
├── manager/                  # 原包路径转发，共用根包默认 Manager
├── store/                    # Store 契约、文档编码/合并/投影与 NoopStore、MemoryStore
│   └── sqlstore/             # 复用应用 SQL 连接池的持久化实现
└── gospan/                   # 有封存边界的进程内实现
```

依赖方向为 `manager → timeline → internal/cache → store → model`，`sqlstore → store`。
录制器是内部事实构造实现，对象式 API 集中在 Handle；共享值类型通过 `timeline.Stage`、`timeline.Snapshot`
等别名对外提供，存储包不依赖录制或管理生命周期。
统一缓存实现持有 `jellydator/ttlcache/v3` 和 Store；最大数量与 LRU 控制缓存驻留，保存周期独立配置。
miss 从 Store 恢复，写入缺失 ID 才创建；Read 不强制提交 Store，阻塞 Flush 是显式检查点。
默认 NoopStore 不保存文档，纯内存模式只在缓存中保留完整事实；导出格式和时机归消费方。
保存采用尽力而为策略：淘汰时尝试最终保存，失败报告后释放缓冲；Finish 只记录业务结果。
ID 的业务含义、执行调度与数据库保留期归应用。详细契约见 `docs/timeline.md`。

## 关键约定

1. 收录评审问三件事：stdlib 真没有吗（含最新版本）？是真通用还是某项目的业务形状？是薄封装（三五行、不重造底层能力）吗？
2. 语义约定：Uniq 系保留**首次出现**且保序；所有函数 nil-in-nil-out。
3. 消费方（ccr / hostel / …）内部不许再就地手写本库已有的操作——消费仓的 AGENTS.md 应有对应约定。
4. 根目录 `VERSION` 维护版本号（`vX.Y.Z`）。提交改动时同步按语义化版本 bump `VERSION`；发布的 release tag 必须与文件中的版本一致。

## References

- `docs/timeline.md`：操作时间线、阶段生命周期与快照一致性

- 首个消费方与孵化史：[case-code-review](https://github.com/qiankunli/case-code-review) `pkg/stdx`（已迁出）
- 第二个消费方：[hostel](https://github.com/qiankunli/hostel)——`osx`/`randx`/`shellx`/`tarx`/`filepathx` 均由其内部手写实现迁入
