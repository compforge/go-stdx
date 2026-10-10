# AGENTS.md — go-stdx

## 项目定位与边界

Go stdlib 扩展库，长期对标 Java 里 Guava 的位置——项目手写 helper 之前先来这里找。收录的是**基本、常见、三五行就能封装好的数据操作**，免得各服务各写一遍。与现有生态分工：samber/lo 占泛型集合、gods 占数据结构、lancet 是 kitchen-sink；go-stdx 拼的是纪律而非广度。**名字即收录纪律**：stdlib 没有的才进（手搓 max、复刻 slices.Clone、包一层 strconv 都不属于这里）。**不追求零依赖**——底层数据的生成能力（uuid、hash 等）直接依赖成熟基础库、不自己实现，本库只封装调用方要传来传去的那个形态；但也不滥引，只引成熟的基础件。stdlib 补齐等价物后这里的条目废弃删除。符合定位即可收录，不设重复次数门槛。

## 代码地图

子包镜像 stdlib 命名（`slicesx` / `stringsx` / `osx` / `ptrx` / `filepathx` / `tarx` / `shellx` / `randx` / `uuid`），调用点读起来像它扩展的那个标准库。一包一职责，包内保持小。

`timeline` 按 ID 聚合阶段事实，操作级 Start / Finish 可独立省略。

根包收口公开入口与类型别名；`manager` 拥有录制和管理生命周期，`cache` 统一缓存与保存，
`store` / `model` 承载持久化契约和值模型。下层包不反向依赖根包。
ID、业务结果与快照导出归调用方，缓存采用尽力而为语义；使用默认 NoopStore 时，完整事实仅保存在缓存。
目录树、生命周期与一致性契约见 [操作时间线](docs/timeline.md)。

## 关键约定

1. 收录评审问三件事：stdlib 真没有吗（含最新版本）？是真通用还是某项目的业务形状？是薄封装（三五行、不重造底层能力）吗？
2. 语义约定：Uniq 系保留**首次出现**且保序；所有函数 nil-in-nil-out。
3. 消费方（ccr / hostel / …）内部不许再就地手写本库已有的操作——消费仓的 AGENTS.md 应有对应约定。
4. 根目录 `VERSION` 维护版本号（`vX.Y.Z`）。提交改动时同步按语义化版本 bump `VERSION`；发布的 release tag 必须与文件中的版本一致。

## References

- `docs/timeline.md`：操作时间线、阶段生命周期与快照一致性

- 首个消费方与孵化史：[case-code-review](https://github.com/qiankunli/case-code-review) `pkg/stdx`（已迁出）
- 第二个消费方：[hostel](https://github.com/qiankunli/hostel)——`osx`/`randx`/`shellx`/`tarx`/`filepathx` 均由其内部手写实现迁入
