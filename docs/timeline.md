# 操作时间线

## 概念与边界

Timeline 是按调用方提供的 ID 聚合的阶段事实集合。不同组件和进程可以独立记录阶段，
通过共享 Store 汇总。ID 的生成、业务含义和唯一性范围由应用决定。

Stage 是一段工作或等待，保留身份、名称、父阶段、实际区间、状态和可选执行者 Actor。
现场执行用 Begin / End，已知真实起止时间的外部事实用 Record 补录。StageHandle 表示
一个阶段的逻辑身份；并行执行流分别创建阶段。阶段名用于查找当前文档中唯一运行的阶段，
持久化身份是 StageID。

操作级 Start 和 Finish 是可选事实，分别提供开始边界与最终结果。它们可以独立出现，
阶段可以先于操作边界到达。没有 Finish 时操作结果为 unknown；没有 Start 时操作开始时间
未知。阶段是否全部结束、本地缓存是否淘汰，都不决定业务结果。

Snapshot 是一次读取的独立视图，Document 是持久化的最新事实。前者带有捕获时间和采集状态，
后者保存合并修订号。业务可以直接 JSON 序列化快照，无需再定义 DTO。

## 使用流程

应用启动时创建 `manager.Manager` 并通过 `manager.SetDefault` 安装默认实例。
业务主路径只有六个接口；需要隔离实例时使用显式 Manager 的同名方法。

| 接口 | 语义 |
| --- | --- |
| Begin(id, name, ...StageOption) | 开始阶段，按需创建本地 timeline |
| End(id, name, result, ...EndOption) | 结束指定阶段 |
| Record(id, Stage) | 补录完整区间，按需创建本地 timeline |
| Read(ctx, id) | 读取缓存快照，miss 时从 Store 加载 |
| Start(id, operation, ...Attribute) | 可选，记录操作开始时间、名称和属性 |
| Finish(id, result) | 可选，记录操作结束时间和结果 |

Manager 同时持有缓存与 Store，六个入口共用一套加载、修改和保存策略。命中缓存时直接
操作内存文档；miss 时查询 Store，写操作在未找到文档时创建，Read 返回 ErrNotFound。
查询错误直接返回，不把故障当作不存在。后台 worker 合并并保存事实；Read 返回当前缓存
视图，需要等待持久化时显式调用 FlushID。业务失败保存在 Stage 或 Snapshot 的结果中。

Begin 返回的句柄可以忽略，End 按名称查找唯一运行中的阶段。同名阶段可以顺序重复；
并行同名阶段返回 ErrAmbiguousStage，调用方使用各自的句柄精确结束。StageHandle.End 和
SetAttributes 即时返回记录错误。只有结束事实被成功接收后，该阶段才退出运行中阶段集合。

属性通过 Begin 的 WithAttributes、End 的 WithEndAttributes、Start 的 Attribute 参数或
完整 Stage 数据传入。需要执行中更新时可使用 StageHandle.SetAttributes。值在记录调用
返回前完成 JSON 编码，后续修改调用方对象不会改变已接收事实。

Manager.BeginContext 继承 context 中属于同一 timeline 的 StageRef，保留取消与截止时间。
WithParent 显式指定关联；父阶段可以缺失或迟到，SDK 不通过远端查询验证它是否存在。
跨进程只传播纯数据 StageRef。请求取消不自动结束实际工作。

Start 的相同调用保留文档中已经接受的时间，包括缓存 miss 后恢复的开始时间；名称或初始
属性不同则冲突。Finish 第一次被接受后固定结果，后续相同结果幂等，结果变更返回 ErrConflict。Finish 之后仍可
记录阶段，也可补入尚缺失的 Start。操作边界不是本地对象的开关。

Start 与 Finish 分别保留各次调用时记录的时间。先 Finish 后 Start 可以产生倒置的操作
边界及负的 Snapshot.Duration；这是允许的观测结果。SDK 原样保留时间与差值，不交换
边界或将负值截为零，也不据此推算真实业务执行区间。

## 加载缓存与容量

缓存用于降低存储访问频率。Manager 直接复用 `jellydator/ttlcache/v3` 的条目管理、LRU
和淘汰回调；Store 查询、未找到时的创建、异步保存均由 Manager 统一决定。同一 ID 的并发
miss 合并为一次加载，每次存储 IO 有超时，取消一个读取者不影响其它等待者。

访问更新 LRU 顺序，达到 MaxTimelines 时淘汰最近最少使用的条目。淘汰只释放内存副本，Store 中的事实仍可恢复。Manager 返回的
阶段句柄只持有 ID，后续 End、SetAttributes 可重新加载阶段；按名称 End 使用恢复后的文档。
缓存淘汰不决定业务结果，缺少结束事实的阶段仍保持 running。

后台保存成功后，干净副本可以继续服务读取，直到 LRU 淘汰；需要立即释放时可调用
Manager.Evict。需要先落盘再移除时，先等待 FlushID 成功。淘汰有待保存事实时会安排最终
保存，失败则报告并释放缓冲；此时再次加载只能得到 Store 中已有的事实。

缓存采用尽力而为语义：温热缓存可能落后于其它进程，进程退出或淘汰保存失败可能丢失尚未
落盘的事实。它适合观测与诊断。要求每次读取最新状态或每次写入持久化的应用应直接使用 Store。

## 接收与持久化

核心 Recorder 维护单个 writer 的修订号和计时，通过 `timeline.Writer` 提交事实。独立
Recorder 使用显式 Flush 的本地缓冲；Manager 的六个入口从缓存文档恢复录制状态，并在
同一条目锁内构造和接收变化，因此拒绝的写入不会推进文档状态或让阶段提前结束。

后台 IO 在条目锁之外执行，记录操作可以继续修改内存。提交时固定批次，不确定的失败
保留相同内容重试，后续变化进入下一批。仍在缓存中的条目按后台周期重试；淘汰后的最终
保存失败便释放待保存事实，避免存储不可用时无限占用内存。永久冲突退出对应批次的重试。

MaxPendingTimelines 限制待保存条目数，包含被淘汰后等待最终保存的条目；MaxPendingUpdates
限制每个条目的待保存更新数，同一阶段尚未提交的变化会合并。容量不足返回 ErrBufferFull，
调用方可重试。MaxTimelines 限制缓存条目数；单条文档的阶段数和属性字节大小由应用约束。

输入校验和本地已知冲突直接返回，不修改缓存。后台保存失败通过 OnError 报告，默认使用
不含事实内容的 slog 日志，同一持续故障只报告一次，淘汰最终失败会再次报告。
DroppedUpdates 累计容量拒绝和最终保存丢弃，RejectedUpdates 累计存储永久拒绝。

Read 返回的 Collection.LocalFlushed 表示当前条目没有待保存事实或已知保存损失；
Collection.StoreRead 表示本次读取成功加载了 Store。命中缓存时 StoreRead 为 false。
两者都不宣称分布式数据已经完整。FlushID 是当前实例的持久化检查点；跨进程交接时可在
发布完成之前等待 FlushID，再让读取者淘汰旧缓存或直接查询 Store。

## 存储与合并

`timeline/store` 统一拥有 Store 契约、Update、Document、文档编码/合并/快照投影和 MemoryStore，
SQL 实现位于 `timeline/store/sqlstore`。录制与存储共用公开的 `timeline/model` 值模型，业务也可继续使用
`timeline.Stage`、`timeline.Snapshot`；存储无需依赖 Recorder 或 Manager，避免循环引用。

`store.Store.Merge` 将 Update 原子合并进同一 ID 的 Document。Document 保存每个阶段的最新状态，
不保存事件日志。MemoryStore 使用锁，SQL Store 使用行版本 CAS；公共 MergeDocument
提供相同的纯数据合并规则。

阶段的开始时间、名称、父阶段和 Actor 不可修改。每个现场 writer 独立递增 StageUpdate
修订号：低版本忽略，同版本同内容幂等，同版本不同内容冲突，终态不可重开。Record 的
完整区间不要求调用方管理修订号；稳定 StageID 用于重复采集去重，冲突区间整体拒绝。

操作开始和结束边界分别合并。Finish-only 写入不会清除已经存储的 Start，迟到 Start 也
不会清除既有终态。开始边界携带的属性仍按其 writer 的修订号更新；缺失边界不等于覆盖。
多个进程提交不相同的同一边界时返回 ErrConflict，SDK 不提供调度、租约或自动接管。

业务重试应使用新的阶段身份；存储重试使用相同身份和内容。显式来源时间原样保留，
独立 Recorder 的现场阶段保留单调时钟测得的 elapsed_ns。Manager 从文档恢复阶段，
耗时通过已记录的起止时间计算；持久化不能恢复单调时钟。跨主机时间只用于展示，不推断精确因果顺序。
阶段按开始时间和 ID 排列，保留重叠，不能将阶段耗时相加作为操作总耗时。

Actor 可选，表示阶段执行者，其 ID 和 Name 可独立省略。Code 是独立于 Status 和 Error
的调用方约定值，SDK 不解释其业务含义。

Attributes 使用 map[string]json.RawMessage，保留大整数精度。JSON 输出 attributes，读取
也接受 fields，非 null 的 attributes 优先。Document 与 Snapshot 使用文档内 actors
字典，阶段的 actor_ref 从 1 开始；单独 Stage JSON 仍保存完整 Actor。引用只属于当前
payload，解码和合并后重新编码，多个 writer 不会混用字典索引。

SQL Store 复用调用方的 database/sql 连接池，一条 timeline 一行。示例 MySQL 表：

```sql
CREATE TABLE timelines (
    id VARCHAR(191) COLLATE utf8mb4_bin NOT NULL PRIMARY KEY,
    payload JSON NOT NULL,
    version BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    KEY timeline_retention (updated_at, id)
) DEFAULT CHARSET=utf8mb4;
```

SQL 使用问号参数与普通列 CAS，不使用方言专属 JSON 更新。重复投递不推进版本或更新时间。
自动化测试覆盖 SQLite 多连接和独立进程，不代替 MySQL/DM 实库验收。连接池、migration、
保留期由应用管理；清理后迟到写入可能重建文档，SDK 不提供永久删除墓碑。

## 启停与其它实现

安装默认 Manager 后启动业务生产者。SetDefault 只切换引用，已有句柄仍属于原实例。
服务退出时先停止生产者，再用独立限时 context 调用 Shutdown；它拒绝新写入、停止后台
循环，尝试保存剩余数据。最终保存失败会返回错误并释放缓冲，Store 连接最后由应用关闭。
Shutdown 后仍可通过 Read 直接查询 Store。

`timeline/gospan` 提供有封存边界的进程内实现，Finish 等待本地阶段结束后关闭 writer，
之后可以补录完整区间。Registry 仅用于这种显式句柄的本地查找，不参与 Manager 的缓存。
Snapshot 的 Summary、RunningStages、LatestFailedStage 是纯数据查询，不改变采集状态。
