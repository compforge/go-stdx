# 操作时间线

## 概念与边界

Timeline 是按调用方提供的 ID 聚合的阶段事实集合。不同组件和进程可以独立记录阶段，
通过共享 Store 汇总。ID 的生成、业务含义和唯一性范围由应用决定。

Stage 是一段工作或等待，保留身份、名称、父阶段、实际区间、状态和可选执行者 Actor。
现场执行用 Begin / End，已知真实起止时间的外部事实用 Record 补录。StageHandle 表示
当前本地执行流；并行执行流分别创建阶段。阶段名是本地查找便利，持久化身份是 StageID。

操作级 Start 和 Finish 是可选事实，分别提供开始边界与最终结果。它们可以独立出现，
阶段可以先于操作边界到达。没有 Finish 时操作结果为 unknown；没有 Start 时操作开始时间
未知。阶段是否全部结束、本地缓存是否过期，都不决定业务结果。

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
| Read(ctx, id) | 提交该 ID 的本地记录，读取当前快照 |
| Start(id, operation, ...Attribute) | 可选，记录操作开始时间、名称和属性 |
| Finish(id, result) | 可选，记录操作结束时间和结果 |

五个写入接口只编码和接收本地事实，后台 worker 负责持久化。Read 是显式的采集检查点，
返回快照和记录错误；业务失败保存在 Stage 或 Snapshot 的结果中。无数据时返回 ErrNotFound。

Begin 返回的句柄可以忽略，End 按名称查找唯一运行中的阶段。同名阶段可以顺序重复；
并行同名阶段返回 ErrAmbiguousStage，调用方使用各自的句柄精确结束。StageHandle.End 和
SetAttributes 即时返回记录错误。只有结束事实被成功接收后，该阶段才退出名称索引。

属性通过 Begin 的 WithAttributes、End 的 WithEndAttributes、Start 的 Attribute 参数或
完整 Stage 数据传入。需要执行中更新时可使用 StageHandle.SetAttributes。值在记录调用
返回前完成 JSON 编码，后续修改调用方对象不会改变已接收事实。

Manager.BeginContext 继承 context 中属于同一 timeline 的 StageRef，保留取消与截止时间。
WithParent 显式指定关联；父阶段可以缺失或迟到，SDK 不通过远端查询验证它是否存在。
跨进程只传播纯数据 StageRef。请求取消不自动结束实际工作。

Start 的相同本地调用保留第一次接受的时间；名称或初始属性不同则冲突。Finish 的第一次
有效调用固定结果，后续相同结果幂等，已接受结果的变更返回 ErrConflict。Finish 之后仍可
记录阶段，也可补入尚缺失的 Start。操作边界不是本地对象的开关。

## TTL 与 LRU

Manager 使用 `jellydator/ttlcache/v3` 管理本地 timeline，配置项为 TTL 和 MaxTimelines。
TTL 从本地条目创建时开始计算；访问更新 LRU 顺序，但不延长 TTL。达到最大数量时，新建
条目淘汰最近最少使用的 timeline。正常使用不要求显式释放。

过期或淘汰会使本地阶段查找和记录句柄失效。旧句柄的新写入返回 ErrExpired；按 ID 的新
写入可以创建新的本地上下文，继续汇入同一个持久化文档。Read 不创建本地条目，也不续期。
缺少 End 的已存阶段仍是 running，不会因为缓存清理而被改成成功或失败。

缓存条目与提交队列分属两个生命周期。待提交 writer 只保留必要的身份、采集错误和批次，
不持有被淘汰的阶段索引。缓存淘汰不会删除持久化记录，也不会丢弃已经接收的写入。
缓存清理独立于存储 IO 运行，数据库阻塞不能阻止到期回收。

MaxActiveStages 限制缓存内可按名称寻址的运行中阶段；MaxPendingHandles 与
MaxPendingUpdates 独立限制待提交队列。TTL/LRU 管理 timeline 数量，队列背压通过
ErrBufferFull 即时返回。属性字节大小由应用约束。

## 记录接收与失败恢复

核心 Recorder 维护计时、写入修订号和事实，通过 `timeline.Writer` 接收接口提交数据。
Manager 实现有界后台 writer；独立 `timeline.New` 使用显式 Flush 的本地缓冲实现。
核心包不依赖 Manager、缓存库或进程默认实例。

终态决定与队列接收分开：End / Finish 第一次有效调用固定结果及其时间，但只有提交队列
接收后才封存本地状态。缓冲满时调用方可以重试相同入口；重试保留原始结果，不会让存储
永久停在 running。未被接收的写入不会被冒充为已经持久化。

后台提交先固定本轮批次，IO 在记录锁之外执行。结果不确定时保留相同内容和修订号重试；
提交期间的新变化留在后续批次。通知只用于唤醒，合并通知不会丢弃队列中的唯一事实。

暂时错误与不确定提交按 writer 退避重试，并受单次 IO 超时约束。ErrConflict、
ErrInvalidStage、ErrInvalidAttribute 表示确定的永久拒绝：退出该批次的重试，累计
RejectedUpdates，并保留采集错误。其它 writer 可以继续提交；永久冲突不会无限占用队列。
后台失败通过 OnError 报告，默认使用不包含记录内容的 slog 日志。

采集错误归属于本地 timeline 的缓存生命周期。参与方已经没有待提交数据、或者阶段已经
结束，都不会让该错误从 Read 中消失。缓存淘汰后，通过全局计数与错误回调观察历史损失。
DroppedUpdates 记录容量拒绝次数；拒绝后重试成功不等于曾经接受的数据发生丢失。

Read 返回的 Collection.LocalFlushed 表示本次本地提交检查点成功且没有已知采集损失；
Collection.StoreRead 表示此次存储读取成功。两者都不宣称远端进程已经上报完整。
严格跨进程交接仍需要执行方 FlushID 成功后发布业务完成，再由读取方采集。

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
现场阶段额外保留单调时钟测得的 elapsed_ns。跨主机时间只用于展示，不推断精确因果顺序。
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
循环并排空已接收数据。失败排空可以重试，Store 连接最后由应用关闭。内存缓冲不提供
进程崩溃后的恢复保证。

`timeline/gospan` 提供有封存边界的进程内实现，Finish 等待本地阶段结束后关闭 writer，
之后可以补录完整区间。Registry 仅用于这种显式句柄的本地查找，不参与 Manager 的缓存。
Snapshot 的 Summary、RunningStages、LatestFailedStage 是纯数据查询，不改变采集状态。
