# 操作时间线

Timeline 将并行运行的多个调用方实例围绕同一操作产生的阶段记录汇总为统一时间线，用于观察整体进度、执行结果和耗时，并追溯问题。

## 概念与边界

Timeline 按应用提供的 opid 汇总阶段记录。多个实例使用数据库作为共享渠道，周期性保存本地
变化、加载其它实例的变化。它用于观测和诊断，尽力让各实例看到相近视图，不保证强一致或
最终完整。业务 ID、操作结果、保留期和快照导出由应用决定。

StageID 表示逻辑阶段，Actor 表示执行者。一个操作内以 `(StageID, ActorKey)` 唯一定位
阶段记录：Actor.ID 非空时使用 ID，否则使用 Name；两者至少一个非空。ID 和 Name 属于
不同命名空间，有 ID 时 Name 只影响展示。只提供 Name 后再补入 ID 会形成不同身份。

不同 Actor 可以并发操作同一个 StageID，各自的结果分别保留。同一复合键保存当前状态，
按存储实际接收顺序覆盖，不保留每次调用历史。不同进程的 revision 不参与全局新旧裁决。
完全相同的状态重复提交不产生变化；重试旧状态仍可能覆盖竞争 writer 的更新。

现场执行用 Begin / End，外部完整区间用 Record。StageHandle 同时持有 StageID 和 Actor
身份；ParentID 关联逻辑父阶段，不指定某个 Actor 的具体贡献。父阶段缺失或迟到不阻止
子阶段记录。共享逻辑阶段的多个 Actor 可以有不同区间、属性与结果。

父阶段只由 `WithParent` 或补录 Stage 的 `ParentID` 显式指定；空值表示没有父阶段，
不会默认关联操作根阶段。带 context 的辅助方法保留取消与截止时间并绑定新阶段身份，
不会读取 context 推断父阶段。父阶段可以缺失或迟到，SDK 不通过远端查询验证它是否存在。
跨进程只传播纯数据 StageRef。请求取消不自动结束实际工作。

Start / Finish 分别提供可选的操作开始、结束边界。阶段可先于操作边界到达，Finish 后仍
可记录阶段。不同来源分别提交边界，缺失边界不清除已有事实；重复相同调用保留已知时间，
不同调用替换对应边界。没有 Finish 时操作结果为 unknown。阶段全部结束、缓存淘汰和
后台同步成功都不决定业务终态。

Snapshot 是一次读取的独立视图，Document 是持久化的当前记录集合。Snapshot 的阶段列表
允许重复 StageID，通过 Actor 区分；它可直接 JSON 序列化。Summary 在重复 StageID 时
显示 Actor，RunningStages、LatestFailedStage 只查询已知记录，不判定业务因果。

## 使用流程

应用启动时创建 Manager，并通过 SetDefault 安装默认实例。Config.Actor 可提供默认执行者，
Begin 的 WithStageActor 可以覆盖它。阶段句柄 End 自动使用创建时的 Actor；按名称 End
使用默认 Actor，或通过 WithEndActor 指定来源，再匹配该 Actor 唯一运行中的同名阶段。
同名但不同 StageID 的并行阶段可通过各自句柄精确结束。

业务使用 Begin / End / Record / Read，以及可选的 Start / Finish。录制调用返回表示本地
接收成功；缓存 miss 时可能查询 Store。参数编码、容量不足和存储查询错误直接返回。
仅在接收成功后推进本地录制状态，失败可以重试。

`Read(ctx, id, false)` 命中缓存立即返回，同时尽力投递加载队列；miss 同步查询 Store。
`Read(ctx, id, true)` 等待一次定向刷新，将远端文档与本地待保存记录合并。刷新不隐含
Flush，也看不到其它实例尚未提交的记录。刷新失败返回错误和可用的本地视图；Store 中
暂不存在文档不会清空本地事实，两边都没有时返回 ErrNotFound。

`Flush(ctx, id, false)` 将 ID 投递到保存队列后立即返回。`Flush(ctx, id, true)` 建立本实例
提交检查点，等待调用时已经接收的记录提交成功，之后的新记录不延长检查点。调用方超时
不取消已由后台接手的提交。保存失败可直接返回，后台周期仍会重试可重试错误。

服务停止时先停止业务生产者，再用独立限时 context 调用 Shutdown。它停止两个后台循环，
尝试排空剩余记录；最终保存失败会返回错误并释放缓冲。Store 连接最后由应用关闭。
Shutdown 后仍可直接读取 Store。

## 后台同步与容量

Manager 通过 cache 持有两个常驻后台协程，分别负责保存和加载。两个有缓冲的 channel
都只传 opid，待提交内容保存在 cache 中。一个 BatchLimit 同时限制保存批次、MGet ID
数量和 Latest 返回数量。

保存协程接受两种来源：

- saveCh 主动指定的 ID，可提前唤醒保存。
- FlushInterval 到期后，从待保存集合轮转获取的一批 ID，包括待重试和已淘汰待最终保存的条目。

两种来源合并去重后受同一个批次上限约束。周期到期时保留处理周期条目的机会，剩余名额
优先处理指定 ID。超出批次的记录留待后续处理。保存串行执行；连续 channel 唤醒之间有
短暂间隔，避免空转或无间歇访问数据库。普通录制不会为每次更新唤醒保存协程。

加载协程被 loadCh 或 LoadInterval 唤醒后，先批量取出并去重 ID，执行 MGet，再执行
Latest(after, limit)。队列为空仍执行 Latest，MGet 失败也不阻止 Latest。每轮成功的
Latest 推进后续时间窗口，并保留一段重叠；失败保留窗口。首次从最近的一批文档开始。

Latest 是有限的最近更新窗口，繁忙时可能遗漏部分操作；写入进程的时钟偏差也影响覆盖。
定向队列补充所需 ID，需要及时性的 caller 使用 fresh 读取。特殊 ID 获得近实时处理机会，
实际延迟受队列、批次和 Store 响应影响。

加载通过统一路径合并远端状态，保留本地未提交和正在提交的批次。读取期间若本地完成
保存，也不能被该次较旧的读取覆盖。远端加载结果不进入待写队列，避免循环回写。
后台查询不提升 LRU 热度；Latest 只在容量允许时预热未知操作。

MaxTimelines 限制缓存文档数，访问更新 LRU，容量满淘汰最近最少使用条目。MaxPendingTimelines
包含已淘汰但等待最终保存的条目；MaxPendingUpdates 限制每个操作待保存的更新数量。
同阶段、同 Actor 尚未提交的状态可以合并。单文档阶段数与属性体积由应用约束。

channel 满时非阻塞投递可以跳过：保存记录仍由周期扫描兜底，加载请求允许遗漏。
Flush 等待的是实际提交结果，不是入队结果。淘汰只释放本地副本，不决定阶段状态；淘汰
触发最终保存，失败后报告并释放缓冲。缓存中的可重试提交按周期重试，永久无效提交退出重试。

后台 IO 在条目锁外执行，每次调用有 ExportTimeout。OnError 可由两个后台协程并发调用，
应快速返回且不调用 Shutdown；批量加载错误使用空 ID。默认日志不包含阶段或属性内容。
DroppedUpdates 统计容量拒绝和最终丢弃，RejectedUpdates 统计存储永久拒绝。

## 存储与一致性

Store 提供原子 Merge、单 ID Read、定向 MGet，以及按更新时间过滤的 Latest。
MGet 缺失项省略，返回顺序不作保证；Latest 使用 `updated_at > after`，按
`updated_at DESC, id DESC` 排序并截断到 limit。非正 limit 返回空结果。

SQL Store 复用应用的 database/sql 连接池，一条操作一行，以行版本 CAS 防止并发整文档
覆盖。CAS 竞争时重新读入并合并各 Actor 的记录。示例 MySQL 表：

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

只有内容发生变化才推进 version 和 updated_at。更新时间由写入进程 UTC 时钟提供，
不是跨实例的严格提交序。查询复用问号参数及批量 IN / LIMIT；自动化验证使用 SQLite
多连接和独立进程，不替代目标数据库实库验收。连接池、migration 和数据清理由应用管理。

Read 返回的 Collection.LocalFlushed 表示当前条目没有待保存记录或已知保存损失；
Collection.StoreRead 表示本次读取成功加载了 Store。两者都不宣称分布式数据完整。
共享数据库是通信渠道，SDK 不承担租约、调度或全局顺序协调。

NoopStore 接受并丢弃提交，单 ID 查询返回 ErrNotFound，批量查询返回空结果。
NewManager(nil, config) 和独立 Handle 默认使用它。纯内存事实仅保存在本地缓存，Flush
成功只表示后端接受提交；淘汰或退出后无法恢复。需要共享状态时配置共享的 Store。

## 时间、编码与其它入口

时间与区间原样保留。先 Finish 后 Start 可能得到负的操作耗时，不交换或截断边界。
对象式 Handle 的现场阶段可保留单调时钟 elapsed_ns；从文档恢复的阶段使用已记录时间
计算耗时。跨主机时间只用于展示，不推断精确因果；并行阶段耗时不可相加作为操作总耗时。

属性在录制返回前完成 JSON 编码，保留大整数精度。输出使用 attributes，读取也接受
历史 fields。Document 与 Snapshot 的 actors 字典、actor_ref 仅在当前 payload 内有效，
解码得到完整 Actor 后再合并。历史空 Actor 仍可解码和读取，新录制要求 ID 或 Name。

对象式 `timeline.New(id, ...Option)` 返回 Handle，使用 WithActor 配置执行者；独立 Handle
没有后台协程，通过 Flush / Snapshot / Finish 显式提交。Manager.NewWriter 返回的 Handle
复用 Manager 缓存及后台同步。`timeline/gospan` 是具有封存边界的进程内实现，阶段通过
WithStageActor 指定执行者，Finish 等待本地阶段结束，封存后仍可补录完整区间。

目录职责保持 `timeline → manager → cache → store → model`：根包收口公开 API，manager
构造录制状态，cache 管理同步及容量，store 承载存储协议，model 承载值类型及读取投影。
