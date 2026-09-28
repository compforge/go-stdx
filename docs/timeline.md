# 操作时间线

## 概念与边界

一条 Timeline 对应一次业务操作。不同进程可以构造绑定相同 ID 的本地句柄，独立记录
stage，通过共享 Store 汇总。ID 的生成、业务含义和唯一性范围归 caller；SDK 不解析
sandbox、conversation 等业务概念。New 只构造句柄，不查询远端，不重置已有操作。

Stage 表示一段实际工作或等待。每次执行生成独立 UUID，重试业务产生新阶段；网络
重试沿用同一条 Record 的 ID。一个 stage 由其创建方更新，其修订号递增；多实例写入
通常意味着分别创建 stage，不意味着竞争修改同一个 stage。

Actor 可选，用于标记执行该阶段的实例。ID 和 Name 都可独立省略，例如 Pod UID 与
Pod name，或只填 worker name。它不代表发起请求的用户。WithActor 在构造句柄时设置，
该句柄创建的阶段自动携带它；空 Actor 不出现在 JSON 中。

Snapshot、StageRecord、StageRef 都是纯数据，支持标准 JSON 序列化。业务代码可以直接
持久化或返回 Snapshot，不必再定义一套 DTO。属性在记录调用返回前完成 JSON 编码，
使用 map[string]json.RawMessage 保存，FieldValue[T] 解码时不会先经过 float64。

## 协作流程

协调方构造句柄并调用 Start，记录业务开始和操作类型。异步组件只需业务 ID、同一
Store 的配置和可选 Actor，就能构造自己的句柄。普通函数可以直接接收 Timeline；
NewContext / FromContext 仅是可选的传递便利。

Begin 返回的 context 携带父阶段身份，保留原来的取消和截止时间。StageFromContext
可以取出纯数据 StageRef，经消息或数据库传给别的进程；NewStageContext 把它附到
接收方自己的 context。同一 timeline 的父身份会被继承，其他 timeline 的身份不会
产生跨操作父子边。只有业务 ID 时，阶段直接挂在确定性的操作根下。

Begin、SetFields、End 只编码并缓存事实，不执行远端 IO。End 第一次决定阶段结果，
后续调用不修改它。Flush(ctx) 确认本句柄此前的记录写入 Store，失败保留原始记录和
幂等键以便重试。句柄不创建后台 goroutine；caller 在业务交接点或丢弃句柄前 Flush。
长阶段如需展示运行态，应在 Begin 后 Flush，不能只在 End 后上报。

协调方在业务结束时调用 Finish，它记录终态并读取当前快照。业务结束不会封锁后续
阶段记录，延迟上报可以继续汇入 Snapshot。根开始和根结束各使用固定幂等键，已经
持久化的结果不能被新句柄覆盖；其他协调方提交不同内容会得到 ErrRecordConflict。
业务协调方及其接管规则由 caller 确定，timeline 不承担调度、租约或任务恢复。

请求超时不等于业务结束。调用方可以 Snapshot 查看进度，后台实际完成方仍负责
Finish。记录错误由 Flush、Snapshot、Finish 返回，业务错误保存在阶段或操作结果中。

## 采集范围

Snapshot 先 Flush 本句柄，再从 Store 读取一致视图。Collection.LocalFlushed 表示
本句柄检查点之前的记录已成功提交且属性可编码；Collection.StoreRead 表示此次读取
成功。这两个字段都不宣称所有进程的缓冲区已排空，也不能发现尚未开始记录的参与者。

业务 Status 与这两个采集事实独立。操作可以已经 succeeded，但部分阶段还显示
running；也可以所有已知阶段均结束，却仍有另一个进程尚未上报。不能据此推断全局
完整。严格完整性需要 caller 的业务完成协议：例如各执行方先 Flush 成功，再发布
任务完成状态；协调方在确认这些状态后读取 Snapshot。

进程崩溃会丢失未 Flush 的记录。已持久化但没有结束记录的 stage 保持 running，不能
自动变成成功。SDK 不通过增加全局 stage 登记屏障来改变业务调度路径。

快照包含起止时间、捕获时间、状态、错误、属性和阶段关系。StageID 是不透明字符串。
结束阶段额外保留执行方使用单调时钟测得的 elapsed_ns；它优先用于阶段耗时。
跨主机时间戳只用于展示，不推断精确因果顺序，不通过平移伪造无时钟偏差的区间。
阶段按开始时间和 ID 排列，保留重叠，不能求和作为整体耗时。共享快照的捕获时间
随读取更新；业务 Finish 后再次读取仍可能看到补报。

## 存储与重试

Store.Append 接收不可变 Record，可部分成功。Store 在同一 timeline 下按 Record ID
去重；同一 ID 内容不同返回 ErrRecordConflict。Stage 更新携带完整阶段和修订号，
汇总选择最大修订号，乱序的旧 running 更新不会覆盖 finished。幂等处理与汇总由 SDK
提供，不要求业务代码维护 map 或拼接整份 JSON。

MemoryStore 通过锁提供进程内一致视图。SQL Store 复用 caller 的 database/sql 连接池，
不建库、不配置连接池、不启动清理线程、不在 Finish 关闭连接。它使用问号参数和固定
表名 timeline_records。caller 通过部署 migration 创建表，并配置语句一致读取所需的
隔离级别、IO 超时和连接容量。每次读取使用一个 SQL statement，避免跨页混合视图。
SQL 实现按数据库生成的 record_seq 排序；跨连接并发写的顺序不代表业务因果关系。
根记录的唯一键保证竞争写入不能替换已接受的开始或终态。

MySQL 表结构示例：

```sql
CREATE TABLE timeline_records (
    record_seq BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    timeline_id VARCHAR(191) COLLATE utf8mb4_bin NOT NULL,
    record_id VARCHAR(64) COLLATE utf8mb4_bin NOT NULL,
    payload LONGTEXT NOT NULL,
    UNIQUE KEY timeline_record_identity (timeline_id, record_id),
    KEY timeline_record_order (timeline_id, record_seq)
) DEFAULT CHARSET=utf8mb4;
```

SQLite 集成测试需要 CGO，使用 INTEGER PRIMARY KEY AUTOINCREMENT。其他数据库必须由调用方
迁移提供等价的自增主键、唯一约束与文本类型，并确认驱动的参数和读取行为。
SDK 自动化测试覆盖 SQLite 多连接及多个独立进程；这些结果不代替 MySQL/DM 实库验收。

记录保留和清理周期归部署方。清理时应覆盖整个操作的保留期，并考虑迟到上报；timeline
不会因为某个实例 Finish 就删除共享记录。数据库失败时，caller 决定是否让业务继续，
但不能把失败的 Flush 当作已确认持久化。SQL 不额外打开连接池；SQLite driver 仅用于测试。

## 本地 gospan 实现

timeline/gospan 用 gospan 的阶段事件投影进程内数据，没有共享 Store。它在构造时
开始操作，Finish 要求本地阶段已结束，随后关闭 writer；结束后的句柄不会继续录制。
这是有明确封存边界的本地实现。需要跨实例写入与迟到上报时使用 timeline.New 和 Store。

本地实现的私有检查点确保此前记录已进入投影；输出采用相同的 Collection 与纯数据
类型。Registry 只查找该进程内的活跃句柄，不参与跨实例关联。两种实现都不要求
caller 在 context 中绑定 Timeline，也不把后端句柄带入 Snapshot。
