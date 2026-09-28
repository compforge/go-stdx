# 操作时间线

## 概念与边界

一条 Timeline 对应一次业务操作。不同进程可以构造绑定相同 ID 的本地句柄，独立记录
stage，通过共享 Store 汇总。ID 的生成、业务含义和唯一性范围归 caller；SDK 不解析
sandbox、conversation 等业务概念。New 只构造句柄，不查询远端，不重置已有操作。

Stage 表示一段实际工作或等待。每次执行生成独立 UUID，重试业务产生新阶段；网络
重试沿用同一份状态和修订号。一个 stage 由其创建方更新，其修订号递增；多实例写入
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
后续调用不修改它。Flush(ctx) 确认本句柄此前的记录写入 Store，失败保留原始状态和
修订号以便重试。句柄不创建后台 goroutine；caller 在业务交接点或丢弃句柄前 Flush。
长阶段如需展示运行态，应在 Begin 后 Flush，不能只在 End 后上报。

协调方在业务结束时调用 Finish，它记录终态并读取当前快照。业务结束不会封锁后续
阶段记录，延迟上报可以继续汇入 Snapshot。根状态由调用 Start 的句柄写入，使用递增修订号；开始边界和已接受的终态不能
被覆盖，冲突写入返回 ErrConflict。只有阶段的组件不调用 Start、SetFields 或 Finish。
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

Store.Merge 接收 Update：可选的完整根状态、当前写入方更新过的阶段。Document 是
持久化纯数据，保存整个 timeline 的最新状态，不保留事件日志，也不保存 CapturedAt 或
Collection 这类读取时事实。Snapshot 从最新 Document 生成，结束后不缓存另一份最终快照。

每个阶段独立递增 Revision；低版本忽略，同版本同内容幂等，同版本不同内容返回
ErrConflict。开始边界、名称、父阶段和 Actor 不可改；终态不可重开。根字段也作为带
修订号的完整状态提交，乱序重试不会把新字段改回旧值。一个 Update 中有冲突时整体拒绝。
MergeDocument 提供相同的纯数据合并规则供 Store 实现复用，业务无需维护合并逻辑。

MemoryStore 通过锁原子合并。SQL Store 使用固定表 timelines，一条 timeline 一行。
先读取 payload 和 version，在 Go 中合并，仅按预期 version 更新；竞争失败后重新读、
重新合并并退避重试，受 caller context 约束。创建竞争通过主键处理。重复投递不推进
版本或更新时间。SQL 不使用数据库专属 JSON 更新表达式，JSON 字段比较容忍对象键排序
和空白归一化，保留大整数精度。数据库错误可能意味着提交结果不确定，caller 可重试
同一份 Update；SDK 不用未确认的持久化替代成功确认。

MySQL 表结构示例：

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

id 由 caller 定义并直接关联业务，不重复增加 target_id。version 仅用于行级 CAS，和
payload 内根/阶段的 Revision 分工不同。created_at 是首次持久化时间，updated_at 是
最后一次接受更新的时间；业务完成时间在 payload.finished_at，不能混用。

caller 负责 migration、连接池、IO 超时与保留策略。SDK 复用 database/sql 连接池，
不创建/关闭池，不建表，不启动 GC。SQLite 测试使用 TEXT、INTEGER、DATETIME；DM 可用
CLOB、BIGINT、TIMESTAMP。驱动需要支持问号参数及单语句一致读取。自动化测试覆盖 SQLite
多连接和独立进程，这些结果不代替 MySQL/DM 实库验收。

Read 无记录返回 ErrNotFound。caller 可以将 Document 直接 JSON 序列化，也可以通过
Document.Snapshot 生成纯数据视图；只有真正读写成功的采集路径才能设置 Collection。
记录清理应考虑业务保留期和迟到上报。SDK 不理解业务行，也不提供永久删除墓碑；若清理
后仍有 writer 上报，可能重新创建文档，部署方应处理此类孤立文档的保留期。

## 本地 gospan 实现

timeline/gospan 用 gospan 的阶段事件投影进程内数据，没有共享 Store。它在构造时
开始操作，Finish 要求本地阶段已结束，随后关闭 writer；结束后的句柄不会继续录制。
这是有明确封存边界的本地实现。需要跨实例写入与迟到上报时使用 timeline.New 和 Store。

本地实现的私有检查点确保此前记录已进入投影；输出采用相同的 Collection 与纯数据
类型。Registry 只查找该进程内的活跃句柄，不参与跨实例关联。两种实现都不要求
caller 在 context 中绑定 Timeline，也不把后端句柄带入 Snapshot。
