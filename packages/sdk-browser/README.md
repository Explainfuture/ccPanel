# @ccpanel/sdk-browser（开发预览）

ESM打包产物已验证TypeScript Bundler及NodeNext解析（浏览器项目启用DOM类型库），包括泛型错误事件名/属性的编译期拒绝。构建和pnpm打包后可在仓库执行`python3 scripts/check-packed-sdks.py artifacts`（Windows使用`python`），从仓库外的临时消费者复核产物。

通过 pnpm 打包安装。需要 HTTPS 接入端、projectId、公开 writeKey 和明确 consent。调用 track(name, properties) 异步返回本地入队结果，不代表服务器已接收；flush 返回当前批次确认情况。先识别用户可调用 identify，退出登录调用 reset，撤回采集用 setConsent(false)，销毁实例用 destroy。

当前默认：IndexedDB 有界队列、20 条或 3 秒调度、48 KiB 批次、10 秒请求超时、7 天 TTL、指数退避抖动。页面 hidden/pagehide 使用 sendBeacon 尽力补发并保留记录，下次普通 fetch 确认后清除。每个新事件 UUID v4，重试复用 ID。敏感属性名默认过滤，默认页面上下文不采集 URL 查询参数。

IndexedDB 不可用时进入有界内存缓冲并调用 onError；内存事件关闭后无法恢复。存储恢复后以原 ID 和重试元数据迁回持久队列，恢复探测间隔受限。撤回/重新授权操作串行执行，先停采集、再清空旧记录，避免并发调用遗留旧数据。getQueueStatus 区分持久和内存记录数。

慢存储下未完成的track另有预算：主队列持久时最多1000个调用/5MiB序列化事件，主队列内存时100个/512KiB，任一先达到即返回`rejected`、reason为`rejected_pending_overflow`并调用onError；不会无限排队等名额。条数已满时不再读取新调用的properties；字节预算按验证、快照、隐私过滤后的完整事件UTF-8大小计算。写入失败、队列满或撤回完成后释放预留。Node SDK及trackFor沿用此规则。

getQueueStatus新增`pending_writes`、`pending_bytes`、`pending_limit_count`、`pending_limit_bytes`。pending不表示已经持久入队，原count/bytes仍只统计队列记录；两者在写入完成与返回之间可能短暂重叠，不可相加当唯一事件数。待写预算与已存队列预算独立，序列化字节不是JavaScript对象开销或进程RSS上限；主持久队列失败后的内存缓冲仍限100条/512KiB。业务应处理明确拒绝，不能无界重试或解释为服务器已收到。

开发验收页面 /sdk-lab.html 需要手工输入测试项目与写入凭证，显式产生 sdk_lifecycle_test。它不是自动采集入口。真实浏览器关闭矩阵、设备性能验收仍未全部完成，不能作为已通过 PRD 全部验收的稳定版发布。

项目可导出 ProjectEvents 类型，并调用 `createAnalytics<ProjectEvents>(options)`：事件名获得补全，必填属性与值类型在编译期检查；不提供泛型时仍支持通用事件。服务端的运行时校验始终保留，TypeScript 不能作为可信数据边界。

同项目已有撤回标记时，重新构造`consent:true`不会覆盖撤回；业务确认重新授权后须调用`setConsent(true)`并等待完成。支持Web Locks且本地存储可用时，跨标签页授权清理串行执行；其他页面不会自动重新授权。不可用时仍需业务同意管理层通知各页面，不能据此宣称全部浏览器一致。

运行时仅布尔值`true`授予采集权限；字符串`"true"`/`"false"`、数字、对象或缺失值均不授权。`setConsent`收到任何非`true`值时按撤回处理，立即停采集并执行队列清理；不要直接传入环境变量或存储字符串。Node SDK沿用相同规则。

可选 `propertyAllowlist` 按事件名限制顶层 properties，例如 `{ checkout_started: ['product_id', 'amount'], button_clicked: ['button'] }`。一旦配置，未列出的事件仍可上报，但 properties 为空；未批准字段不会被读取或写入队列。配置在初始化时复制，后续修改原对象不会扩大采集范围。嵌套敏感键过滤始终保留，白名单不能绕过密码/令牌等过滤；它不控制身份或 SDK 的固定上下文字段。最多配置 1000 个事件，每事件最多 100 个属性键。Node SDK 使用相同选项和语义。

事件字符串和属性键必须兼容 PostgreSQL UTF-8/JSONB：拒绝 NUL 和未配对的 UTF-16 代理项，正常中文与 Emoji 可用。被拒绝的事件返回明确状态，不进入无限数据库重试。

业务时间可作为第三个参数传入，例如 `track('order_paid', { amount: 10 }, { event_time: new Date(paidAt).toISOString() })`。不传则使用调用时刻；时间必须为带 `Z` 的 UTC 字符串，按现有协议校验（最多过去七天、最多未来五分钟）。无效时间返回 `rejected/invalid_event_time` 并触发 onError，不入队。时间在首次异步等待前冻结，重试及本地恢复保留原时间和 ID；服务端接收时仍会再次校验，因此长期离线可能过期。空属性事件可以传 `undefined` 占位后再传时间选项。该参数不改变用户身份，也不改变接收时间 received_at。

`getQueueStatus().counters` 公开本实例生命周期累计诊断：`expired` 为清理的过期事件数，`rejected_overflow` 为队列满拒绝次数，`rejected_pending_overflow` 为待入队预算拒绝次数，`storage_error` 为存储操作失败次数（不是丢失事件数，事件可能已降级到内存）。计数在调用 onError 前更新，回调异常不影响计数；返回独立快照，最多累加至 Number.MAX_SAFE_INTEGER。重建实例重新计数，不跨标签页聚合、不持久化、不随授权撤回/重新授权清零。只有数字，无事件属性或身份信息。原 `expired:N` 回调保持兼容。队列 TTL 按原始入队时间计算，业务 event_time 在接入时另行校验。
