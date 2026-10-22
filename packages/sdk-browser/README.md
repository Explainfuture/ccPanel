# @ccpanel/sdk-browser（开发预览）

通过 pnpm 打包安装。需要 HTTPS 接入端、projectId、公开 writeKey 和明确 consent。调用 track(name, properties) 异步返回本地入队结果，不代表服务器已接收；flush 返回当前批次确认情况。先识别用户可调用 identify，退出登录调用 reset，撤回采集用 setConsent(false)，销毁实例用 destroy。

当前默认：IndexedDB 有界队列、20 条或 3 秒调度、48 KiB 批次、10 秒请求超时、7 天 TTL、指数退避抖动。页面 hidden/pagehide 使用 sendBeacon 尽力补发并保留记录，下次普通 fetch 确认后清除。每个新事件 UUID v4，重试复用 ID。敏感属性名默认过滤，默认页面上下文不采集 URL 查询参数。

IndexedDB 不可用时进入有界内存缓冲并调用 onError；内存事件关闭后无法恢复。存储恢复后以原 ID 和重试元数据迁回持久队列，恢复探测间隔受限。撤回/重新授权操作串行执行，先停采集、再清空旧记录，避免并发调用遗留旧数据。getQueueStatus 区分持久和内存记录数。

开发验收页面 /sdk-lab.html 需要手工输入测试项目与写入凭证，显式产生 sdk_lifecycle_test。它不是自动采集入口。真实浏览器关闭矩阵、设备性能验收仍未全部完成，不能作为已通过 PRD 全部验收的稳定版发布。

项目可导出 ProjectEvents 类型，并调用 `createAnalytics<ProjectEvents>(options)`：事件名获得补全，必填属性与值类型在编译期检查；不提供泛型时仍支持通用事件。服务端的运行时校验始终保留，TypeScript 不能作为可信数据边界。

可选 `propertyAllowlist` 按事件名限制顶层 properties，例如 `{ checkout_started: ['product_id', 'amount'], button_clicked: ['button'] }`。一旦配置，未列出的事件仍可上报，但 properties 为空；未批准字段不会被读取或写入队列。配置在初始化时复制，后续修改原对象不会扩大采集范围。嵌套敏感键过滤始终保留，白名单不能绕过密码/令牌等过滤；它不控制身份或 SDK 的固定上下文字段。最多配置 1000 个事件，每事件最多 100 个属性键。Node SDK 使用相同选项和语义。
