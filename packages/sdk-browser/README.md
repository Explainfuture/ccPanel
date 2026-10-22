# @ccpanel/sdk-browser（开发预览）

通过 pnpm 打包安装。需要 HTTPS 接入端、projectId、公开 writeKey 和明确 consent。调用 track(name, properties) 异步返回本地入队结果，不代表服务器已接收；flush 返回当前批次确认情况。先识别用户可调用 identify，退出登录调用 reset，撤回采集用 setConsent(false)，销毁实例用 destroy。

当前默认：IndexedDB 有界队列、20 条或 3 秒调度、48 KiB 批次、10 秒请求超时、7 天 TTL、指数退避抖动。页面 hidden/pagehide 使用 sendBeacon 尽力补发并保留记录，下次普通 fetch 确认后清除。每个新事件 UUID v4，重试复用 ID。敏感属性名默认过滤，默认页面上下文不采集 URL 查询参数。

IndexedDB 不可用时进入有界内存缓冲并调用 onError；内存事件关闭后无法恢复。尚未完成真实浏览器关闭矩阵、设备性能验收和恢复后内存迁移验证，不能作为已通过 PRD 全部验收的稳定版发布。
