# @ccpanel/sdk-node（开发预览）

当前提供 ESM 包，运行环境 Node.js 22.12+。从仓库先构建，再通过 pnpm 打包安装；不需要 DOM 类型库。共享类型使用 `.js` 相对声明路径，支持 TypeScript NodeNext 的严格声明检查。

```ts
import { createNodeAnalytics } from '@ccpanel/sdk-node';

type Events = { worker_started: { name: string } };
const analytics = createNodeAnalytics<Events>({
  endpoint: 'https://your-analytics.example/v1/batch',
  projectId: process.env.ANALYTICS_PROJECT_ID!,
  writeKey: process.env.ANALYTICS_WRITE_KEY!,
  consent: true,
  outboxPath: './runtime/analytics-outbox.json',
});
await analytics.track('worker_started', { name: 'billing' });
// 在应用正常停服流程中等待关闭。
await analytics.shutdown();
```

`track` 返回本地入队状态，不能当作服务器接收或数据库可查询证明。配置 `outboxPath` 才使用持久文件队列；省略时是有界内存队列，进程退出后不能恢复。一个队列文件只允许一个活进程写入，不能跨主机共享。关闭等待已排队写入并释放锁；损坏文件/不明锁保持拒绝，不自动覆盖源文件。实际进程崩溃与认领边界见仓库 `docs/SDK-PERFORMANCE.md`。

当前实例沿用有状态身份；不要在多用户并发请求之间共享 `identify/reset` 来回切换身份。示例是单身份/系统事件，每事件独立身份的服务端接口仍待补齐。TypeScript泛型用于事件名和属性约束，服务端运行时校验仍然有效。

SDK和整个平台尚未通过完整PRD发布门槛；ESM类型/包导入验收不等于生产容量、真实设备或全部故障模型通过。
