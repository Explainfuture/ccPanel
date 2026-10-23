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

多用户服务端共用实例时，使用逐事件身份接口：

```ts
type BusinessEvents = { order_paid: { amount: number } };
const shared = createNodeAnalytics<BusinessEvents>({
  endpoint: 'https://your-analytics.example/v1/batch',
  projectId: process.env.ANALYTICS_PROJECT_ID!,
  writeKey: process.env.ANALYTICS_WRITE_KEY!,
  consent: true,
  outboxPath: './runtime/business-outbox.json',
});
await Promise.all([
  shared.trackFor('opaque-user-A', 'order_paid', { amount: 10 }),
  shared.trackFor('opaque-user-B', 'order_paid', { amount: 20 }),
]);
await shared.shutdown();
```

每次调用在首次异步等待前冻结身份、属性与新事件ID，不切换实例身份，重试/文件恢复保留原快照。`trackFor`不自动附加服务器实例的匿名ID，也不自动创建匿名关联；浏览器关联继续通过其`identify`协议建立。已有`track/identify/reset`仍是有状态单身份语义，不要用它们在并发请求之间切换用户。

传入稳定、非空、最多256字符的业务用户ID，不使用姓名或电子邮件代替。服务端须从自身已验证的业务上下文取得用户ID；该接口不负责业务登录鉴权。它沿用实例的项目/凭证，不是跨公司切换入口。授权撤回、属性隐私过滤和运行时事件校验仍然有效，泛型仍约束事件名及必填属性。

SDK和整个平台尚未通过完整PRD发布门槛；ESM类型/包导入验收不等于生产容量、真实设备或全部故障模型通过。
