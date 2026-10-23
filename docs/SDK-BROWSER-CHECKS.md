# SDK 业务时间与退出 TTL 的浏览器检查

使用已构建的 Browser SDK，在独立 Playwright Chrome 会话运行：

1. `pnpm build`
2. 在仓库根目录单独终端运行 `python -m http.server 5195 --bind 127.0.0.1`（仅本机监听，测试后关闭）。
3. `npx --yes --package @playwright/cli playwright-cli -s=sdk175 open http://127.0.0.1:5195 --browser chrome`
4. `npx --yes --package @playwright/cli playwright-cli -s=sdk175 run-code --filename scripts/browser-sdk-time-expiry.cjs`
5. 读取 Result 的 passed 及具体字段，然后 `npx --yes --package @playwright/cli playwright-cli -s=sdk175 close`。失败时也关闭该专用会话与本地 HTTP 服务。

脚本仅使用合成凭证和随机项目标识；用真实 IndexedDB、页面 reload 和浏览器 sendBeacon。接收端通过 Playwright route 模拟 503/202，显式推进 Date.now 以覆盖重试等待及七天 TTL。没有连接 VPS/Kafka/PG。脚本访问本地 SDK 构建产物；运行前必须构建，避免误测旧产物。

断言：业务时间在调用前后修改选项时不变；503 后页面重载仍恢复原 ID/时间/载荷；退出批次跳过过期项且每周期最多一次；beacon true 不视为确认或清理；后续普通 flush 的过期计数只增加一次；destroy 后不再发送。此处直接调用 SDK beacon，未模拟操作系统杀进程、真实用户关窗、后台节流或 BFCache。

步骤 175 实测：2026-10-23，Chrome 155.0.8059.85，passed=true。冻结源 b54da1b，Browser SDK SHA256 `40788b0b6152ba4dd5a6a16a6a1376f5b79b1a9531698539f06cdbf8b7173d2b`。真实 IndexedDB、重载恢复及 sendBeacon 均通过，expired=1。初次导航 README.md 被浏览器识别为下载，改为同源根页面后执行成功；下载未参与测试。已关闭专用浏览器会话与本地服务。

这不代表当前/上一稳定 Chrome/Edge/Firefox/Safari 或真实 Android/iOS 的完整矩阵通过，也不代表线上接入、容量或关窗送达保证。

## 原生导航与未确认事件恢复（步骤 176）

沿用上面的本地服务与构建步骤，打开独立会话 sdk176，执行 `npx --yes --package @playwright/cli playwright-cli -s=sdk176 run-code --filename scripts/browser-sdk-pagehide-recovery.cjs`，结束后关闭 sdk176 和本地服务。

脚本不手动 dispatch 生命周期事件或调用 beacon，通过真实同源导航触发 pagehide。仅包装 navigator.sendBeacon 来记录原生调用返回值，仍委托原生 API；另记录 pagehide.isTrusted/persisted。导航后先直接读取真实 IndexedDB，确认未确认事件存在，再初始化新页面 SDK，以普通精确 202 确认后清空。接收端仍为 Playwright context route，并非 VPS。

2026-10-23 Chrome 155.0.8059.85 实测：isTrusted=true、persisted=false，原生 beacon queued=true；拦截器观察到退出请求为 0，恢复请求为 1。因此只证明原生生命周期调用及未确认持久恢复，**退出送达未证实**。退出请求未观察到不证明服务器收到，也不证明浏览器必然未发。初版等待退出请求 30 秒超时；保留该事实，并改为分别报告原生排队与接收证据，未将 queued=true 当送达。

真实 IndexedDB 中的原事件在导航后保留，新页面普通发送使用相同 ID/时间/完整载荷，精确 202 后 count=0。测试未修改时钟；不是操作系统关闭/强杀、BFCache 或真实移动设备验证。源码仍为 b54da1b 对应 SDK 构建，VPS 仍为 012。专用浏览器和本地服务已关闭。Result passed 仅表示其 scope 指定的生命周期与持久恢复断言，exit_delivery_proven 独立报告，不能用它通过完整 A05 或设备矩阵。

## 无拦截的本机 HTTP 接收验证（步骤 177）

先 `pnpm build`，单独终端执行 `node scripts/browser-sdk-native-receiver.mjs`，只监听 127.0.0.1:5196。接收端只提供固定测试页面、已构建 SDK、合成事件接收和按项目读取的内存回执；不暴露仓库目录、不写磁盘、不调用外部服务。请求体限 64KiB、内存回执限 100 项；结束后关闭接收进程即清除回执。

打开专用会话并运行：

```powershell
npx --yes --package @playwright/cli playwright-cli -s=sdk177 open http://127.0.0.1:5196 --browser chrome
npx --yes --package @playwright/cli playwright-cli -s=sdk177 run-code --filename scripts/browser-sdk-native-pagehide.cjs
npx --yes --package @playwright/cli playwright-cli -s=sdk177 close
```

脚本不注册 Playwright route、不替换 navigator、不修改时钟或派发生命周期事件。真实同源导航后，检查接收端回执以及初始化 SDK 前的原 IndexedDB 记录。2026-10-23 Chrome 155.0.8059.85 实测 passed=true，可信 pagehide/persisted=false，接收端收到退出请求 1（Sec-Fetch-Mode=no-cors）和恢复请求 1（cors）；两者完整事件相同。退出接收端即使返回 202，本地记录仍存在；只有恢复普通请求精确确认后才 count=0。源码和构建 hash 与步骤 175 相同。浏览器和接收端已终止。

这补充了本机真实 HTTP 的退出接收证据；保留步骤 176 的未观察到事实，不将该旧拦截测试改记为成功接收。接收端为合成回执服务，没有 Kafka/PG 幂等落库；不等于公网、真实 OS 关窗/强杀、BFCache 或全设备矩阵。

## VPS HTTPS → Kafka → PostgreSQL（步骤 178）

2026-10-23 使用最新本地 Browser 构建（步骤 175 的 SHA256），服务器保持 m4-trends-012/schema22，没有发布替换。先确认 ready 和资源（可用内存约 322MiB），保存原表指纹及消费位点，再通过 load-fixture.mjs 创建隔离公司；只把该项目允许来源设为 http://127.0.0.1:5197。凭证通过 SSH 存本地私有 .local/browser178-fixture.json，未提交或输出。

可复用检查代码：browser-sdk-vps-origin.mjs 提供固定本机页面/SDK/公开写凭证（不提供登录密码）；browser-sdk-vps-pagehide.cjs 在独立 Chrome 会话持久排队一条显式昨天业务时间的合成事件，然后实际导航；browser-sdk-vps-verify.mjs 使用临时账号从真实公司 API 查询，**必须在恢复 SDK 前**确认原 ID 和业务时间可见；browser-sdk-vps-recover.cjs 先读 IndexedDB，再恢复普通发送。文件名 browser178-* 是该次隔离验收工作文件，不是生产配置。运行前必须新建隔离夹具、核对 PUBLIC_ORIGIN 和项目 origins；不要使用真实公司凭证或在共享浏览器会话运行。验证失败须保留证据再清理，不得复用旧夹具冒充新测试。

实测 Chrome 155.0.8059.85：原生 pagehide isTrusted=true/persisted=false；VPS 在新页面初始化 SDK 前即显示事件，event_time=2026-10-22T11:26:14.231Z。导航后 IndexedDB 仍为 1，恢复普通 confirmed=1/pending=0。无 Playwright 网络拦截、无时钟模拟、无 navigator 包装。

服务端以客户端原 ID 精确验证 raw/dedup/jobs 各 1，processed=1、bucket_events=1、DLQ=0。ID 集合 SHA256 `434f3465962da4e4a74b91857a4b60ddefc718ded354f3facb8776aa8659fad3`。消费总位点 15614→15616，两个分区 7725/7891，PG=Kafka committed=high，lag=0。清理临时公司、登录会话和两端夹具后，33 个原业务关系指纹一致、临时公司 0、HTTPS ready；浏览器与本机服务均终止。Kafka 仍按保留策略保存历史合成消息，不宣称物理抹除。

本次仅一条唯一事件、两次上报，不是容量测试，也未覆盖 sendBeacon false、操作系统强杀、全部浏览器/真实移动设备、BFCache 或公网故障矩阵。不会据此通过完整 A05/容量门槛。
