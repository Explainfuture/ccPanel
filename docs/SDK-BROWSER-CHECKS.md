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
