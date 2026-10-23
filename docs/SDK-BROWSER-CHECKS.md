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
