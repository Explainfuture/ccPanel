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

## 原生 sendBeacon=false 与重载恢复（步骤 185）

使用步骤177的本机接收端（新增单个有界 /hold、/hold-state、/release-hold，仅供隔离验收；最大65536字节、15秒自动释放，最多一个占位请求），打开独立会话 sdk185，执行 `scripts/browser-sdk-native-beacon-false.cjs`。

方法参考 [Fetch Standard keepalive 配额](https://fetch.spec.whatwg.org/#http-network-or-cache-fetch)：保持一个真实65536字节 keepalive请求等待响应，确认接收端已收完请求体后调用SDK beacon。未替换 navigator、未拦截网络、未改时钟。该场景是共享配额竞争，不是在产品中降低事件/批次限制，也不新增SDK keepalive重试。

2026-10-23 Chrome155.0.8059.85：原生beacon返回false，真实IndexedDB仍1，接收端事件回执0；释放占位请求并原生reload（原SDK监听器仍在），新SDK普通cors请求用原ID取得202后count0，接收端只有1次事件回执。脚本先试验过主动destroy旧实例，再移除该步骤后重跑，通过；最终脚本验证了旧监听器仍存在的原生重载。Browser构建SHA256 158e90697d30571e855a552e8a94fe6e1fa99e215bc44fb326615e43ae0887f4。专用浏览器/接收端已关闭。

此证据补充Chrome原生false分支；调用SDK beacon方法触发原生API，未通过关窗触发第一次false，未覆盖VPS/Kafka/PG、系统强杀、BFCache或完整设备矩阵。不能把本机包回执测试记为完整A05或容量通过。

## 跨引擎记录（步骤 186）

本机 Edge 安装版为133.0.3065.82，本步骤未执行；不能将它作为当前/上一稳定版本的覆盖证据。通过 Playwright CLI 的 `install-browser firefox` 安装独立测试引擎 Firefox156.0（Playwright firefox v1553，缓存 firefox-1553），没有替换用户默认浏览器或服务器依赖。

执行本地接收端和专用 firefox186 会话：

```powershell
npx --yes --package @playwright/cli playwright-cli install-browser firefox
npx --yes --package @playwright/cli playwright-cli -s=firefox186 open http://127.0.0.1:5196 --browser firefox
npx --yes --package @playwright/cli playwright-cli -s=firefox186 run-code --filename scripts/browser-sdk-native-pagehide.cjs
npx --yes --package @playwright/cli playwright-cli -s=firefox186 run-code --filename scripts/browser-sdk-native-beacon-false.cjs
npx --yes --package @playwright/cli playwright-cli -s=firefox186 close
```

2026-10-23，本次 SDK 构建仍为158e90697d30571e855a552e8a94fe6e1fa99e215bc44fb326615e43ae0887f4。无拦截 HTTP 导航用例 passed=true：可信pagehide/persisted=false、真实IndexedDB、退出no-cors1/恢复cors1、完整事件一致，beacon的202不清记录，普通202后清空。跨引擎产品行为在这个场景相同。

第二条用例失败于前置断言 `native beacon was not rejected by occupied keepalive quota`：65536字节keepalive占位已准备好，但Firefox beacon仍true。本次无法用Chrome的配额方法诱发Firefox原生false，不能记作false后恢复通过，也不能仅据此前置条件差异判为SDK丢数据或推断具体浏览器实现原因。脚本finally释放占位并destroy；随后关闭专用浏览器和本机接收端。没有为“通过”放宽断言或修改SDK。

| 引擎与来源 | 本机原生导航+HTTP+持久恢复 | 原生false及重载恢复 | 验证范围 |
|---|---|---|---|
| Chrome155.0.8059.85，已安装Chrome | 步骤180/177通过 | 步骤185通过 | 所记录SDK构建的单机场景，非完整版本矩阵 |
| Firefox156.0，Playwright测试引擎v1553 | 步骤186通过 | 未验证：配额前置条件未达成 | 不等于独立发行版当前/上一稳定版验收 |

该表不是PRD支持矩阵完成表；真实桌面稳定版本矩阵、Android/iOS、系统关闭/强杀、BFCache及完整A05仍缺证据。VPS及容量配置不变。

上述186时缺失的本机Chrome BFCache证据由步骤208补充如下；其他引擎/版本/真实移动设备缺口不被扩大通过。

## 原生 IndexedDB 失效与重建（步骤 187）

在步骤177本机接收端启动后，用专用Chrome测试会话执行 `scripts/browser-sdk-native-idb-recovery.cjs`。该脚本仅对 http://127.0.0.1:5196 使用CDP Storage.clearDataForOrigin（indexeddb），使已打开的原生SDK数据库连接关闭，再track触发真实存储错误；不替换indexedDB/fetch/navigator，不拦截网络。

2026-10-23 Chrome155.0.8059.85：track返回queued-memory，memory_count=1、storage_error可观察；flush重建原生持久库，真实HTTP以相同ID取得202后count=0，确认持久库存在，无未处理pageerror。最终脚本强化“恢复后库存在”断言并重跑通过，node语法检查通过。SDK构建与185一致，没有产品代码变化。

先尝试的配额方法不能记作通过：CDP overrideQuotaForOrigin报告quota=0/overrideActive=true，但小写入及有界填充均未触发QuotaExceededError；普通上下文和独立持久测试profile复核均未满足前置条件。没有认定实现根因，也没有用mock填补缺口。因此真实QuotaExceeded分支仍未验证；原生连接关闭/恢复是另一项分别记录的存储失效场景，不将两者等同。

测试结束恢复配额、清除测试origin IndexedDB/localStorage、关闭专用浏览器及接收端。自动审批拒绝了递归删除 .local/quota187-profile（返回blocked by policy），未改用其他工具绕过；清理后的测试浏览器配置目录保留在Git忽略范围，未提交。脚本finally始终尝试清库和destroy；只允许在专用测试origin运行，不能用于真实公司网站。

此场景不验证存储被清除前的旧事件可恢复（删除的数据本就可能丢失），只验证清除后新track的显式降级与同ID恢复；不等于磁盘配额、全A09、设备矩阵或VPS容量通过。

## 原生BFCache连续返回与同实例恢复（步骤208）

使用已安装Chrome155.0.8059.85、Windows、真实IndexedDB和loopback HTTP接收端，SDK构建SHA256为449f14f4719751a13ef1a666ff141511dd223ddc93b246968d87bf2688c4b661。产品SDK源码未改。安全结果见 [冻结JSON](evidence/bfcache-chrome155-208.json)，配置见 [专用启动配置](../infra/browser-native-bfcache.config.json)。

Playwright默认禁用BFCache；该专用配置仅移除disable-back-forward-cache及三个背景节流相关自动化开关，不强制伪造缓存或派发生命周期事件。新增/bf-start、/bf-next为合成静态可缓存页面，API/SDK/回执路由仍no-store；不能把此夹具header条件扩展成所有业务网站都能进BFCache。

```powershell
node scripts/browser-sdk-native-receiver.mjs
# 另一终端；测试结束后先close浏览器，再Ctrl+C停止接收端
npx --yes --package @playwright/cli playwright-cli -s=bfcache208 open http://127.0.0.1:5196/bf-start --config=infra/browser-native-bfcache.config.json
npx --yes --package @playwright/cli playwright-cli -s=bfcache208 run-code --filename scripts/browser-sdk-native-bfcache.cjs
npx --yes --package @playwright/cli playwright-cli -s=bfcache208 close
```

首轮单次返回通过后，强化为同一文档/同一SDK实例连续两次真实导航和goBack，重新开专用上下文复核通过：两次trusted pagehide.persisted=true及pageshow.persisted=true，文档随机标记保持；每次退出原生beacon得到202但真实IDB原记录保留，恢复仅由原生visibilitychange自动触发普通fetch，没有显式flush/重建SDK。配置发送周期60秒，要求10秒内恢复；不是靠等周期发送。四次实际HTTP模式严格为no-cors/cors/no-cors/cors，两对完整载荷分别一致，新操作ID不同，最终队列0、未处理pageerror0。

若没有观察到同文档/可信persisted pageshow，脚本返回passed=false/precondition且不认定SDK丢失，不能把CLI退出0当验收通过。这里实测结果确实passed=true。finally仅在结果采集后destroy SDK，用CDP清除专用origin的IDB/localStorage；CDP未用于制造BFCache恢复。浏览器与接收端已关闭，5196端口确认无监听。

CI只做新脚本语法及既有回归，不宣称在CI执行了本机Chrome原生用例。该证据仅补A07中这个单机BFCache窗口，未覆盖并发多标签/崩溃租约所有组合、其他引擎、当前/上一稳定版完整矩阵、Android/iOS、OS强杀、Kafka/PG或容量。首个控制台404来自夹具未提供favicon，不是pageerror；原生用例未观察到JS未处理异常。
## BFCache返回期间跨文档授权变更（步骤209）

脚本`scripts/browser-sdk-native-bfcache-consent.cjs`沿用208接收端和原生Chrome配置，将run-code的filename替换为该文件即可。冻结结果见[evidence JSON](evidence/bfcache-consent-chrome155-209.json)。Chrome155.0.8059.85、SDK449f14f构建、真实IDB和HTTP，两种独立场景均passed=true：B撤回授权后返回A；以及B撤回后重新授权、留下新队列再返回A。

要求A同文档、可信persisted恢复，未观察到时返回前提失败而非伪造事件。恢复后2000ms内无自动新回执，旧SDK仍拒绝track/beacon；检查整份持久化队列与B留下的状态一致，证明旧实例不会误删新授权事件。随后显式授权才发送有效新事件，完整载荷匹配、新身份、旧事件不重放、最终队列0、pageerror0。每场景两次HTTP为退出no-cors与授权后cors，撤回前已被接收的beacon不因客户端撤回而删除。

本机测试结束后清专用origin并关闭浏览器/接收端。CI仅检查脚本语法和既有回归；2000ms是有界负向观察，不代表无限时长、全设备、Kafka/PG或容量保证。SDK产品源码未改。


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
