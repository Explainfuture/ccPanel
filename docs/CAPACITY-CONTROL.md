# 容量测试的对账与停止组件

步骤223实现了严格数据库对账、首次停止请求发布器和可选stdin租约；步骤224已将独立停止轮询和租约接入 `load-check-public.mjs`。跨机控制器、整个窗口部署锁/采样器终态/冻结/安全清理仍未实现，不能据此启动正式容量窗口或宣称失联保护已覆盖整个流程。被测应用仍为014/schema22，这两个步骤不部署应用或改变资源配置。

`load-fixture.mjs verify` 继续只接受明确拥有的合成公司/项目/用户及有界UUID清单。每轮使用新的RR只读事务，同时检查三张表的总数、原始事件唯一ID和双向精确ID集合、dedup的stored状态、DLQ为0，以及聚合任务的processed_at、无错误和processed_epoch等于项目当前privacy_epoch。可见桶总数须等于原事件数；这不是逐bucket或完整payload证明。桶表/可见view缺失拒绝成功，不再跳过聚合检查。

尚待完成或旧/NULL epoch的任务允许按原15秒重试预算重新聚合；每轮先ROLLBACK，等500ms，再建立新快照读取真实Worker提交。15秒不是任意锁/调度/文件系统情况下的总进程退出上限；SQL另有5秒statement及200ms lock时限，外层仍须监督原进程的真实终态。对账成功不证明Kafka位点、客户端已退出、未知发送已消除或允许清理fixture。

`load-first-stop.mjs` 的 `publishFirstStop(path, reason)` 仅接受resource_safety/operator_requested。完整UTF8内容在首次await前固定；同目录临时文件wx→实际write/fsync/close→排他hardlink发布，已有目标绝不替换、读取或删除。返回published或already_latched，后者只表示存在，不认证内容/类型。只有确认属于自身且inode不变的临时文件可清理；发布后错误报告不确定并保留最终目标。POSIX同步父目录；Windows没有此处的可移植目录fsync，不作掉电保证。调用方必须拥有稳定私有父目录；静态别名拒绝不代表防御敌对并发目录替换。操作示例见CAPACITY的步骤170。

`load-control-lease.mjs` 的 `attachControlLease({input, runId, onAbort, timeoutMs=5000})` 仅用于独占、未解码的可读pipe。它返回ready和close：首次合法新序号前ready不成功，随后onAbort仍然有效；调用方不能把一次ready当成永久准入。runId为16–128位ASCII字母/数字/下划线/横线。每帧必须精确为如下顺序的规范JSON及LF，总长最多256字节：

```json
{"v":1,"run_id":"EXAMPLE_RUN_000001","seq":"1"}
```

seq为1到9223372036854775807的规范十进制字符串。严格增大才刷新本地performance.now期限；重复序号不续期，倒退、错run、额外/重复字段、非规范字节、超长、EOF、输入错误及本地到期均锁存首个观察故障。runId只是对应标签，不认证来源；未来bridge须只在核对本轮真实远端状态后推进心跳，不能仅按本地定时器续命。

close/故障移除自身timer/listener并pause专用输入；原生pipe可用时unref，不destroy调用方stream或操作内部_handle。错误回调自身抛出时，用固定安全错误终止，不吞异常后让工作负载继续。租约模块本身不读写manifest/文件或发送网络请求。

生成器现在按 `CCPANEL_LOAD_RUN_ID` 是否存在明确选择stdin_monotonic_lease/unleased；空值/无效标签拒绝，不默默退回无租约。启用后固定5000ms，本例不提供可放宽时限的环境项；初次ready前不登录，有预存stop时也不登录。两种模式均记录control的mode/run_id/lease_ms，以及实际加载的四文件SHA（生成器、普通证据发布、租约、安全stop读取），未启用租约的旧用法明确标记unleased，不能由未来外层当成正式有租约窗口。

`load-stop-reader.mjs` 仅初次lstat ENOENT表示无请求。其他路径/打开/读取/关闭不确定性统一invalid_stop_request：实际正规文件、支持时NOFOLLOW/NONBLOCK、1024字节上限/完整EOF、path与fd各自前后dev/inode/size/mtime/ctime稳定，Windows跨path/fd只允许已验证的非零同inode。单reason字段、两种值的JSON可含合法转义/ASCII空白，重复/额外字段、BOM/坏UTF8、链接/FIFO/替换/增长截断均拒绝。稳定nlink2目标可读；发布器收尾unlink会变ctime，合法并发发布可能保守误拒，不能声称零误拒或敌对目录原子快照。

独立1秒单在途stop轮询从登录前持续到generation、每页visibility及最终原请求settlement结束，首次观察只输出一条固定字段诊断。lease故障映射control_lost并只保留通用control_code；不会用诊断写后台manifest。ID完成实际manifest fsync后、加入发送队列前再次检查，POST和各请求入口也检查已锁存停止状态。收到停止不缩短原请求的12/15秒时限；最终等待原sender/observer/query/progress，清理轮询/lease与已跟踪timer后，由外层观察原进程真实close。文档中的5秒只为健康本地观察范围，不是任意网络/FS/OS情况下的进程退出上限。结果/进度输出位置以lstat检查，已有文件/目录/悬空链接在manifest/登录/请求前拒绝，原条目不删除。

验证分开记录：真实PG回归制造同数量但A缺失/B两行的原始事件陷阱、failed dedup、缺表/缺view、旧/NULL epoch，独立观察子进程ROLLBACK后调用实际aggregateNext提交，再由原进程自然成功；另未修复NULL epoch完整等预算后拒绝。首次停止发布器使用真实子进程竞争和委托原生文件系统故障，Windows12项/Linux14项；Windows文件symlink需权限，该平台只声明junction拒绝，Linux另覆盖symlink与目录sync。租约在Windows/Linux各30项，真实pipe、时钟与子进程覆盖重放/错误/到期/关闭/回调错误；硬杀真实coordinator后，detached孤儿自行写fsync的abort及exit-handler回执。该孤儿没有独立原生终态句柄观察，报告明确orphan_native_terminal_observed=false，不拿回执代替完整客户端close证明。

本地失败测试也保留记录：Windows仅pause导致pipe仍引用事件循环，首次watchdog失败后改为公开unref；非detached孤儿未证明、native listener基线断言及Linux命令引用错误均未计为通过。文件身份比较只针对Windows实际fstat/lstat的dev差异采用非零inode核对，路径后续仍按dev+inode识别自身文件。

步骤224在实际原生成器/原子发布器/本机HTTP上联调，测试adapter只改写合成HTTPS URL到loopback HTTP，保留原生fetch/AbortSignal与实际fsync-before-POST。最终Windows12场景/Linux14场景均由原ChildProcess close观察自然退出：租约正常完成且父pipe不关闭、ready前零登录、EOF/固定5秒到期、慢登录、原生POST12秒、分页drain及最终trend15秒、401清理、预存输出拒绝；Windows文件symlink权限不足明确未测，Linux实际悬空result/progress均测。CI41521270593三job成功，Linux原有accounting/Posix信号也执行通过，另47安全读取检查。显式stop/EOF本地观测最大值在最终CI为971ms，是一次测量；整套未测试TLS/SSH/VPS或完整bridge，也未在该集成用例硬杀coordinator，不能与上文独立租约孤儿用例合并扩大范围。见evidence/generator-controls-224.json。

正式窗口仍须完成全程deploy.lock、shared工具SHA固定、跨机控制与失联停止、原client/sampler真实终态、不可覆盖冻结、PG/Kafka动态逐分区屏障、原业务/监控政策保留及清理门禁。上报结果不明/强杀/异常发送先撤key隔离保留fixture；不能用一次lag0或等待60秒推导永无迟到后直接删project。保持原200MiB安全余量、CPU/Swap、1800秒与正常采集/保留要求。本步骤没有VPS负载、容量通过、来源认证、连续可用或恢复准入结论。
