# 容量测试的对账与停止组件

步骤223实现严格数据库对账、首次停止发布器和可选stdin租约；步骤224将停止轮询与租约接入真实生成器；步骤225已实现跨机协调器、远端监督及数据库操作入口。工具实现与正式验收分开：014/schema22的完整30分钟窗口、全程部署锁和异常收口仍须取得本轮实际证据，不能因组件通过就宣称准入。被测应用及资源配置不因这些操作工具而改变。

`load-fixture.mjs verify` 继续只接受明确拥有的合成公司/项目/用户及有界UUID清单。每轮使用新的RR只读事务，同时检查三张表的总数、原始事件唯一ID和双向精确ID集合、dedup的stored状态、DLQ为0，以及聚合任务的processed_at、无错误和processed_epoch等于项目当前privacy_epoch。可见桶总数须等于原事件数；这不是逐bucket或完整payload证明。桶表/可见view缺失拒绝成功，不再跳过聚合检查。

尚待完成或旧/NULL epoch的任务允许按原15秒重试预算重新聚合；每轮先ROLLBACK，等500ms，再建立新快照读取真实Worker提交。15秒不是任意锁/调度/文件系统情况下的总进程退出上限；SQL另有5秒statement及200ms lock时限，外层仍须监督原进程的真实终态。对账成功不证明Kafka位点、客户端已退出、未知发送已消除或允许清理fixture。

`load-first-stop.mjs` 的 `publishFirstStop(path, reason)` 仅接受resource_safety/operator_requested。完整UTF8内容在首次await前固定；同目录临时文件wx→实际write/fsync/close→排他hardlink发布，已有目标绝不替换、读取或删除。返回published或already_latched，后者只表示存在，不认证内容/类型。只有确认属于自身且inode不变的临时文件可清理；发布后错误报告不确定并保留最终目标。POSIX同步父目录；Windows没有此处的可移植目录fsync，不作掉电保证。调用方必须拥有稳定私有父目录；静态别名拒绝不代表防御敌对并发目录替换。操作示例见CAPACITY的步骤170。

`load-control-lease.mjs` 的 `attachControlLease({input, runId, onAbort, timeoutMs=5000})` 仅用于独占、未解码的可读pipe。它返回ready和close：首次合法新序号前ready不成功，随后onAbort仍然有效；调用方不能把一次ready当成永久准入。runId为16–128位ASCII字母/数字/下划线/横线。每帧必须精确为如下顺序的规范JSON及LF，总长最多256字节：

```json
{"v":1,"run_id":"EXAMPLE_RUN_000001","seq":"1"}
```

seq为1到9223372036854775807的规范十进制字符串。严格增大才刷新本地performance.now期限；重复序号不续期，倒退、错run、额外/重复字段、非规范字节、超长、EOF、输入错误及本地到期均锁存首个观察故障。runId只是对应标签，不认证来源；协调器须只在核对本轮真实远端状态后推进心跳，不能仅按本地定时器续命。

close/故障移除自身timer/listener并pause专用输入；原生pipe可用时unref，不destroy调用方stream或操作内部_handle。错误回调自身抛出时，用固定安全错误终止，不吞异常后让工作负载继续。租约模块本身不读写manifest/文件或发送网络请求。

生成器现在按 `CCPANEL_LOAD_RUN_ID` 是否存在明确选择stdin_monotonic_lease/unleased；空值/无效标签拒绝，不默默退回无租约。启用后固定5000ms，本例不提供可放宽时限的环境项；初次ready前不登录，有预存stop时也不登录。两种模式均记录control的mode/run_id/lease_ms，以及实际加载的四文件SHA（生成器、普通证据发布、租约、安全stop读取），未启用租约的旧用法明确标记unleased，不能作为正式有租约窗口。

`load-stop-reader.mjs` 仅初次lstat ENOENT表示无请求。其他路径/打开/读取/关闭不确定性统一invalid_stop_request：实际正规文件、支持时NOFOLLOW/NONBLOCK、1024字节上限/完整EOF、path与fd各自前后dev/inode/size/mtime/ctime稳定，Windows跨path/fd只允许已验证的非零同inode。单reason字段、两种值的JSON可含合法转义/ASCII空白，重复/额外字段、BOM/坏UTF8、链接/FIFO/替换/增长截断均拒绝。稳定nlink2目标可读；发布器收尾unlink会变ctime，合法并发发布可能保守误拒，不能声称零误拒或敌对目录原子快照。

独立1秒单在途stop轮询从登录前持续到generation、每页visibility及最终原请求settlement结束，首次观察只输出一条固定字段诊断。lease故障映射control_lost并只保留通用control_code；不会用诊断写后台manifest。ID完成实际manifest fsync后、加入发送队列前再次检查，POST和各请求入口也检查已锁存停止状态。收到停止不缩短原请求的12/15秒时限；最终等待原sender/observer/query/progress，清理轮询/lease与已跟踪timer后，由外层观察原进程真实close。文档中的5秒只为健康本地观察范围，不是任意网络/FS/OS情况下的进程退出上限。结果/进度输出位置以lstat检查，已有文件/目录/悬空链接在manifest/登录/请求前拒绝，原条目不删除。

验证分开记录：真实PG回归制造同数量但A缺失/B两行的原始事件陷阱、failed dedup、缺表/缺view、旧/NULL epoch，独立观察子进程ROLLBACK后调用实际aggregateNext提交，再由原进程自然成功；另未修复NULL epoch完整等预算后拒绝。首次停止发布器使用真实子进程竞争和委托原生文件系统故障，Windows12项/Linux14项；Windows文件symlink需权限，该平台只声明junction拒绝，Linux另覆盖symlink与目录sync。租约在Windows/Linux各30项，真实pipe、时钟与子进程覆盖重放/错误/到期/关闭/回调错误；硬杀真实coordinator后，detached孤儿自行写fsync的abort及exit-handler回执。该孤儿没有独立原生终态句柄观察，报告明确orphan_native_terminal_observed=false，不拿回执代替完整客户端close证明。

本地失败测试也保留记录：Windows仅pause导致pipe仍引用事件循环，首次watchdog失败后改为公开unref；非detached孤儿未证明、native listener基线断言及Linux命令引用错误均未计为通过。文件身份比较只针对Windows实际fstat/lstat的dev差异采用非零inode核对，路径后续仍按dev+inode识别自身文件。

步骤224在实际原生成器/原子发布器/本机HTTP上联调，测试adapter只改写合成HTTPS URL到loopback HTTP，保留原生fetch/AbortSignal与实际fsync-before-POST。最终Windows12场景/Linux14场景均由原ChildProcess close观察自然退出：租约正常完成且父pipe不关闭、ready前零登录、EOF/固定5秒到期、慢登录、原生POST12秒、分页drain及最终trend15秒、401清理、预存输出拒绝；Windows文件symlink权限不足明确未测，Linux实际悬空result/progress均测。CI41521270593三job成功，Linux原有accounting/Posix信号也执行通过，另47安全读取检查。显式stop/EOF本地观测最大值在最终CI为971ms，是一次测量；整套未测试TLS/SSH/VPS或完整bridge，也未在该集成用例硬杀coordinator，不能与上文独立租约孤儿用例合并扩大范围。见evidence/generator-controls-224.json。

## 步骤225：已实现的编排与尚未通过的正式窗口

`run-capacity-window.mjs`读取私有SSH配置并使用固定命令；秘密fixture经独立artifact管道写入私有目录，不进入日志。`capacity-window-remote.py`监督准备、资源检查、原采样器及收口；数据库CLI提供baseline、create、status、revoke、verify、cleanup六命令，分别冻结原状态、创建夹具、读取状态、撤key、对账及有条件清理。不依赖ignored脚本，也不启用备份目的地。

Node保存原SSH与原生成器的ChildProcess句柄，只有实际close才记录退出码/信号。正式初始化诊断之后，还要读取同一child已同步的manifest，核对PID、四文件原始SHA及租约参数，才发送首帧。随后每次续租都须匹配新nonce/序号和同一远端原生所有者；旧回复、换实例、断连或超时不会续命。生产child去除NODE_OPTIONS注入并强制TLS验证；测试工厂的结果明确不代表实际SSH/TLS。

原场景保持1EPS、约1KiB、趋势10秒和1800秒。采样器固定1980秒，不因等待或失败重开、加时；1940秒时若还未收到客户端终态就触发覆盖余量保护。客户端结束后最多等210秒，确认同一采样器自然退出、主机帧覆盖本次终态接收时刻，才允许大上传与数据库收尾。2700秒总预算到期触发停止，不能当作已观察全部进程关闭；停止不缩短原12/15秒在途请求时限，也不把30秒drain循环当作进程硬上限。

远端用PID1 FDStore保存flock的同一打开文件描述，经BARRIER确认，由同Invocation的StopPost最后移除。私有probe.lock验证不代表正式全程deploy.lock通过，也不外推宿主机、PID1或StopPost自身死亡的保证。只停止证明属于本run的采样器、操作容器并清理临时env，不停止四个产品服务。

成功删除夹具要求本轮原客户端明确完成、实际退出0且无信号/watchdog，原始结果与回执绑定，稳定ID在raw/dedup/jobs中精确对应，stored状态、当前privacy epoch及可见桶总数正确。撤key后再核对PG/Kafka逐分区边界，在privacy排他事务中清理并复核原业务。未知发送、强杀、中止、缺失回执或屏障不满足时，只撤key并隔离保留项目，冻结记录cleanup=deferred；一次lag0或等待60秒不证明永无迟到。

冻结按26种固定role核对唯一角色、大小及SHA，清单按存在文件列出。秘密fixture、freeze自身及未来controller StopPost回执不进入清单。客户端原文件、DB回执和wire消息绑定同一次实际close；SSH最终close另行观察，不由freeze替代。失败原文件保留，不能覆盖、重排或改baseline补绿。Docker挂载按固定字段投影排序，数组换序不算漂移，真实变化仍拒绝。

最终f2源的三次私有探针已观察：正常原SSH close0、37次新鲜回复、同一10秒采样器自然完成；EOF在preparing因bridge_stale中止，SSH close0、16次回复；MainSIGKILL得到原SSH close255及同Invocation的signal/9终态，保留failed部分冻结。后两次采样器被停止，均不算完成。三次StopPost清理中FDStore仍持锁，最后明确移除至0，独立私有flock可得；标准化runtime前后原字节相等，HTTPS ready。这仅证明私有锁/探针路径，未运行正式协调器、DB、客户端或全程deploy.lock。[CI41538313510](https://github.com/Explainfuture/ccPanel/actions/runs/41538313510) 对f6f7dc9的三项任务全部通过：527单测/9跳过、21套真实PG及Kafka联合检查，含1835条消息与1800个唯一事件。原生本地remote25项、coordinator17项与VPS私有探针分开记账，原始SHA见 `evidence/capacity-window-controls-225.json`。

工作负载、资源、精确ID、原业务与四历史政策报告仍须离机分别核验，全机预算包括监督单元及Docker操作容器的开销，产品四容器指标另列。

保持200MiB安全余量、CPU/Swap及正常采集/保留要求。当前没有正式30分钟通过、来源认证、连续可用、HA或恢复准入结论。用户暂无独立备份目的地，备份配置仍默认关闭；这些控制工具不将可配置方案变成已激活的同步或恢复能力。
