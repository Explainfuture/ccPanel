# 试验机容量验收

本报告针对单机试验环境，不代表生产吞吐承诺。最终结果以完成的测试记录为准，运行中的快照不计为通过。

## 场景与运行方式

PRD 首轮基线：持续 30 分钟，每秒生成一个约 1 KiB 的事件，每 10 秒查询一次趋势，每 50 个事件额外重发原 ID。使用临时独立公司、用户、项目和写入密钥，结束后删除夹具，不污染业务数据。

在已部署的发布目录运行：

```sh
cd /opt/ccpanel/current/infra
docker compose exec -T api node scripts/load-check.mjs 1800
```

同时在宿主机另行采样 `vmstat 10 181`、`docker stats --no-stream`、`df -h`，测试前后比较容器重启次数、OOM 状态。压力生成器运行在 API 容器内，报告应注明其额外开销；此测试不覆盖客户端公网延迟。

API 容器内 `/tmp/ccpanel-load-progress.json` 为最近一分钟快照，`/tmp/ccpanel-load-result.json` 为最终对账。结果不含凭证。运行前删除或核对旧结果时间，避免误读此前测试。

## 测量口径

- 接入耗时：单次 HTTP 请求开始到 Kafka 确认后的 202 响应，包括重复请求。
- 可见耗时：从生成事件到轮询发现 PG 去重记录；原始事件与去重记录同事务提交。轮询约每秒执行，因此含采样间隔，不能当作精确的 Worker 执行耗时。
- 查询耗时：趋势接口 HTTP 响应耗时，查询随本次事件数量增长；未模拟大历史数据或复杂漏斗。
- 对账：原始事件、去重记录、聚合任务均应恰好等于生成的唯一事件数，DLQ 为零；已确认数、待重试数和待观察数单独列出。

脚本检查健康场景接入 p95 < 1 秒、PG 可见 p95 < 10 秒、没有错误和未清空积压。无 OOM/重启、无持续 swap 换入换出、磁盘保留至少 20% 由宿主机证据另行核实，不能只看脚本 `passed`。

## 2026-10-23 首轮结果

配置为 1 vCPU、约 961 MiB RAM、1 GiB swap、20 GB 磁盘，Kafka 单副本。后端冻结为 m4-recovery-001，测试期间有静态验收页及下一版发布包上传，未切换后端、执行迁移或安装依赖。压力生成器与服务共享 API 容器资源。

| 指标 | 结果 |
|---|---:|
| 持续时间 / 目标流量 | 1800 秒 / 1 EPS |
| 唯一生成 / 已确认 / PG 原始事件 | 1800 / 1800 / 1800 |
| 去重记录 / 聚合任务 | 1800 / 1800 |
| 同 ID 额外重试 | 35 次 |
| DLQ / 错误 / 最终积压 | 0 / 0 / 0 |
| 接入 p95 | 53.33 ms |
| PG 轮询可见 p95 | 1065 ms |
| 趋势查询 p95 | 231.05 ms |
| 容器重启 / OOM | 均为 0 |
| 磁盘占用 / 可用 | 27% / 约 14 GiB |
| 180 次 vmstat 的平均 CPU 空闲 | 75.79% |
| 有 swap 活动的采样 | 54 / 180 |
| 连续 swap 活动最长时段 | 100 秒 |
| swap 换入 / 换出最大值 | 120 / 4908 KiB/s |

结论：事件闭环、幂等和本场景延迟目标通过；**整体资源门槛尚未通过**。出现重复 swap 活动，不能凭无 OOM 推断内存充足。应将生成器移到独立机器后复测，必要时减少非必要内存开销或升级 RAM，不降低 Kafka 成功确认标准。不得据此提升负载或宣称生产 SLA。

服务器证据保存在受限共享目录 `load-baseline-20261023.json` 与 `load-vmstat-baseline-20261023.log`。测试结束后已清理临时公司夹具，再部署新版本；后续版本需单独记录测试条件。

独立生成器复测及资源门槛通过后才能分级增加流量；还需完成异机备份恢复、故障窗口强杀、磁盘满和浏览器矩阵，不能把链路对账通过表述为整套生产验收通过。

## 独立生成器复测流程

使用 `scripts/load-fixture.mjs create` 在 API 容器创建临时公司，将输出直接重定向到权限 600 的私有文件，不在终端展示凭证。通过已校验主机指纹的 SFTP 取到本机，再运行：

```sh
node scripts/load-check-public.mjs PRIVATE_FIXTURE_JSON PRIVATE_RESULT_JSON 1800
```

本机经 HTTPS 请求真实接入、事件列表和趋势 API，延迟包含公网往返；它与首轮容器内的延迟不能直接当成同一口径比较。结果包含原始 event_id 列表，不包含密码或写入密钥。将结果回传到当前发布的 scripts/ 受限文件，再用夹具工具的 verify 模式与 PG 逐 ID 对账，最后 cleanup 清理专用公司。服务器另行记录 vmstat、OOM/重启和磁盘。

首个公网运行的生成/观察串行，受网络往返影响，61 条耗时约 65 秒，因此作为诊断结束，不计为正式容量结果；其采样重定向缺口也保留记录。修复后独立计时生成，发送、可见性观察和趋势查询各自最多一个在途；时钟最大偏差超过 1 秒直接判失败。10 条试跑最大偏差 14 ms、全部 ID 对账通过。新公司已重新启动 1800 秒正式测试，资源采样在生成前启用。资源门槛仍为待验证状态。

## 2026-10-23 独立生成器正式结果

UTC 17:29:36.169–17:59:36.524，后端固定 m4-query-jobs-001，Windows 工作站独立计时生成，全部流量经受信任公网 HTTPS，测试期间没有部署或迁移。

| 指标 | 结果 |
|---|---:|
| 时长 / 生成 / 确认 / PG 逐 ID 匹配 | 1800 秒 / 1800 / 1800 / 1800 |
| 原始 / 去重 / 聚合任务 | 1800 / 1800 / 1800 |
| 同 ID 额外重试 | 35 次 |
| 错误 / DLQ / 最终积压 | 0 / 0 / 0 |
| 生成时钟最大偏差 | 19 ms |
| 公网接入 p95 | 241.88 ms |
| 经事件 API 观察可见 p95 | 2690 ms |
| 公网趋势查询 p95 | 830.27 ms |
| 容器重启 / OOM | 均为 0 |
| 平均 CPU 空闲 / 磁盘占用 | 79.84% / 27% |
| 有 swap 活动的采样 | 101 / 180 |
| 连续活动最长时段 | 120 秒 |
| swap 换入 / 换出最大值 | 1060 / 2 KiB/s |

结论：独立生成器证明提供速率和逐 ID 数据闭环、幂等与延迟目标通过；**资源门槛仍未通过**。本轮主要为换入、换出很少，不能把任何 swap 活动都解释为同等程度的内存抖动，但持续换入仍需分析、调整和复测，不因去掉生成器就自动宣称内存达标。保留现有确认标准，不提高流量。

180 次宿主机采样在生成前启用，采样结束与生成尾部有几秒边界差异；最终全生命周期 OOM/重启状态另行核实。测试后将全部原始 ID 交由 PG 对账，并已清理临时公司。证据位于本机 `.local/load-public-clocked-result.json` 和服务器 `load-public-clocked-baseline-20261023.log`；当前新版后端需另行测试。

## schema10 新版复测（已结束，资源门槛未通过）

2026-10-23在m4-integrity-001/schema10启动同一1EPS/1800秒公网独立生成器与每10秒趋势查询；结果文件为工作站`.local/capacity-integrity-result.json`，运行期间保持发布版本固定。此处仅记录已启动，不将旧版结果套用为新版容量。

新增有限时长只读采样脚本capacity-sample.py，每10秒保存持续宿主机遥测与四个固定容器的cgroup memory.current、memory.events、cpu.stat；不读取进程参数或环境，不逐轮运行Docker CLI。采样器从负载前开始、计划运行1980秒，32MiB/CPU5%上限，首轮实际MemoryCurrent约6.18MiB；观察器开销计入同机资源，不从报告中扣除。容器ID与重启/OOM起始状态另存受限基线，结束仍需对比。

启动时服务ccpanel-capacity-integrity-20261022210155已确认active/running，首5帧有效；证据目录为服务器`/opt/ccpanel/shared/capacity-integrity.dMizyM`。采样过期、cgroup缺失等写错误帧而非伪造零值。最终已确认生成器退出0、采样器自然结束inactive/dead且Result=success/ExecMainStatus=0；没有依据文件存在推断成功。

最终资源分析使用scripts/capacity-report-cli.ts：输入完整samples.jsonl、公网load结果、基线、结束metadata及一个不存在的输出文件。基线可为`{release,containers}`；本次早期采样基线是容器字典，结束metadata需从原始release.txt读取releaseStart，并带当前release和containers（id/restartCount/oomKilled/startedAt）。不能手填推断版本或只读取最后一个OOM状态代替生命周期计数。

报告须同时包住负载开始/结束的捕获时间和真实宿主机计数时间，保留边界区间的少量额外观察。缺失/过期/错误帧、超过20秒捕获间隔、计数器回退、版本变化、容器替换均使evidenceValid=false；重复宿主机样本不重复累加。输出加权CPU空闲、各容器内存峰值、最小磁盘余量、实际swap字节与速率/最长连续活动、OOM和重启。conservativeResourcePass只在证据完整且无swap活动等条件时给出充分保守通过；有swap活动必须单独评估PRD的持续性要求，不据“swap已占用”直接判定抖动，也不自行改写验收标准。此报告不替代逐事件ID和DLQ对账。

### 新版最终实测

UTC 2026-10-22 21:02:08.030–21:32:08.038，1800秒，固定m4-integrity-001/schema10。构建与流量生成在工作站，服务器包含有限资源采样与少量SSH只读运维/文件暂存开销；未部署、更改配置或在窗口内启动额外数据库测试。资源分析取负载前一帧至源计数覆盖结束的首帧，共183帧，均有效，捕获与宿主机时间覆盖完整，evidenceValid=true。后续数据库验证不在该窗口内。

| 指标 | 结果 |
|---|---:|
| 生成 / 确认 / PG逐ID匹配 | 1800 / 1800 / 1800 |
| 原始 / 去重 / 聚合任务 | 1800 / 1800 / 1800 |
| 额外重复请求 / DLQ / 最终积压 / 错误 | 35 / 0 / 0 / 0 |
| 生成时钟最大偏差 | 16ms |
| 公网接入p95 / 可见p95 / 趋势查询p95 | 235.64ms / 1475ms / 717.47ms |
| CPU空闲 / 最低可用内存 | 51.01% / 266.92MiB |
| API / Worker / Kafka / PG memory.current采样峰值 | 69.31 / 35.87 / 222.28 / 66.92MiB |
| 最低磁盘可用比例 | 64.46% |
| 容器替换 / 重启 / OOM事件 | 0 / 0 / 0 |
| swap换入 / 换出总量 | 95.71MiB / 173.32MiB |
| 出现swap活动的有效增量区间 | 159 |
| 最长连续swap活动 | 1293.76秒（约21.6分钟） |
| swap换入 / 换出最高速率 | 2305.14 / 10099.91KiB/s |

链路、去重和延迟通过；**资源门槛未通过**，持续swap活动明确存在，不提高流量或宣称稳定容量。memory.current峰值是10秒观察到的采样峰值，不是内核memory.peak全生命周期峰值。结束后仅按已验证测试公司清理数据与账号，原有关系指纹完全保留，HTTPS正常。

首轮PowerShell时间转换丢失UTC偏移和毫秒，导出时间偏差8小时38ms，被CLI的窗口一致性校验拒绝，没有产生错误通过报告。保留初始错误导出，改为直接读取负载JSON中的UTC completed_at；确认初始183帧本来就覆盖真实UTC窗口，再正确导出并核对摘要。工作站证据为`.local/capacity-integrity-result.json`、`capacity-integrity-resource-report.json`与相应快照/状态文件，服务器保留同目录证据。端上与服务器测试凭据已移除。

窗口结束后的只读进程快照显示Kafka Java进程及fwupd有较多换出页，swappiness=60；这只是后续定位线索，不足以归因全部持续换页。后续需评估系统服务用途、JVM与内核内存策略，调整后重新测量；不通过关闭WAL/fsync、跳过确认或改写验收标准换取表面通过。

## swappiness10 专用KVM配置复测（运行中）

完成PROGRESS步骤69的环境核实与可回退配置后，固定同一m4-integrity-001/schema10，以同一工作站生成器启动新1800秒/1EPS复测。服务为ccpanel-capacity-20261022214411，证据目录`/opt/ccpanel/shared/capacity-integrity.Hivo2c`；工作站结果目标`.local/capacity-tuned-result.json`。开始时实际服务active/running、首两帧有效，生成器已启动；尚无最终资源结论。

本轮起始基线额外保存swappiness、页大小/总内存、相关unit状态、四个容器memory.max/memory.swap.max/cpu.max，以及API/Worker/Compose文件摘要。结束快照重新独立采集，报告按排序后的JSON内容比较；缺失或变化使资源证据无效，不因键顺序不同误报。配置读取不包含环境、进程参数、数据库URL或完整Docker配置。启动脚本拒绝已有运行中的容量服务或残留测试公司，生成新目录，不重用上次数据。

109项测试与类型检查通过，包括配置变化/缺失及CLI实际接线；Python与远程bash语法检查通过。结束时必须使用新版capacity-report-cli（本地已构建为`.local/capacity-tuned-report.mjs`），避免旧产物缺少配置核验。本轮不变更部署、限额或内核配置，不启动额外数据库测试；原未通过结论保持到实际完成和重新对账。

## 内存配置调整后的完整复测（2026-10-23）

UTC21:44:22.648–22:14:23.187，实际窗口1800.539秒，仍为m4-integrity-001/schema10，1EPS、约1KiB事件、每10秒趋势查询。内存配置与服务状态在起止独立核验一致，evidenceValid=true、configurationChecked=true，183/183窗口帧有效、无采样缺口；生成器实际退出0后读取原始UTC结果再导出，无中断或重启负载。

| 项目 | 实测 |
|---|---:|
| 生成 / 确认 / PG唯一ID / 去重记录 / 聚合任务 | 1800 / 1800 / 1800 / 1800 / 1800 |
| 重复请求 / DLQ / 请求错误 / 最终积压 | 35 / 0 / 0 / 0 |
| 接入 / 可查询 / 趋势查询p95 | 217.34 / 1437 / 647.75ms |
| 最大调度延迟 | 18ms |
| CPU空闲 / 最低可用内存 | 79.25% / 311.94MiB |
| API / Worker / Kafka / PG观察内存峰值 | 75.43 / 34.80 / 212.70 / 93.42MiB |
| 最低磁盘可用比例 | 64.43% |
| 容器替换 / 重启 / OOM | 0 / 0 / 0 |
| Swap换入 / 换出 | 24.85 / 0.90MiB |
| Swap活动增量区间 / 最长连续活动 | 102 / 120.61秒 |
| Swap换入 / 换出最高速率 | 803.18 / 84.46KiB/s |

链路、去重、数据对账与延迟通过；Swap明显减少但仍连续活动，**资源门槛仍未通过**，conservativeResourcePass=false。保持试验预览，不因优化有效就放宽PRD或承诺生产吞吐。未采用swapoff、清缓存或降低Kafka确认标准。该窗口未运行schema11–15或聚合处理器，不能把结论外推到新功能。

证据保留于服务器`/opt/ccpanel/shared/capacity-integrity.Hivo2c`及工作站`.local/capacity-tuned-{result,resource-report,baseline,final}.json`、`capacity-tuned-samples.jsonl`；不可变结束快照185帧，报告选择其中183帧覆盖完整窗口，SHA256及结束时间一致。逐条verify后仅清理本次合成公司/项目/账号，两端夹具凭据已删除，原业务关系指纹与升级前一致，消费进度检查通过，HTTPS就绪。完整窗口导出后才运行新的隔离PG验收；采样器按原有限时配置自行结束，另核对最终状态。

UTC22:17:19实际复核本轮有限采样器为inactive/dead、Result=success、ExecMainStatus=0；它按原33分钟配置自行退出，无人为停止或重启。

## schema16活动聚合基线（已完成，资源未通过）

新独立运行使用m4-dashboard-001/schema16，label=schema16-active-aggregation，固定配置仍为swappiness10与相同容器限额。采样器ccpanel-capacity-20261022231949，证据目录/opt/ccpanel/shared/capacity-integrity.HMjyru；生成器输出.local/capacity-schema16-result.json。夹具初始化容器先退出，再等待两份新宿主机帧后开始1EPS/趋势10秒/30分钟负载；本轮包含实际聚合处理器，结尾将额外核对已处理任务及桶计数。以下记录本轮实际完整结束对账及资源分析。


UTC2026-10-22 23:20:21.065至23:50:21.076，实际1800.011秒，生成器原会话正常退出0。未经修改的原JSON上传并冻结187帧样本，报告选择183/183有效帧完整覆盖窗口，SHA与结束UTC毫秒一致，evidenceValid/configurationChecked均true；运行期间配置/实例未替换。

| 指标 | 实测 |
|---|---:|
| 唯一事件 / 额外重复确认 | 1800 / 35 |
| raw / dedup / 聚合任务 / 已处理聚合 | 1800 / 1800 / 1800 / 1800 |
| 可见桶事件总数 / DLQ / 未可见 / 剩余积压 / 错误 | 1800 / 0 / 0 / 0 / 0 |
| 接入 / 端到端可见 / 趋势查询 p95 | 228.99 / 1448 / 642.74 ms |
| 最大生成调度延迟 | 17ms |
| CPU空闲 / 最低可用内存 | 77.21% / 291.25MiB |
| API / Worker / Kafka / PG观察内存峰值 | 57.01 / 31.55 / 248.59 / 90.43MiB |
| 最低磁盘可用比例 | 64.28% |
| 容器替换 / 重启 / OOM | 0 / 0 / 0 |
| Swap换入 / 换出 | 15.46 / 3.00MiB |
| Swap活动增量区间 / 最长连续活动 | 117 / 451.41秒 |
| Swap换入 / 换出最高速率 | 507.31 / 282.02KiB/s |

链路、重试去重、唯一ID及活动聚合对账通过；持续Swap仍存在，**PRD资源门槛未通过**，conservativeResourcePass=false。不得以较低换入总量忽略更长连续活动，也不将单机实验外推为生产高可用或高并发承诺。本轮没有schema17保留/分区维护，后续版本不能直接沿用其资源结论。

原始证据在服务器/opt/ccpanel/shared/capacity-integrity.HMjyru，以及工作站.local/capacity-schema16-{result,resource-report,baseline,final}.json和capacity-schema16-samples.jsonl。结束快照冻结后才运行verify/cleanup；1800个稳定event_id全部匹配，活动聚合任务与可见桶1800、0DLQ。仅删除本次合成公司/项目/账号及两端凭据，所有原关系旧字段/数量指纹与升级前一致，唯一预期变化为全局consumer_progress（包括前次4条UI夹具及本轮1835条Kafka记录）；当前next_offset合计10713，实际进度完整性检查通过、HTTPS就绪。UTC23:53:02实际复核原有限采样器inactive/dead、Result=success、ExecMainStatus=0，按原33分钟配置自然结束，未人为停止或重启。

## 2026-10-23：schema17 / m4-telemetry-auth-004

固定上述预览release和资源配置，物理保留开启，1EPS、720字节padding的约1KiB合成事件，同时每10秒尝试一次趋势查询（单在途）。原负载会话71955实际退出0；UTC02:09:30.374至02:39:31.063，共1800.689秒，1800个唯一ID全部确认且可见，35次重复请求；最终backlog/not_visible/errors均0，最大生成调度延迟16ms。程序未记录精确查询执行次数或实际平均请求字节，故不声称完成180次查询或精确平均字节。

采样器ccpanel-capacity-20261023020816有限1980秒，初始文件观察超时后核对实际进程active并手工取得两个新鲜基线，沿用同一进程，没有重启或修改窗口。冻结原始189帧，正式窗口选取183/183有效帧，SHA校验和配置前后核对通过；evidenceValid/configurationChecked均true。工作站同期有源码/本地UI验证工作，未做整机静默隔离；服务器配置与服务未改，生成调度延迟如实记录，不作跨版本因果比较。

| 项目 | 实测 |
|---|---|
| 接入 / 可见 / 趋势查询p95 | 324.16 / 3117 / 842.67 ms |
| raw / dedup / 完成聚合任务 / 可见桶贡献 | 各1800 |
| 三阶段原始ID集合匹配 / DLQ | 各1800 / 0 |
| CPU空闲 / 最低可用内存 | 70.35% / 251.46MiB |
| API / Worker / Kafka / PG观察内存峰值 | 61.59 / 31.26 / 257.00 / 91.75MiB |
| 磁盘最低可用 / 容器替换或重启 / OOM | 64.02% / 0 / 无 |
| Swap换入 / 换出 | 30.62 / 16.64MiB |
| Swap活动区间 / 最长连续活动 | 73 / 88.36秒 |
| Swap换入 / 换出最高速率 | 1517.86 / 1430.85KiB/s |

**数据链路对账通过；保守资源验收未通过**：conservativeResourcePass=false、swapReviewRequired=true。没有放宽原门槛，不能凭无OOM、较短连续活动或接收延迟达标宣称生产就绪/高并发。当前报告仅此release，未部署的host资源缓存/UI不在测量内；持久监控、完整浏览器矩阵、离机恢复/RPO/RTO和生产HA仍待完成。

冻结资源证据后，使用通过6套PG控制器的新版只读验证器直接匹配raw、dedup及aggregation_jobs的1800个ID，已处理任务及桶贡献1800。集合SHA256为864231f0fa9344312425433eb5e6bcb120f85212861de44e187b039d5b8ba40b；另替换一个请求ID，实机验证1799/1800明确失败，没有改数据库记录。原始证据：/opt/ccpanel/shared/capacity-integrity.IusWZe；工作站.local/capacity-auth121-*。

清理后所有原表指纹一致，只有消费进度按1800正常+35重复推进：p0=6257/p1=6304，总12561，与Kafka committed/high一致、lag0。两端临时凭证、合成PG公司/用户/事件/聚合已清除，临时DB0；Kafka合成消息按保留期存在。门禁open、完整性检查通过、HTTPS ready，采样器实际inactive/dead、Result success、ExecMainStatus0，自然结束。
