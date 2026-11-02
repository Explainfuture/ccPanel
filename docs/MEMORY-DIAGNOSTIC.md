# 预览内存诊断

`python3 scripts/memory-diagnostic.py`在Linux宿主机只读检查固定四个ccPanel容器的cgroup v2，以及宿主机/proc。需要读取Docker状态和进程统计的权限。仅使用docker inspect的PID投影，不读取环境变量、完整inspect、命令行参数、事件或数据库凭证。

输出包含宿主机MemAvailable/Swap等KiB瞬时值、累计pswpin/pswpout/pgmajfault、内存PSI，以及各容器memory.current/max/high/swap.current/swap.max、选定memory.stat、memory.events和PSI。进程只显示PID、comm名称、RSS/Swap KiB，按Swap取前20。进程RSS和VmSwap可能包含共享页，不能简单求和当宿主机物理内存。累计计数要在同一实例边界求差；Swap占用不等于当前持续换入换出，major fault也不一定来自Swap。

容器停止、非cgroup v2或文件不可读会明确失败，不填0冒充正常。该工具是诊断，不能替代有固定版本/容器/配置、时间窗口、稳定事件ID对账的容量验收。

2026-10-23 UTC02:06:27，m4-telemetry-auth-004/schema17的一次实际快照：宿主机MemAvailable329704KiB，Swap占107452KiB；API/Worker cgroup Swap0，PG约4.58MiB，Kafka20.37MiB。进程Swap较大者为Java20.25MiB、fail2ban12MiB、自动更新9.13MiB、dockerd8.05MiB、containerd7MiB。其余宿主机进程也占Swap；不能仅把宿主机全部Swap归因Kafka或把各PG进程重复相加。

四容器memory.events max/oom/oom_kill均0，当前内存压力很低；紧邻一次vmstat的四个1秒增量样本si/so均0。首行vmstat是开机累计平均，不当作当前速率。**这些短快照不推翻schema16的30分钟容量Swap失败，也不证明最新版本容量通过。**本轮未修改Swap、内存/heap、复制、确认或服务参数；不执行swapoff、drop_caches或停安全更新来制造结果。

原始快照：服务器/opt/ccpanel/shared/memory-diagnostic120.json及工作站.local/memory-diagnostic120.json。下一步在最新版本固定配置做一次正式30分钟测试；保持原PRD门槛，期间不部署/故障注入或调整参数，根据实际结果决定后续资源方案。

## 步骤143：schema19冻结窗口诊断

同一份capacity-integrity.HfanpA冻结样本新增诊断字段，不修改原报告、窗口或门槛。CPU加权分项（沿用两位截取）user17.98%、system6.97%、idle74.67%、iowait0.28%、steal0.07%。四容器memory.events high/max增量、CPU nr_throttled/throttled_usec增量均0。没有捕获容器限额触发或CPU限流证据，不能仅凭Swap结果就认定需调高容器上限；这也不否定宿主机换页或其他压力。

capacity-report现在返回cpuBreakdownPercent及containerPressureDeltas；可选计数任何帧缺失就返回null，不填0。可选计数在窗口内回退使证据无效，新增诊断在无效窗口全部null。分项使用完整区间累计差，不平均每帧百分比；诊断字段不放宽既有保守资源门槛。增强报告在.local/capacity-alerts139-diagnostic-report.json，原capacity-alerts139-resource-report.json保持原样，conservativeResourcePass仍false。

窗口外UTC04:45:53只读快照：可用307636KiB、Swap占226552KiB；cgroup Swap API4.16MiB/Worker0/PG7.42/Kafka132.14。Java进程VmSwap118784KiB，其RSS含共享页；不能与cgroup数强行相等或将全部宿主机Swap归因Kafka。该快照晚于正式窗口，且发生在集合验证/清理之后，不定位窗口内Swap I/O来源，也不证明减heap或改内存限制有效。服务器配置、内存/heap/确认/副本和安全服务均未改，未重跑同配置追求绿色。快照保存在服务器/opt/ccpanel/shared/memory-diagnostic143.json和本机.local/memory-diagnostic143.json。

第二次快照143b距首次261.718秒，后台采集/维护仍运行，称低业务活动观察而非严格空闲对照。宿主机pswpin增28页、pswpout0、pgmajfault29；cgroup major fault API4、Worker0、PG0、Kafka17。相关字段存在且非递减，容器id/startedAt/restartCount重新核对与冻结边界一致。major fault可能含文件页故障，采集本身和宿主机其他进程也有开销，不能将这些数直接等同Swap I/O或外推正式30分钟归因。143b同名文件保存双方，增强diagnostic-report已另存原容量证据目录，原冻结文件和资源结论未改。

## 步骤220：当前013连接与JVM复核

2026-11-02 UTC16:00只读快照（本地已为11月3日）：可用317152KiB，PG memory.current113086464字节，其中file97202176/anon8867840、swap8163328；Kafka current215937024、swap191565824。这些是不同维度的瞬时值，不能把PG后端共享RSS或JVM堆数字直接相加，也不把Swap占用当当前活动。受限原始文件位于服务器memory220.BOLInH和工作站.local，不发布查询文本、连接串或进程环境。

按实际容器地址关联pg_stat_activity，再以PG命名空间PID读取smaps_rollup：修正host(client_addr)前的首次标注未匹配IPv4的/32后缀，只用修正后的第二份作角色判断。该次API3个连接、Worker3个；其中API两个业务后端Private_Dirty分别1652/1668KiB。代码的每进程max4池中还须保留恢复租约；Worker存在嵌套持连接的消费路径，不随意缩池。错开三个API轮询可能只减少数MiB私有页，不能解释全部Kafka换出量或证明容量通过，收益须再以实际运行器对照测量。

固定PerfData字段另外读到Serial young/full collection pauses，确认实际收集器为Serial；没有读取VM参数/环境、附加JVM或触发GC。数字计数为young2605/full95，nmethodSize35423456/code23416056、老年代used105945536字节；used不是完整GC后的存活量，不能据此确定可削减的堆。官方4.1.2 [Docker launch](https://raw.githubusercontent.com/apache/kafka/4.1.2/docker/jvm/launch)先加入CDS参数，因而[run-class的空选项默认G1分支](https://raw.githubusercontent.com/apache/kafka/4.1.2/bin/kafka-run-class.sh)可能不应用；这解释了不能仅看通用脚本默认值判断运行时GC，但不声称已核查全部启动环境。未修改JVM/容器限额、Swap或安全服务，也未启动新的容量负载。

随后20.007秒低业务活动观察（非受控空闲）绑定同一四容器ID/启动/重启与主机实例边界，五次MemAvailable读数313196/312828/312692/314060/313952KiB，累计换入8192字节、换出0。仅用于区分当前短时活动与换出占用，不用20秒替代30分钟或推翻旧完整窗口的持续Swap失败；原始swap-activity-short.json在同一受限目录。
