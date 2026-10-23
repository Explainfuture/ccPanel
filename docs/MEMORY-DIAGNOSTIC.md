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
