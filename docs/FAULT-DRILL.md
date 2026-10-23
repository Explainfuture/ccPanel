# 预览环境受控故障演练

仅对已授权、未接生产流量的预览执行。脚本会停止Worker、PostgreSQL和Kafka，HTTP短暂不可用。先确认离机备份/隔离恢复、当前版本和无其他测试；本脚本不替代备份。具体实测见PROGRESS步骤114。

把`fault-check-remote.sh`与`fault-check.mjs`放在同一服务器操作目录，JS文件需容器用户可读。设置`CCPANEL_EXPECTED_RELEASE`为已核实的当前release名，再运行shell。控制器应独立于API容器且有外层运行时限；不要在API容器内控制其自身停机。脚本核对release、取得deploy.lock、拒绝运行中的容量服务及遗留fixture，并先检查HTTPS就绪；不会覆盖遗留状态。

测试顺序：停止Worker→Kafka接收事件1但PG仍0→启动后PG1；再次停Worker→接收事件2并保存Kafka消费位点→停止PG并启动Worker→核对PG停机且Kafka位点未推进→PG恢复后PG2；停Kafka→上报503→恢复后事件3收到202且PG3；注入无效event_id合成消息→同公司DLQ可查。最后按fixture公司/项目标识验证所有权，以事务清理其DLQ、聚合桶/任务、原始/去重、凭证及公司，成功提交后移除私有状态文件。脚本遵循配置的事件topic/group，不硬编码默认命名空间。

EXIT/TERM/INT有恢复启动保护；失败时保留fixture和日志便于检查，不自动删除未知数据。外层强制SIGKILL/宿主机故障不保证shell trap执行，运维仍须检查四服务与门禁。遗留fixture禁止直接重开测试，先依据其UUID与标识使用cleanup或保留副本后人工处理。

数据库全断导致共享恢复租约丢失时API/Worker按设计退出，容器自动重启；本测试证明重启恢复及不错误确认/推进，**不证明同一进程持有有效租约时的瞬时事务失败重试**。期间重启计数不能当容量测试“无重启”证据。新instance指标归零，须按instance_id分析；本次没有制造成功写入回执或清除完整性门禁。

完成后核对控制服务终态/退出码、真实HTTPS就绪、恢复门禁、PG消费进度和原数据指纹。预期仅全局消费进度按本次流记录变化；fixture PG数据与凭证应清除。Kafka中的合成原始/DLQ消息按其保留期存在，不能声称物理即时删除。该测试未覆盖DLQ broker单独失败、磁盘满、提交边界强杀、主机断电和异机切流，也不提供高可用或容量结论。

## 独立 DLQ 确认拒收演练（步骤164）

`scripts/dlq-ack-failure-remote.sh` 在预览机核对 `CCPANEL_EXPECTED_RELEASE`、schema22、无容量测试、就绪和至少200MiB可用内存，持有部署锁，暂停原API/Worker。它创建全新临时数据库，并只对随机 `ccpanel.drill.dlqack.<16hex>` 命名空间创建两个topic/一个group；运行当前已部署的真实 `dist/worker.mjs`，不修改主业务topic、位点或恢复门禁。两条源消息固定在同分区：无效ID毒事件在前，正常事件在后。受限临时数据库的新建清单导出/离线apply只用于初始化空测试库，不作为主数据库恢复的可信源证明。

测试DLQ设置且回读确认 `max.message.bytes=128`，制造真实 Produce 拒收。必须观测到broker消息过大拒绝及Worker非零退出；两次间隔快照均要求PG位点0、Kafka未提交、源高位2、DLQ高位0、原始/去重/索引均0。恢复测试DLQ上限为1048588并启动新Worker后，PG/已消费分区Kafka位点均2，原始/去重各1、DLQ索引1、DLQ高位1，核对毒事件精确topic/partition/offset及hash、正常事件精确event_id。空分区Kafka仍可为-1而PG初始化0，不伪称该分区已有提交。

控制器删除临时数据库，确认测试topic/group均消失，删除临时环境/清单；暂停期间主数据库44个public关系（包括恢复门禁和导入scope）逐行指纹完全相同，再恢复原服务并等待HTTPS就绪。失败也执行清理/启动保护并保留受限日志；外层SIGKILL/主机损毁仍需人工检查。整个演练有意中断预览，不能作为无中断/无重启容量证据。

首次两轮的RF1/minISR2注入无效：消息正常得到确认，验证器超时，均记录失败，不作为A15通过证据。Kafka4.1.2的[实际Partition实现](https://github.com/apache/kafka/blob/4.1.2/core/src/main/scala/kafka/cluster/Partition.scala#L354-L360)将有效minISR取配置与副本数的较小值；仅把RF1的minISR配置为2不能制造缺副本拒收。后续改为明确验证过的消息上限拒收。最终实测见PROGRESS步骤164。

本项证明的是DLQ对该记录拒收时不跳过，以及修复配置后新进程恢复；不证明独立DLQ broker网络断连、真实缺副本、磁盘满、同进程无限重试或高可用，A15其余条件仍按各自证据验收。

## 同一 Worker 的真实瞬时事务回滚/重试（步骤194）

`scripts/pg-transaction-retry-remote.sh` 沿用隔离演练控制器：核对精确release/schema22、HTTPS ready、无容量采样器、至少200MiB可用内存并持有deploy.lock，暂时停止主API/Worker。仅创建全新临时数据库和随机 ccpanel.drill.pgretry.<16hex> topics/group，运行已部署真实dist/worker.mjs，不修改主topic/位点或恢复门禁。先前最新备份/隔离恢复证据见步骤193。

故障在临时库的aggregation_jobs BEFORE INSERT触发器中注入SQLSTATE40001；该位置在persist已插入去重/原始记录之后。非事务sequence统计确实到达触发器的尝试次数，开关表仅控制目标event_id。两个有效事件按同分区顺序发送，第一条故障期间第二条必须阻塞。Worker恢复租约通过pg_locks中的实际ShareLock检查，在启动/两次故障快照/恢复后要求同一PG后端；同时核对相同Worker PID仍存活。

故障期至少两次实际触发、两份间隔快照要求raw/dedup/aggregation_jobs均0、PG消费位点0、Kafka未提交(-1或0)、DLQ高位0；撤销开关后不重启Worker，要求原PID/原共享租约后端消费至2、Kafka分区0提交2、两条精确event_id各一次、两个processed任务/两个桶贡献、DLQ0。空分区PG0/Kafka-1按未消费处理，不伪称已提交。

2026-10-23 013实测两轮成功：第一轮4yzjz9证明同进程回滚/恢复；补充共享锁直接断言后，用全新资源D0Rwar复核成功。最终实际触发/数据库重试日志均3次，故障两快照三表均0、PG0/Kafka-1；恢复同PID/同租约后端，raw/dedup/jobs/processed/贡献各2，DLQ0。主库全部44个public关系（含恢复控制表）前后指纹完全相同，测试topics/group/临时库删除、受限env/ledger清理，原服务启动并HTTPS ready。安全结果日志已带回工作站。

这补充了114未覆盖的“PG仍在线且持有有效租约时同进程事务错误重试”证据。错误为真实PG触发器注入的40001，不等于真实并发序列化冲突、网络断开、所有SQLSTATE/磁盘故障、COMMIT边界强杀或独立DLQ broker失败。主服务有计划暂停，不能用容器restart计数替代无中断/容量保证。

## 当前013 Worker的原始提交边界SIGKILL（步骤195）

历史59/60使用早期暂存Worker的证据保留，不直接当成当前schema22构建全部窗口通过。新 `worker-commit-kill-remote.sh` 沿用194守护控制器（release/schema/ready/内存/部署锁/无容量检查），暂停主API/Worker，只对随机临时DB/topics/group运行当前真实dist/worker.mjs，无生产代码测试开关。

在临时库aggregation_jobs插入和consumer_progress更新前安装有条件PG触发器，由独立控制连接持有advisory锁。观察到真实PG waiter后读取可见数据/消费位点，再确认waiter仍在才SIGKILL目标子进程并核对实际终止信号；等待PG释放等待锁后再次检查数据/位点，撤销故障并启动新PID回放。该锁窗口在实际SQL中，未用人为等待猜测COMMIT时刻；Worker原有5秒statement_timeout不修改，未达到目标状态则失败。

013实测9keKOT passed：

- before_raw_commit：聚合任务插入前被锁住，raw/dedup/jobs可见0，PG进度0、Kafka未提交；SIGKILL后仍0，新Worker恢复两条精确事件/任务/贡献。
- after_raw_commit：下一组首事件原始事务已提交，raw/dedup/jobs可见3（含上一组2），进度UPDATE仍被锁住、PG/Kafka均停在2；SIGKILL后已提交3条保留，新Worker将第三条识别为duplicate，再消费第四条。
- 最终raw/dedup/jobs/processed/桶贡献各4，duplicates严格1、DLQ0；PG/Kafka/source分区0均4，空分区PG0/Kafka-1不当作已消费。四个客户端原event_id集合精确匹配，无更新ID凑恢复。

隔离topics/group/DB及env/ledger清理通过；服务暂停期间主库44个public关系（包括恢复控制表）指纹完全相同，随后恢复API/Worker/HTTPS ready。主库raw2/dedup3/DLQ0、位点总15618、临时DB0、仍013。安全结果双方保存；主服务为计划暂停，不当容量或无中断证明。

本轮只复核当前构建的原始事务提交前、原始提交后但独立进度提交前两个窗口。独立进度已经COMMIT而Kafka未提交的旧代理证据未据此升级为当前版本证明；主机掉电、磁盘损坏和多副本容灾也未覆盖。

## 当前013独立进度COMMIT后/Kafka提交前强杀（步骤196）

`checkpoint-window-remote.sh` 沿用隔离控制器和当前真实dist/worker.mjs，暂停主API/Worker，仅临时DB/topics/group。协调器直连PG；只有测试Worker的PG连接指向同一一次性容器内的协议代理，代理上游为postgres:5432、没有宿主机发布端口。复用已部署checkpoint-commit-proxy.mjs，源文件SHA25635c51212605bdac0f2b1b6fac619560c2eb85e53b6834d68825aa220223c3154与本机相同，无业务测试开关或结果篡改。

代理识别消费进度UPDATE所在事务，接到PG实际CommandComplete/COMMIT后仅扣留这次成功响应，写安全布尔标记。控制器除标记外还直连查询：PG next_offset=1已持久化，raw/dedup/jobs可见1，而Kafka仍未提交、后续第二事件未处理；间隔快照要求状态稳定。实际SIGKILL并检查信号后，PG1/raw1仍保留。代理只扣一次，新PID恢复连接正常通过；Kafka从未提交位置重放首事件产生duplicate1，然后处理第二事件。

首轮GlpWmb通过后，补充显式总aggregation_jobs=2断言，用新资源n0ffhQ复核最终脚本通过。最终raw/dedup/jobs/processed/桶贡献各2、duplicates1、DLQ0、精确原event_id集合；PG/Kafka/source分区0均2，空分区PG0/Kafka-1如实记录未消费。全程是实际PG/Worker/Kafka/代理和真实SIGKILL，不用sleep猜测server是否已COMMIT。

服务暂停期间主库44个public关系（含恢复控制表）指纹同，临时topics/group/DB/env/ledger清理；随后原服务ready。主库raw2/dedup3/DLQ0、位点总15618、临时DB0、仍013。安全结果双方保存。结合195，当前构建的三个指定提交/进度边界均有本机实测，但仅限合成事件、PG/Kafka持续可用和该单机配置；不等于宿主机断电、磁盘持久性、多副本容灾、容量或无中断保证。代理仅支持本次私网明文PG协议路径，不用于生产或外部流量。
