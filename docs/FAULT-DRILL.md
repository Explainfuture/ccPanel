# 预览环境受控故障演练

仅对已授权、未接生产流量的预览执行。脚本会停止Worker、PostgreSQL和Kafka，HTTP短暂不可用。先确认离机备份/隔离恢复、当前版本和无其他测试；本脚本不替代备份。具体实测见PROGRESS步骤114。

把`fault-check-remote.sh`与`fault-check.mjs`放在同一服务器操作目录，JS文件需容器用户可读。设置`CCPANEL_EXPECTED_RELEASE`为已核实的当前release名，再运行shell。控制器应独立于API容器且有外层运行时限；不要在API容器内控制其自身停机。脚本核对release、取得deploy.lock、拒绝运行中的容量服务及遗留fixture，并先检查HTTPS就绪；不会覆盖遗留状态。

测试顺序：停止Worker→Kafka接收事件1但PG仍0→启动后PG1；再次停Worker→接收事件2并保存Kafka消费位点→停止PG并启动Worker→核对PG停机且Kafka位点未推进→PG恢复后PG2；停Kafka→上报503→恢复后事件3收到202且PG3；注入无效event_id合成消息→同公司DLQ可查。最后按fixture公司/项目标识验证所有权，以事务清理其DLQ、聚合桶/任务、原始/去重、凭证及公司，成功提交后移除私有状态文件。脚本遵循配置的事件topic/group，不硬编码默认命名空间。

EXIT/TERM/INT有恢复启动保护；失败时保留fixture和日志便于检查，不自动删除未知数据。外层强制SIGKILL/宿主机故障不保证shell trap执行，运维仍须检查四服务与门禁。遗留fixture禁止直接重开测试，先依据其UUID与标识使用cleanup或保留副本后人工处理。

数据库全断导致共享恢复租约丢失时API/Worker按设计退出，容器自动重启；本测试证明重启恢复及不错误确认/推进，**不证明同一进程持有有效租约时的瞬时事务失败重试**。期间重启计数不能当容量测试“无重启”证据。新instance指标归零，须按instance_id分析；本次没有制造成功写入回执或清除完整性门禁。

完成后核对控制服务终态/退出码、真实HTTPS就绪、恢复门禁、PG消费进度和原数据指纹。预期仅全局消费进度按本次流记录变化；fixture PG数据与凭证应清除。Kafka中的合成原始/DLQ消息按其保留期存在，不能声称物理即时删除。该测试未覆盖DLQ broker单独失败、磁盘满、提交边界强杀、主机断电和异机切流，也不提供高可用或容量结论。
