# 容量窗口的监控历史快照

运行样本正常保留7天/最多50000条；三类告警历史为14天/最多5000条。014只读检查已遇到样本到期及压力历史满额，容量测试需要解释正常裁剪。原严格校验器 `capacity-preservation.py` 保留；新工具只声明最终状态中按既有策略仍须保留的原记录是否完整。

`capture-monitor-preservation.ts` 从一个 UTC、只读、可重复读事务导出四张历史表的全部主键、策略时间和 PostgreSQL 完整行 SHA256。每页250条，内部游标保留 PG 时间文本，导出微秒及 bigint 使用十进制字符串。捕获兼容恢复/DDL共享租约，要求schema22及已打开的源恢复门禁；不写数据库记录。

JSONL包含实际数据库名/OID、服务端版本、postmaster开始时间、快照开始/读取时钟和调用方标签；行按真实裁剪顺序排序，末尾包含行数和原始字节前缀哈希。单行最多4096字节、文件32MiB、全局100000行，各表仍受既有数量上限约束。私有目录须由执行账号拥有，输出排他创建、文件及目录同步；失败保留部分文件，发布不确定时即使已有footer也须按命令失败处理。

排序和游标必须引用表限定的原生列，如 `t.assessed_at,t.id`。`SELECT id::text AS id ... ORDER BY id` 会按文本输出别名排序，同时间戳下可出现11排在9前面，并与bigint分页游标不一致。捕获器在写行前使用BigInt时间/ID、固定source序及规范UUID检查跨页严格递增，每表只保留上一键。真实PG回归使用三类历史各505个同微秒ID、三页、9/10、99/100、999/1000和超过JS安全整数的边界；旧SQL陷阱也直接在PG中复现。依据 [PostgreSQL 17 ORDER BY说明](https://www.postgresql.org/docs/17/sql-select.html#SQL-ORDERBY)。

可在工作站编译操作工具，再以受限 Linux 进程运行：

```sh
pnpm exec esbuild scripts/capture-monitor-preservation.ts --bundle --platform=node --target=node22 --format=esm --packages=external --outfile=.local/monitor-capture.mjs
# 通过受限配置显式注入 CCPANEL_MONITOR_DATABASE_URL；没有 DATABASE_URL 回退。
node .local/monitor-capture.mjs <同一窗口标签> <私有绝对路径/before.jsonl>
# 在另一个时点使用同一窗口标签和新的输出路径捕获 after.jsonl。
python3 scripts/monitor-preservation.py verify before.jsonl after.jsonl new-report.json
```

编译结果需要可解析既有 `pg` 依赖。捕获器要求POSIX私有权限；离线Python校验可在Windows执行。捕获成功退出0仅表示 `captured_unverified` 命令完成，调用方还须核对完整文件SHA及实际进程退出。部分文件、重复/未知字段、主键、乱序、错误footer或混用数据库/源标签均拒绝。

离线校验逐条检查：

- 仍存在的每一条原记录必须保持完整行哈希与策略时间，即使已经符合裁剪条件也不能改写。
- 缺失原记录必须组成原排序中的最旧前缀，出现中间断洞就拒绝。
- 每条缺失记录必须严格早于末份快照事务开始时刻的到期线，或位于“原记录与最终新增记录并集”按既有策略保留的最新N条之外。到期等界仍须保留；不要求所有过期记录都已被删除，因为实际清理每次最多100条。

两个时点最多相隔一小时。相同数据库元数据和标签用于防混用；内容哈希和调用方标签不认证来源。该检查可能保守拒绝合法的回填、先删后插及重插场景。例如容量2时原记录时间为10/20，先删除10再插入时间5的迟到记录，最终并集仍要求10，校验会拒绝。消失的中间新增记录也不会增加允许丢失原记录的范围。

报告字段为 `policy_required_originals_preserved`。它只描述两个端点的既有保留策略要求；删除原因、实际删除时间、整个期间的连续可用性、中间新增记录完整性、来源认证、容量及恢复准入标记均为false。容量控制流程还必须独立核对原业务表、控制状态、真实客户端ID、PG/Kafka位置、规则新鲜度、原始样本、版本/配置及实际终态。

取消时，捕获会话强制并实际检查 `client_connection_check_interval=100ms`。这是会话配置，既有应用和数据库全局配置保持原值。依据 [PostgreSQL 17 连接检查说明](https://www.postgresql.org/docs/17/runtime-config-connection.html#GUC-CLIENT-CONNECTION-CHECK-INTERVAL)，内核报告的断连可以在查询期间被检查；仍须独立观察服务端会话和租约退出，不能仅用本地socket关闭或一次观测耗时推导取消时限。

步骤222新增20号实际PG套件：真实关系锁建立快照屏障后，并发调用四个生产存储/裁剪函数；覆盖65000条原记录、微秒分页边界、超过JS安全整数的ID、3条分页边界样本的独立Node完整行哈希检查、实际100条到期清理及剩余过期行、CLI和锁等待超时/取消。首次CI在取消后的服务端清理断言失败，修正会话检查后 [41505334213](https://github.com/Explainfuture/ccPanel/actions/runs/41505334213) 与 [41510135220](https://github.com/Explainfuture/ccPanel/actions/runs/41510135220)全部通过，35项离线检查也通过。86ms是其中一次CI的实际观测，不作为普遍保证。

VPS首轮在基线/捕获前失败，未建立原状态比较；后续控制器改用直接Docker调用并保留原阶段/退出状态。第二轮两份文件哈希/footer完整，但离线校验明确拒绝同时间ID文本排序；原文件未重排，不能作为通过证据。修复5065b55及新增真实PG数字分页回归的 [41513066564](https://github.com/Explainfuture/ccPanel/actions/runs/41513066564) 三项CI均成功后，第三轮使用新目录/单元完成双捕获与清理，工作站核对原字节SHA并运行原严格解析器成功。

第三轮每份45442行/10105840字节，两个快照事务开始相隔51.673291秒。45437条仍存在原记录完整行哈希相同；缺失的2条样本严格到期、3条压力历史超额，均为原排序最旧前缀，无法解释缺失为0。37张非监控public关系指纹及独立配置边界相同；正常变化的四张历史/三张状态表不混称静态指纹。原监督单元实际等待退出0、辅助会话/容器/凭据文件清理完成，HTTPS ready。监督单元97.492秒/CPU2.600秒/内存峰值显示20.1M，不包含独立Docker捕获容器成本；不代表业务容量、整体主机净节省或连续可用。完整端点报告及失败凭证哈希见 [证据](evidence/monitor-preservation-222.json)。
