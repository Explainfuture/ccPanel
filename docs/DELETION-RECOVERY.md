# 删除清单与恢复门禁（待真实验收）

该流程在本地实现，尚未替代已部署版本的恢复流程。上线前需完成“删除前备份→源库删除→旧备份恢复→最新清单应用→重放”真实演练，不能把解析测试当作恢复证明。

清单包含命名空间身份哈希、删除任务截止时间和项目隐私版本，不包含事件载荷、原始用户 ID 或登录凭证；哈希仍作为受限数据管理。必须从可信源库获取最新清单，权限 600 保存，经 SSH 传输并按备份工具加密离机。禁止从旧恢复库重新导出清单来绕过门禁。

```sh
set -euo pipefail
umask 077
# 在可信源库的 Compose 目录执行；失败不覆盖上一份有效清单：
docker compose run --rm -T --no-deps api node dist/deletion-ledger.mjs export > /opt/ccpanel/shared/ledger.pending.json
mv /opt/ccpanel/shared/ledger.pending.json /opt/ccpanel/shared/deletion-ledger.json
# 切换到隔离/离线目标库的 Compose 配置后执行，不能指向仍在线源库：
docker compose run --rm -T --no-deps -e CCPANEL_OFFLINE_RESTORE=true api node dist/deletion-ledger.mjs apply < /opt/ccpanel/shared/deletion-ledger.json
docker compose run --rm -T --no-deps api node dist/deletion-ledger.mjs check
```

apply 需要明确的离线模式；API/Worker 持有共享会话租约，目标仍在线时不能取得排他租约。应用程序启动先取得共享租约并检查门禁，恢复中或恢复失败时不开放。PG 连接失效将使服务退出交给监督器恢复，Kafka 未提交位点仍重放；此故障行为待新版本再演练。

备份不携带 recovery_gate 和 recovery_import_scopes 的运行状态；恢复后缺失/关闭门禁均不能启动应用，迁移默认创建关闭状态。首次升级也需在停服务后从可信当前源库导出清单，并通过 CCPANEL_RECOVERY_LEDGER 显式提供，禁止自动信任恢复库的旧清单。

校验格式、跨公司引用、重复身份、未知字段、完整引用图和时间。预览预算为 8 MiB、每类最多 10000 条；清单超过 1 小时则拒绝。1 小时只是陈旧清单拒绝阈值，不代表允许 1 小时隐私丢失：操作者必须证明所用副本覆盖恢复前已接受的删除；自动可靠异机同步未配置前不进入生产。

目标缺少源清单里的公司/项目元数据时停止恢复，保持关闭，需补齐对应可信元数据；不能静默跳过范围。每项目原子导入屏障、撤销项目凭证、使旧查询快照失效，分批清理后才开放。清理超时/失败保留关闭状态及已提交进度，使用同一未变更清单重试；导入回执避免把已完成批次无限重开。

门禁控制和指纹对账中的业务数据分开处理：运行控制表不属于备份数据一致性对账，但必须验证恢复后默认关闭。所有回滚候选必须识别隐私屏障、可见性视图和恢复门禁；早期版本不支持该回滚路径。
