# 备份与恢复演练

## 当前范围

PG 自定义格式全库备份包含原始事件、去重、身份、报表、任务、配置和权限。传输通过已校验主机指纹的 SSH；离机副本用 AES-256-GCM 加密，密钥独立保存，不能丢失或随备份公开。临时明文文件在验证后删除。

Kafka 中已确认但尚未写入 PG 的事件不包含在 PG 备份中。本流程不替代 Kafka 多副本/灾备，也不承诺单机损毁零丢失。删除清单及屏障实现后，恢复上线必须重新应用当前清单，不能只恢复旧数据库；该验收仍待完成。

## 操作流程

1. 将 `scripts/backup-fingerprint.sql` 放在当前发布目录的 scripts/ 中，以 root 执行 `bash scripts/backup-remote.sh`。输出受限备份目录，不输出凭证。该演练要求安静窗口，前后指纹不同则停止并重试，不宣称完成对账；pg_dump 本身仍使用一致性快照。
2. 用校验过主机密钥的 SFTP 下载 database.dump、fingerprint.jsonl、completed-at.txt 到受限的离机目录。
3. 在备份机器运行 `node scripts/encrypt-backup.mjs encrypt INPUT OUTPUT KEY_FILE`。密钥首次生成后复用，输出不得已存在；使用 GCM 验证头部和内容，篡改时不保留解密结果。
4. 用同一工具的 decrypt 模式还原到临时受限文件，回传到 `/opt/ccpanel/shared/restore/演练编号/database.dump`，并附 fingerprint.jsonl。
5. 设置 `CCPANEL_RESTORE_FILE`，执行 `scripts/restore-check-remote.sh`。脚本只创建并删除以 ccpanel_restore_ 开头的随机临时数据库，不覆盖运行库。pg_restore 单事务/遇错退出，再对所有 public 关系的行数与内容指纹进行比较。
6. 验证成功后删除两端临时明文 dump，保留加密离机副本、密钥和受限验收记录。加密输出与密钥不得提交 Git。

## 2026-10-23 实测

- 源：m4-query-jobs-001 / PostgreSQL 17.7；备份完成 UTC 2026-10-22 17:19:41。
- 离机目标：开发者 Windows 工作站，已下载、加密保存、解密后回传，未使用源主机原始 dump 恢复。
- 原始备份 49703 字节，加密信封 49739 字节；完整业务表行数/内容指纹一致。样本缺口为 0，不将其解释为生产 RPO=0。
- 临时 PG 数据库的创建、导入与对账共 4 秒；清理后核对时距备份完成 105 秒。后者包含手动传输/操作等待，不是自动灾难恢复承诺。
- 演练数据库及两端临时明文已清理；离机加密文件位于本机 `.local/backups/`，密钥位于 `.local/backup-keys/`，均被 Git 忽略。

当前尚无持续在线的自动异机目的地，因此备份间隔/RPO 未设定，整机重建和切换流量的 RTO 未测量。该项只证明这份离机 PG 副本可读且数据一致，不能据此判定所有试运行门槛通过。定时备份、保留期、异地冗余与删除清单恢复仍需补齐。
