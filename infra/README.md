# 受限单机试验配置

此配置保留真实 Kafka 与 PostgreSQL，RF=1，不具备高可用；内存限额是试验值，不是性能承诺。不得把数据库和 Kafka 端口发布到公网。构建在开发机完成，再上传 dist、package.json、锁文件、workspace 配置与 infra；服务端只安装生产依赖。

在部署目录 infra/.env（不要提交）配置 POSTGRES_PASSWORD、DATABASE_URL、KAFKA_BROKERS=kafka:9092、PUBLIC_ORIGIN、COOKIE_SECURE=true。运行 docker compose --env-file .env -f compose.yaml up -d postgres kafka；等待 Kafka 成功启动后，用同一 node 镜像和环境执行 dist/migrate.mjs 与一次性 dist/bootstrap.mjs。bootstrap 需要独立随机 ADMIN_PASSWORD，不复用 SSH 密码。

迁移默认关闭恢复门禁，API/Worker 启动前必须离线应用可信删除清单。仅对全新初始化、没有历史数据的源库，可在 bootstrap 后显式 export 初始清单并 apply；旧备份必须使用覆盖恢复前删除请求的独立最新清单，不能从旧恢复库重新 export 替代。命令、共享租约及失败边界见 docs/DELETION-RECOVERY.md。

Nginx 使用 HTTPS。当前 VPS 预览已使用受信任 IP 证书并配置自动续期；新环境需单独配置和验证证书、挑战路径、续期计时器及部署钩子，不能将预览证书复制用于其他主机。域名/CDN 接入须验证控制权与可达性，不能为绕过证书问题将管理密码通过明文 HTTP 发送。

升级前备份 PG，将新版本上传独立 releases 目录，保留旧版本；服务入口 current 指向选定版本，只有迁移兼容时才允许回退旧程序。不自动删除数据卷，不运行 compose down -v。保留 Docker 日志轮转，监控 OOM、Swap、磁盘与 Kafka lag。

迁移7之后的 Worker 必须理解去重登记 failed/stored 状态。旧 Worker 会误将 failed 登记视为已成功事件，因此禁止回滚到早期版本；回滚候选也必须保留隐私屏障、可见性视图及恢复租约。scripts/deploy-remote.sh 先预检文件权限、停 API/Worker 再迁移，成功就绪后才切换静态入口。

运维命令不得把凭据输出到日志或完整进程参数；测试连接覆盖使用私有 env 文件，诊断只输出脱敏状态。实际启动、故障和容量结果记入 docs/PROGRESS.md，不以启动成功替代 PRD 验收。

## 宿主机资源采样（当前仅观测）

Linux 采样器由 scripts/install-host-probe.sh 安装，源码与 systemd 单元在 infra/host-probe。当前 VPS 已从受控暂存目录安装，ccpanel-host-probe.timer 每10秒更新 /opt/ccpanel/shared/telemetry/host.json。卷路径缓存配置权限600，样本权限644，不含路径、凭据或进程参数；卷重建后须重新解析配置。

TypeScript 读取器只接受32KiB以内的版本1完整样本，拒绝未知字段、重复磁盘、时间不一致和非法资源计数。CPU为累计tick、swap为累计页，不能直接当作百分比或当前交换速率；需结合两次样本的间隔计算。实际Docker卷使用f_bavail/f_favail，不能把root专用保留空间算作应用可用空间。

API 必须只读挂载 telemetry **目录**，不能只绑定 host.json 文件，否则宿主机原子替换后容器仍会读取旧inode。当前m4-integrity-001/schema10已启用该挂载及背压保护，聚合/邀请新功能尚未部署。发布脚本在停止旧API/Worker前验证挂载、样本协议/新鲜度与配置，首次部署必须先安装采样器。现有一次性检查工具已改读持续样本，不再临时制造“新鲜”采样来掩盖定时器失效。

新背压代码同时保护普通上报和管理重放，503带Retry-After，Worker继续消费以便追赶。已观测保留缺口写入PG完整性暂停门禁，正常进程重启或重复迁移不会清除；缺失既有门禁行也不自动开放。当前PG门禁不能证明旧备份之后没有新的事故，旧备份恢复仍需核对独立的最新运维事故状态；该恢复集成未完成前不进入生产。单进程预算也不是集群共享限额或逐字节硬容量证明。

## 专用 KVM 试验机的内存配置

scripts/preview-memory-profile.sh只适用于确认独占用途的KVM/vda单盘环境：要求根挂载来自vda、没有device-mapper或其他磁盘、固件刷新任务不在执行，所有目标unit状态均已识别。apply将原swappiness及unit状态保存到受限/opt/ccpanel/shared/memory-profile-before.json后，停用并屏蔽fwupd及multipathd常驻/激活路径，停用固件刷新timer，设置vm.swappiness=10。保留APT系统更新、SSH、fail2ban、宿主机遥测、swap容量与平台数据持久化配置。

仅在这些前提核实后使用`bash scripts/preview-memory-profile.sh apply`，`check`查看状态；需要恢复时执行`rollback`，校验自有sysctl文件后按原状态恢复，并保留审计快照。脚本拒绝覆盖已有原始记录，不用于物理机、SAN/多路径或通用服务器。已完成现行机apply与健康核验，未执行现行机rollback演练。调整属于试验配置，不能替代同版本30分钟复测，也不承诺1GiB服务器生产可用。

## 公司开通命令（schema13，尚未部署）

公司开通只通过部署命令，不开放公共注册。全新bootstrap会显式登记首个平台开通者；已有部署升级后不会把公司管理员自动提升为平台开通者，需要受控执行一次initialize-operator，为一个已存在的平台账号建立权限。该权限只用于开通元数据，不绕过任何公司成员关系或授予事件查询权。

在root拥有、0600的独立环境文件中配置CCPANEL_OPERATOR_MODE=true、CCPANEL_OPERATOR_EMAIL。首次显式初始化使用`docker compose run --rm -T --no-deps --env-from-file /root/ccpanel-provision.env api node dist/company-provision.mjs initialize-operator`；全新bootstrap已完成初始化时无需再次调用。create还需CCPANEL_PROVISION_REQUEST_ID（固定UUID）、CCPANEL_COMPANY_NAME、CCPANEL_OWNER_EMAIL、CCPANEL_OWNER_PASSWORD。命令同上，将最后一个参数改为create；密码只放受限文件，不进入命令参数、输出或Git。

同一请求ID及公司/管理员身份重试返回原回执，不重新创建或重设密码；reused=true时原凭据不变，即使重试环境中的密码不同。相同ID改变公司/管理员身份会拒绝。首管理员必须是新平台账号；已有账号由公司管理员在新公司中发起邀请并主动接受，开通命令不改其密码或静默授予权限。创建公司、账号、成员关系、回执及审计同事务，恢复门禁关闭或独占恢复进行时拒绝执行。平台开通者可用revoked_at撤销，后续命令重新检查权限；开通后不自动成为该公司成员。
