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

API 后续必须只读挂载 telemetry **目录**，不能只绑定 host.json 文件，否则宿主机原子替换后容器仍会读取旧inode。运行 API 尚未挂载并执行 HTTP 背压；当前探针和规则不代表保护已上线。现有一次性检查工具已改读持续样本，不再临时制造“新鲜”采样来掩盖定时器失效。
