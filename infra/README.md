# 受限单机试验配置

此配置保留真实 Kafka 与 PostgreSQL，RF=1，不具备高可用；内存限额是试验值，不是性能承诺。不得把数据库和 Kafka 端口发布到公网。构建在开发机完成，再上传 dist、package.json、锁文件、workspace 配置与 infra；服务端只安装生产依赖。

在部署目录 infra/.env（不要提交）配置 POSTGRES_PASSWORD、DATABASE_URL、KAFKA_BROKERS=kafka:9092、PUBLIC_ORIGIN、COOKIE_SECURE=true。运行 docker compose --env-file .env -f compose.yaml up -d postgres kafka；等待 Kafka 成功启动后，用同一 node 镜像和环境执行 dist/migrate.mjs 与一次性 dist/bootstrap.mjs，再启动 api/worker。bootstrap 需要独立随机 ADMIN_PASSWORD，不复用 SSH 密码。

Nginx 使用 HTTPS；无自有域名时预览使用包含服务器 IP SAN 的自签证书，浏览器需要显式信任，不能当作正式公开发布。接入 Cloudflare/可信证书前先验证域名控制权和可达性。不能为绕过证书问题将管理密码通过明文 HTTP 发送。

升级前备份 PG，将新版本上传独立 releases 目录，保留旧版本；服务入口 current 指向选定版本，只有迁移兼容时才允许回退旧程序。不自动删除数据卷，不运行 compose down -v。保留 Docker 日志轮转，监控 OOM、Swap、磁盘与 Kafka lag。

当前仅初始配置；实际启动、故障和容量结果记入 docs/PROGRESS.md，不以启动成功替代 PRD 验收。
