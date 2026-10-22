# 开发进度与验证记录

实施依据：PRD v1.1。只将已实现且验证通过的能力标为完成；线上演示不等于生产验收。

## 分支与提交规范

每个可验证步骤使用 feat/NN-topic 或 fix/NN-topic 分支；后续分支从上一阶段通过验证的提交继续，均推送 origin。提交正文包含 Changes（改动）、Reasoning（思路）、Validation（验证）。阶段成果暂不自动合并 main，保留清晰的可审查分支历史。

## 当前里程碑

- M1 工程与协议：进行中。
- M1 SDK、鉴权、Kafka/PG 闭环：未完成。
- M2 图表、报表、仪表盘：未完成。
- M3 身份、漏斗、留存、分群：未完成。
- M4 容量与恢复验收：未完成。

### 步骤 03：VPS 真实基础设施与集成验证（2026-10-23）

Docker Compose 启动 Kafka 4.1.2、PostgreSQL 17.7、Node 22 API/Worker，数据卷持久化、内存限额、日志轮转。修复首次 Kafka 卷属主问题，保持 broker 非 root 运行。Nginx HTTPS 预览使用自签证书，尚无可信域名，不算公开正式发布。管理账号独立随机密码仅保存在受限部署配置和本机 .local（忽略提交）。

真实集成 scripts/integration.mjs 已在 VPS 通过：Kafka 202 确认、重复事件与聚合任务去重、跨公司查询/项目 ID 越权拒绝、viewer 操作限制、载荷冲突 DLQ、移除成员即时失效。测试使用临时公司和独立随机账号，结束后清理夹具，不伪造产品业务数据。

采样：容器 Kafka 203.3 MiB、API 42.0 MiB、Worker 23.7 MiB、PG 16.6 MiB；主机可用约 353 MiB，Swap 已占用约 223 MiB（启动/安装期间产生，尚未证明无持续换页），磁盘剩余约 14 GB。没有执行 30 分钟负载或恢复验收，不作生产承载承诺。

### 步骤 02：公司鉴权与真实接入服务

实现 Hono 接入、会话/CSRF、公司成员检查、项目与写入凭证管理；Kafka 确认后才返回 202。消费者在 PG 事务登记唯一键、写原始事件和聚合任务，COMMIT 后推进连续位点；载荷冲突写 DLQ。服务器已安装 Docker/Compose/Nginx。

验证：类型检查、8 项协议/鉴权边界/密码测试和后端构建通过。真实数据库与 Kafka 集成验证在步骤 03 执行。分析、已有账号邀请接受、删除屏障等未实现，不算 M1/M3 完成。

### 步骤 01：工程与事件协议

pnpm workspace、严格 TypeScript、Vitest 和共享事件类型；定义单事件/批次字节上限、JSON 安全性、UUID v4、时间窗口和稳定载荷规范化。拒绝 SDK 自报公司归属，归属只从服务端凭证推导。验证：pnpm install --frozen-lockfile、pnpm typecheck、pnpm test（3 项协议测试）、pnpm build 全部通过。pnpm 12 使用 allowBuilds 显式放行 esbuild 构建脚本，依赖精确版本写入锁文件。
