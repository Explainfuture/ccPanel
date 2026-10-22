# ccPanel

开源事件埋点与产品分析平台，目标是逐步覆盖 Mixpanel 的核心分析场景。

当前阶段：M1 接入链路与后台开发预览已在 VPS 运行，M2 基础事件趋势已实现；完整 PRD 验收尚未完成。多租户按公司隔离，同公司成员可查看全部项目与分析数据。

技术方向：TypeScript · React · shadcn/ui · Tailwind CSS · Hono · PostgreSQL · Kafka · pnpm。

- [实施计划](docs/PLAN.md)
- [架构与可靠性设计](docs/ARCHITECTURE.md)
- [产品需求 PRD v1.1](docs/PRD.md)
- [开发进度、分支与实测记录](docs/PROGRESS.md)
- [单机部署说明](infra/README.md)
- [历史图片与生成提示词](docs/architecture-image-prompt.txt)（v1 参考，不是当前实施规格）

单机试运行与生产高可用采用不同部署规格；不承诺无限吞吐或所有故障下零丢失。

## 开发与验证

Node.js 22.12+，pnpm 12.3.4。

```sh
pnpm install --frozen-lockfile
pnpm typecheck
pnpm test
pnpm build
pnpm --dir packages/sdk-browser pack --pack-destination ../../artifacts
pnpm --dir packages/sdk-node pack --pack-destination ../../artifacts
```

配置环境变量后运行 API/Worker；数据库、Kafka 和迁移操作见部署说明。前端开发入口 `pnpm dev:web`。管理账号通过一次性 bootstrap 创建，不开放公共注册，不复用 SSH 密码。凭据、会话文件及部署私钥不得提交。

当前预览包含登录、公司/项目选择、项目创建、写入凭证、SDK 接入测试、事件明细、基础趋势和成员管理。报表保存、漏斗/留存/分群、删除屏障、完整运维与容量验证继续按 PRD 迭代，不能将预览作为全部功能已实现的版本。
