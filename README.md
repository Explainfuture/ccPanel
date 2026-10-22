# ccPanel

开源事件埋点与产品分析平台，目标是逐步覆盖 Mixpanel 的核心分析场景。

当前阶段：基础架构已确认，已补齐 SDK 可靠投递设计与 PRD v1.1，尚未实现产品。多租户按公司隔离，同公司成员可查看全部项目与分析数据。

技术方向：TypeScript · React · shadcn/ui · Tailwind CSS · Hono · PostgreSQL · Kafka · pnpm。

- [实施计划](docs/PLAN.md)
- [架构与可靠性设计](docs/ARCHITECTURE.md)
- [产品需求 PRD v1.1](docs/PRD.md)
- [历史图片与生成提示词](docs/architecture-image-prompt.txt)（v1 参考，不是当前实施规格）

单机试运行与生产高可用采用不同部署规格；不承诺无限吞吐或所有故障下零丢失。
