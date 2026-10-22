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

### 步骤 01：工程与事件协议

pnpm workspace、严格 TypeScript、Vitest 和共享事件类型；定义单事件/批次字节上限、JSON 安全性、UUID v4、时间窗口和稳定载荷规范化。拒绝 SDK 自报公司归属，归属只从服务端凭证推导。验证：pnpm install --frozen-lockfile、pnpm typecheck、pnpm test（3 项协议测试）、pnpm build 全部通过。pnpm 12 使用 allowBuilds 显式放行 esbuild 构建脚本，依赖精确版本写入锁文件。
