# 预览发布与独立依赖准备

步骤221实际发布 `m4-polling-014/schema22`，冻结源码 `b9b213f`。运行证据见 [polling-release-221.json](evidence/polling-release-221.json)；容量准入仍见 [CAPACITY.md](CAPACITY.md)。

发布前先运行 typecheck/build，并以干净、已提交的完整 HEAD 打包：

```sh
python3 scripts/package-release.py --source-commit <完整40位HEAD> --output <新的绝对归档路径>
```

工具从 Git 读取已提交的部署文件，校验完整 dist、静态资源引用和 schema，拒绝多余旧 bundle、链接、凭据路径与覆盖输出。tar/gzip 时间、权限及属主固定，清单记录源码树和逐文件哈希；排除含源码的 `.mjs.map`。它不能证明忽略目录里的构建产物来自指定源码，操作员仍须先重新构建。本轮实际重复归档字节一致。

三份依赖输入 `package.json`、`pnpm-lock.yaml`、`pnpm-workspace.yaml` 任一变化时，部署必须提供 `CCPANEL_DEPENDENCIES_DIR`。部署前验证已完成的私有 stage，独立 `cp -a` 后复验副本和源树；安装器不会运行在旧版本的硬链接依赖上。输入相同时仍可只读复用旧树。

1 GiB VPS 的在线安装实际触发了 200 MiB 可用内存保护。因此本轮采用工作站 Linux 构建、服务器导入的路径，未降低阈值。直接安装器保留为有资源/时间限制的可选工具，失败目录不含完成标记。

工作站的 `build-release-dependencies.sh` 要求 Linux、非 root，且没有 `/opt/ccpanel/current`。提供原生 Linux x64 Node 22.22.3 和 pnpm 12.3.4 的绝对路径及 SHA256：

```sh
export CCPANEL_NODE_BINARY=<node绝对路径> CCPANEL_NODE_SHA256=<sha256>
export CCPANEL_PNPM_BINARY=<原生pnpm绝对路径> CCPANEL_PNPM_SHA256=<sha256>
export CCPANEL_ARCHIVE_FILE=<候选归档绝对路径> CCPANEL_ARCHIVE_SHA256=<sha256>
export CCPANEL_DEPENDENCIES_OUTPUT=<新的绝对路径.tar.gz>
bash scripts/build-release-dependencies.sh
```

构建只提取三份输入；冻结锁文件、忽略构建脚本、复制导入，store 位于产物外。300 秒预算覆盖安装、实际 Linux 导入及打包。产物首项为工具版本、输入哈希和完整文件清单；它没有完成标记。确定性归档针对同一文件树，独立安装可能产生含工作目录或时间的 pnpm 元数据，不保证两次安装字节相同。

将源码匹配的导入器、校验器和两个归档上传至私有位置，再在 VPS 设置：

```sh
export CCPANEL_EXPECTED_RELEASE=m4-sdk-013
export CCPANEL_ARCHIVE_FILE=/opt/ccpanel/shared/<候选>.tar.gz
export CCPANEL_ARCHIVE_SHA256=<候选sha256>
export CCPANEL_DEPENDENCIES_ARTIFACT=/opt/ccpanel/shared/<依赖>.tar.gz
export CCPANEL_DEPENDENCIES_SHA256=<依赖sha256>
bash /opt/ccpanel/shared/<工具目录>/import-release-dependencies.sh
```

当前准备器/导入器明确固定旧版本013，后续版本会被拒绝，须先修改并验证预期版本。导入器持部署锁，拒绝活跃容量任务、未就绪服务或低于300 MiB的可用内存；两个 SHA、输入、所有内容和链接图全部验证后才写新 `runtime-deps.XXXXXX`。Python预验证设192 MiB虚拟地址上限，这不是实测RSS。随后在只读、无网络、UID1000、有资源上限的容器导入12项生产依赖；容器运行期间可用内存低于200 MiB或读数未知会终止本次容器。180秒预算、后续余量检查、容器归属核对及失败撤销标记保持生效。`.completed.json` 绑定精确版本、三份输入和冻结树；构建收据是操作员提供的对应关系，独立构建来源认证仍为 false。

依赖准备完成后才进行现有受保护升级：关闭物理保留及采集、暂停 API/Worker、冻结原字段指纹、生成新 dump/完整v2清单、工作站使用已有 AES-GCM key 加解密并逐字节比较、将解密副本送回实际隔离恢复。实际成功后才写备份证明并 apply；不重置 Kafka/PG 位点，不导入旧库导出的清单，也不自动回滚数据库。核对原投影及单独排除的恢复控制表后，重新启用保留和采集，完成临时公司闭环与清理。

本轮恢复校验覆盖42个关系，排除 `recovery_gate`/`recovery_import_scopes` 的表数据；升级比较覆盖41个原字段投影，另外两表单独比较。启用采集后监控历史会正常写入和裁剪，不把其全量前缀永远不变作为本轮结论。容量窗口仍须事先解决历史保留边界与外层控制，不能由此次发布推导通过。

自动离机备份配置继续禁用；用户暂未提供独立目的地。工作站人工加密副本与隔离恢复不等同于持续异机同步、最新状态可信准入或整机故障恢复验收。
