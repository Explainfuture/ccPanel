# 可配置离机备份方案

状态：配置设计及只读配置预检已实现，默认禁用；已有显式本地/SFTP候选复制组件，完整远程备份执行器未实现。用户暂时没有独立目的地，先完成本方案；不安装定时器、不连接用户外部存储、不创建云资源，也不将现有工作站手动加密副本称为自动异机备份。配置模板见 infra/backup/offhost.example.json。显式backup:copy-sftp可读取启用配置仅传输已有候选，禁用配置不访问凭证或网络；没有自动执行捕获/验证发布/调度的程序。

## 部署与配置

备份协调器部署在独立机器上，由它拉取源VPS数据，并在该机器加密、验真、上传目的地。加密密钥仅在协调器和独立密钥保管处，源VPS与对象存储都不得持有。目的地可以是协调器自己的受限磁盘，也可以经SFTP上传另一台机器。已有local与sftp候选组件；S3兼容对象存储留到后续adapter，不假定Cloudflare或任何免费额度。

模板enabled=false时，其余待填字段允许null；执行器必须直接报告disabled，不探测网络、生成密钥、创建目录或登记成功。启用前必须填写独立worker标识、源固定SSH身份和可信主机指纹、预期release/schema、密钥文件引用、目的地身份及保留策略。所有File字段只引用协调器上的受限文件，不嵌入密码、私钥或token。配置不得提交填充后的实际凭证路径或主机信息；机器上的配置用权限0600保存。

| 配置域 | 必须表达的含义 |
| --- | --- |
| worker | 协调器必须与源VPS不同；由操作员核实独立故障域，不能只比较域名/IP字符串 |
| source | SSH固定known_hosts/密钥文件，精确expectedRelease和expectedSchema；主机变化或版本不符即失败 |
| encryption | 现有CCPBK001 AES-256-GCM信封；32字节密钥必须预先存在，自动运行不悄悄生成新密钥 |
| destination | local要求协调器本地绝对目录；sftp要求独立host、port、user、knownHostsFile、identityFile与绝对remoteRoot；拒绝source主机上的目录充当离机副本 |
| schedule | fullEveryMinutes默认360、stateEveryMinutes默认5；只是拟定间隔，不是已达成RPO |
| retention | 默认保留7份每日、4份每周、3份每月快照；最新验证可恢复集合及其状态链不得被普通轮转删除 |
| limits | 单任务锁、总运行时限、源导出字节上限、失败保留窗口、状态最大陈旧时间；超限显式失败 |

选择sftp时，destination.kind改为sftp，root置null，sftp填写以下结构。此为字段示例，不含可用目的地或凭证：

```json
{
  "host": null,
  "port": 22,
  "user": null,
  "knownHostsFile": null,
  "identityFile": null,
  "remoteRoot": null
}
```

配置验证器已实现：enabled必须为布尔值，version必须为1；分钟/秒/份数/字节限制为正安全整数，port为1–65535，maxAttempts为1–10，stateMaxAgeSeconds必须小于现有v2导入1小时新鲜度上限，状态任务间隔必须小于该陈旧阈值。启用时只允许与kind匹配的目的地字段，所有null必填引用必须补齐，拒绝未知字段和不支持的加密格式。前缀只能为单个安全目录名，不允许点段、斜杠或控制字符。加密密钥不能与SSH文件引用相同路径或落在local目的地目录中。凭证文件须在协调器本地验证权限及非符号链接，不能把文件内容写入日志。

### 当前只读预检命令

```sh
pnpm backup:check infra/backup/offhost.example.json
```

默认模板输出disabled/transferEnabled=false/credentialsInspected=0，退出0；只读取有界配置文件并校验结构，不读取任何引用凭证、创建目录/密钥或联网。禁用状态仍拒绝拼错字段和非法配置，不将字符串false当布尔值。

启用配置先验证配置文件及SSH引用、32字节加密密钥的文件类型/大小/权限；POSIX使用NOFOLLOW、NONBLOCK并比较打开前后inode/dev，拒绝符号链接、非普通文件、组/其他用户可访问文件。不会读取凭证内容，也不会验证SSH key/known_hosts实际格式、主机身份、网络连通性或故障域。Windows ACL检查尚未实现，启用配置返回credential_permissions_unverified，不能把POSIX mode当ACL证明。

即使以上元数据检查全部通过，backup:check仍仅输出configured_executor_unavailable/transferEnabled=false，退出2；主机身份与独立故障域验证均false。其他配置/文件错误退出1，只含固定错误码/字段名，不回显路径、主机或秘密值。显式复制组件不等于完整run流程或定时器，退出2不能被调度器当成功备份。后续执行器仍须做原源版本与目的地独立性验证；字符串不同不能排除DNS别名或同机目录。

## 两条数据流

### SFTP候选目的地组件（步骤213）

`pnpm backup:copy-sftp /private/offhost.json /private/work/candidate /private/download backup-id`显式传输已有候选。禁用模板输出disabled/退出0，不访问源/下载目录、引用凭证或网络；启用配置须为自有私有文件，destination.kind=sftp。服务端的remoteRoot/prefix须由操作员预先创建为自有私有目录；组件不自动创建根目录。只读取目的地身份与已存在候选，不连接source、不读取加密密钥、不执行pg_restore或切流。

使用固定ssh2 1.17.0（可选原生构建禁用）和公钥认证，不使用密码、agent、交互提示、远程exec或首次连接自动信任。专用known_hosts支持精确主机/端口、逗号别名和OpenSSH |1|哈希条目；拒绝标记行、通配/否定模式、非法编码或缺少匹配主机，完整原始主机key逐字节比较。SSH私钥须为已有私有普通文件，暂不支持加密私钥口令。主机pin通过不证明与原源处于不同故障域。

源检查与凭证校验后才建连接；验证远端canonical目录、数值UID及私有模式，独占创建ID集合和wx文件。64KiB有界上传、要求文件fsync扩展、重新下载到独占私有本地集合，核对密文/清单hash和源未变后，才以hardlink扩展排他发布远端capture.json并回读该清单。所需扩展见[ssh2 SFTP API](https://github.com/mscdex/ssh2/blob/v1.17.0/SFTP.md)。不覆盖/清空既有或残缺集合；断连、取消、超时等保留私有残片供调查，新ID重试。发出发布请求后的异常保守报告sftp_publish_uncertain。

成功transferred_unverified/退出2，hostKeyVerified与downloadReadbackChecked为true；独立故障域、恢复、最新状态和源身份仍未验证。SFTP文件fsync不等于远端父目录fsync，remoteDirectoryDurabilityVerified明确false；未发布verified索引，也不宣称断电持久性。全程共享有界时限，超时/取消销毁SSH连接并等待本地句柄清理；不能强制中断阻塞内核I/O。启用仍需真实独立目的地和完整恢复/状态链/发布验收。

CI专用loopback OpenSSH夹具使用临时主机/用户key、临时高端口和私有目录，只校验协议、字节、下载后认证解密和取消/错误拒绝；它不是独立机器或真实PG备份恢复证明。SSHD夹具仅因/tmp祖先权限使用StrictModes no；传输组件自身的本地/远端权限检查仍保留。正式模板保持禁用，未连接用户VPS或外部存储。

步骤214另将真实PG候选套件贯通SFTP：同一份capture CLI生成的加密dump/清单，经本地候选复制、固定主机key上传和下载，再从下载目录走认证解密CLI、独立明文hash、真实隔离pg_restore及范围指纹/公司夹具对账；不从传输前副本恢复，也不将213合成载荷的字节证据拼接成PG证明。远端与下载集合只含密文/清单，原始候选标记不提升。新增loopback夹具限显式CI标志、等待恢复回调结束及daemon/端口/私有目录清理后才返回；VPS分支不加载额外SSH helper。实际结果见PROGRESS步骤214，不代表独立故障域/远端目录断电耐久性/可信最新状态或切流验收。

214夹具仅验证正常回调或捕获异常后的清理；CI父进程强制SIGKILL不保证执行finally，未验收强制中止后的进程树/工作目录回收。正式执行器仍须宿主监督与外部回收，不应直接把CI夹具当生产作业控制器。

### 候选认证解密入口（步骤212）

`pnpm backup:decrypt-candidate /private/destination/id /private/key /private/work/authenticated.dump 22`显式验证已有候选并生成受限工作明文。现有32字节密钥必须为自有私有普通文件，拒绝最终路径符号链接、不自动创建密钥；输出父目录须已存在、私有且canonical，已有输出绝不覆盖。CLI沿用明文预算CCPANEL_BACKUP_MAX_BYTES，核心maxBytes为密文预算。

先检查候选并固定密文/清单hash，再从同一密文文件句柄分块解密CCPBK001 AES256-GCM；认证完成前的明文仅写本次独占创建的0600临时文件。核对密文、明文字节数/hash、源复核与fsync后，重新读取实际落盘明文再核对，才排他hardlink发布输出并同步父目录。回执decrypted_unverified/authenticatedDecryption=true/plaintextReadbackChecked=true，退出2；认证成功不证明可信原源身份、完整关系范围、最新状态或可恢复，四个未验证标记仍false，不写verified manifest/成功水位。

认证失败、明文不符、受控取消或普通I/O失败时，关闭持有句柄、清零本次密钥Buffer，并尝试仅清理本次创建的临时路径；wx碰撞文件、已有/竞争输出和已发布输出不删。清理失败明确decrypt_cleanup_failed；输出发布后错误为decrypt_publish_uncertain并保留输出，不能当成功。成功输出是敏感工作文件，由恢复控制器在核验/恢复后清除。进程强杀/断电、阻塞内核I/O或删除失败不保证明文自动消失，后续作业控制器仍须负责私有工作目录生命周期。

该入口不执行pg_restore、不导入最新v2状态、不打开恢复门禁，也不替换历史手工encrypt-backup.mjs接口。CI从本地目的地副本通过此真实CLI解密，再独立hash和实际隔离PG恢复；不是独立目的地/源身份/最新状态/整机RPO或RTO验收。

### 本地目的地复制组件（步骤211）

`pnpm backup:copy-local /private/work/candidate /private/destination backup-id 22`显式复制一份已有候选；目的地根目录须预先存在、POSIX私有且归当前用户持有。它不读取/启用offhost模板，不连接VPS/SFTP、不创建目的地根目录或定时器。CLI的CCPANEL_BACKUP_MAX_BYTES沿用明文预算，核心API的maxBytes为密文预算。

先检查源候选，排除源/目的地重叠、符号链接和不安全权限，检查目的地空间余量（并非预留）。安全ID下独占mkdir，受限文件wx创建，64KiB有界复制并fsync；源和目的地的密文/清单均重新读取比较hash。步骤210检查回执增加manifestSha256，以绑定复制前后的同一份清单。源复核一致后才以排他hardlink发布capture.json，随后同步目录并再检查目的地。复制不产生明文、不读取密钥。

成功回执为copied_unverified/destinationReadbackChecked=true，退出2；原始capture.json逐字节不变，所有可信来源/恢复/异机/最新状态标记仍false，没有verified manifest或最近成功索引。同名已存在集合（包括残缺集合）永不覆盖；I/O失败、取消或超限保留本次私有残缺集合供调查，需新ID重试，不自动递归删除。清单发布后的任何异常报告local_copy_publish_uncertain，不能按成功处理。普通进程仍可修改其自有文件，此处是接口不覆盖保证，不是WORM存储。

组件有共享总时限/取消检查和读取前后元数据检查；不能强制中断阻塞内核I/O，调用方仍须持有可信目录及任务锁。空间余量不足、权限问题或Windows ACL未验证均拒绝。CI使用同一运行器临时目录验证复制/回读，并从目的地副本认证解密、实际恢复；这不是独立故障域或离机传输验收。SFTP组件见步骤213；完整远程编排、可信状态连续同步/独立锚点保管、验证发布与轮转调度仍待实现。

### 候选集合只读检查（步骤210）

`pnpm backup:inspect /absolute/private/candidate 22`检查单份候选，不读取加密密钥、不写文件、不联网。要求POSIX受限自有目录和普通文件，拒绝符号链接、额外文件（包括残留工作明文）、缺文件、过大清单、非法/提升状态字段、错误schema和指纹结构错误（包括重复关系或恢复控制表）。这里只检查关系名及计数/hash的结构，不证明清单包含该schema的完整关系范围；完整范围须由可信schema清单和实际恢复对账验证。指纹中的PG bigint保持文本，不经JavaScript浮点数。

检查器按64KiB分块读取密文，在调用者上限内核对CCPBK001头、字节数及SHA256；核对读取前后文件元数据/路径inode，读取后再比较清单，使用有界运行时间和取消检查。CLI的CCPANEL_BACKUP_MAX_BYTES与backup:capture同为明文字节预算（默认268435456），检查时自动增加36字节信封开销；核心API的maxBytes直接表示密文上限。不能把上限当作磁盘空间预留或对阻塞内核I/O的强制中断。目录归可信协调器持有，调用者仍须任务锁，不能将检查结果当后续文件不会变化的保证。

通过输出captured_unverified/cipherIntegrityChecked=true、退出2；错误仅固定码、退出1。Windows ACL未验证则拒绝。hash与清单一致不证明清单来自原源，也不证明AES-GCM认证、明文正确、最新状态完整或可恢复；sourceAuthenticated/recoveryVerified/offhostVerified/latestStateVerified仍false。不写verified manifest或成功水位，不开启调度。后续传输/恢复必须重新校验所用字节并完成认证解密、可信来源和实际恢复验证。

完整快照流：源PG一致性快照 → dump及同一快照的范围指纹/manifest → 经固定主机密钥SSH拉取 → 协调器逐文件加密 → 解密到私有临时文件并核对hash → 上传不可变对象 → 从目的地重新下载并认证解密 → 隔离PG实际恢复与范围对账 → 原子发布verified manifest。备份ID唯一，既有对象不得覆盖；中断只能留下incomplete，不能更新最近成功时间。

当前backup-remote.sh要求安静窗口的前后指纹相同，监控采集开启时不适合直接挂定时器。步骤202新增withBackupSnapshot共享PG快照原语，协调只读REPEATABLE READ事务的指纹与回调中的pg_dump --snapshot；步骤203增加captureBackupArchive流式有界落盘原语，输出仍为captured_unverified。步骤204让快照在BEGIN前非阻塞取得现有迁移/分区DDL锁的共享会话租约；已有受管DDL在执行则拒绝备份，备份期间分区维护退后，结束释放。尚未接入实际远程备份执行器或替换旧脚本。后续执行器仍须外层发布锁/版本绑定、manifest与加密传输/验真，不能直接移除原脚本对账条件。固定schema/release及工具hash写入manifest。恢复控制表按既有规则不靠旧dump重开；备份指纹覆盖关系沿明确SQL清单，不能称所有public表全覆盖。

最新状态流目标：从原源导出删除屏障、项目保留策略/版本、完整性暂停与消费进度，持续加密保存；这条流与full snapshot独立更新。现有deletion-ledger v2仅含删除和保留策略，不含完整性事故/消费进度，不能据此声称状态流完整。每份记录source标识、生成时间、清单版本、文件hash、前一份hash、所有项目覆盖范围和备份关联ID。不能从恢复目标自导出充当可信最新源；发现缺项目、策略回退、序列缺口或新鲜度不足，则恢复门禁保持关闭。原源损毁且最新状态无法可信取得时，必须报告无法证明可恢复，不能拿旧dump中的状态替代。

### PG源恢复状态导出（步骤215）

`pnpm --silent backup:export-state source-id 22 > /private/state.json`使用显式CCPANEL_BACKUP_DATABASE_URL；先用umask 077和私有目录保护重定向输出，随后必须加密。stdout是敏感恢复元数据，不应直接打印到共享日志。该命令为手动只读导出，不读取/启用offhost模板、不自行存储/上传或调度；缺显式源URI即拒绝，不回退到应用DATABASE_URL。

导出前以共享会话租约排除受管DDL和离线恢复，要求source recovery_gate已开放、schema22；在同一UTC REPEATABLE READ READ ONLY快照中读取完整v2删除/保留清单、当前完整性gate、consumer_streams及其全部PG分区进度、consumer_progress_audit最大ID。导出不修改数据、不打开门禁、不快进消费位点。暂停的ingest integrity gate允许导出，以保留事故；源recovery gate关闭则拒绝。全程有界超时关闭本地连接；服务端依赖有界statement_timeout/后端退出释放会话锁，本地close不等于服务端已经清理。PG验收保持测试锁不释放，另行有界观察实际后端消失及租约释放。旧deletion-ledger export复用同一读取器，v1/v2/apply协议保持。

新包装格式version1、status=exported_unverified，原v2嵌入deletion；嵌套时间与顶层一致，消费offset和audit最大ID保持int64文本。严格检查流/分区完整性（相对PG现有表），最多10000流/10000位置、details64KiB、v2清单8MiB、总体16MiB；默认新鲜度600秒且小于1小时，允许源时钟最多超前300秒。缺项、重复分区、非法/提升验证标记、schema或时间不符均拒绝。

成功退出2、source.authenticated/latest_state_verified/kafka_observation_verified均false。source-id是操作员提供的标签，不是密码学身份；audit_high_water只是审计表最大ID，不是可信链首、连续状态序号或Kafka高水位。此格式包含当前gate及PG进度，不是全历史事故审计归档；Kafka实际保留范围/提交位点仍需独立验证。尤其不能把最新源next_offset写到旧备份以跳过缺失事件。来源认证、状态连续加密投递、跨记录防回退及独立锚点、恢复导入与门禁验收仍待实现。

加密认证只证明密文完整和持钥者身份，不单独证明它是最新、完整或未被回滚的原源状态。源授权导出协议、可信高水位以及独立存储的链首锚点须在执行器实现/演练阶段验证。恢复既要原始事件备份，也要可信最新状态；Kafka已确认但尚未入PG的消息仍须单独的多副本/灾备方案。

## 调度、轮转与恢复

### 签名状态记录与外部锚点校验（步骤216）

新增纯TypeScript/Node crypto原语signRecoveryStateRecord、verifyRecoveryStateRecord、verifyRecoveryStateChain与状态连续性检查；当前没有签名密钥文件加载、持久化链首更新、调度或恢复放行命令，不生成正式密钥。调用方必须从独立信任控制面提供固定Ed25519公钥、预期source-id和最新链首锚点，不能使用记录自带公钥或由待验证记录自行推导的锚点作为信任根。

签名覆盖version1、source-id、int64文本序号、previous_hash、备份ID/清单hash与完整源状态。Ed25519签名和记录SHA256使用不同固定域；记录hash覆盖长度帧、原始payload及签名，外层JSON字段顺序不影响hash。固定算法/密钥类型、canonical base64、严格UTF8/JSON往返、字段/大小/时间检查均拒绝歧义或自称可信输入。备份关联只是被签名的声明，仍须单独验证所指备份真实存在且hash正确。

初版从genesis连续检查，支持Iterable并限制最多10000记录；历史前缀可过期，但最后一份必须满足实际时钟新鲜度并精确匹配独立传入锚点的source/key fingerprint/序号/hash/时间。缺锚点、缺记录、乱序、重放较短前缀至新锚点、签名/公钥不符均拒绝。追加签名并不做单写者CAS，两个发布者仍可能签出分叉；持久化发布与并发控制、受保护检查点/裁剪和密钥轮换流程尚未实现。

相邻已验签状态还按当前写入协议拒绝项目/屏障/流缺失、策略版本/保留边界/隐私epoch/位点/审计高水位回退和流重新初始化；事故gate变化至少要有更高审计ID。允许合法增加项目/分区、保留期延长但不回退可见边界，以及重复删除用更晚保护边界的新任务替换旧任务引用。这里不要求保留已不再被屏障引用的旧任务，也不把审计ID增加当作事故解除或门禁放行授权。未来合法元数据删除/重置需要另行审计协议，不能静默绕过规则。

成功只证明所给公钥与锚点下的签名/连续性对应关系；sourceEndpointVerified、anchorCustodyVerified、latestStateVerified仍false，原状态验证标记不变。如果攻击者能同时回退记录与独立锚点，本原语无法发现新记录被隐藏；测试明确保留这个边界。签名也不证明发布者读取了正确原源，持私钥者可以签任意内容。记录payload是base64明文而非加密，持久化/投递前仍须保护敏感元数据。实际PG用例使用内存临时key和外部变量模拟锚点，备份关联为显式夹具，不冒充正式签名密钥/独立锚点保管或真实备份绑定验收。

协调器定时器运行，源VPS不持有离机目的地写入凭证。快照与状态任务分别使用单任务锁，发布索引和轮转使用短时共同锁；长时间dump/恢复核验不得阻塞5分钟状态导出。有界重试/退避，失败不移动成功水位；stale状态与最近成功时间单独记录。目标通知目前仅本地日志和退出码；独立通知接收方仍待配置，不能声称已通知人。

轮转只处理精确prefix下带本系统有效manifest的对象，先建立保留集合，保留恢复必需的状态链与最新成功集合，再删除非保留对象；未知文件和未验证上传不能作为删旧依据。配置只能限制本系统对象，不允许递归清空目录。故障/未完成集合按受限失败保留期清理，先保留可调查证据。

恢复流程为指定快照与可信最新状态 → 下载验hash/认证解密 → 新临时数据库单事务恢复 → 验release/schema/快照指纹 → 离线应用原源最新清单和策略 → 清理/检查屏障、事故与消费位点 → 隔离Kafka/DLQ回放验证 → 操作员选择受控切流。自动备份任务不自动切换生产库，也不清除完整性事故；保留缺口须走现有审计恢复流程。

## 启用验收与当前未完成项

1. 已有配置/只读文件预检；disabled不访问引用文件，实际CLI无新文件，启用后不宣称执行器可用。尚须在执行器阶段验证真实目的地故障域、SSH指纹、凭证有效性与网络行为；不得记录密钥或明文事件。
2. 对真实独立目的地验证上传/下载、不可变发布、加密认证和实际PG隔离恢复；网络中断/认证失败/空间不足/超时只产生失败，不污染成功水位。
3. 原源持续新增删除、保留策略与事故时，状态流保持完整和新鲜；旧dump加最新清单恢复并重放旧Kafka/DLQ不复活数据；丢失链首/倒退/陈旧输入必须阻断。
4. 轮转不能删最新可恢复集合、依赖状态链或未知对象，凭证轮换/目的地切换均先验证新路径再停旧路径。
5. 记录故障时最后验证可恢复的数据时间，测量快照RPO、状态新鲜度、完整整机重建及人工切流RTO。默认360/5分钟仅目标参数，不直接换算为保证；本机PG恢复3秒也不是整机RTO。

当前已有工具与证据：固定SSH主机身份、手动离机加密/往返解密、单份实际隔离PG恢复、v2原源清单与恢复门禁、当前保留缺口拒收及提交边界演练；共享快照原语已在独立CI PG17.7完成实际dump/restore、并发增删改及独立既有SQL交叉核对，43关系一致。尚缺执行器接入、在线备份VPS验收、可信状态连续同步/锚点、独立目的地实测、自动轮转与调度、独立通知、整机切流和量化RPO/RTO。本方案交付不将这些项标为完成。

步骤203补充CI证据：共享快照实际pg_dump改为有界流式文件捕获，独立复核文件hash后流式输入pg_restore，43关系仍一致；原生本地Node子进程的超限/失败/空输出/超时/取消/竞争输出检查在Linux通过。共享快照超时会等待回调实际清理。以上仍是执行器组件，不启用配置模板、传输目的地或调度。

步骤205新增自动化专用encrypt-stream模式：只读已有受限32字节密钥，不生成新密钥，cipher stdout交给有界捕获原语；格式继续CCPBK001。PG验收改为先加密、认证解密并核对原始hash，再仅从解密副本流式恢复。此为CI本地加密往返，不冒充独立目的地上传/下载或可信最新状态同步。

步骤206提供显式单次本地捕获命令backup:capture，组合上述组件生成独占候选目录与capture.json；详见BACKUP-RECOVERY。它不读取/启用offhost模板，不自动建SSH隧道、上传或调度。候选退出2并保留全部未验证标记，不能更新最近成功备份水位。源连接、客户端版本和Docker目标同源性仍由操作员核对，releaseVerified保持false。
