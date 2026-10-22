# 查询结果导出

基线：后台查询任务保留24小时。admin/analyst可通过`GET /api/tenants/:tenant/projects/:project/query-jobs/:job/download?format=json|csv`下载已完成结果，viewer只有查询/查看权限。下载复用任务结果及提交时的报表版本/时区，不重跑分析。

每次请求检查当前登录会话、目标公司/项目、当前成员角色、任务发起者仍为有效成员、过期时间和当前隐私版本。序列化期间锁定成员与项目隐私边界；请求处理中发生的角色变更也重新读取。不存在、跨公司、过期、隐私已改变、发起者退出公司的对象返回404；只读角色403，未完成409，格式无效400，文件预算超出413。错误响应没有attachment头，不生成截断的“成功文件”。任务列表和详情同样隐藏失效发起者的结果。

JSON包含版本1封套、公司/项目/任务ID、创建/完成/导出时间、完整查询条件、报表快照和结果，保留原JSON值及null。CSV为UTF-8 BOM、双引号转义和CRLF，列为path/type/value，path采用JSON Pointer（数组为索引，斜杠与波浪号转义）。它包含全部元数据和结果，保留未成熟留存的null、类型、空数组/对象及展示截断标记。CSV文本单元格带制表符前缀，用于表格查看；程序导入原值使用JSON。这一处理参考[OWASP CSV Injection](https://github.com/OWASP/www-community/blob/master/pages/attacks/CSV_Injection.md)，不宣称所有表格工具都以同一方式解释CSV。

预算：源JSON最多2MiB，实际文件最多1MiB；CSV最多10000个叶子/空集合记录、32层。超过预算拒绝，建议缩小查询范围。每API进程最多一个在途下载；Node通过响应finish/close释放额度，取消或10秒超时也释放，避免最后一块预读就被当作已发送。准备阶段使用数据库全局导出互斥锁，单语句/锁等待2秒；每公司/账号每分钟最多10次，繁忙返回Retry-After。响应为attachment/no-store/nosniff，不提供免鉴权长期公开链接。

审计`query_export_prepared`只记录项目/任务/格式/字节数，表示服务端准备输出，不代表终端已完整保存。中途断开仍可能发生，文件完整性依赖HTTP完整读取与Content-Length。下载后已在用户设备上的副本不属于服务端存储的派生结果。

验收：126项单元测试/类型检查/构建，另有`scripts/query-export-check.ts`在隔离PG验证真实Hono响应、类型、CSV前缀、文件头/审计、角色/公司/隐私/期限、预算、取消、数据库互斥、限频、发起者移除后的列表/详情/下载拒绝，以及请求途中提交降权的403。Node适配器finish额度用事件替身检查；实际公网浏览器下载待整合部署。检查中的完成结果为显式合成夹具，查询实际执行另由报表PG验收覆盖。
