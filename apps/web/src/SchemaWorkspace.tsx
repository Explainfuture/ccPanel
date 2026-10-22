import { useEffect, useState, type FormEvent } from "react";
import { ArrowDownToLine, Braces, Save } from "lucide-react";
import { api, ApiError } from "./api";
import { Button } from "./components/ui/button";
import { Input } from "./components/ui/input";
type Schema = {
  event_name: string;
  required_properties: Record<string, string>;
  version: number;
};
type Project = { timezone: string; origins: string[]; schema_mode: string };
export default function SchemaWorkspace({
  prefix,
  project,
  canEdit,
  onSaved,
}: {
  prefix: string;
  project: Project;
  canEdit: boolean;
  onSaved: () => Promise<void>;
}) {
  const [schemas, setSchemas] = useState<Schema[]>([]),
    [error, setError] = useState(""),
    [notice, setNotice] = useState(""),
    [name, setName] = useState(""),
    [rules, setRules] = useState("{}"),
    [busy, setBusy] = useState(false);
  const refresh = async () =>
    setSchemas((await api<{ schemas: Schema[] }>(prefix + "/schemas")).schemas);
  useEffect(() => {
    let active = true;
    api<{ schemas: Schema[] }>(prefix + "/schemas")
      .then((v) => {
        if (active) setSchemas(v.schemas);
      })
      .catch(() => {
        if (active) setError("暂时无法读取事件规范。");
      });
    return () => {
      active = false;
    };
  }, [prefix]);
  async function save(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError("");
    setNotice("");
    setBusy(true);
    try {
      let required;
      try {
        required = JSON.parse(rules);
      } catch {
        throw new Error("属性规则必须是有效 JSON 对象。");
      }
      await api(prefix + "/schemas/" + encodeURIComponent(name), {
        method: "PUT",
        body: JSON.stringify({ required_properties: required }),
      });
      await refresh();
      setNotice("事件规范已保存，后续上报按新版本校验。");
    } catch (e) {
      setError(
        e instanceof ApiError
          ? "保存失败：请检查事件名、属性类型与操作权限。"
          : e instanceof Error
            ? e.message
            : "保存失败。",
      );
    } finally {
      setBusy(false);
    }
  }
  async function settings(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError("");
    setNotice("");
    setBusy(true);
    const f = new FormData(e.currentTarget);
    try {
      await api(prefix + "/settings", {
        method: "PUT",
        body: JSON.stringify({
          timezone: f.get("timezone"),
          schema_mode: f.get("schema_mode"),
          origins: String(f.get("origins"))
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean),
        }),
      });
      await onSaved();
      setNotice("项目设置已更新。");
    } catch {
      setError("设置保存失败，请检查时区、来源和操作权限。");
    } finally {
      setBusy(false);
    }
  }
  return (
    <div>
      {error ? (
        <div className="error banner" role="alert">
          {error}
        </div>
      ) : null}
      {notice ? (
        <div className="notice" role="status">
          {notice}
        </div>
      ) : null}
      <section className="panel">
        <div className="panel-heading">
          <div>
            <h2>项目事件规范</h2>
            <p className="muted">业务定义事件与必填属性，SDK 负责可靠发送。</p>
          </div>
          <Button variant="outline" asChild>
            <a href={prefix + "/schemas/types"}>
              <ArrowDownToLine />
              下载 TypeScript 类型
            </a>
          </Button>
        </div>
        {schemas.length ? (
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>事件名称</th>
                  <th>必填属性</th>
                  <th>版本</th>
                  {canEdit ? <th>操作</th> : null}
                </tr>
              </thead>
              <tbody>
                {schemas.map((s) => (
                  <tr key={s.event_name}>
                    <td>
                      <span className="event-link">
                        <Braces size={15} />
                        {s.event_name}
                      </span>
                    </td>
                    <td>
                      <code>{JSON.stringify(s.required_properties)}</code>
                    </td>
                    <td>v{s.version}</td>
                    {canEdit ? (
                      <td>
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() => {
                            setName(s.event_name);
                            setRules(
                              JSON.stringify(s.required_properties, null, 2),
                            );
                          }}
                        >
                          编辑
                        </Button>
                      </td>
                    ) : null}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="empty-inline">
            尚未登记事件。宽松模式仍可接收符合通用协议的事件。
          </p>
        )}
        {canEdit ? (
          <form className="schema-form panel-body" onSubmit={save}>
            <label>
              事件名称
              <Input
                value={name}
                onChange={(e) => setName(e.target.value)}
                required
                maxLength={128}
                placeholder="例如 purchase_completed"
              />
            </label>
            <label>
              必填属性与类型
              <textarea
                className="code-editor"
                value={rules}
                onChange={(e) => setRules(e.target.value)}
                spellCheck={false}
                rows={6}
                required
                aria-label="必填属性与类型"
              />
            </label>
            <p className="fine-print">
              例如 {`{"order_id":"string","amount":"number"}`}
              。可用类型：string、number、boolean、object、array、null。更新规范会递增版本，不重写历史已接收事件。
            </p>
            <Button disabled={busy}>
              <Save />
              保存事件规范
            </Button>
          </form>
        ) : null}
      </section>
      <section className="panel">
        <div className="panel-heading">
          <h2>项目接入设置</h2>
        </div>
        <form className="panel-body query-form" onSubmit={settings}>
          <div className="query-fields">
            <label>
              校验模式
              <select
                name="schema_mode"
                defaultValue={project.schema_mode}
                disabled={!canEdit}
              >
                <option value="loose">宽松：允许未登记事件</option>
                <option value="strict">严格：拒绝未登记事件</option>
              </select>
            </label>
            <label>
              分析时区
              <Input
                name="timezone"
                defaultValue={project.timezone}
                readOnly={!canEdit}
                required
                placeholder="Asia/Shanghai"
              />
            </label>
            <label>
              允许的网页来源
              <Input
                name="origins"
                defaultValue={project.origins.join(", ")}
                readOnly={!canEdit}
                placeholder="https://your-site.com"
              />
            </label>
          </div>
          <p className="fine-print">
            严格模式会拒绝未登记的业务事件，请先完成事件登记。来源填协议与域名，多个来源用逗号分隔。平台账号权限不由写入凭证授予。
          </p>
          {canEdit ? (
            <Button className="mt-5" disabled={busy}>
              <Save />
              保存项目设置
            </Button>
          ) : null}
        </form>
      </section>
    </div>
  );
}
