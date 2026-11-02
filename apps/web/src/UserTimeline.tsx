import { useEffect, useState } from "react";
import { Activity, X } from "lucide-react";
import { api } from "./api";
import { Button } from "./components/ui/button";
type Event = {
  event_id: string;
  event_name: string;
  distinct_id: string;
  event_time: string;
  received_at: string;
  properties: Record<string, unknown>;
  context: Record<string, unknown>;
};
export default function UserTimeline({
  prefix,
  identity,
  onClose,
}: {
  prefix: string;
  identity: string;
  onClose: () => void;
}) {
  const [rows, setRows] = useState<Event[]>([]),
    [canonical, setCanonical] = useState(identity),
    [page, setPage] = useState(0),
    [busy, setBusy] = useState(true),
    [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    setBusy(true);
    setError("");
    api<{ canonical_id: string; events: Event[] }>(
      prefix +
        "/users/events?" +
        new URLSearchParams({ distinct_id: identity, page: String(page) }),
    )
      .then((r) => {
        if (active) {
          setCanonical(r.canonical_id);
          setRows(r.events);
        }
      })
      .catch(() => {
        if (active) {
          setError("暂时无法读取用户时间线，请稍后重试。");
          setRows([]);
        }
      })
      .finally(() => {
        if (active) setBusy(false);
      });
    return () => {
      active = false;
    };
  }, [prefix, identity, page]);
  return (
    <div className="modal-backdrop">
      <section
        className="modal timeline-modal"
        role="dialog"
        aria-modal="true"
        aria-labelledby="timeline-heading"
      >
        <div className="panel-heading">
          <div>
            <h2 id="timeline-heading">用户行为时间线</h2>
            <p className="muted mono">{canonical}</p>
          </div>
          <Button
            variant="ghost"
            size="icon"
            aria-label="关闭用户时间线"
            autoFocus
            onClick={onClose}
          >
            <X />
          </Button>
        </div>
        {canonical !== identity ? (
          <p className="fine-print panel-body">
            此匿名标识已关联到上述用户，时间线同时包含关联前后的事件。
          </p>
        ) : null}
        {error ? (
          <p role="alert" className="error panel-body">
            {error}
          </p>
        ) : null}
        <div className="timeline-list panel-body">
          {rows.map((e) => (
            <article key={e.event_id}>
              <div className="timeline-dot">
                <Activity size={14} />
              </div>
              <div>
                <div className="timeline-title">
                  <strong>{e.event_name}</strong>
                  <time>{new Date(e.event_time).toLocaleString("zh-CN")}</time>
                </div>
                <p className="fine-print">
                  接收于 {new Date(e.received_at).toLocaleString("zh-CN")} ·
                  来源 {String(e.context.source || "未知")}
                </p>
                <p className="mono muted">{e.event_id}</p>
                <details>
                  <summary>查看事件属性</summary>
                  <pre>{JSON.stringify(e.properties, null, 2)}</pre>
                </details>
              </div>
            </article>
          ))}
          {!rows.length ? (
            <p className="empty-inline">
              {busy ? "正在读取…" : "当前保留范围内没有此用户的事件。"}
            </p>
          ) : null}
        </div>
        <div className="panel-footer">
          <Button
            size="sm"
            variant="outline"
            disabled={!page || busy}
            onClick={() => setPage((n) => n - 1)}
          >
            上一页
          </Button>
          <span>第 {page + 1} 页 · 相同时间按 ID 稳定展示，不推断先后</span>
          <Button
            size="sm"
            variant="outline"
            disabled={rows.length < 50 || busy}
            onClick={() => setPage((n) => n + 1)}
          >
            下一页
          </Button>
        </div>
      </section>
    </div>
  );
}
