import {useEffect,useState,type FormEvent} from 'react';
import {Copy,MailPlus,RefreshCw} from 'lucide-react';
import {api,ApiError} from './api';
import {Button} from './components/ui/button';
import {Input} from './components/ui/input';
import {invitationLink} from './invitation-link';
type Invitation={id:string;email:string;role:string;expires_at:string;accepted_at:string|null;revoked_at:string|null};
const roles:Record<string,string>={admin:'管理员',analyst:'分析成员',viewer:'只读成员'};
const message=(error:unknown)=>error instanceof ApiError?({already_member:'该账号已经是本公司成员。',invalid_invitation:'请填写有效邮箱和角色。',invitation_limit:'本公司的有效邀请已达上限，请先撤销不再使用的邀请。',rate_limited:'操作过于频繁，请稍后再试。',forbidden:'当前账号没有邀请管理权限。',not_found:'邀请已被处理或当前权限已失效，请刷新列表。',csrf_failed:'会话校验失败，请刷新页面。'}[error.code]??'邀请操作暂未完成，请刷新列表核对。'):'网络结果尚未确认，请刷新列表；重新创建同邮箱邀请会使旧链接失效。';
export default function CompanyInvitations({tenantId,companyName,refresh}:{tenantId:string;companyName:string;refresh:number}){
  const [rows,setRows]=useState<Invitation[]>([]),[version,setVersion]=useState(0),[loading,setLoading]=useState(true),[busy,setBusy]=useState(false),[error,setError]=useState(''),[notice,setNotice]=useState(''),[issued,setIssued]=useState<{email:string;link:string}|null>(null);
  useEffect(()=>{const abort=new AbortController();setLoading(true);api<{invitations:Invitation[]}>(`/api/tenants/${tenantId}/invitations`,{signal:abort.signal}).then(data=>{if(!abort.signal.aborted){setRows(data.invitations);setLoading(false);}}).catch(e=>{if(!abort.signal.aborted){if(e instanceof ApiError&&[401,403,404].includes(e.status)){setRows([]);setIssued(null);}setError(message(e));setLoading(false);}});return()=>abort.abort();},[tenantId,version,refresh]);
  async function create(event:FormEvent<HTMLFormElement>){
    event.preventDefault();if(busy)return;const form=event.currentTarget,values=new FormData(form);setBusy(true);setError('');setNotice('');setIssued(null);
    try{const value=await api<{email:string;token:string}>(`/api/tenants/${tenantId}/invitations`,{method:'POST',body:JSON.stringify({email:values.get('email'),role:values.get('role')})});setIssued({email:value.email,link:invitationLink(value.token,location.origin)});form.reset();setVersion(v=>v+1);}
    catch(e){setError(message(e));}finally{setBusy(false);}
  }
  async function revoke(id:string){if(busy)return;setBusy(true);setError('');try{await api(`/api/tenants/${tenantId}/invitations/${id}`,{method:'DELETE'});setIssued(null);setNotice('邀请已撤销。');setVersion(v=>v+1);}catch(e){setError(message(e));}finally{setBusy(false);}}
  return <section className="panel invitation-panel" aria-labelledby="invite-heading">
    <div className="panel-heading"><div><h2 id="invite-heading">邀请已有账号</h2><p className="muted">加入 {companyName} 后，可查看本公司全部项目。</p></div><MailPlus size={20}/></div>
    <div className="panel-body">
      <form className="invitation-form" onSubmit={create}><label>受邀邮箱<Input name="email" type="email" required maxLength={254} placeholder="colleague@company.com"/></label><label>公司角色<select name="role" defaultValue="analyst"><option value="analyst">分析成员</option><option value="viewer">只读成员</option><option value="admin">管理员</option></select></label><Button disabled={busy}>{busy?'正在处理…':'创建邀请链接'}</Button></form>
      <p className="fine-print">链接有效期 7 天。请将链接交给对方，对方须登录该邮箱账号后主动接受。</p>
      {issued?<div className="invitation-issued"><strong>交给 {issued.email}</strong><label>邀请链接<Input value={issued.link} readOnly aria-label="邀请链接" onFocus={e=>e.currentTarget.select()}/></label><div className="invitation-actions"><Button type="button" variant="outline" size="sm" onClick={async()=>{try{await navigator.clipboard.writeText(issued.link);setNotice('邀请链接已复制。');}catch{setNotice('请选中链接手动复制。');}}}><Copy/>复制链接</Button><Button type="button" variant="ghost" size="sm" onClick={()=>setIssued(null)}>关闭链接</Button></div><p className="fine-print">链接仅在本次创建后显示。需要重新发送时可再创建，旧链接会失效。</p></div>:null}
      {error?<p role="alert" className="error">{error}</p>:null}{notice?<p role="status">{notice}</p>:null}
      <div className="invitation-list-heading"><h3>最近的邀请</h3><Button type="button" variant="ghost" size="sm" disabled={busy||loading} onClick={()=>{setError('');setVersion(v=>v+1);}}><RefreshCw/>刷新</Button></div>
      {loading?<p className="muted" role="status">正在读取邀请…</p>:rows.length?<div className="table-scroll"><table><thead><tr><th>邮箱</th><th>角色</th><th>状态</th><th>有效期至</th><th>操作</th></tr></thead><tbody>{rows.map(row=>{const state=row.revoked_at?'已撤销':row.accepted_at?'已接受':Date.parse(row.expires_at)<=Date.now()?'已过期':'待接受';return<tr key={row.id}><td>{row.email}</td><td>{roles[row.role]}</td><td><span className="scope-badge">{state}</span></td><td>{new Date(row.expires_at).toLocaleString('zh-CN',{hour12:false})}</td><td>{state==='待接受'?<Button type="button" variant="outline" size="sm" disabled={busy} onClick={()=>void revoke(row.id)}>撤销</Button>:'—'}</td></tr>;})}</tbody></table></div>:<p className="muted">还没有邀请。在上方填写已有平台账号的邮箱，创建加入链接。</p>}
    </div>
  </section>;
}
