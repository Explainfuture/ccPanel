import {useEffect,useState,type FormEvent} from 'react';
import {ArrowRight,Building2} from 'lucide-react';
import {api,ApiError} from './api';
import {Button} from './components/ui/button';
import {Input} from './components/ui/input';
import {parseInvitationInput} from './invitation-link';
type Preview={company_name:string;email:string;role:string;expires_at:string;already_accepted:boolean};
export default function AcceptInvitation({secret,email,onToken,onClose,onAccepted,onSwitchAccount}:{secret:string;email:string;onToken:(token:string)=>void;onClose:()=>void;onAccepted:(tenant:string)=>Promise<void>;onSwitchAccount:()=>Promise<void>}){
  const [preview,setPreview]=useState<Preview|null>(null),[error,setError]=useState(''),[loading,setLoading]=useState(false),[busy,setBusy]=useState(false),[refresh,setRefresh]=useState(0);
  useEffect(()=>{if(!secret)return;const abort=new AbortController();setPreview(null);setError('');setLoading(true);api<Preview>('/api/invitations/preview',{method:'POST',body:JSON.stringify({token:secret}),signal:abort.signal}).then(value=>{if(!abort.signal.aborted){setPreview(value);setLoading(false);}}).catch(e=>{if(!abort.signal.aborted){setError(e instanceof ApiError&&e.status===404?'邀请不可用、已过期，或与当前登录邮箱不匹配。请使用受邀账号，或联系管理员重新创建邀请。':'暂时无法读取邀请，请重试。');setLoading(false);}});return()=>abort.abort();},[secret,email,refresh]);
  function paste(event:FormEvent<HTMLFormElement>){event.preventDefault();try{onToken(parseInvitationInput(String(new FormData(event.currentTarget).get('link')||''),location.origin));setError('');}catch{setError('请输入当前平台的完整邀请链接。');}}
  async function accept(){if(busy||!preview)return;setBusy(true);setError('');try{const result=await api<{tenant_id:string}>('/api/invitations/accept',{method:'POST',body:JSON.stringify({token:secret})});await onAccepted(result.tenant_id);}catch(e){setError(e instanceof ApiError&&e.status===404?'邀请已失效或成员权限已变更，请联系管理员。':'加入结果尚未确认，请重试同一邀请以核对。');}finally{setBusy(false);}}
  return <main className="invitation-landing"><section className="panel invitation-card" aria-labelledby="accept-invitation-heading"><div className="panel-heading"><div><p className="eyebrow">公司邀请</p><h1 id="accept-invitation-heading">加入公司的数据工作区</h1></div><Building2 size={25}/></div><div className="panel-body">
    <p className="muted">当前账号：{email}</p>
    {!secret?<form className="invitation-paste" onSubmit={paste}><label>邀请链接<Input name="link" autoComplete="off" required placeholder="粘贴管理员发来的链接"/></label><Button>查看邀请<ArrowRight/></Button></form>:null}
    {loading?<p role="status">正在核对邀请…</p>:null}
    {preview?<><div className="invitation-summary"><h2>{preview.company_name}</h2><p>加入后可查看本公司全部项目，操作角色为 <strong>{{admin:'管理员',analyst:'分析成员',viewer:'只读成员'}[preview.role]}</strong>。</p><p className="fine-print">{preview.already_accepted?'此账号已接受该邀请。':'邀请有效期至 '+new Date(preview.expires_at).toLocaleString('zh-CN',{hour12:false})}</p></div><Button disabled={busy} onClick={()=>void accept()}>{busy?'正在核对加入结果…':preview.already_accepted?'进入公司':'接受邀请并加入'}<ArrowRight/></Button></>:null}
    {error?<p className="error" role="alert">{error}</p>:null}
    <div className="invitation-actions">{secret&&!loading&&!preview?<Button type="button" variant="outline" disabled={busy} onClick={()=>setRefresh(v=>v+1)}>重新核对</Button>:null}<Button type="button" variant="ghost" disabled={busy} onClick={async()=>{setBusy(true);try{await onSwitchAccount();}catch{setError('退出账号暂未完成，请重试。');}finally{setBusy(false);}}}>切换登录账号</Button><Button type="button" variant="ghost" disabled={busy} onClick={onClose}>返回工作区</Button></div>
  </div></section></main>;
}
