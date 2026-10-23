"""Offline post-cleanup fingerprints; never silently ignore original monitor rows."""
import hashlib,json,os,pathlib,re,sys
MUTABLE={'public.consumer_progress','public.operational_samples','public.operational_alert_states','public.operational_alert_history'}
PREFIX={'public.operational_samples':'received_at,source,instance_id,observed_at','public.operational_alert_history':'id'}
def parse(text):
    if len(text.encode('utf-8'))>1024*1024:raise ValueError('Oversized fingerprints')
    rows={}
    for line in text.splitlines():
        value=json.loads(line)
        if not isinstance(value,list) or len(value)!=3:raise ValueError('Invalid fingerprint row')
        name,count,fingerprint=value
        if not isinstance(name,str) or not re.fullmatch(r'public\.[a-z_][a-z0-9_]*',name) or name in rows:raise ValueError('Invalid or duplicate relation')
        if type(count) is not int or not 0<=count<2**63 or type(fingerprint) is not int or not -2**63<=fingerprint<2**63:raise ValueError('Invalid exact integer fingerprint')
        rows[name]=(count,fingerprint)
    if not rows or len(rows)>512:raise ValueError('Invalid relation inventory')
    return rows
def prefix_sql(before):
    if not set(PREFIX).issubset(before):raise ValueError('Missing monitor baseline')
    return '\n'.join("SELECT json_build_array('%s',count(*),coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)) FROM (SELECT * FROM %s ORDER BY %s LIMIT %d) t;"%(name,name,order,before[name][0]) for name,order in PREFIX.items())+'\n'
def compare(before,after,prefix):
    reasons=[];checked=0
    if not MUTABLE.issubset(before) or not MUTABLE.issubset(after):raise ValueError('Missing capacity baseline relations')
    if set(prefix)!=set(PREFIX):raise ValueError('Expected exact original monitor subset inventory')
    partition=lambda name:re.fullmatch(r'public\.events_\d{4}_(0[1-9]|1[0-2])',name) is not None
    for name in sorted(set(before)|set(after)):
        if name not in before:
            if not(partition(name) and after[name]==(0,0)):reasons.append('unexpected_relation:'+name)
        elif name not in after:
            if not(partition(name) and before[name]==(0,0)):reasons.append('missing_relation:'+name)
        elif name not in MUTABLE:
            checked+=1
            if before[name]!=after[name]:reasons.append('original_relation_changed:'+name)
    for name in PREFIX:
        if before[name]!=prefix[name]:reasons.append('original_monitor_rows_changed:'+name)
        if after[name][0]<before[name][0]:reasons.append('monitor_inventory_decreased:'+name)
    if before['public.consumer_progress'][0]!=after['public.consumer_progress'][0]:reasons.append('consumer_partition_inventory_changed')
    if before['public.operational_alert_states'][0]!=4 or after['public.operational_alert_states'][0]!=4:reasons.append('incomplete_alert_rule_inventory')
    return {'preserved':not reasons,'reasons':reasons,'checked_business_relations':checked,'original_monitor_rows':{name:before[name][0] for name in PREFIX},'changed_mutable_relations':[name for name in sorted(MUTABLE) if before[name]!=after[name]],'consumer_offsets_and_fixture_ids_require_independent_proof':True}
def main(args):
    if args[0]=='sql' and len(args)==2:
        print(prefix_sql(parse(pathlib.Path(args[1]).read_text(encoding='utf-8'))),end='');return
    if args[0]!='verify' or len(args)!=5:raise ValueError('Usage: sql baseline.jsonl | verify baseline.jsonl after.jsonl original-prefix.jsonl new-report.json')
    raw=[pathlib.Path(name).read_bytes() for name in args[1:4]]
    report=compare(*[parse(value.decode('utf-8')) for value in raw]);report['input_sha256']=[hashlib.sha256(value).hexdigest() for value in raw]
    fd=os.open(args[4],os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'w',encoding='utf-8') as file:json.dump(report,file);file.flush();os.fsync(file.fileno())
    print(json.dumps(report));return 0 if report['preserved'] else 1
if __name__=='__main__':
    try:sys.exit(main(sys.argv[1:]))
    except (ValueError,IndexError):raise SystemExit('Invalid preservation evidence; no source rows displayed')
