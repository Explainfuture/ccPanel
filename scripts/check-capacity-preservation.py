import importlib.util,json,pathlib,tempfile,hashlib
spec=importlib.util.spec_from_file_location('preservation',str(pathlib.Path(__file__).with_name('capacity-preservation.py')))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
def text(rows):return '\n'.join(json.dumps([name,*value]) for name,value in rows.items())
before={'public.events':(2,-4080913262736524102),'public.consumer_progress':(2,77),'public.operational_samples':(140,9223372036854775807),'public.operational_alert_states':(4,22),'public.operational_alert_history':(8,-9223372036854775808),'public.events_2026_08':(0,0)}
parsed=module.parse(text(before));assert parsed==before and parsed['public.operational_samples'][1]==9223372036854775807
prefix={name:before[name] for name in module.PREFIX};after=dict(before);after['public.consumer_progress']=(2,99);after['public.operational_samples']=(260,33);after['public.operational_alert_states']=(4,44);after['public.operational_alert_history']=(12,55);del after['public.events_2026_08'];after['public.events_2026_12']=(0,0)
assert module.compare(parsed,after,prefix)['preserved']
for kind in ('business','monitor','unknown_table','populated_partition','rule_inventory','consumer_inventory','monitor_inventory'):
    a=dict(after);p=dict(prefix)
    if kind=='business':a['public.events']=(2,-4080913262736524101)
    if kind=='monitor':p['public.operational_samples']=(140,9223372036854775806)
    if kind=='unknown_table':a['public.unexpected']=(0,0)
    if kind=='populated_partition':a['public.events_2026_12']=(1,1)
    if kind=='rule_inventory':a['public.operational_alert_states']=(3,44)
    if kind=='consumer_inventory':a['public.consumer_progress']=(1,99)
    if kind=='monitor_inventory':a['public.operational_samples']=(139,33)
    assert not module.compare(parsed,a,p)['preserved'],kind
for invalid in ['["public.events",true,0]','["public.events",1,1.5]','["public.events",1,9223372036854775808]','["public.events;DROP",1,0]', '["public.events",1,0]\n["public.events",1,0]']:
    try:module.parse(invalid);raise AssertionError('Expected rejection')
    except ValueError:pass
sql=module.prefix_sql(parsed);assert 'LIMIT 140' in sql and 'ORDER BY received_at,source,instance_id,observed_at' in sql and 'ORDER BY id LIMIT 8' in sql
with tempfile.TemporaryDirectory(prefix='ccpanel-preservation-') as folder:
    root=pathlib.Path(folder);files=[root/name for name in ['before.jsonl','after.jsonl','prefix.jsonl']]
    for file,rows in zip(files,[before,after,prefix]):file.write_text(text(rows),encoding='utf-8')
    report=root/'report.json';assert module.main(['verify',*[str(file) for file in files],str(report)])==0
    value=json.loads(report.read_text(encoding='utf-8'));assert value['preserved'] and value['input_sha256']==[hashlib.sha256(file.read_bytes()).hexdigest() for file in files]
    try:module.main(['verify',*[str(file) for file in files],str(report)]);raise AssertionError('Expected overwrite rejection')
    except FileExistsError:pass
print('Exact int64, original monitor subsets, business rows, inventories and conservative partition checks passed')
