import subprocess,struct,json,re
from pathlib import Path
pid=int(subprocess.check_output(['docker','inspect','-f','{{.State.Pid}}','ccpanel-kafka-1']).decode())
p=Path('/proc')/str(pid)/'root/tmp/hsperfdata_appuser/1'
assert p.stat().st_size<=1048576
b=p.read_bytes();assert len(b)>=32 and b[:4]==bytes.fromhex('cafec0c0')
assert b[4] in (0,1) and b[5:8]==bytes([2,0,1])
order='<' if b[4] else '>'
used,overflow=struct.unpack_from(order+'ii',b,8);offset,count=struct.unpack_from(order+'ii',b,24)
assert 32<=offset<=used<=len(b) and overflow==0 and 0<=count<=10000
selected={}
pattern=r'(sun\.gc\.generation\.\d+\.(capacity|minCapacity|maxCapacity)|sun\.gc\.generation\.\d+\.space\.\d+\.(used|capacity)|sun\.gc\.(metaspace|compressedclassspace)\.(used|capacity|maxCapacity)|sun\.gc\.policy\.(maxHeapSize|minHeapSize)|java\.threads\.(live|daemon))'
for _ in range(count):
 assert offset+20<=used
 length,nameoff,vector=struct.unpack_from(order+'iii',b,offset);datatype=b[offset+12];dataoff=struct.unpack_from(order+'i',b,offset+16)[0]
 assert 20<=length<=used-offset and 20<=nameoff<length and 0<=dataoff<length
 end=b.find(b'\0',offset+nameoff,offset+length);assert end!=-1
 name=b[offset+nameoff:end].decode('ascii')
 if re.fullmatch(pattern,name):
  assert datatype==ord('J') and vector==0 and dataoff+8<=length
  value=struct.unpack_from(order+'q',b,offset+dataoff)[0];assert value>=0
  selected[name]=value
 offset+=length
assert p.read_bytes()[:32]==b[:32],'structure_changed_retry_required'
assert selected
print(json.dumps({'source':'HotSpot perfdata 2.0 numeric allowlist','counters':selected,'no_heap_dump':True,'no_attach':True}))
