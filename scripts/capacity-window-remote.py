#!/usr/bin/env python3
"""Finite, private capacity-window control. This never grants capacity acceptance.

The deploy flock is transferred as its original open file description to PID1.
That custody covers normal cleanup and the tested MainPID-kill/StopPost path;
it does not promise survival of host/PID1/StopPost failure. Parent directories
are private operator custody, not an adversarial concurrent filesystem sandbox.
"""
import array
import hashlib
import json
import os
from pathlib import Path
import queue
import re
import select
import selectors
import socket
import stat
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
try:
    import fcntl
except ImportError:
    fcntl = None

HEX = re.compile(r'^[a-f0-9]{64}$')
RUN = re.compile(r'^[a-f0-9]{32}$')
INT = re.compile(r'^(0|[1-9][0-9]{0,18})$')
POS = re.compile(r'^[1-9][0-9]{0,18}$')
RELEASE = Path('/opt/ccpanel/releases/m4-polling-014')
SHARED = Path('/opt/ccpanel/shared')
DEPLOY_LOCK = SHARED / 'deploy.lock'
TOOLS = {'controller':'controller.py', 'coordinator':'run-capacity-window.mjs',
         'sampler':'capacity-sample.py', 'host_state':'capacity-host-state.py',
         'capture':'capture-monitor-preservation.mjs', 'db_operator':'window225-db.mjs',
         'first_stop':'load-first-stop.mjs', 'load_check':'load-check-public.mjs',
         'load_evidence':'load-evidence.mjs', 'load_lease':'load-control-lease.mjs',
         'load_stop_reader':'load-stop-reader.mjs'}
CLIENT = ['load_check','load_evidence','load_lease','load_stop_reader']
NAMES = ['api','worker','kafka','postgres']
PRODUCT = {'api_sha256':'f87bcc8d07b2d84fc87ec59e6e91773c8870ef96be6db86ed86bd6718f8731a9',
           'worker_sha256':'fafdbe510b80547d20da6f2f8821cd60066f5a78e9b2999db2aba0a4a07f8d29',
           'sdk_sha256':'449f14f4719751a13ef1a666ff141511dd223ddc93b246968d87bf2688c4b661'}
PHASES = {'preparing','ready','running','settling','freezing','cleaning','completed','aborted','failed'}
ABORTS = {'resource_memory','resource_disk','resource_invalid','bridge_stale','owner_changed',
          'sampler_failed','sampler_coverage_budget','client_spawn_failed','protocol_invalid',
          'operator_requested','preparation_failed','db_status_failed','operation_failed'}
FILES = {'run':('run.json',16384), 'owner':('owner.json',4096), 'ready':('ready.json',16384),
         'binding':('binding.json',16384), 'fixture':('fixture.json',8192),
         'freeze':('freeze.json',65536), 'sampler_owner':('sampler-owner.json',4096),
         'sampler_terminal':('sampler-terminal.json',16384),
         'resource_baseline':('resource-baseline.json',262144), 'resource_final':('resource-final.json',262144),
         'samples':('samples.jsonl',8*1024*1024), 'monitor_before':('monitor-before.jsonl',32*1024*1024),
         'monitor_after':('monitor-after.jsonl',32*1024*1024), 'db_baseline':('baseline.json',262144),
         'db_verified':('verified.json',262144), 'db_cleanup':('cleanup.json',262144),
         'db_cleanup_deferred':('cleanup-deferred.json',262144), 'key_revoked':('key-revoked.json',16384),
         'fixture_created':('fixture-created.json',16384), 'client_wire_terminal':('client-wire-terminal.json',4096),
         'terminal_accepted':('terminal-accepted.json',4096), 'controller_terminal':('controller-terminal.json',16384),
         'business_after':('business-after.jsonl',2*1024*1024),
         'client_result':('client-result.json',2*1024*1024), 'client_manifest':('client-manifest.json',2*1024*1024),
         'client_terminal':('client-terminal.json',32768), 'client_progress':('client-progress.json',65536),
         'client_stdout':('client-stdout.log',262144), 'client_stderr':('client-stderr.log',65536)}
UPLOAD = {'client_result','client_manifest','client_terminal','client_progress','client_stdout','client_stderr'}
ENV_KEYS = {'DATABASE_URL','KAFKA_BROKERS','PUBLIC_ORIGIN','KAFKA_EVENTS_TOPIC','KAFKA_DLQ_TOPIC',
            'KAFKA_GROUP_ID','KAFKA_RF','KAFKA_MIN_ISR','INGEST_MAX_LAG_MESSAGES','INGEST_MAX_LAG_AGE_MS',
            'INGEST_MAX_PROBE_AGE_MS','INGEST_MAX_DISK_AGE_MS','INGEST_MIN_FREE_BYTES',
            'INGEST_MIN_FREE_PERCENT','INGEST_MIN_FREE_INODES','INGEST_MAX_INFLIGHT'}

class WindowError(Exception):
    pass

class WindowFatal(Exception):
    pass

def require(condition, code):
    if not condition:
        raise WindowError(code)

def sha(raw):
    return hashlib.sha256(raw).hexdigest()

def encoded(value):
    return (json.dumps(value, separators=(',', ':'), ensure_ascii=False, allow_nan=False)+'\n').encode('utf-8')

def no_duplicates(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, 'duplicate_json_key')
        value[key] = item
    return value

def decode(raw, maximum, wire=False):
    require(isinstance(raw, bytes) and 0 < len(raw) <= maximum, 'invalid_json_size')
    try:
        text = raw.decode('utf-8', 'strict')
        value = json.loads(text, object_pairs_hook=no_duplicates,
                           parse_float=(lambda _: (_ for _ in ()).throw(WindowError('noninteger_wire_number'))) if wire else float,
                           parse_constant=lambda _: (_ for _ in ()).throw(WindowError('invalid_json')))
        if wire:
            require(raw.endswith(b'\n') and b'\r' not in raw and encoded(value) == raw, 'noncanonical_wire')
        return value
    except (ValueError, UnicodeError, TypeError):
        raise WindowError('invalid_json')

def shape(value, keys):
    require(type(value) is dict and set(value) == set(keys), 'invalid_shape')

def integer(value, positive=False):
    return type(value) is str and bool((POS if positive else INT).fullmatch(value)) and int(value) <= 9223372036854775807

def digest(value):
    return type(value) is str and bool(HEX.fullmatch(value))

def run_id(value):
    return type(value) is str and bool(RUN.fullmatch(value))

def us():
    return str(time.time_ns()//1000)

def same(left, right):
    return (left.st_dev,left.st_ino,left.st_size,left.st_mtime_ns,left.st_ctime_ns) == (right.st_dev,right.st_ino,right.st_size,right.st_mtime_ns,right.st_ctime_ns)

def private_directory(path):
    path = Path(path)
    info = os.lstat(str(path))
    require(path.is_absolute() and path.resolve() == path and stat.S_ISDIR(info.st_mode)
            and info.st_uid == 0 and stat.S_IMODE(info.st_mode) == 0o700, 'unsafe_directory')
    return info

def stable_file(path, maximum, private=True, collect=True, owner_uid=0):
    path = Path(path)
    before = os.lstat(str(path))
    require(path.resolve() == path and stat.S_ISREG(before.st_mode) and before.st_nlink == 1
            and before.st_uid == owner_uid and (not private or stat.S_IMODE(before.st_mode) == 0o600)
            and 0 <= before.st_size <= maximum, 'unsafe_file')
    descriptor = os.open(str(path), os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0))
    chunks, total, checksum = [], 0, hashlib.sha256()
    try:
        require(same(before,os.fstat(descriptor)), 'file_changed')
        while True:
            chunk = os.read(descriptor,65536)
            if not chunk:
                break
            total += len(chunk)
            require(total <= maximum, 'file_too_large')
            checksum.update(chunk)
            if collect:
                chunks.append(chunk)
        require(total == before.st_size and same(before,os.fstat(descriptor)) and same(before,os.lstat(str(path))), 'file_changed')
    finally:
        os.close(descriptor)
    return {'bytes':total,'sha256':checksum.hexdigest()}, b''.join(chunks) if collect else None

def sync_directory(path):
    descriptor = os.open(str(path),os.O_RDONLY|getattr(os,'O_DIRECTORY',0)|getattr(os,'O_NOFOLLOW',0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

def publish(path, value, raw=False):
    data = value if raw else encoded(value)
    descriptor = os.open(str(path),os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600)
    try:
        at = 0
        while at < len(data):
            at += os.write(descriptor,data[at:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    sync_directory(Path(path).parent)
    return {'bytes':len(data),'sha256':sha(data)}

def artifact_path(state, role):
    if role in FILES:
        return state/FILES[role][0], FILES[role][1]
    if re.fullmatch(r'status_[1-9][0-9]{0,18}',role) and integer(role[7:],True):
        return state/('status-'+role[7:]+'.json'),262144
    raise WindowError('invalid_artifact_role')

def validate_run(value, probe=False):
    shape(value,['v','kind','run_id','release','schema','operator_commit','profile','expected_origin','expected_runtime_sha256','tools_sha256'])
    require(value['v'] == 1 and type(value['v']) is int and value['kind'] == 'run' and run_id(value['run_id'])
            and value['release'] == RELEASE.name and value['schema'] == 22 and type(value['schema']) is int
            and re.fullmatch('[a-f0-9]{40}',value['operator_commit'] or '')
            and value['profile'] == ('window225_probe_v1' if probe else 'window225_fixed_v1')
            and digest(value['expected_runtime_sha256']), 'invalid_run')
    require(type(value['expected_origin']) is str and re.fullmatch(r'https://[A-Za-z0-9.-]+(?::[0-9]{1,5})?',value['expected_origin']), 'invalid_origin')
    shape(value['tools_sha256'],TOOLS)
    require(all(digest(item) for item in value['tools_sha256'].values()), 'invalid_tools')
    return value

def validate_request(value, expected_run, owner_hash, previous):
    require(type(value) is dict, 'invalid_request')
    common = ['v','kind','run_id','owner_sha256','seq','nonce']
    extra = {'heartbeat':['client'], 'abort':['code'], 'client_terminal':['client','close','termination','artifacts']}
    require(value.get('kind') in extra, 'invalid_request_kind')
    shape(value,common+extra[value['kind']])
    require(type(value['v']) is int and value['v'] == 1 and value['run_id'] == expected_run
            and value['owner_sha256'] == owner_hash and integer(value['seq'],True)
            and int(value['seq']) > previous and run_id(value['nonce']), 'invalid_request_binding')
    client = value.get('client')
    if client is not None:
        shape(client,['pid','spawn_nonce'])
        require(type(client['pid']) is int and 0 < client['pid'] <= 4294967295 and run_id(client['spawn_nonce']), 'invalid_client')
    if value['kind'] == 'abort':
        require(value['code'] in ABORTS, 'invalid_abort_code')
    elif value['kind'] == 'client_terminal':
        require(client is not None and value['termination'] in ['completed','aborted','missing'], 'invalid_client_terminal')
        close = value['close']
        shape(close,['exit_code','signal','observed_at'])
        require((type(close['exit_code']) is int and 0 <= close['exit_code'] <= 4294967295 and close['signal'] is None)
                or (close['exit_code'] is None and close['signal'] in ['SIGINT','SIGTERM','SIGKILL','SIGABRT','SIGSEGV','SIGHUP','SIGPIPE','SIGBUS','SIGILL','SIGFPE','SIGQUIT']), 'invalid_native_close')
        require(type(close['observed_at']) is str and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z',close['observed_at']), 'invalid_native_close')
        require(type(value['artifacts']) is list and 1 <= len(value['artifacts']) <= len(UPLOAD), 'invalid_artifact_inventory')
        seen = set()
        for item in value['artifacts']:
            shape(item,['role','bytes','sha256'])
            require(item['role'] in UPLOAD and item['role'] not in seen and type(item['bytes']) is int
                    and 0 <= item['bytes'] <= FILES[item['role']][1] and digest(item['sha256']), 'invalid_artifact_reference')
            seen.add(item['role'])
        require('client_terminal' in seen, 'missing_client_terminal')
        if value['termination'] == 'completed':
            require({'client_result','client_manifest','client_terminal','client_stdout','client_stderr'} <= seen, 'missing_client_artifact')
    return value

class Lines:
    def __init__(self, limit=4096):
        self.pending = bytearray()
        self.limit = limit
    def feed(self, chunk):
        require(type(chunk) is bytes and b'\x00' not in chunk, 'invalid_wire_bytes')
        self.pending.extend(chunk)
        result = []
        while b'\n' in self.pending:
            at = self.pending.index(10)+1
            require(at <= self.limit, 'wire_too_large')
            result.append(bytes(self.pending[:at]))
            del self.pending[:at]
        require(len(self.pending) < self.limit, 'wire_too_large')
        return result
    def eof(self):
        require(not self.pending, 'partial_wire')

def bounded(argv, timeout=5, maximum=1048576, input_bytes=None, allowed=(0,)):
    process = subprocess.Popen(argv,stdin=subprocess.PIPE if input_bytes is not None else subprocess.DEVNULL,
                               stdout=subprocess.PIPE,stderr=subprocess.PIPE, start_new_session=True)
    monitor = selectors.DefaultSelector()
    result = {'out':bytearray(),'error':bytearray()}
    pending = memoryview(input_bytes or b'')
    deadline = time.monotonic()+timeout
    try:
        for pipe,name in [(process.stdout,'out'),(process.stderr,'error')]:
            os.set_blocking(pipe.fileno(),False)
            monitor.register(pipe,selectors.EVENT_READ,name)
        if process.stdin:
            if pending:
                os.set_blocking(process.stdin.fileno(),False)
                monitor.register(process.stdin,selectors.EVENT_WRITE,'input')
            else:
                process.stdin.close()
        while monitor.get_map():
            require(time.monotonic() < deadline, 'command_timeout')
            for key,event in monitor.select(min(.25,max(0,deadline-time.monotonic()))):
                if key.data == 'input':
                    try:
                        written = os.write(key.fd,pending[:65536])
                    except BrokenPipeError:
                        written,pending = 0,memoryview(b'')
                    pending = pending[written:]
                    if not pending:
                        monitor.unregister(key.fileobj)
                        key.fileobj.close()
                else:
                    chunk = os.read(key.fd,65536)
                    if not chunk:
                        monitor.unregister(key.fileobj)
                        key.fileobj.close()
                        continue
                    result[key.data].extend(chunk)
                    require(len(result[key.data]) <= (maximum if key.data == 'out' else 65536), 'command_output_overflow')
        process.wait(timeout=max(.01,deadline-time.monotonic()))
        require(process.returncode in allowed, 'command_failed')
        return bytes(result['out'])
    finally:
        monitor.close()
        if process.poll() is None:
            os.killpg(process.pid,9)
            process.wait(timeout=3)
        for pipe in [process.stdin,process.stdout,process.stderr]:
            if pipe:
                pipe.close()

def write_deadline(descriptor,raw,seconds=1):
    os.set_blocking(descriptor,False)
    at,deadline = 0,time.monotonic()+seconds
    while at < len(raw):
        remaining = deadline-time.monotonic()
        require(remaining > 0, 'output_timeout')
        if not select.select([],[descriptor],[],min(remaining,.25))[1]:
            continue
        try:
            at += os.write(descriptor,raw[at:])
        except BlockingIOError:
            continue

def properties(unit, fields):
    raw = bounded(['systemctl','show',unit,'--property='+','.join(fields)],2,32768).decode('utf-8','strict')
    result = dict(line.split('=',1) for line in raw.splitlines())
    require(set(result) == set(fields), 'unit_properties_missing')
    return result

def service_numbers(unit, fields):
    raw = bounded(['busctl','call','org.freedesktop.systemd1','/org/freedesktop/systemd1',
                   'org.freedesktop.systemd1.Manager','GetUnit','s',unit],2,4096).decode().strip()
    match = re.fullmatch(r'o "(/org/freedesktop/systemd1/unit/[A-Za-z0-9_]+)"',raw)
    require(match is not None, 'unit_object_missing')
    result = {}
    for field in fields:
        raw = bounded(['busctl','get-property','org.freedesktop.systemd1',match[1],
                       'org.freedesktop.systemd1.Service',field],2,4096).decode().strip()
        match_value = re.fullmatch(r'[tu] ([0-9]+)',raw)
        require(match_value is not None, 'unit_property_invalid')
        result[field] = int(match_value[1])
    return result

def start_ticks(pid):
    raw = Path('/proc/%d/stat'%pid).read_text()
    fields = raw[raw.rfind(')')+2:].split()
    require(len(fields) >= 20 and fields[0] not in ['Z','X'] and POS.fullmatch(fields[19]), 'native_process_missing')
    return fields[19]

def notify(message, descriptor=None):
    address = os.environ.get('NOTIFY_SOCKET','')
    require(address.startswith('/') or address.startswith('@'), 'notify_socket_missing')
    if address.startswith('@'):
        address = '\0'+address[1:]
    with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as channel:
        channel.settimeout(2)
        channel.connect(address)
        channel.sendmsg([message.encode('ascii')],[] if descriptor is None else
                        [(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[descriptor]).tobytes())])

def barrier():
    read_fd, write_fd = os.pipe()
    try:
        notify('BARRIER=1',write_fd)
        os.close(write_fd)
        write_fd = None
        require(bool(select.select([read_fd],[],[],3)[0]) and os.read(read_fd,1) == b'', 'fdstore_barrier_failed')
    finally:
        if write_fd is not None:
            os.close(write_fd)
        os.close(read_fd)

def lock_available(path):
    descriptor = os.open(str(path),os.O_RDWR|getattr(os,'O_NOFOLLOW',0))
    try:
        try:
            fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(descriptor,fcntl.LOCK_UN)
        return True
    finally:
        os.close(descriptor)

def context(state, run_sha, self_sha, probe=False):
    require(sys.platform == 'linux' and os.geteuid() == 0 and fcntl is not None, 'root_linux_required')
    state = Path(state)
    pattern = r'/opt/ccpanel/shared/window225'+('-probe' if probe else '')+r'\.[A-Za-z0-9]{6}'
    require(re.fullmatch(pattern,str(state)) and digest(run_sha) and digest(self_sha), 'invalid_launch_arguments')
    private_directory(state)
    script = Path(__file__).resolve()
    require(re.fullmatch(r'/opt/ccpanel/shared/window225-tools\.[A-Za-z0-9]{6}/controller.py',str(script)), 'unsafe_controller_path')
    private_directory(script.parent)
    require(stable_file(script,262144)[0]['sha256'] == self_sha, 'controller_hash_mismatch')
    ref, raw = stable_file(state/'run.json',16384)
    require(ref['sha256'] == run_sha, 'run_hash_mismatch')
    run = validate_run(decode(raw,16384),probe)
    require(run['tools_sha256']['controller'] == self_sha, 'controller_hash_mismatch')
    for role, name in TOOLS.items():
        require(stable_file(script.parent/name,4*1024*1024)[0]['sha256'] == run['tools_sha256'][role], 'tool_hash_mismatch')
    return state,script,run

def unit_name(run, probe=False):
    return ('ccpanel-window225-probe-' if probe else 'ccpanel-capacity-window225-')+run['run_id']+'.service'

def launch(state, run_sha, self_sha, probe=False):
    state,script,run = context(state,run_sha,self_sha,probe)
    require(set(item.name for item in state.iterdir()) == {'run.json'}, 'run_already_started')
    unit = unit_name(run,probe)
    stop = '/usr/bin/python3 %s stop-post %s %s %s%s'%(script,state,run_sha,self_sha,' probe' if probe else '')
    args = ['systemd-run','--quiet','--wait','--pipe','--collect','--unit='+unit,
            '--property=Type=exec','--property=User=root','--property=RuntimeMaxSec='+('60' if probe else '2700'),
            '--property=TimeoutStopSec='+('30' if probe else '120'),'--property=MemoryMax=64M',
            '--property=CPUQuota=5%','--property=KillMode=control-group','--property=NotifyAccess=all',
            '--property=FileDescriptorStoreMax=1','--property=FileDescriptorStorePreserve=yes',
            '--property=NoNewPrivileges=yes','--property=ProtectSystem=strict','--property=ProtectHome=yes',
            '--property=ReadWritePaths='+str(state)+('' if probe else ' '+str(DEPLOY_LOCK)),
            '--property=ExecStopPost='+stop,'/usr/bin/python3',str(script),'serve',str(state),run_sha,self_sha]
    if probe:
        args.append('probe')
    os.execvp(args[0],args)

def unit_owner(state,run,script,self_sha,probe=False):
    unit = unit_name(run,probe)
    fields = ['User','MainPID','InvocationID','ActiveState','SubState','RuntimeMaxUSec','TimeoutStopUSec',
              'MemoryMax','CPUQuotaPerSecUSec','KillMode','NotifyAccess','FileDescriptorStoreMax',
              'FileDescriptorStorePreserve','NFileDescriptorStore','ExecStopPost']
    props = properties(unit,fields)
    numeric = service_numbers(unit,['RuntimeMaxUSec','TimeoutStopUSec','MemoryMax','CPUQuotaPerSecUSec'])
    require(props['User'] == 'root' and props['MainPID'] == str(os.getpid()) and run_id(props['InvocationID'])
            and props['InvocationID'] == os.environ.get('INVOCATION_ID') and props['ActiveState'] in ['activating','active']
            and numeric == {'RuntimeMaxUSec':(60 if probe else 2700)*1000000,'TimeoutStopUSec':(30 if probe else 120)*1000000,
                            'MemoryMax':67108864,'CPUQuotaPerSecUSec':50000}
            and props['KillMode'] == 'control-group' and props['NotifyAccess'] == 'all'
            and props['FileDescriptorStoreMax'] == '1' and props['FileDescriptorStorePreserve'] == 'yes'
            and ('%s stop-post %s '%(script,state)) in props['ExecStopPost'], 'unit_contract_mismatch')
    return {'unit':unit,'invocation_id':props['InvocationID'],'main_pid':os.getpid(),'start_ticks':start_ticks(os.getpid())}

def hold_lock(state, native, run_sha, probe=False):
    path = state/'probe.lock' if probe else DEPLOY_LOCK
    if probe:
        descriptor = os.open(str(path),os.O_RDWR|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600)
    else:
        info = os.lstat(str(path))
        require(stat.S_ISREG(info.st_mode) and info.st_uid == 0 and info.st_nlink == 1, 'deploy_lock_unsafe')
        descriptor = os.open(str(path),os.O_RDWR|getattr(os,'O_NOFOLLOW',0))
    try:
        fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
        info = os.fstat(descriptor)
        require((info.st_dev,info.st_ino) == (os.lstat(str(path)).st_dev,os.lstat(str(path)).st_ino), 'deploy_lock_changed')
        publish(state/'lock-intent.json',{'run_sha256':run_sha,'native_owner':native,'path':str(path),
                'device':str(info.st_dev),'inode':str(info.st_ino),'fd_name':'window225-lock'})
        notify('FDSTORE=1\nFDNAME=window225-lock\nFDPOLL=0',descriptor)
        barrier()
        require(properties(native['unit'],['NFileDescriptorStore'])['NFileDescriptorStore'] == '1', 'fdstore_missing')
    finally:
        os.close(descriptor)
    require(not lock_available(path), 'fdstore_lock_not_held')
    publish(state/'lock-custody.json',{'original_open_file_description_transferred':True,'main_local_fd_closed':True,
            'pid1_store_count':1,'independent_flock_blocked':True,'host_pid1_stoppost_survival_claimed':False})

def project_mounts(mounts):
    """Bind the same four mount fields independently of Docker array order."""
    require(type(mounts) is list and len(mounts) <= 256, 'mount_inventory_invalid')
    result, destinations = [],set()
    fields = ['Type','Source','Destination','RW']
    for mount in mounts:
        require(type(mount) is dict and all(key in mount for key in fields), 'mount_inventory_invalid')
        value = {key:mount[key] for key in fields}
        require(all(type(value[key]) is str and len(value[key]) <= 4096 and not any(ord(char) < 32 or ord(char) == 127 for char in value[key])
                    for key in ['Type','Source','Destination'])
                and value['Type'] and value['Destination'].startswith('/') and type(value['RW']) is bool
                and value['Destination'] not in destinations, 'mount_inventory_invalid')
        destinations.add(value['Destination'])
        result.append(value)
    return sorted(result,key=lambda value:(value['Destination'],value['Source'],value['Type'],value['RW']))

def container_inspection():
    raw = bounded(['docker','inspect']+['ccpanel-'+name+'-1' for name in NAMES],8,2*1024*1024)
    values = decode(raw,2*1024*1024)
    require(type(values) is list and len(values) == 4, 'container_inventory_invalid')
    result, environments = {}, {}
    for name, item in zip(NAMES,values):
        require(item.get('Name') == '/ccpanel-'+name+'-1' and digest(item.get('Id'))
                and re.fullmatch(r'sha256:[a-f0-9]{64}',item.get('Image',''))
                and item['State']['Running'] is True and item['State']['OOMKilled'] is False
                and item['RestartCount'] == 0 and type(item['State']['Pid']) is int, 'container_unhealthy')
        env = {}
        for pair in item['Config']['Env']:
            key, separator, value = pair.partition('=')
            require(separator and key not in env and not any(char in value for char in '\r\n\0'), 'container_env_invalid')
            env[key] = value
        environments[name] = env
        group = Path('/sys/fs/cgroup/system.slice/docker-'+item['Id']+'.scope')
        limits = {field:(group/field).read_text().strip() for field in ['memory.max','memory.swap.max','cpu.max']}
        events = dict(line.split() for line in (group/'memory.events').read_text().splitlines())
        require(all(INT.fullmatch(value) for value in events.values()) and events.get('oom') == '0' and events.get('oom_kill') == '0', 'container_oom')
        result[name] = {'id':item['Id'],'image_id':item['Image'],'started_at':item['State']['StartedAt'],
                        'restart_count':item['RestartCount'],'pid':item['State']['Pid'],'start_ticks':start_ticks(item['State']['Pid']),
                        'limits':limits,'memory_events':{key:events[key] for key in ['oom','oom_kill']},'environment_sha256':sha(encoded(env)),
                        'host_config':{key:item['HostConfig'][key] for key in ['Memory','MemorySwap','NanoCpus','CpuPeriod','CpuQuota']},
                        'mounts':project_mounts(item['Mounts']),
                        'networks':sorted(item['NetworkSettings']['Networks'])}
    return result,environments

def runtime_snapshot():
    require(Path('/opt/ccpanel/current').resolve() == RELEASE, 'release_changed')
    containers,env = container_inspection()
    paths = {'api_sha256':'dist/api.mjs','worker_sha256':'dist/worker.mjs','sdk_sha256':'dist/web/sdk/ccpanel.js','compose_sha256':'infra/compose.yaml'}
    artifacts = {key:stable_file(RELEASE/path,16*1024*1024,False,False)[0]['sha256'] for key,path in paths.items()}
    require(all(artifacts[key] == expected for key,expected in PRODUCT.items()), 'release_hash_mismatch')
    for key in ENV_KEYS:
        require(env['api'].get(key) == env['worker'].get(key), 'application_env_mismatch')
    require(env['api'].get('OPERATIONAL_COLLECTION_ENABLED') == 'true' and env['worker'].get('OPERATIONAL_COLLECTION_ENABLED') == 'true'
            and env['worker'].get('RETENTION_ENABLED') == 'true', 'collection_retention_disabled')
    for name, memory in [('api',128),('worker',96),('postgres',160),('kafka',384)]:
        require(containers[name]['limits']['memory.max'] == str(memory*1024*1024), 'resource_limits_changed')
    require(env['api'].get('NODE_OPTIONS') == '--max-old-space-size=64' and env['worker'].get('NODE_OPTIONS') == '--max-old-space-size=48'
            and env['kafka'].get('KAFKA_HEAP_OPTS') == '-Xms128m -Xmx192m', 'resource_flags_changed')
    require(all(containers[name]['networks'] == ['ccpanel_default'] for name in NAMES), 'network_changed')
    require(bounded(['docker','image','inspect','node:22.22.3-bookworm-slim','--format','{{.Id}}'],5,1024).decode().strip() == containers['api']['image_id'], 'node_image_mismatch')
    result = {'version':1,'release':str(RELEASE),'schema':22,'containers':containers,'artifacts':artifacts,
              'swappiness':Path('/proc/sys/vm/swappiness').read_text().strip()}
    return result,env

def telemetry(path=SHARED/'telemetry/host.json'):
    _, raw = stable_file(path,32768,False)
    value = decode(raw,32768)
    shape(value,['version','sampledAt','disks','host'])
    now = time.time_ns()//1000000
    require(value['version'] == 1 and type(value['sampledAt']) is int and 0 <= now-value['sampledAt'] <= 30000
            and type(value['disks']) is list and len(value['disks']) == 2, 'resource_invalid')
    seen = set()
    for disk in value['disks']:
        shape(disk,['kind','sampledAt','totalBytes','availableBytes','totalInodes','availableInodes'])
        require(disk['kind'] in ['kafka','postgres'] and disk['kind'] not in seen and disk['sampledAt'] == value['sampledAt']
                and all(integer(disk[key]) for key in ['totalBytes','availableBytes','totalInodes','availableInodes'])
                and int(disk['totalBytes']) > 0 and 0 <= int(disk['availableBytes']) <= int(disk['totalBytes'])
                and 0 <= int(disk['availableInodes']) <= int(disk['totalInodes']), 'resource_invalid')
        require(int(disk['availableBytes'])*100 >= int(disk['totalBytes'])*20, 'resource_disk')
        seen.add(disk['kind'])
    host = value['host']
    shape(host,['memoryBytes','swapInPages','swapOutPages','pageBytes','cpu','loadAverage'])
    shape(host['memoryBytes'],['total','available','swapTotal','swapFree','dirty'])
    require(all(integer(item) for item in host['memoryBytes'].values()) and integer(host['swapInPages']) and integer(host['swapOutPages'])
            and int(host['memoryBytes']['total']) > 0 and int(host['memoryBytes']['available']) <= int(host['memoryBytes']['total'])
            and int(host['memoryBytes']['swapFree']) <= int(host['memoryBytes']['swapTotal'])
            and int(host['memoryBytes']['dirty']) <= int(host['memoryBytes']['total']), 'resource_invalid')
    shape(host['cpu'],['userTicks','systemTicks','idleTicks','ioWaitTicks','stealTicks','totalTicks','ticksPerSecond'])
    require(all(integer(host['cpu'][key]) for key in ['userTicks','systemTicks','idleTicks','ioWaitTicks','stealTicks','totalTicks'])
            and sum(int(host['cpu'][key]) for key in ['userTicks','systemTicks','idleTicks','ioWaitTicks','stealTicks']) == int(host['cpu']['totalTicks'])
            and type(host['cpu']['ticksPerSecond']) is int and 1 <= host['cpu']['ticksPerSecond'] <= 10000000
            and type(host['pageBytes']) is int and 512 <= host['pageBytes'] <= 1073741824 and host['pageBytes']&(host['pageBytes']-1) == 0
            and type(host['loadAverage']) is list and len(host['loadAverage']) == 3
            and all(type(item) in [int,float] and 0 <= item < float('inf') for item in host['loadAverage']), 'resource_invalid')
    return value

def host_guard(runtime=None):
    raw = Path('/proc/meminfo').read_text()
    values = re.findall(r'^MemAvailable:[ \t]+([0-9]+)[ \t]+kB$',raw,re.M)
    require(len(values) == 1, 'resource_invalid')
    require(int(values[0]) >= 204800, 'resource_memory')
    for path in [Path('/'),SHARED]:
        disk = os.statvfs(str(path))
        require(disk.f_blocks > 0 and disk.f_bavail*100 >= disk.f_blocks*20, 'resource_disk')
    frame = telemetry()
    if runtime:
        require(Path('/opt/ccpanel/current').resolve() == RELEASE and Path('/proc/sys/vm/swappiness').read_text().strip() == runtime['swappiness'], 'owner_changed')
        for name,item in runtime['containers'].items():
            require(start_ticks(item['pid']) == item['start_ticks'], 'owner_changed')
            group = Path('/sys/fs/cgroup/system.slice/docker-'+item['id']+'.scope')
            require(all((group/field).read_text().strip() == value for field,value in item['limits'].items()), 'owner_changed')
            events = dict(line.split() for line in (group/'memory.events').read_text().splitlines())
            require(events.get('oom') == item['memory_events'].get('oom') and events.get('oom_kill') == item['memory_events'].get('oom_kill'), 'owner_changed')
    return frame

def pg_gates():
    sql = "SET statement_timeout='3s'; SET lock_timeout='200ms'; SELECT (SELECT count(*)=1 AND bool_and(NOT blocked) FROM recovery_gate) AND (SELECT count(*)=1 AND bool_and(NOT blocked AND reason IS NULL) FROM ingest_integrity_gate) AND (SELECT max(version)=22 FROM schema_migrations);"
    require(bounded(['docker','exec','-i','ccpanel-postgres-1','psql','-X','-q','-U','ccpanel','-d','ccpanel','-At','-v','ON_ERROR_STOP=1'],6,4096,sql.encode()).strip() == b't', 'gate_closed')

def business_after(state,run):
    fixture = None
    try:
        value,fixture_ref = read_json(state/'fixture.json',8192)
        created,_ = read_json(state/'fixture-created.json',16384)
        binding_ref,_ = stable_file(state/'binding.json',16384)
        require(created.get('kind') == 'capacity_fixture_created' and created.get('status') == 'created'
                and created.get('run_id') == run['run_id'] and created.get('binding_sha256') == binding_ref['sha256']
                and created.get('fixture_sha256') == fixture_ref['sha256']
                and value.get('run_id') == run['run_id'], 'fixture_scope_unverified')
        require(all(re.fullmatch('[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}',value.get(key,'')) for key in ['tenant','user']), 'fixture_scope_unverified')
        fixture = value
    except FileNotFoundError:
        pass
    # UUID literals come only from a successfully committed, hash-bound creation
    # receipt. An uncertain creation is intentionally not excluded from B0.
    filters = "''"
    if fixture:
        filters = ("CASE WHEN c.relname='tenants' THEN ' WHERE id<>''%s''::uuid' "
                   "WHEN c.relname='users' THEN ' WHERE id<>''%s''::uuid' "
                   "WHEN c.relname='sessions' THEN ' WHERE user_id<>''%s''::uuid' "
                   "WHEN EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attname='tenant_id' AND NOT a.attisdropped) "
                   "THEN ' WHERE tenant_id IS DISTINCT FROM ''%s''::uuid' ELSE '' END")%(fixture['tenant'],fixture['user'],fixture['user'],fixture['tenant'])
    excluded = "'operational_samples','operational_alert_states','operational_alert_history','pressure_signal_states','pressure_signal_history','dlq_alert_states','dlq_alert_history'"
    sql = ("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL timezone='UTC'; SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='200ms';\n"
           "SELECT json_build_object('kind','source','database',current_database(),'oid',(SELECT oid::text FROM pg_database WHERE datname=current_database()),"
           "'postmaster_started_us',((extract(epoch FROM pg_postmaster_start_time())*1000000)::bigint)::text,'schema',(SELECT max(version) FROM schema_migrations))::text;\n"
           "SELECT format('SELECT json_build_object(''table'',%%L,''count'',count(*)::text,''hash'',coalesce(bit_xor(hashtextextended(row_to_json(t)::text,0)),0)::text)::text FROM (SELECT * FROM public.%%I%%s) t;',c.relname,c.relname,%s) "
           "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p') AND c.relname NOT IN (%s) ORDER BY c.relname\n\\gexec\nROLLBACK;\n")%(filters,excluded)
    raw = bounded(['docker','exec','-i','ccpanel-postgres-1','psql','-X','-q','-U','ccpanel','-d','ccpanel','-At','-v','ON_ERROR_STOP=1'],30,2*1024*1024,sql.encode())
    rows = [decode(line,4096) for line in raw.splitlines() if line]
    require(1 < len(rows) <= 1025 and rows[0].get('schema') == 22 and rows[0].get('kind') == 'source', 'business_observation_invalid')
    baseline,_ = read_json(state/'baseline.json',262144)
    original = baseline['observation']['state']['database']
    require(rows[0]['database'] == original['name'] and rows[0]['oid'] == original['oid']
            and rows[0]['postmaster_started_us'] == original['postmaster_started_us'], 'business_source_changed')
    require([row['table'] for row in rows[1:]] == sorted(set(row['table'] for row in rows[1:])), 'business_inventory_invalid')
    data = encoded({'v':1,'kind':'business_after','run_id':run['run_id'],'fixture_excluded':fixture is not None,
                    'scope':'public_business_controls_excludes_seven_monitor_relations','consumer_progress_requires_separate_vector_proof':True})+b''.join(encoded(row) for row in rows)
    publish(state/'business-after.jsonl',data,True)

def read_json(path, maximum=262144):
    ref, raw = stable_file(path,maximum)
    return decode(raw,maximum),ref

def monitor_integrity(path, receipt):
    ref,_ = stable_file(path,32*1024*1024,True,False)
    require(ref['bytes'] > 0 and receipt.get('status') == 'captured_unverified' and receipt.get('bytes') == ref['bytes']
            and receipt.get('sha256') == ref['sha256'] and receipt.get('sourceAuthenticated') is False
            and receipt.get('causalPruneVerified') is False, 'monitor_receipt_invalid')
    with path.open('rb') as file:
        file.seek(max(0,ref['bytes']-4096))
        tail = file.read()
    line = tail.splitlines(keepends=True)[-1]
    require(line.endswith(b'\n'), 'monitor_footer_missing')
    footer = decode(line,4096)
    checksum, remaining = hashlib.sha256(),ref['bytes']-len(line)
    with path.open('rb') as file:
        while remaining:
            chunk = file.read(min(65536,remaining))
            require(bool(chunk), 'monitor_partial')
            checksum.update(chunk)
            remaining -= len(chunk)
    require(footer.get('kind') == 'footer' and footer.get('prefix_sha256') == checksum.hexdigest()
            and footer.get('rows') == receipt.get('rows') and footer.get('counts') == receipt.get('counts')
            and receipt.get('prefixSha256') == checksum.hexdigest(), 'monitor_footer_invalid')
    return ref

class Operations:
    def __init__(self,state,script,run,runtime=None,environments=None):
        self.state,self.script,self.run = state,script,run
        self.unit = unit_name(run)
        self.name = 'ccpanel-window225-'+run['run_id']+'-operator'
        self.runtime = runtime
        self.environments = environments
    def setup(self):
        require(self.environments['api'].get('PUBLIC_ORIGIN') == self.run['expected_origin'], 'origin_changed')
        env = {key:self.environments['api'][key] for key in ENV_KEYS if key in self.environments['api']}
        env.update({'NODE_OPTIONS':'--max-old-space-size=32','CCPANEL_WINDOW_CONFIGURATION_SHA256':self.run['expected_runtime_sha256'],
                    'CCPANEL_WINDOW_PRODUCT_ROOT':'/app','CCPANEL_WINDOW_TOOL_ROOT':'/operator/client',
                    'CCPANEL_MONITOR_DATABASE_URL':env['DATABASE_URL']})
        require(all(re.fullmatch('[A-Z_][A-Z0-9_]*',key) and not any(c in value for c in '\r\n\0') for key,value in env.items()), 'invalid_operator_env')
        environment_ref = publish(self.state/'operator.env',b''.join((key+'='+value+'\n').encode() for key,value in sorted(env.items())),True)
        publish(self.state/'operator-context.json',{'run_id':self.run['run_id'],'unit':self.unit,'container':self.name,
                'image_id':self.runtime['containers']['api']['image_id'],'network':'ccpanel_default','environment_sha256':environment_ref['sha256']})
    def remove(self):
        inventory = bounded(['docker','ps','-a','--filter','label=ccpanel.window225.run='+self.run['run_id'],'--format','{{.Names}}'],5,4096).decode().splitlines()
        require(all(name == self.name for name in inventory), 'unknown_owned_container')
        if inventory:
            labels = bounded(['docker','inspect','--format','{{index .Config.Labels "ccpanel.window225.state"}}|{{index .Config.Labels "ccpanel.window225.run"}}',self.name],5,4096).decode().strip()
            require(labels == str(self.state)+'|'+self.run['run_id'], 'container_ownership_mismatch')
            bounded(['docker','rm','-f',self.name],6,4096)
    def command(self,command,sequence=None,capture=None,timeout=45):
        self.remove()
        context,_ = read_json(self.state/'operator-context.json',4096)
        require(context == {'run_id':self.run['run_id'],'unit':self.unit,'container':self.name,
                           'image_id':context['image_id'],'network':'ccpanel_default','environment_sha256':context['environment_sha256']}
                and re.fullmatch(r'sha256:[a-f0-9]{64}',context['image_id']), 'operator_context_invalid')
        require(stable_file(self.state/'operator.env',65536)[0]['sha256'] == context['environment_sha256'], 'operator_env_changed')
        args = ['docker','run','--rm','--pull','never','--name',self.name,'--label','ccpanel.window225.run='+self.run['run_id'],
                '--label','ccpanel.window225.state='+str(self.state),'--user','0:0','--network','ccpanel_default','--read-only',
                '--cap-drop','ALL','--security-opt','no-new-privileges','--pids-limit','32','--memory','96m','--memory-swap','128m',
                '--cpus','0.5','--env-file',str(self.state/'operator.env'),'-v',str(RELEASE)+':/app:ro',
                '-v',str(RELEASE/'node_modules')+':/operator/node_modules:ro','-v',str(self.state)+':/evidence',
                '-v',str(self.script.parent/TOOLS['db_operator'])+':/operator/window225-db.mjs:ro',
                '-v',str(self.script.parent/TOOLS['capture'])+':/operator/capture.mjs:ro']
        for role in CLIENT:
            args += ['-v',str(self.script.parent/TOOLS[role])+':/operator/client/'+TOOLS[role]+':ro']
        args += ['--workdir','/operator','--entrypoint','node',context['image_id']]
        if capture:
            args += ['/operator/capture.mjs','window225-'+self.run['run_id'],'/evidence/monitor-'+capture+'.jsonl']
        else:
            args += ['/operator/window225-db.mjs',command,'/evidence']+([sequence] if sequence else [])
        try:
            raw = bounded(args,timeout,16384,allowed=(0,2) if command == 'cleanup' else (0,))
            receipt = decode(raw,16384)
            if capture:
                monitor_integrity(self.state/('monitor-'+capture+'.jsonl'),receipt)
                publish(self.state/('monitor-'+capture+'-receipt.json'),raw,True)
            else:
                shape(receipt,['version','kind','run_id','command','status','output_file','output_sha256','capacity_verified','source_authenticated'])
                expected = {'baseline':'baseline.json','create':'fixture-created.json','revoke':'key-revoked.json','verify':'verified.json',
                            'cleanup':'cleanup-deferred.json' if receipt.get('status') == 'cleanup_deferred' else 'cleanup.json','status':'status-'+str(sequence)+'.json'}[command]
                require(receipt['version'] == 1 and receipt['kind'] == 'capacity_window_db_receipt' and receipt['run_id'] == self.run['run_id']
                        and receipt['command'] == command and receipt['output_file'] == expected and receipt['capacity_verified'] is False
                        and receipt['source_authenticated'] is False and digest(receipt['output_sha256']), 'operator_receipt_invalid')
                require(stable_file(self.state/expected,262144)[0]['sha256'] == receipt['output_sha256'], 'operator_receipt_invalid')
            return receipt
        finally:
            self.remove()

def sample_frames(state, owner_uid=0):
    path = state/'samples.jsonl'
    try:
        before = os.lstat(str(path))
        size = before.st_size
    except FileNotFoundError:
        return []
    require(0 <= size <= 8*1024*1024, 'sampler_output_invalid')
    require(stat.S_ISREG(before.st_mode) and before.st_uid == owner_uid and before.st_nlink == 1
            and stat.S_IMODE(before.st_mode) == 0o600, 'sampler_output_invalid')
    # At most 199 bounded rows; ignore only the still-being-written last row.
    rows = []
    descriptor = os.open(str(path),os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0))
    with os.fdopen(descriptor,'rb') as file:
        opened = os.fstat(file.fileno())
        require((opened.st_dev,opened.st_ino) == (before.st_dev,before.st_ino) and stat.S_ISREG(opened.st_mode), 'sampler_output_invalid')
        while True:
            raw = file.readline(65537)
            if not raw:
                break
            require(len(raw) <= 65536, 'sampler_output_invalid')
            if not raw.endswith(b'\n'):
                break
            row = decode(raw,65536)
            require(type(row) is dict and type(row.get('capturedAt')) is int and type(row.get('elapsedMs')) is int
                    and row['capturedAt'] >= 0 and row['elapsedMs'] >= 0 and 'error' not in row
                    and type(row.get('telemetry',{}).get('sampledAt')) is int
                    and 0 <= row['capturedAt']-row['telemetry']['sampledAt'] <= 30000, 'sampler_frame_invalid')
            if rows:
                require(row['capturedAt'] > rows[-1]['capturedAt'] and row['elapsedMs'] > rows[-1]['elapsedMs'], 'sampler_order_invalid')
            rows.append(row)
            require(len(rows) <= 199, 'sampler_output_invalid')
        current = os.lstat(str(path))
        require((current.st_dev,current.st_ino) == (before.st_dev,before.st_ino) and size <= current.st_size <= 8*1024*1024, 'sampler_output_invalid')
    return rows

def sampler_start(state,script,run,containers,probe=False):
    unit = ('ccpanel-window225-probe-sampler-' if probe else 'ccpanel-window225-sampler-')+run['run_id']+'.service'
    seconds = 10 if probe else 1980
    publish(state/'containers.json',{name:containers[name]['id'] for name in NAMES})
    stop = '/usr/bin/python3 %s sampler-stop %s %s%s'%(script,state,run['run_id'],' probe' if probe else '')
    args = ['systemd-run','--quiet','--wait','--pipe','--collect','--unit='+unit,
            '--property=Type=exec','--property=User=root','--property=MemoryMax=32M','--property=CPUQuota=5%',
            '--property=RuntimeMaxSec='+('25' if probe else '2050'),'--property=TimeoutStopSec=20',
            '--property=KillMode=control-group','--property=NoNewPrivileges=yes','--property=ProtectSystem=strict',
            '--property=ProtectHome=yes','--property=PrivateNetwork=yes','--property=ProtectControlGroups=yes',
            '--property=CapabilityBoundingSet=','--property=ReadWritePaths='+str(state),'--property=ExecStopPost='+stop,
            '/usr/bin/python3',str(script.parent/TOOLS['sampler']),'--config',str(state/'containers.json'),
            '--output',str(state/'samples.jsonl'),'--seconds',str(seconds)]
    out = os.open(str(state/'sampler-wait.stdout'),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    error = os.open(str(state/'sampler-wait.stderr'),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        process = subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=out,stderr=error)
    finally:
        os.close(out)
        os.close(error)
    deadline = time.monotonic()+10
    while time.monotonic() < deadline:
        props = properties(unit,['MainPID','InvocationID','ActiveState'])
        if props['MainPID'] != '0' and run_id(props['InvocationID']) and props['ActiveState'] in ['active','activating']:
            owner = {'v':1,'kind':'sampler_owner','run_id':run['run_id'],'unit':unit,'invocation_id':props['InvocationID'],
                     'main_pid':int(props['MainPID']),'start_ticks':start_ticks(int(props['MainPID'])),
                     'started_monotonic_us':str(service_numbers(unit,['ExecMainStartTimestampMonotonic'])['ExecMainStartTimestampMonotonic']),
                     'source_sha256':run['tools_sha256']['sampler'],'seconds':seconds}
            publish(state/'sampler-owner.json',owner)
            return process,owner
        require(process.poll() is None, 'sampler_launch_failed')
        time.sleep(.1)
    raise WindowError('sampler_owner_timeout')

def sampler_terminal(state, expected_run, probe=False):
    owner,_ = read_json(state/'sampler-owner.json',4096)
    require(owner['run_id'] == expected_run and owner['seconds'] == (10 if probe else 1980), 'sampler_owner_mismatch')
    props = properties(owner['unit'],['InvocationID','ExecMainPID','Result','ExecMainCode','ExecMainStatus',
                                      'ExecMainStartTimestampMonotonic','ExecMainExitTimestampMonotonic'])
    require(props['InvocationID'] == owner['invocation_id'] == os.environ.get('INVOCATION_ID')
            and props['ExecMainPID'] == str(owner['main_pid']), 'sampler_owner_mismatch')
    try:
        ref,_ = stable_file(state/'samples.jsonl',8*1024*1024,True,False)
        rows = sample_frames(state)
    except (OSError,WindowError):
        ref,rows = None,[]
    natural = props['Result'] == 'success' and props['ExecMainCode'] == '1' and props['ExecMainStatus'] == '0'
    clock = service_numbers(owner['unit'],['ExecMainStartTimestampMonotonic','ExecMainExitTimestampMonotonic'])
    complete = bool(natural and ref and len(rows) == owner['seconds']//10+1 and rows[-1]['elapsedMs'] >= owner['seconds']*1000
                    and str(clock['ExecMainStartTimestampMonotonic']) == owner['started_monotonic_us']
                    and clock['ExecMainExitTimestampMonotonic']-clock['ExecMainStartTimestampMonotonic'] >= owner['seconds']*1000000)
    publish(state/'sampler-terminal.json',{'v':1,'kind':'sampler_terminal','run_id':expected_run,'owner':owner,
            'native':props,'native_monotonic_us':clock,'samples':ref,'frames':len(rows),'last_frame':rows[-1] if rows else None,
            'natural_native_completion':natural,'complete_fixed_duration':complete,
            'capacity_verified':False,'source_authenticated':False})

def owned_sampler_stop(state,run):
    try:
        owner,_ = read_json(state/'sampler-owner.json',4096)
    except FileNotFoundError:
        return
    require(owner['run_id'] == run['run_id'] and run_id(owner['invocation_id']), 'sampler_owner_mismatch')
    try:
        props = properties(owner['unit'],['InvocationID','MainPID','ActiveState'])
        require(props['InvocationID'] == owner['invocation_id'], 'sampler_owner_mismatch')
        if props['ActiveState'] in ['active','activating']:
            require(props['MainPID'] == str(owner['main_pid']) and start_ticks(owner['main_pid']) == owner['start_ticks'], 'sampler_owner_mismatch')
            bounded(['systemctl','stop',owner['unit']],24,4096)
    except WindowError:
        # A GC'd unit is not evidence of success. A previously captured native
        # terminal is the only acceptable reason not to act on an absent unit.
        terminal,_ = read_json(state/'sampler-terminal.json',16384)
        require(terminal['owner'] == owner, 'sampler_terminal_missing')

def binding_value(run,runtime):
    return {'version':1,'kind':'capacity_window_binding','run_id':run['run_id'],'planned_events':1800,
            'product':{'release':RELEASE.name,'source_commit':'b9b213f2313151c1e21be30e924a9af3a1f84a82','schema':22,**runtime['artifacts']},
            'containers':{name:{key:runtime['containers'][name][key] for key in ['id','started_at','restart_count','image_id']} for name in NAMES},
            'tools':{key:run['tools_sha256'][role] for key,role in [('db_operator_sha256','db_operator'),('generator_sha256','load_check'),
                     ('evidence_sha256','load_evidence'),('lease_sha256','load_lease'),('reader_sha256','load_stop_reader'),
                     ('sampler_sha256','sampler'),('controller_sha256','controller')]},
            'namespace':{},'configuration_sha256':run['expected_runtime_sha256']}

class Supervisor:
    def __init__(self,state,script,run,run_sha,self_sha,probe=False):
        self.state,self.script,self.run,self.run_sha,self.self_sha,self.probe = state,script,run,run_sha,self_sha,probe
        self.native = unit_owner(state,run,script,self_sha,probe)
        hold_lock(state,self.native,run_sha,probe)
        self.owner = {'v':1,'kind':'owner','run_id':run['run_id'],'run_sha256':run_sha,**self.native,
                      'controller_sha256':self_sha,'lock_custody':'pid1_fdstore'}
        self.owner_ref = publish(state/'owner.json',self.owner)
        self.phase,self.abort,self.client,self.terminal = 'preparing',None,None,None
        self.ready,self.freeze,self.runtime,self.operations = None,None,None,None
        self.sampler,self.sampler_owner,self.sampler_done,self.sampler_started = None,None,None,None
        self.sampler_ending,self.sampler_exit_seen = False,None
        self.fixture_at,self.terminal_accepted_ms,self.post_allowed = None,None,False
        self.last_seq,self.last_nonce,self.last_message = 0,None,time.monotonic()
        self.last_guard,self.last_status,self.last_gate = 0,0,0
        self.pending,self.task,self.task_result,self.task_error = False,None,queue.Queue(),None
        self.input_eof,self.freeze_at = False,None
        self.deadline = time.monotonic()+(50 if probe else 2580)
        self.cleanup = 'not_started'
        self.status_sequence = 0
        self.finalizing = False
        self.cancel_started = False
        self.task_kind = None
    def emit(self,value):
        raw = encoded(value)
        require(len(raw) <= 8192, 'reply_too_large')
        write_deadline(sys.stdout.fileno(),raw)
    def latch(self,code):
        if self.abort is None:
            self.abort = {'code':code if code in ABORTS else 'operation_failed','observed_at_us':us(),'phase':self.phase}
            publish(self.state/'abort.json',self.abort)
    def fresh_owner(self):
        props = properties(self.native['unit'],['MainPID','InvocationID','ActiveState','NFileDescriptorStore'])
        require(props['MainPID'] == str(self.native['main_pid']) and props['InvocationID'] == self.native['invocation_id']
                and props['ActiveState'] in ['active','activating'] and props['NFileDescriptorStore'] == '1'
                and start_ticks(self.native['main_pid']) == self.native['start_ticks'], 'owner_changed')
    def submit(self,kind,fn):
        require(not self.pending, 'operation_already_pending')
        self.pending = True
        self.task_kind = kind
        def execute():
            try:
                self.task_result.put((kind,fn(),None))
            except Exception as error:
                self.task_result.put((kind,None,str(error) if isinstance(error,WindowError) else 'operation_failed'))
        self.task = threading.Thread(target=execute,daemon=True)
        self.task.start()
    def prepare(self):
        def checkpoint():
            require(self.abort is None, 'preparation_aborted')
        host_guard()
        units = bounded(['systemctl','list-units','--type=service','--state=running','ccpanel-capacity-*','ccpanel-window*','--no-legend','--plain'],5,16384).decode().splitlines()
        require(all(line.split()[0] == self.native['unit'] for line in units if line.strip()), 'another_window_active')
        runtime,env = runtime_snapshot()
        if not self.probe:
            require(sha(encoded(runtime)) == self.run['expected_runtime_sha256'], 'runtime_binding_mismatch')
        host = decode(bounded(['/usr/bin/python3',str(self.script.parent/TOOLS['host_state'])],35,262144),262144)
        publish(self.state/'resource-baseline.json',{'version':1,'runtime':runtime,'configuration':host,'captured_at_us':us()})
        self.runtime = runtime
        checkpoint()
        if self.probe:
            self.sampler,self.sampler_owner = sampler_start(self.state,self.script,self.run,runtime['containers'],True)
            self.sampler_started = int(self.sampler_owner['started_monotonic_us'])
            self.fixture_at = time.time_ns()//1000000
            return None
        pg_gates()
        bounded(['curl','--max-time','5','-fsS',self.run['expected_origin']+'/ready'],7,65536)
        self.last_gate = time.monotonic()
        binding = binding_value(self.run,runtime)
        binding['namespace'] = {'events':env['api'].get('KAFKA_EVENTS_TOPIC','ccpanel.events'),
                                'dlq':env['api'].get('KAFKA_DLQ_TOPIC','ccpanel.events.dlq'),
                                'group':env['api'].get('KAFKA_GROUP_ID','ccpanel-writer-v1')}
        publish(self.state/'binding.json',binding)
        self.operations = Operations(self.state,self.script,self.run,runtime,env)
        self.operations.setup()
        self.operations.command('baseline')
        checkpoint()
        self.last_status = time.monotonic()
        self.operations.command(None,capture='before',timeout=90)
        checkpoint()
        self.operations.command('create')
        checkpoint()
        self.fixture_at = time.time_ns()//1000000
        self.sampler,self.sampler_owner = sampler_start(self.state,self.script,self.run,runtime['containers'])
        self.sampler_started = int(self.sampler_owner['started_monotonic_us'])
        return None
    def make_ready(self,rows):
        if self.probe:
            if rows:
                self.phase = 'ready'
            return
        fresh = [row for row in rows if row['capturedAt'] >= row['telemetry']['sampledAt'] >= self.fixture_at]
        if len({row['telemetry']['sampledAt'] for row in fresh}) < 2:
            return
        require(rows[-1] == fresh[-1] and fresh[-1]['elapsedMs'] <= 90000, 'sampler_ready_budget')
        roles = {role:stable_file(artifact_path(self.state,role)[0],artifact_path(self.state,role)[1],True,False)[0] for role in ['binding','db_baseline','monitor_before','fixture','sampler_owner']}
        ready = {'v':1,'kind':'ready','run_id':self.run['run_id'],'owner_sha256':self.owner_ref['sha256'],
                 'binding_sha256':roles['binding']['sha256'],'configuration_sha256':self.run['expected_runtime_sha256'],
                 'business_before_sha256':roles['db_baseline']['sha256'],'monitor_before_sha256':roles['monitor_before']['sha256'],
                 'sampler_owner_sha256':roles['sampler_owner']['sha256'],'fixture_ref':{'role':'fixture',**roles['fixture']},'captured_at_us':us()}
        self.ready = publish(self.state/'ready.json',ready)
        self.phase = 'ready'
    def sampler_state(self):
        if self.sampler is None:
            return None,[]
        rows = sample_frames(self.state)
        elapsed = (time.monotonic_ns()//1000-self.sampler_started)//1000
        if self.sampler_done is None and self.sampler.poll() is not None:
            terminal,_ = read_json(self.state/'sampler-terminal.json',16384)
            require(terminal['owner'] == self.sampler_owner, 'sampler_owner_mismatch')
            self.sampler_done = terminal
            if self.sampler.returncode != 0 or terminal['complete_fixed_duration'] is not True:
                self.latch('sampler_failed')
        if self.sampler_done is None:
            try:
                require(start_ticks(self.sampler_owner['main_pid']) == self.sampler_owner['start_ticks'], 'sampler_failed')
            except (OSError,WindowError):
                # Main exit is not the --wait terminal. Permit only the same
                # completed native sampler's bounded StopPost transition.
                ending = properties(self.sampler_owner['unit'],['InvocationID','ExecMainPID','ActiveState','ExecMainCode','ExecMainStatus'])
                require(ending['InvocationID'] == self.sampler_owner['invocation_id']
                        and ending['ExecMainPID'] == str(self.sampler_owner['main_pid']) and ending['ActiveState'] == 'deactivating'
                        and ending['ExecMainCode'] == '1' and ending['ExecMainStatus'] == '0'
                        and elapsed >= self.sampler_owner['seconds']*1000, 'sampler_failed')
                self.sampler_ending = True
                self.sampler_exit_seen = self.sampler_exit_seen or time.monotonic()
                require(time.monotonic()-self.sampler_exit_seen <= 25, 'sampler_failed')
            if rows:
                require(0 <= time.time_ns()//1000000-rows[-1]['capturedAt'] <= 20000, 'sampler_failed')
            else:
                require(elapsed <= 20000, 'sampler_failed')
        if not self.ready and self.phase == 'preparing' and not self.pending and not self.abort:
            self.make_ready(rows)
        if not self.probe and not self.terminal and elapsed >= 1940000:
            self.latch('sampler_coverage_budget')
        if self.terminal:
            covered = any(row['capturedAt'] >= self.terminal_accepted_ms and row['telemetry']['sampledAt'] >= self.terminal_accepted_ms for row in rows)
            # Upload/operator work starts only after the original fixed sampler
            # and its own --wait have both naturally ended, not merely lag=0.
            self.post_allowed = bool(covered and self.sampler_done and self.sampler_done['complete_fixed_duration'])
            if self.post_allowed and not (self.state/'transfer-admission.json').exists():
                publish(self.state/'transfer-admission.json',{'run_id':self.run['run_id'],'owner_sha256':self.owner_ref['sha256'],
                        'original_sampler_wait_completed':True,'terminal_accepted_ms':self.terminal_accepted_ms})
        return elapsed,rows
    def status(self,request):
        try:
            self.fresh_owner()
        except Exception:
            self.latch('owner_changed')
            raise
        try:
            elapsed,_ = self.sampler_state()
        except Exception:
            self.latch('sampler_failed')
            elapsed = (time.monotonic_ns()//1000-self.sampler_started)//1000 if self.sampler_started else None
        can_feed = bool(not self.probe and self.ready and not self.abort and not self.terminal
                        and self.phase in ['ready','running'] and elapsed is not None and elapsed < 1940000
                        and not self.sampler_ending
                        and time.monotonic()-self.last_guard < 2 and time.monotonic()-self.last_status < 45
                        and time.monotonic()-self.last_gate < 25)
        return {'v':1,'kind':'status','run_id':self.run['run_id'],'owner_sha256':self.owner_ref['sha256'],
                'seq':request['seq'],'nonce':request['nonce'],'server_us':us(),'phase':self.phase,
                'native_owner':self.native,'ready_sha256':self.ready['sha256'] if self.ready else None,
                'freeze_sha256':self.freeze['sha256'] if self.freeze else None,'may_feed_client':can_feed,
                'abort':self.abort,'sampler_elapsed_ms':str(elapsed) if elapsed is not None else None,
                'post_window_work_allowed':self.post_allowed}
    def request(self,raw):
        value = validate_request(decode(raw,4096,True),self.run['run_id'],self.owner_ref['sha256'],self.last_seq)
        require(value['nonce'] != self.last_nonce, 'nonce_replayed')
        self.last_seq,self.last_nonce,self.last_message = int(value['seq']),value['nonce'],time.monotonic()
        subject = value.get('client')
        if subject:
            if self.client is None:
                require(self.ready is not None and self.phase == 'ready' and value['kind'] == 'heartbeat' and not self.probe, 'client_before_ready')
                self.client = subject
                self.phase = 'running'
                publish(self.state/'client-subject.json',subject)
            else:
                require(subject == self.client, 'client_subject_changed')
        if value['kind'] == 'abort':
            self.latch(value['code'])
        if value['kind'] == 'client_terminal':
            require(not self.probe and self.client == value['client'] and self.terminal is None, 'client_terminal_replayed')
            self.terminal = value
            self.terminal_accepted_ms = time.time_ns()//1000000
            publish(self.state/'client-wire-terminal.json',raw,True)
            publish(self.state/'terminal-accepted.json',{'run_id':self.run['run_id'],'accepted_at_us':us(),
                    'sampler_elapsed_ms':str((time.monotonic_ns()//1000-self.sampler_started)//1000)})
            self.phase = 'settling'
        self.emit(self.status(value))
        if self.freeze:
            self.freeze_at = time.monotonic()
    def finish(self):
        self.phase = 'cleaning'
        cleanup = 'deferred'
        errors = []
        if self.operations:
            try:
                self.operations.command('revoke',timeout=40)
                if not self.abort and self.terminal and self.terminal['termination'] == 'completed' and self.post_allowed:
                    self.operations.command('verify',timeout=60)
                    if not self.abort:
                        receipt = self.operations.command('cleanup',timeout=40)
                        cleanup = 'completed' if receipt['status'] == 'cleaned' else 'deferred'
            except Exception:
                errors.append('database_finalization_unverified')
            for name,observe in [('runtime',self.observe_final_runtime),('business',lambda:business_after(self.state,self.run)),
                                 ('monitor',lambda:self.operations.command(None,capture='after',timeout=90))]:
                try:
                    host_guard(self.runtime)
                    observe()
                except Exception:
                    errors.append(name+'_after_unverified')
        self.cleanup = cleanup
        publish(self.state/'finalization.json',{'run_id':self.run['run_id'],'cleanup':cleanup,'errors':errors,'capacity_verified':False})
        if errors:
            raise WindowError('finalization_unverified')
    def observe_final_runtime(self):
        current,_ = runtime_snapshot()
        require(current == self.runtime, 'runtime_changed')
        host = decode(bounded(['/usr/bin/python3',str(self.script.parent/TOOLS['host_state'])],35,262144),262144)
        publish(self.state/'resource-final.json',{'version':1,'runtime':current,'configuration':host,'captured_at_us':us()})
    def freeze_now(self):
        require(not self.pending, 'freeze_operation_active')
        self.phase = 'freezing'
        transfer = os.open(str(self.state/'transfer.lock'),os.O_RDWR|os.O_CREAT|getattr(os,'O_NOFOLLOW',0),0o600)
        try:
            fcntl.flock(transfer,fcntl.LOCK_EX|fcntl.LOCK_NB)
            refs = []
            for role,(name,maximum) in FILES.items():
                if role in ['freeze','fixture','controller_terminal']:
                    continue
                try:
                    ref,_ = stable_file(self.state/name,maximum,True,False)
                except FileNotFoundError:
                    continue
                refs.append({'role':role,**ref})
            outcome = 'aborted' if self.abort else ('completed' if self.probe or self.cleanup == 'completed' else 'failed')
            find = lambda role:next((ref['sha256'] for ref in refs if ref['role'] == role),None)
            value = {'v':1,'kind':'freeze','run_id':self.run['run_id'],'run_sha256':self.run_sha,'owner_sha256':self.owner_ref['sha256'],
                     'controller_outcome':outcome,'client_terminal_sha256':find('client_terminal'),
                     'sampler_terminal_sha256':find('sampler_terminal'),'artifacts':refs,'cleanup':self.cleanup,
                     'capacity_verified':False,'source_authenticated':False,'latest_state_verified':False,'recovery_verified':False}
            self.freeze = publish(self.state/'freeze.json',value)
        except Exception:
            # Do not loop/rewrite an exclusive publication after an uncertain
            # filesystem failure; native nonzero close remains authoritative.
            raise WindowFatal('freeze_unverified')
        finally:
            os.close(transfer)
        self.phase = {'completed':'completed','aborted':'aborted','failed':'failed'}[outcome]
    def uploaded(self):
        transfer = os.open(str(self.state/'transfer.lock'),os.O_RDWR|os.O_CREAT|getattr(os,'O_NOFOLLOW',0),0o600)
        try:
            try:
                fcntl.flock(transfer,fcntl.LOCK_SH|fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            for expected in self.terminal['artifacts']:
                role = expected['role']
                try:
                    actual,_ = stable_file(self.state/FILES[role][0],FILES[role][1],True,False)
                except FileNotFoundError:
                    return False
                require(actual == {'bytes':expected['bytes'],'sha256':expected['sha256']}, 'uploaded_artifact_changed')
            return True
        finally:
            os.close(transfer)
    def tick(self):
        now = time.monotonic()
        if now-self.last_guard >= 1 and not self.freeze:
            host_guard(self.runtime)
            self.last_guard = time.monotonic()
        if not self.freeze and now-self.last_message > 8:
            self.latch('bridge_stale')
        if now >= self.deadline and not self.freeze:
            self.latch('operation_failed')
        while not self.task_result.empty():
            kind,result,error = self.task_result.get_nowait()
            self.pending = False
            if error:
                self.latch('preparation_failed' if kind == 'prepare' else 'operation_failed')
                self.task_error = error
                if kind == 'finish':
                    self.finalizing = False
                    self.cleanup = 'deferred'
                    publish(self.state/'finalization-error.json',{'code':error,'capacity_verified':False})
                    self.freeze_now()
            elif kind == 'gate':
                self.last_gate = time.monotonic()
            elif kind == 'status':
                self.last_status = time.monotonic()
            elif kind == 'finish':
                self.finalizing = False
                self.freeze_now()
        if self.freeze:
            return
        elapsed,rows = self.sampler_state()
        if self.probe and self.sampler_done and not self.pending:
            self.cleanup = 'not_started'
            self.freeze_now()
            return
        if self.abort:
            if self.pending and self.task_kind != 'finish' and not self.cancel_started:
                self.cancel_started = True
                def cancel_owned():
                    try:
                        if self.operations:
                            self.operations.remove()
                        owned_sampler_stop(self.state,self.run)
                    except Exception:
                        # StopPost independently retries only proved ownership.
                        pass
                threading.Thread(target=cancel_owned,daemon=True).start()
            if not self.pending and not self.finalizing:
                def abort_finish():
                    if self.operations:
                        self.operations.remove()
                    owned_sampler_stop(self.state,self.run)
                    if self.operations and (self.state/'fixture.json').exists():
                        self.finish()
                self.finalizing = True
                self.submit('finish',abort_finish)
            return
        if self.terminal and self.post_allowed and not self.pending and not self.finalizing:
            if self.uploaded():
                self.finalizing = True
                self.submit('finish',self.finish)
                return
        if not self.pending and not self.finalizing and self.operations:
            if now-self.last_gate >= 15:
                self.submit('gate',pg_gates)
            elif now-self.last_status >= 30:
                self.status_sequence += 1
                self.submit('status',lambda:self.operations.command('status',str(self.status_sequence),timeout=25))
    def serve(self):
        self.emit(self.owner)
        self.submit('prepare',self.prepare)
        selector = selectors.DefaultSelector()
        os.set_blocking(sys.stdin.fileno(),False)
        selector.register(sys.stdin.fileno(),selectors.EVENT_READ)
        lines = Lines()
        try:
            while True:
                try:
                    self.tick()
                except (OSError,ValueError,KeyError,WindowError) as error:
                    self.latch(str(error) if str(error) in ABORTS else 'operation_failed')
                if self.freeze and (self.input_eof or self.freeze_at is not None and time.monotonic()-self.freeze_at >= .25):
                    return 0
                for key,mask in selector.select(.1):
                    chunk = os.read(key.fd,4096)
                    if not chunk:
                        self.input_eof = True
                        selector.unregister(key.fd)
                        try:
                            lines.eof()
                        except WindowError:
                            self.latch('protocol_invalid')
                        if not self.freeze:
                            self.latch('bridge_stale')
                        continue
                    try:
                        for raw in lines.feed(chunk):
                            self.request(raw)
                    except (ValueError,KeyError,TypeError,WindowError,OSError):
                        self.latch('protocol_invalid')
        finally:
            selector.close()

def load_existing(state):
    state = Path(state)
    private_directory(state)
    run_ref,raw = stable_file(state/'run.json',16384)
    value = decode(raw,16384)
    probe = value.get('profile') == 'window225_probe_v1' if type(value) is dict else False
    script = Path(__file__).resolve()
    return context(str(state),run_ref['sha256'],stable_file(script,262144)[0]['sha256'],probe)+(probe,)

def artifact_owner(state,expected_run,owner_sha):
    state,script,run,probe = load_existing(state)
    require(run['run_id'] == expected_run and digest(owner_sha), 'artifact_binding_mismatch')
    owner,ref = read_json(state/'owner.json',4096)
    require(ref['sha256'] == owner_sha and owner['run_id'] == expected_run and owner['controller_sha256'] == run['tools_sha256']['controller'], 'artifact_binding_mismatch')
    return state,script,run,owner,probe

def read_artifact(state,expected_run,owner_sha,role,expected_sha):
    state,script,run,owner,probe = artifact_owner(state,expected_run,owner_sha)
    require(digest(expected_sha), 'artifact_binding_mismatch')
    path,maximum = artifact_path(state,role)
    if role == 'fixture':
        ready,_ = read_json(state/'ready.json',16384)
        require(ready['fixture_ref']['sha256'] == expected_sha, 'fixture_not_ready')
    ref,_ = stable_file(path,maximum,True,False)
    require(ref['sha256'] == expected_sha, 'artifact_hash_mismatch')
    # No secret is printed by the control server. This separately requested raw
    # pipe is explicitly directed by the coordinator into its private file.
    descriptor = os.open(str(path),os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0))
    try:
        before = os.fstat(descriptor)
        require(stat.S_ISREG(before.st_mode) and before.st_uid == 0 and stat.S_IMODE(before.st_mode) == 0o600
                and before.st_nlink == 1 and before.st_size == ref['bytes'], 'artifact_changed')
        checksum = hashlib.sha256()
        remaining = ref['bytes']
        deadline = time.monotonic()+15
        while remaining:
            chunk = os.read(descriptor,min(65536,remaining))
            require(bool(chunk), 'artifact_changed')
            checksum.update(chunk)
            write_deadline(sys.stdout.fileno(),chunk,max(.001,deadline-time.monotonic()))
            remaining -= len(chunk)
        require(checksum.hexdigest() == expected_sha and same(before,os.fstat(descriptor)) and same(before,os.lstat(str(path))), 'artifact_changed')
    finally:
        os.close(descriptor)

def receive_artifact(state,expected_run,owner_sha,role,expected_sha,size):
    state,script,run,owner,probe = artifact_owner(state,expected_run,owner_sha)
    require(not probe and role in UPLOAD and digest(expected_sha) and type(size) is str
            and INT.fullmatch(size) and int(size) <= FILES[role][1], 'invalid_upload')
    try:
        os.lstat(str(state/'freeze.json'))
    except FileNotFoundError:
        pass
    else:
        raise WindowError('run_frozen')
    admitted,_ = read_json(state/'transfer-admission.json',4096)
    require(admitted['run_id'] == expected_run and admitted['owner_sha256'] == owner_sha
            and admitted['original_sampler_wait_completed'] is True, 'upload_before_sampler_terminal')
    props = properties(owner['unit'],['MainPID','InvocationID','ActiveState'])
    require(props['MainPID'] == str(owner['main_pid']) and props['InvocationID'] == owner['invocation_id']
            and props['ActiveState'] == 'active' and start_ticks(owner['main_pid']) == owner['start_ticks'], 'owner_changed')
    wire,_ = read_json(state/'client-wire-terminal.json',4096)
    require(any(item == {'role':role,'bytes':int(size),'sha256':expected_sha} for item in wire['artifacts']), 'unexpected_upload')
    # One independently authenticated upload at a time. Do not renew the bridge
    # lease here or overwrite partial/final bytes after a failed transfer.
    lock = os.open(str(state/'transfer.lock'),os.O_RDWR|os.O_CREAT|getattr(os,'O_NOFOLLOW',0),0o600)
    descriptor = None
    try:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        final = state/FILES[role][0]
        try:
            os.lstat(str(final))
        except FileNotFoundError:
            pass
        else:
            raise WindowError('upload_exists')
        partial = state/('.incoming-'+role+'.partial')
        descriptor = os.open(str(partial),os.O_WRONLY|os.O_CREAT|os.O_EXCL|getattr(os,'O_NOFOLLOW',0),0o600)
        checksum,received,deadline = hashlib.sha256(),0,time.monotonic()+15
        incoming = sys.stdin.fileno()
        os.set_blocking(incoming,False)
        while True:
            remaining = deadline-time.monotonic()
            require(remaining > 0, 'upload_timeout')
            require(bool(select.select([incoming],[],[],min(remaining,1))[0]) or time.monotonic() < deadline, 'upload_timeout')
            try:
                chunk = os.read(incoming,65536)
            except BlockingIOError:
                continue
            if not chunk:
                break
            received += len(chunk)
            require(received <= int(size), 'upload_too_large')
            checksum.update(chunk)
            offset = 0
            while offset < len(chunk):
                offset += os.write(descriptor,chunk[offset:])
        require(received == int(size) and checksum.hexdigest() == expected_sha, 'upload_hash_mismatch')
        os.fsync(descriptor)
        identity = os.fstat(descriptor)
        os.close(descriptor)
        descriptor = None
        require(same(identity,os.lstat(str(partial))), 'upload_changed')
        os.link(str(partial),str(final))
        sync_directory(state)
        require((identity.st_dev,identity.st_ino) == (os.lstat(str(partial)).st_dev,os.lstat(str(partial)).st_ino), 'upload_changed')
        os.unlink(str(partial))
        sync_directory(state)
        write_deadline(sys.stdout.fileno(),encoded({'v':1,'kind':'artifact_received','run_id':expected_run,'owner_sha256':owner_sha,
                                                   'role':role,'bytes':received,'sha256':expected_sha}))
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(lock)

def partial_freeze(state,run,owner,run_sha):
    refs = []
    for role,(name,maximum) in FILES.items():
        if role in ['freeze','fixture','controller_terminal']:
            continue
        try:
            ref,_ = stable_file(state/name,maximum,True,False)
            refs.append({'role':role,**ref})
        except (FileNotFoundError,WindowError):
            continue
    lookup = lambda role:next((item['sha256'] for item in refs if item['role'] == role),None)
    publish(state/'freeze.json',{'v':1,'kind':'freeze','run_id':run['run_id'],'run_sha256':run_sha,
            'owner_sha256':sha(encoded(owner)),'controller_outcome':'failed','client_terminal_sha256':lookup('client_terminal'),
            'sampler_terminal_sha256':lookup('sampler_terminal'),'artifacts':refs,'cleanup':'deferred',
            'capacity_verified':False,'source_authenticated':False,'latest_state_verified':False,'recovery_verified':False})

def stop_post(state,run_sha,self_sha,probe=False):
    state,script,run = context(state,run_sha,self_sha,probe)
    unit = unit_name(run,probe)
    props = properties(unit,['InvocationID','ExecMainPID','Result','ExecMainCode','ExecMainStatus','NFileDescriptorStore'])
    require(props['InvocationID'] == os.environ.get('INVOCATION_ID') and run_id(props['InvocationID']), 'stoppost_owner_mismatch')
    intent = None
    try:
        intent,_ = read_json(state/'lock-intent.json',4096)
        require(intent['run_sha256'] == run_sha and intent['native_owner']['invocation_id'] == props['InvocationID']
                and str(intent['native_owner']['main_pid']) == props['ExecMainPID'], 'stoppost_owner_mismatch')
        lock_path = Path(intent['path'])
        require(lock_path == (state/'probe.lock' if probe else DEPLOY_LOCK), 'stoppost_lock_mismatch')
        identity = os.lstat(str(lock_path))
        require(str(identity.st_dev) == intent['device'] and str(identity.st_ino) == intent['inode'], 'stoppost_lock_mismatch')
        require(props['NFileDescriptorStore'] == '1' and not lock_available(lock_path), 'stoppost_lock_not_held')
    except FileNotFoundError:
        require(props['NFileDescriptorStore'] == '0', 'unidentified_fdstore')
    errors = []
    if not probe and (state/'operator-context.json').exists():
        operations = Operations(state,script,run)
        try:
            operations.remove()
        except Exception:
            errors.append('operator_cleanup_failed')
        if (state/'fixture.json').exists() and not (state/'key-revoked.json').exists():
            try:
                operations.command('revoke',timeout=30)
            except Exception:
                errors.append('key_revoke_unverified')
    try:
        owned_sampler_stop(state,run)
    except Exception:
        errors.append('sampler_cleanup_unverified')
    if not (state/'freeze.json').exists() and (state/'owner.json').exists():
        owner,_ = read_json(state/'owner.json',4096)
        partial_freeze(state,run,owner,run_sha)
    if (state/'operator.env').exists():
        try:
            saved,_ = read_json(state/'operator-context.json',4096)
            require(stable_file(state/'operator.env',65536)[0]['sha256'] == saved['environment_sha256'], 'operator_env_changed')
            os.unlink(str(state/'operator.env'))
            sync_directory(state)
        except Exception:
            errors.append('credential_cleanup_unverified')
    # Record native terminal information before collection/GC, after bounded
    # cleanup attempts. It is distinct from the original SSH/systemd wait close.
    publish(state/'controller-terminal.json',{'v':1,'kind':'controller_terminal','run_id':run['run_id'],
            'native':props,'service_result':os.environ.get('SERVICE_RESULT'),'exit_code':os.environ.get('EXIT_CODE'),
            'exit_status':os.environ.get('EXIT_STATUS'),'cleanup_errors':errors,
            'fdstore_held_during_cleanup':intent is not None,'capacity_verified':False})
    if intent is not None:
        # Last action affecting custody: remove the very same stored OFD. There
        # is intentionally no flock reacquisition/release gap in StopPost.
        require(not lock_available(Path(intent['path'])), 'stoppost_lock_not_held')
        notify('FDSTOREREMOVE=1\nFDNAME=window225-lock')
        barrier()
        require(properties(unit,['NFileDescriptorStore'])['NFileDescriptorStore'] == '0'
                and lock_available(Path(intent['path'])), 'fdstore_remove_failed')
        publish(state/'lock-released.json',{'run_id':run['run_id'],'invocation_id':props['InvocationID'],
                'pid1_store_count':0,'independent_flock_available':True,'cleanup_errors':errors,'capacity_verified':False})
    return 1 if errors else 0

def main(args):
    require(bool(args), 'usage')
    mode = args[0]
    if mode == 'configuration' and len(args) == 1:
        require(sys.platform == 'linux' and os.geteuid() == 0, 'root_linux_required')
        value,_ = runtime_snapshot()
        sys.stdout.buffer.write(encoded({'configuration_sha256':sha(encoded(value)),'configuration':value}))
        return 0
    if mode in ['launch','probe-launch'] and len(args) == 4:
        launch(*args[1:],probe=mode == 'probe-launch')
        return 1
    if mode in ['serve','stop-post'] and len(args) in [4,5]:
        probe = len(args) == 5 and args[4] == 'probe'
        require(len(args) == 4 or probe, 'usage')
        if mode == 'stop-post':
            return stop_post(*args[1:4],probe=probe)
        state,script,run = context(*args[1:4],probe=probe)
        return Supervisor(state,script,run,args[2],args[3],probe).serve()
    if mode == 'sampler-stop' and len(args) in [3,4]:
        state,script,run,probe = load_existing(args[1])
        require(run['run_id'] == args[2] and (len(args) == 4 and args[3] == 'probe') == probe, 'usage')
        sampler_terminal(state,args[2],probe)
        return 0
    if mode == 'read-artifact' and len(args) == 6:
        read_artifact(*args[1:])
        return 0
    if mode == 'receive-artifact' and len(args) == 7:
        receive_artifact(*args[1:])
        return 0
    raise WindowError('usage')

if __name__ == '__main__':
    try:
        raise SystemExit(main(sys.argv[1:]))
    except Exception:
        # Never reflect a path, credential, original exception or input payload.
        sys.stderr.write(json.dumps({'code':'window_remote_failed'})+'\n')
        raise SystemExit(1)
