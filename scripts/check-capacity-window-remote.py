#!/usr/bin/env python3
"""Local strict-wire/native-file/native-stdio checks. Never runs a product tool.

Actual systemd/FDStore/SSH/sampler/PG/Kafka behavior needs the separate root-run
private probe and later formal admission. These checks cannot certify it.
"""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time
import unittest

SOURCE = Path(__file__).with_name('capacity-window-remote.py').resolve()
spec = importlib.util.spec_from_file_location('capacity_remote',str(SOURCE))
remote = importlib.util.module_from_spec(spec)
spec.loader.exec_module(remote)
RUN = 'a'*32
OWNER = 'b'*64

def heartbeat(seq='1'):
    return {'v':1,'kind':'heartbeat','run_id':RUN,'owner_sha256':OWNER,'seq':seq,'nonce':'c'*32,'client':None}

def request(value, previous=0):
    return remote.validate_request(value,RUN,OWNER,previous)

def run_value(probe=False):
    return {'v':1,'kind':'run','run_id':RUN,'release':'m4-polling-014','schema':22,'operator_commit':'d'*40,
            'profile':'window225_probe_v1' if probe else 'window225_fixed_v1','expected_origin':'https://load.invalid',
            'expected_runtime_sha256':'e'*64,'tools_sha256':{role:'f'*64 for role in remote.TOOLS}}

def terminal():
    value = heartbeat()
    value.update(kind='client_terminal',client={'pid':123,'spawn_nonce':'d'*32},
                 close={'exit_code':0,'signal':None,'observed_at':'2026-11-03T01:02:03.000Z'},termination='completed',
                 artifacts=[{'role':role,'bytes':0 if role.endswith(('stdout','stderr')) else 10,'sha256':'f'*64}
                            for role in ['client_terminal','client_result','client_manifest','client_stdout','client_stderr']])
    return value

def synthetic_sampler_receipt(pid=123):
    # Pure receipt-schema fixture, not native systemd/time/source evidence.
    owner = {'v':1,'kind':'sampler_owner','run_id':RUN,'unit':'synthetic-no-systemd.service','invocation_id':'d'*32,
             'main_pid':pid,'start_ticks':'123','started_monotonic_us':'1000000','source_sha256':'f'*64,'seconds':10}
    rows = [{'capturedAt':1000,'elapsedMs':0,'telemetry':{'sampledAt':1000}},
            {'capturedAt':11000,'elapsedMs':10000,'telemetry':{'sampledAt':11000}}]
    raw = b''.join(remote.encoded(row) for row in rows)
    ref = {'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
    value = {'v':1,'kind':'sampler_terminal','run_id':RUN,'owner':copy.deepcopy(owner),
             'native':{'InvocationID':owner['invocation_id'],'ExecMainPID':str(pid),'Result':'success','ExecMainCode':'1',
                       'ExecMainStatus':'0','ExecMainStartTimestampMonotonic':'1000000','ExecMainExitTimestampMonotonic':'11000000'},
             'native_monotonic_us':{'ExecMainStartTimestampMonotonic':1000000,'ExecMainExitTimestampMonotonic':11000000},
             'samples':copy.deepcopy(ref),'frames':2,'last_frame':copy.deepcopy(rows[-1]),'natural_native_completion':True,
             'complete_fixed_duration':True,'capacity_verified':False,'source_authenticated':False}
    return owner,value,ref,rows

class Protocol(unittest.TestCase):
    def test_forwarded_pressure_keys_match_actual_application_policy(self):
        source = SOURCE.parents[1]/'packages/server/src/ingest-pressure.ts'
        keys = set(re.findall(r"\['[A-Za-z]+', '(INGEST_[A-Z_]+)',",source.read_text(encoding='utf-8')))
        self.assertEqual(len(keys),7)
        self.assertTrue(keys <= remote.ENV_KEYS)
        self.assertNotIn('INGEST_MAX_PROBE_AGE_MS',remote.ENV_KEYS)
        self.assertNotIn('INGEST_MAX_DISK_AGE_MS',remote.ENV_KEYS)
    def test_sampler_receipt_is_only_bound_ending_evidence_before_original_close(self):
        owner,value,ref,rows = synthetic_sampler_receipt()
        self.assertEqual(remote.sampler_wait_result(None,value,owner,ref,rows),'ending')
        self.assertEqual(remote.sampler_wait_result(0,value,owner,ref,rows),'completed')
        with self.assertRaises(remote.WindowError):remote.sampler_wait_result(1,value,owner,ref,rows)
        for change in ['owner','source','clock','raw-clock','samples','partial']:
            altered = copy.deepcopy(value)
            if change == 'owner':altered['native']['InvocationID'] = 'e'*32
            elif change == 'source':altered['owner']['source_sha256'] = 'e'*64
            elif change == 'clock':altered['native_monotonic_us']['ExecMainExitTimestampMonotonic'] = 10999999
            elif change == 'raw-clock':altered['native']['ExecMainExitTimestampMonotonic'] = '11000001'
            elif change == 'samples':altered['samples']['sha256'] = 'e'*64
            else:del altered['complete_fixed_duration']
            with self.subTest(change=change),self.assertRaises(remote.WindowError):remote.sampler_wait_result(0,altered,owner,ref,rows)
    def test_actual_captured_mount_permutation_keeps_every_bound_field(self):
        # Exact Kafka projection from the first native probe's two preserved
        # raw configuration files (SHA256 3a369ce1... / 114a96e5...). Docker
        # moved its fourth entry first; no mount field actually changed.
        before = [
            {'Type':'volume','Source':'/var/lib/docker/volumes/b8971ea6e9633680e1cf9c311518eba0b1133d6f48047da3382080dcb0cafabd/_data','Destination':'/var/lib/kafka/data','RW':True},
            {'Type':'volume','Source':'/var/lib/docker/volumes/40820cbbe0cc854b869e2a3efdb411f8c6fba7d1e501d99ba74750f8525a7a43/_data','Destination':'/etc/kafka/secrets','RW':True},
            {'Type':'volume','Source':'/var/lib/docker/volumes/03d0a0e456336180c0370f7f9b3aabb76fe7ba74caf8189694bdaa5ba3f4d4a0/_data','Destination':'/mnt/shared/config','RW':True},
            {'Type':'volume','Source':'/var/lib/docker/volumes/ccpanel_kafka_data/_data','Destination':'/tmp/kraft-combined-logs','RW':True}]
        after = [copy.deepcopy(before[index]) for index in [3,0,1,2]]
        original_before,original_after = copy.deepcopy(before),copy.deepcopy(after)
        expected = remote.project_mounts(before)
        self.assertEqual(expected,remote.project_mounts(after))
        self.assertEqual([value['Destination'] for value in expected],['/etc/kafka/secrets','/mnt/shared/config','/tmp/kraft-combined-logs','/var/lib/kafka/data'])
        self.assertEqual(before,original_before);self.assertEqual(after,original_after)
        for field,replacement in [('Type','bind'),('Source','/different/source'),('Destination','/different/destination'),('RW',False)]:
            changed = copy.deepcopy(after);changed[0][field] = replacement
            with self.subTest(field=field):self.assertNotEqual(expected,remote.project_mounts(changed))
        duplicate = copy.deepcopy(after);duplicate.append({**duplicate[0],'Source':'/different/source'})
        with self.assertRaises(remote.WindowError):remote.project_mounts(duplicate)
        for invalid in [[{'Type':'volume','Source':'/missing-destination','RW':True}],
                        [{'Type':'volume','Source':'/source','Destination':'/target','RW':1}]]:
            with self.subTest(invalid=invalid),self.assertRaises(remote.WindowError):remote.project_mounts(invalid)
    def test_exact_run_and_eleven_fixed_tools(self):
        self.assertEqual(len(remote.TOOLS),11)
        self.assertEqual(remote.validate_run(run_value()),run_value())
        self.assertEqual(remote.validate_run(run_value(True),True),run_value(True))
        for change in ['unknown','missing','wrong_hash','profile']:
            value = run_value()
            if change == 'unknown':value['tools_sha256']['shell'] = 'f'*64
            elif change == 'missing':del value['tools_sha256']['sampler']
            elif change == 'wrong_hash':value['tools_sha256']['sampler'] = 'F'*64
            else:value['profile'] = 'window225_probe_v1'
            with self.subTest(change=change),self.assertRaises(remote.WindowError):remote.validate_run(value)
    def test_json_roundtrip_order_is_not_authority(self):
        value = heartbeat()
        self.assertEqual(remote.decode(remote.encoded(value),4096,True),value)
        reverse = dict(reversed(list(value.items())))
        self.assertEqual(remote.decode(remote.encoded(reverse),4096,True),value)
    def test_duplicate_and_noncanonical_wire_refused(self):
        for raw in [b'{"v":1,"v":1}\n',b'{"a":1,"\\u0061":2}\n',b'{}\r\n',b'\xef\xbb\xbf{}\n',
                    b'{}',b'{}\n\n',b'{ }\n',b'{"v":1.0}\n',b'{"x":NaN}\n',b'{} trailing\n',b'\xff\n']:
            with self.subTest(raw=raw),self.assertRaises(remote.WindowError):remote.decode(raw,4096,True)
    def test_binding_and_exact_shapes(self):
        self.assertEqual(request(heartbeat())['seq'],'1')
        for field,value in [('run_id','e'*32),('owner_sha256','d'*64),('nonce','X'*32),('v',True),('extra',0)]:
            item = heartbeat();item[field] = value
            with self.subTest(field=field),self.assertRaises(remote.WindowError):request(item)
    def test_i64_sequences_are_text_and_strictly_new(self):
        self.assertEqual(request(heartbeat('9223372036854775807'))['seq'],'9223372036854775807')
        for seq in ['0','01','-1','1.0','9223372036854775808',1,True,None]:
            with self.subTest(seq=seq),self.assertRaises(remote.WindowError):request(heartbeat(seq))
        with self.assertRaises(remote.WindowError):request(heartbeat('2'),2)
        with self.assertRaises(remote.WindowError):request(heartbeat('1'),2)
    def test_client_subject_shape(self):
        value = heartbeat();value['client'] = {'pid':123,'spawn_nonce':'e'*32}
        self.assertEqual(request(value)['client']['pid'],123)
        for bad in [{'pid':True,'spawn_nonce':'e'*32},{'pid':0,'spawn_nonce':'e'*32},{'pid':123,'spawn_nonce':'short'},{'pid':123,'spawn_nonce':'e'*32,'other':0}]:
            value['client'] = bad
            with self.subTest(bad=bad),self.assertRaises(remote.WindowError):request(value)
    def test_abort_code_cannot_carry_arbitrary_error_or_path(self):
        value = heartbeat();del value['client'];value.update(kind='abort',code='operator_requested')
        self.assertEqual(request(value)['code'],'operator_requested')
        value['code'] = '/private/credential'
        with self.assertRaises(remote.WindowError):request(value)
    def test_terminal_requires_native_close_and_real_file_inventory(self):
        self.assertEqual(request(terminal())['termination'],'completed')
        for change in ['no_client','both_close','neither_close','bad_signal','missing_role','duplicate','unknown_role','oversize','string_bytes']:
            value = terminal()
            if change == 'no_client':value['client'] = None
            elif change == 'both_close':value['close']['signal'] = 'SIGTERM'
            elif change == 'neither_close':value['close']['exit_code'] = None
            elif change == 'bad_signal':value['close'].update(exit_code=None,signal='SIGUNKNOWN')
            elif change == 'missing_role':value['artifacts'] = value['artifacts'][1:]
            elif change == 'duplicate':value['artifacts'][-1] = value['artifacts'][0]
            elif change == 'unknown_role':value['artifacts'][0]['role'] = '../../secret'
            elif change == 'oversize':value['artifacts'][0]['bytes'] = 32769
            else:value['artifacts'][0]['bytes'] = '10'
            with self.subTest(change=change),self.assertRaises(remote.WindowError):request(value)
    def test_missing_workload_can_report_original_signal(self):
        value = terminal();value['termination'] = 'missing';value['close'].update(exit_code=None,signal='SIGKILL')
        value['artifacts'] = [value['artifacts'][0]]
        self.assertEqual(request(value)['close']['signal'],'SIGKILL')
    def test_lines_fragmentation_and_partial_eof(self):
        value = remote.encoded(heartbeat());lines = remote.Lines();result = []
        for part in [value[:3],value[3:17],value[17:-1],value[-1:]]:result += lines.feed(part)
        self.assertEqual(result,[value]);lines.eof()
        lines.feed(b'{')
        with self.assertRaises(remote.WindowError):lines.eof()
    def test_lines_limit_does_not_truncate_then_accept(self):
        for raw in [b'x'*4096,b'\x00\n',b'x'*4096+b'\n']:
            with self.subTest(raw=raw[:5]),self.assertRaises(remote.WindowError):remote.Lines().feed(raw)
    def test_artifact_roles_are_fixed_paths(self):
        state = Path('/private/run')
        self.assertEqual(remote.artifact_path(state,'client_result')[0],state/'client-result.json')
        self.assertEqual(remote.artifact_path(state,'status_9223372036854775807')[0],state/'status-9223372036854775807.json')
        for role in ['../fixture','operator.env','status_0','status_01','status_9223372036854775808','client_result/../../env']:
            with self.subTest(role=role),self.assertRaises(remote.WindowError):remote.artifact_path(state,role)

@unittest.skipUnless(os.name == 'posix','native POSIX permission/no-follow checks only')
class NativeFiles(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='ccpanel-window225-check-')
        self.directory = Path(self.temp.name).resolve()
        self.path = self.directory/'evidence.json'
        self.uid = os.geteuid()
    def tearDown(self):self.temp.cleanup()
    def read(self,path=None,maximum=4096):return remote.stable_file(path or self.path,maximum,owner_uid=self.uid)
    def test_actual_exclusive_publish_and_streamed_hash(self):
        expected = remote.publish(self.path,{'synthetic':True})
        self.assertEqual(self.read()[0],expected)
        with self.assertRaises(FileExistsError):remote.publish(self.path,{'replacement':True})
        self.assertEqual(self.read()[1],b'{"synthetic":true}\n')
    def test_symlink_hardlink_directory_and_fifo_refused(self):
        remote.publish(self.path,{'keep':True})
        link = self.directory/'link';os.symlink(str(self.path),str(link))
        with self.assertRaises(remote.WindowError):self.read(link)
        hard = self.directory/'hard';os.link(str(self.path),str(hard))
        with self.assertRaises(remote.WindowError):self.read(hard)
        with self.assertRaises(remote.WindowError):self.read(self.directory)
        fifo = self.directory/'fifo';os.mkfifo(str(fifo))
        started = time.monotonic()
        with self.assertRaises(remote.WindowError):self.read(fifo)
        self.assertLess(time.monotonic()-started,1)
    def test_private_mode_and_exact_size_cap(self):
        remote.publish(self.path,b'x'*4096,True)
        self.assertEqual(self.read()[0]['bytes'],4096)
        with self.assertRaises(remote.WindowError):self.read(maximum=4095)
        os.chmod(str(self.path),0o644)
        with self.assertRaises(remote.WindowError):self.read()
    def test_empty_sampler_file_is_not_a_ready_frame(self):
        path = self.directory/'samples.jsonl';remote.publish(path,b'',True)
        self.assertEqual(remote.sample_frames(self.directory,owner_uid=self.uid),[])
        now = int(time.time()*1000)
        frame = {'capturedAt':now,'elapsedMs':0,'telemetry':{'sampledAt':now}}
        with path.open('ab') as file:file.write(remote.encoded(frame)+b'{"capturedAt":')
        self.assertEqual(remote.sample_frames(self.directory,owner_uid=self.uid),[frame])
    def test_sampler_order_error_and_oversized_line_refused(self):
        now = int(time.time()*1000)
        row = {'capturedAt':now,'elapsedMs':0,'telemetry':{'sampledAt':now}}
        path = self.directory/'samples.jsonl';remote.publish(path,remote.encoded(row)*2,True)
        with self.assertRaises(remote.WindowError):remote.sample_frames(self.directory,owner_uid=self.uid)
        path.unlink();remote.publish(path,b'x'*65537,True)
        with self.assertRaises(remote.WindowError):remote.sample_frames(self.directory,owner_uid=self.uid)
    def test_native_command_output_cap_and_real_timeout(self):
        self.assertEqual(remote.bounded([sys.executable,'-c','print("native")'],3,1024),b'native\n')
        with self.assertRaises(remote.WindowError):remote.bounded([sys.executable,'-c','print("x"*65536)'],3,1024)
        started = time.monotonic()
        with self.assertRaises(remote.WindowError):remote.bounded([sys.executable,'-c','import time;time.sleep(5)'],.15,1024)
        self.assertLess(time.monotonic()-started,4)

class NativeStdio(unittest.TestCase):
    def test_same_original_native_wait_must_close_after_valid_receipt(self):
        for exit_code in [0,3]:
            child = subprocess.Popen([sys.executable,'-c','import sys;sys.stdout.buffer.write(b"ready\\n");sys.stdout.buffer.flush();sys.stdin.buffer.read();sys.exit(int(sys.argv[1]))',str(exit_code)],
                                     stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                self.assertEqual(child.stdout.readline(),b'ready\n')
                owner,value,ref,rows = synthetic_sampler_receipt(child.pid)
                self.assertIsNone(child.poll())
                self.assertEqual(remote.sampler_wait_result(child.poll(),value,owner,ref,rows),'ending')
                out,error = child.communicate(b'original-parent-release',timeout=5)
                self.assertEqual(out,b'');self.assertEqual(error,b'');self.assertEqual(child.returncode,exit_code)
                if exit_code == 0:self.assertEqual(remote.sampler_wait_result(child.returncode,value,owner,ref,rows),'completed')
                else:
                    with self.assertRaises(remote.WindowError):remote.sampler_wait_result(child.returncode,value,owner,ref,rows)
            finally:
                if child.poll() is None:child.kill();child.communicate(timeout=5)
    def child(self,raw):
        code = '''import importlib.util,sys
s=importlib.util.spec_from_file_location("remote",sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
try:
 lines=m.Lines();count=0;previous=0
 while True:
  part=sys.stdin.buffer.read(7)
  if not part:break
  for raw in lines.feed(part):
   v=m.validate_request(m.decode(raw,4096,True),"a"*32,"b"*64,previous);previous=int(v["seq"]);count+=1
 lines.eof();sys.stdout.buffer.write(m.encoded({"requests":count,"last_seq":str(previous)}))
except Exception:
 sys.stdout.write('{"code":"native_protocol_rejected"}\\n');sys.exit(1)
'''
        child = subprocess.Popen([sys.executable,'-c',code,str(SOURCE)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        out,error = child.communicate(raw,timeout=5)
        self.assertEqual(error,b'')
        return child.returncode,json.loads(out)
    def test_actual_pipe_two_ordered_requests_and_native_close(self):
        second = heartbeat('2');second['nonce'] = 'd'*32
        code,value = self.child(remote.encoded(heartbeat())+remote.encoded(second))
        self.assertEqual(code,0);self.assertEqual(value,{'requests':2,'last_seq':'2'})
    def test_actual_pipe_duplicate_and_partial_requests_fail(self):
        for raw in [remote.encoded(heartbeat())*2,b'{"v":1,"v":1}\n',remote.encoded(heartbeat())[:-1]]:
            with self.subTest(raw=raw[:20]):
                code,value = self.child(raw);self.assertEqual(code,1);self.assertEqual(value,{'code':'native_protocol_rejected'})
    def test_cli_unknown_action_redacts_input(self):
        result = subprocess.run([sys.executable,str(SOURCE),'private-secret-must-not-reflect'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=5)
        self.assertEqual(result.returncode,1);self.assertEqual(result.stdout,b'')
        self.assertEqual(json.loads(result.stderr),{'code':'window_remote_failed'})

if __name__ == '__main__':
    suite = unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__])
    result = unittest.TextTestRunner(stream=sys.stderr,verbosity=1).run(suite)
    print(json.dumps({'passed':result.wasSuccessful(),'tests':result.testsRun,'platform_skips':len(result.skipped),
                      'strict_protocol_and_schema':True,'actual_native_stdio':True,'native_posix_files_checked':os.name == 'posix',
                      'product_commands_executed':False,'ssh_verified':False,'systemd_verified':False,'fdstore_verified':False,
                      'sampler_native_terminal_verified':False,'capacity_verified':False}))
    raise SystemExit(0 if result.wasSuccessful() else 1)
