"""Consume locally produced pnpm SDK tarballs outside the workspace.

Requires the normal build first; never use this to execute untrusted packages.
No registry installs or production endpoints are involved.
"""
import gzip
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile

root = Path(__file__).resolve().parent.parent
archives = Path(sys.argv[1] if len(sys.argv) > 1 else 'artifacts').resolve()
node = shutil.which('node')
assert node, 'Node.js required'
temporary_parent = Path(tempfile.gettempdir()).resolve()
consumer = Path(tempfile.mkdtemp(prefix='ccpanel-sdk-consumer-', dir=str(temporary_parent))).resolve()
environment = dict(os.environ)
for key in ['NODE_OPTIONS', 'NODE_PATH']:
    environment.pop(key, None)

def run(arguments):
    result = subprocess.run(arguments, cwd=str(consumer), env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8', timeout=60)
    if result.returncode:
        raise RuntimeError('Consumer command failed:\n' + result.stdout[:12000] + result.stderr[:12000])
    return result.stdout

def unpack(kind):
    manifest = json.loads((root / ('packages/sdk-' + kind) / 'package.json').read_text(encoding='utf-8'))
    assert manifest['name'] == '@ccpanel/sdk-' + kind
    assert re.fullmatch(r'[0-9A-Za-z.+-]+', manifest['version'])
    filename = 'ccpanel-sdk-' + kind + '-' + manifest['version'] + '.tgz'
    with gzip.open(str(archives / filename), 'rb') as source:
        raw = source.read(8 * 1024 * 1024 + 1)
    assert len(raw) <= 8 * 1024 * 1024, 'SDK archive expansion limit'
    destination = consumer / 'node_modules/@ccpanel' / ('sdk-' + kind)
    destination.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as archive:
        entries = archive.getmembers()
        assert len(entries) <= 256 and sum(entry.size for entry in entries) <= 8 * 1024 * 1024
        for entry in entries:
            parts = PurePosixPath(entry.name).parts
            assert parts and parts[0] == 'package' and '..' not in parts and '\\' not in entry.name and ':' not in entry.name
            assert entry.isdir() or entry.isfile(), 'Only regular SDK files/directories'
            output = destination.joinpath(*parts[1:])
            output.resolve().relative_to(destination.resolve())
            if entry.isdir():
                output.mkdir(parents=True, exist_ok=True)
            else:
                assert len(parts) > 1 and entry.size >= 0
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(archive.extractfile(entry).read())
    packaged = json.loads((destination / 'package.json').read_text(encoding='utf-8'))
    assert packaged['name'] == manifest['name'] and packaged['version'] == manifest['version']
    if kind == 'node':
        assert packaged['engines']['node'] == '>=22.12.0'

try:
    try:
        consumer.relative_to(root)
    except ValueError:
        pass
    else:
        raise RuntimeError('Consumer temporary directory must be outside the workspace')
    for kind in ['node', 'browser']:
        unpack(kind)
    (consumer / 'package.json').write_text(json.dumps({'private': True, 'type': 'module'}), encoding='utf-8')
    for kind, factory in [('node', 'createNodeAnalytics'), ('browser', 'createAnalytics')]:
        (consumer / (kind + '.ts')).write_text("""import {%s} from '@ccpanel/sdk-%s';
type Events={purchase:{amount:number}};
const client=%s<Events>({endpoint:'https://consumer.invalid/v1/batch',projectId:'fixture',writeKey:'fixture',consent:false});
void client.track('purchase',{amount:1});
// @ts-expect-error business event property remains typed
void client.track('purchase',{amount:'wrong'});
// @ts-expect-error undeclared event name remains rejected
void client.track('undeclared',{amount:1});
// @ts-expect-error required event properties cannot be omitted
void client.track('purchase');
client.destroy();
""" % (factory, kind, factory), encoding='utf-8')
    configurations = [
        ('node', 'NodeNext', ['ES2022'], ['node']),
        ('browser', 'ESNext', ['ES2022', 'DOM', 'DOM.Iterable'], []),
        ('browser', 'NodeNext', ['ES2022', 'DOM', 'DOM.Iterable'], []),
    ]
    for index, (kind, module, libraries, types) in enumerate(configurations):
        config = {'compilerOptions': {'target': 'ES2022', 'module': module, 'moduleResolution': 'NodeNext' if module == 'NodeNext' else 'Bundler', 'lib': libraries, 'types': types, 'typeRoots': [str(root / 'node_modules/@types')], 'strict': True, 'skipLibCheck': False, 'noEmit': True}, 'include': [kind + '.ts']}
        file = consumer / ('tsconfig-' + str(index) + '.json')
        file.write_text(json.dumps(config), encoding='utf-8')
        run([node, str(root / 'node_modules/typescript/bin/tsc'), '-p', str(file)])
    (consumer / 'runtime.mjs').write_text("""import assert from 'node:assert/strict';
import {existsSync} from 'node:fs';
import {resolve} from 'node:path';
import {createNodeAnalytics} from '@ccpanel/sdk-node';
import {createAnalytics} from '@ccpanel/sdk-browser';
const sent=[],outbox=resolve('outbox.json');
const options={endpoint:'https://consumer.invalid/v1/batch',projectId:'fixture',writeKey:'fixture',consent:true};
const client=createNodeAnalytics({...options,outboxPath:outbox,fetch:async(_url,init)=>{const ids=JSON.parse(init.body).events.map(e=>e.event_id);sent.push(...ids);return Response.json({accepted_event_ids:ids},{status:202});}});
try{await client.flush();const queued=await client.track('purchase',{amount:1});assert.equal(queued.status,'queued-persistent');await client.flush();assert.deepEqual(sent,[queued.event_id]);assert.equal((await client.getQueueStatus()).count,0);}finally{await client.shutdown();}
assert.equal(existsSync(outbox+'.lock'),false);
const browser=createAnalytics({...options,consent:false,fetch:async()=>{throw new Error('unexpected_transport');}});
try{assert.equal((await browser.getQueueStatus()).count,0);}finally{browser.destroy();}
console.log(JSON.stringify({runtime_imports:true,node_persistent_queue:true,stable_id_confirmed:true,shutdown_releases_lock:true,browser_import_smoke_only:true,network:'stub'}));
""", encoding='utf-8')
    runtime = json.loads(run([node, str(consumer / 'runtime.mjs')]))
    print(json.dumps({'passed': True, 'source': 'pnpm_tarballs', 'outside_workspace': True, 'node_nodenext_without_dom': True, 'browser_bundler_and_nodenext': True, 'strict_library_and_negative_types': True, **runtime}))
finally:
    # Validate the exact resolved target before recursive cleanup on Windows too.
    assert consumer.parent == temporary_parent and consumer.name.startswith('ccpanel-sdk-consumer-')
    shutil.rmtree(str(consumer))
