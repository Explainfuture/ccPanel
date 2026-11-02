#!/usr/bin/env python3
"""Nonsensitive fixed-configuration attestation at capacity run boundaries."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

root = Path('/opt/ccpanel/current')
state = {'version': 2, 'swappiness': int(Path('/proc/sys/vm/swappiness').read_text()),
         'pageBytes': int(os.sysconf('SC_PAGESIZE')), 'units': {}, 'limits': {}, 'artifacts': {}, 'monitoring': {}}
memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
state['memoryTotalBytes'] = str(int(memory['MemTotal'].split()[0]) * 1024)
for unit in ['fwupd.service', 'fwupd-refresh.timer', 'multipathd.service', 'multipathd.socket',
             'ssh.service', 'fail2ban.service', 'apt-daily.timer', 'apt-daily-upgrade.timer', 'ccpanel-host-probe.timer']:
    fields = dict(line.split('=', 1) for line in subprocess.check_output(
        ['systemctl', 'show', unit, '-p', 'ActiveState', '-p', 'UnitFileState'], text=True).splitlines())
    state['units'][unit] = fields
for name in ['api', 'worker', 'kafka', 'postgres']:
    identity = subprocess.check_output(['docker', 'inspect', '--format', '{{.Id}}', 'ccpanel-' + name + '-1'], text=True).strip()
    assert re.fullmatch('[a-f0-9]{64}', identity)
    group = Path('/sys/fs/cgroup/system.slice') / ('docker-' + identity + '.scope')
    state['limits'][name] = {field: (group / field).read_text().strip() for field in ['memory.max', 'memory.swap.max', 'cpu.max']}
    if name in ('api', 'worker'):
        # Fixed booleans only; never export the container environment.
        code = "console.log(JSON.stringify({collection:process.env.OPERATIONAL_COLLECTION_ENABLED==='true'?'enabled':process.env.OPERATIONAL_COLLECTION_ENABLED==='false'?'disabled':'unspecified',retention:process.env.RETENTION_ENABLED==='true'?'enabled':process.env.RETENTION_ENABLED==='false'?'disabled':'unspecified'}))"
        flags = json.loads(subprocess.check_output(['docker', 'exec', 'ccpanel-' + name + '-1', 'node', '-e', code], text=True))
        assert set(flags) == {'collection', 'retention'} and all(value in ('enabled', 'disabled', 'unspecified') for value in flags.values())
        state['monitoring'][name] = flags
for name in ['dist/api.mjs', 'dist/worker.mjs', 'infra/compose.yaml']:
    state['artifacts'][name] = hashlib.sha256((root / name).read_bytes()).hexdigest()
print(json.dumps(state, sort_keys=True))
