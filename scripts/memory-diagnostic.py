#!/usr/bin/env python3
"""Read-only Linux preview memory diagnostic; never reads process arguments/env."""
import datetime
import json
import pathlib
import re
import subprocess


def pairs(path):
    result = {}
    for line in path.read_text().splitlines():
        fields = line.replace(':', '').split()
        if len(fields) >= 2 and fields[1].isdigit():
            result[fields[0]] = int(fields[1])
    return result


root = pathlib.Path('/sys/fs/cgroup').resolve()
services = {}
for service in ['api', 'worker', 'postgres', 'kafka']:
    pid = subprocess.check_output(['docker', 'inspect', '--format', '{{.State.Pid}}', 'ccpanel-' + service + '-1'], text=True).strip()
    if not re.fullmatch(r'[1-9][0-9]*', pid):
        raise RuntimeError('Container not running: ' + service)
    lines = pathlib.Path('/proc', pid, 'cgroup').read_text().splitlines()
    paths = [line[3:] for line in lines if line.startswith('0::/')]
    if len(paths) != 1:
        raise RuntimeError('Expected unified cgroup v2')
    path = (root / paths[0].lstrip('/')).resolve()
    path.relative_to(root)
    row = {}
    for name in ['memory.current', 'memory.max', 'memory.high', 'memory.swap.current', 'memory.swap.max']:
        value = (path / name).read_text().strip()
        row[name] = int(value) if value.isdigit() else value
    row['memory.stat'] = {k: v for k, v in pairs(path / 'memory.stat').items() if k in ['anon', 'file', 'kernel', 'slab', 'active_anon', 'inactive_anon', 'pgfault', 'pgmajfault', 'pgscan', 'pgsteal', 'workingset_refault_anon', 'workingset_refault_file']}
    row['memory.events'] = pairs(path / 'memory.events')
    row['pressure'] = (path / 'memory.pressure').read_text().strip()
    services[service] = row

processes = []
for path in pathlib.Path('/proc').iterdir():
    if not path.name.isdigit():
        continue
    try:
        values = pairs(path / 'status')
        if values.get('VmSwap', 0):
            name = (path / 'comm').read_text().strip()
            processes.append({'pid': int(path.name), 'name': name, 'rss_kib': values.get('VmRSS'), 'swap_kib': values['VmSwap']})
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        continue
processes.sort(key=lambda row: row['swap_kib'], reverse=True)
print(json.dumps({'observed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'scope': 'host_and_fixed_preview_cgroups', 'meminfo_kib': {k: v for k, v in pairs(pathlib.Path('/proc/meminfo')).items() if k in ['MemTotal', 'MemFree', 'MemAvailable', 'SwapTotal', 'SwapFree', 'SwapCached', 'AnonPages', 'Slab', 'SReclaimable', 'SUnreclaim']}, 'vmstat_cumulative': {k: v for k, v in pairs(pathlib.Path('/proc/vmstat')).items() if k in ['pswpin', 'pswpout', 'pgmajfault']}, 'host_memory_pressure': pathlib.Path('/proc/pressure/memory').read_text().strip(), 'services': services, 'largest_swapped_processes': processes[:20], 'semantics': 'instantaneous gauges and cumulative counters; per-process RSS may share pages; not a capacity verdict'}, indent=2))
