#!/usr/bin/env python3
"""Linux host counters only. Never reads process arguments or credentials."""
import argparse
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import time


def host_counters(proc=Path('/proc')):
    memory = {}
    names = {'MemTotal': 'total', 'MemAvailable': 'available',
             'SwapTotal': 'swapTotal', 'SwapFree': 'swapFree', 'Dirty': 'dirty'}
    for line in (proc / 'meminfo').read_text().splitlines():
        fields = line.split()
        key = fields[0].rstrip(':')
        if key in names:
            if len(fields) != 3 or fields[2] != 'kB' or not fields[1].isdigit():
                raise ValueError('invalid_memory_counter')
            memory[names[key]] = str(int(fields[1]) * 1024)
    if len(memory) != len(names):
        raise ValueError('missing_memory_counter')
    if int(memory['available']) > int(memory['total']) or int(memory['swapFree']) > int(memory['swapTotal']):
        raise ValueError('invalid_memory_capacity')
    vm = dict(line.split() for line in (proc / 'vmstat').read_text().splitlines())
    if not vm.get('pswpin', '').isdigit() or not vm.get('pswpout', '').isdigit():
        raise ValueError('invalid_swap_counter')
    first = (proc / 'stat').read_text().splitlines()[0].split()
    if first[0] != 'cpu' or len(first) < 9 or not all(v.isdigit() for v in first[1:9]):
        raise ValueError('invalid_cpu_counter')
    ticks = list(map(int, first[1:9]))
    # guest/guest_nice are already included in user/nice and must not be added.
    cpu = {'userTicks': str(ticks[0] + ticks[1]), 'systemTicks': str(ticks[2] + ticks[5] + ticks[6]),
           'idleTicks': str(ticks[3]), 'ioWaitTicks': str(ticks[4]),
           'stealTicks': str(ticks[7]), 'totalTicks': str(sum(ticks)),
           'ticksPerSecond': int(os.sysconf('SC_CLK_TCK'))}
    load = list(map(float, (proc / 'loadavg').read_text().split()[:3]))
    if len(load) != 3 or any(not math.isfinite(v) or v < 0 for v in load):
        raise ValueError('invalid_load_counter')
    return {'memoryBytes': memory, 'swapInPages': vm['pswpin'], 'swapOutPages': vm['pswpout'],
            'pageBytes': int(os.sysconf('SC_PAGESIZE')), 'cpu': cpu, 'loadAverage': load}


def collect(config):
    if not isinstance(config, list) or len(config) != 2:
        raise ValueError('invalid_volume_config')
    disks, seen = [], set()
    started = int(time.time() * 1000)
    for item in config:
        if set(item) != {'kind', 'path'} or item['kind'] not in ('kafka', 'postgres') or item['kind'] in seen:
            raise ValueError('invalid_volume_config')
        seen.add(item['kind'])
        path = Path(item['path'])
        if not path.is_absolute() or not path.is_dir():
            raise ValueError('invalid_volume_path')
        stat = os.statvfs(path)
        disks.append({'kind': item['kind'], 'sampledAt': started,
                      'totalBytes': str(stat.f_blocks * stat.f_frsize),
                      'availableBytes': str(stat.f_bavail * stat.f_frsize),
                      'totalInodes': str(stat.f_files), 'availableInodes': str(stat.f_favail)})
    return {'version': 1, 'sampledAt': started, 'disks': disks, 'host': host_counters()}


def publish(output, sample):
    # The old, valid file survives collection/encoding/replace failures. A reader
    # sees one complete inode; expiry is handled by the admission policy.
    encoded = json.dumps(sample, separators=(',', ':'), allow_nan=False).encode()
    if len(encoded) > 32768:
        raise ValueError('sample_too_large')
    output = Path(output)
    fd, temporary = tempfile.mkstemp(prefix='.host-', suffix='.tmp', dir=output.parent)
    try:
        with os.fdopen(fd, 'wb') as file:
            os.chmod(temporary, 0o644)
            file.write(encoded)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, output)
        if hasattr(os, 'O_DIRECTORY'):
            directory = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    try:
        config = json.loads(Path(args.config).read_text())
        publish(args.output, collect(config))
    except Exception as error:
        print(json.dumps({'code': 'host_probe_failed', 'type': type(error).__name__}), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
