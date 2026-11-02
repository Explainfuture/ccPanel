#!/usr/bin/env python3
"""Finite read-only host/cgroup observer. No process arguments or env capture."""
import argparse
import json
import os
from pathlib import Path
import re
import time


def counters(path):
    return {key: int(value) for key, value in (line.split() for line in path.read_text().splitlines())}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--seconds', type=int, required=True)
    args = parser.parse_args()
    if not 10 <= args.seconds <= 3600:
        raise ValueError('invalid_duration')
    config = json.loads(Path(args.config).read_text())
    if set(config) != {'api', 'worker', 'kafka', 'postgres'} or any(not re.fullmatch('[a-f0-9]{64}', value) for value in config.values()):
        raise ValueError('invalid_container_inventory')
    groups = {name: Path('/sys/fs/cgroup/system.slice') / ('docker-' + identity + '.scope') for name, identity in config.items()}
    started = time.monotonic()
    fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as output:
        for index in range(args.seconds // 10 + 1):
            time.sleep(max(0, started + index * 10 - time.monotonic()))
            frame = {'capturedAt': int(time.time() * 1000), 'elapsedMs': round((time.monotonic() - started) * 1000)}
            try:
                path = Path('/opt/ccpanel/shared/telemetry/host.json')
                if path.stat().st_size > 32768:
                    raise ValueError('host_frame_too_large')
                telemetry = json.loads(path.read_text())
                if telemetry['version'] != 1 or not 0 <= frame['capturedAt'] - telemetry['sampledAt'] <= 30000:
                    raise ValueError('host_frame_stale')
                frame['telemetry'] = telemetry
                frame['containers'] = {name: {
                    'memoryBytes': int((group / 'memory.current').read_text()),
                    'swapBytes': int((group / 'memory.swap.current').read_text()),
                    'memoryEvents': counters(group / 'memory.events'),
                    'cpu': counters(group / 'cpu.stat'),
                } for name, group in groups.items()}
            except Exception as error:
                # Never replace missing counters with zero or print path/data.
                frame = {**{key: frame[key] for key in ['capturedAt', 'elapsedMs']}, 'error': type(error).__name__}
            output.write(json.dumps(frame, separators=(',', ':')) + '\n')
            output.flush()
        os.fsync(output.fileno())


if __name__ == '__main__':
    main()
