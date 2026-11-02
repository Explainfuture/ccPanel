#!/usr/bin/env python3
"""Conservative end-state preservation of policy-required original monitor rows.

Version1 JSONL binds bounded complete snapshots, not their caller's identity.
The same source_label must identify both snapshots; it is not authentication.
Missing originals must form the oldest prefix and be strictly age-obsolete or
outside the policy's newest-N original/final-new union. Missing intermediate
new rows cannot increase capacity eligibility. Before-insert pruning, backdated
inserts and reinsertion can legitimately fail this conservative endpoint check.
No result proves actual DELETE cause/timing, uninterrupted availability,
capacity acceptance, source authentication or latest recovery state.
captured_us is the header's database clock, not export/file completion time.
"""
import collections
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

TABLES = ('operational_samples', 'operational_alert_history', 'pressure_signal_history', 'dlq_alert_history')
POLICIES = {TABLES[0]: (7, 50000), TABLES[1]: (14, 5000), TABLES[2]: (14, 5000), TABLES[3]: (14, 5000)}
SOURCE_ORDER = {'api': 0, 'host': 1, 'worker': 2}
MAX_BYTES, MAX_ROWS, MAX_LINE = 32 * 1024 * 1024, 100000, 4096
DAY_US, MAX_INTERVAL_US = 86400 * 1000000, 3600 * 1000000
MIN_I64, MAX_I64 = -(2 ** 63), 2 ** 63 - 1
Row = collections.namedtuple('Row', 'pk policy_us row_sha256 order')


class EvidenceError(ValueError):
    pass


def require(condition, code='invalid_monitor_snapshot'):
    if not condition:
        raise EvidenceError(code)


def shape(value, keys):
    require(type(value) is dict and set(value) == set(keys))


def integer_text(value, minimum=MIN_I64, maximum=MAX_I64):
    require(type(value) is str and re.fullmatch(r'0|-?[1-9][0-9]{0,18}', value) is not None)
    number = int(value)
    require(minimum <= number <= maximum)
    return number


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, 'duplicate_monitor_json_key')
        value[key] = item
    return value


def header(value):
    shape(value, ('kind', 'version', 'database', 'schema_version', 'server_version_num', 'postmaster_started_us', 'snapshot_started_us', 'captured_us', 'source_label'))
    require(value['kind'] == 'header' and type(value['version']) is int and value['version'] == 1)
    require(type(value['schema_version']) is int and value['schema_version'] == 22)
    shape(value['database'], ('name', 'oid'))
    name = value['database']['name']
    require(type(name) is str and 1 <= len(name.encode('utf-8')) <= 63 and not any(ord(c) < 32 or ord(c) == 127 for c in name))
    integer_text(value['database']['oid'], 1, 2 ** 32 - 1)
    integer_text(value['server_version_num'], 1, 99999999)
    start = integer_text(value['snapshot_started_us'], 1)
    captured = integer_text(value['captured_us'], 1)
    postmaster = integer_text(value['postmaster_started_us'], 1)
    require(postmaster <= start <= captured, 'invalid_monitor_snapshot_clock')
    require(type(value['source_label']) is str and re.fullmatch(r'[A-Za-z0-9_-]{1,128}', value['source_label']) is not None)
    return value


def row(value):
    shape(value, ('kind', 'table', 'pk', 'policy_us', 'row_sha256'))
    require(value['kind'] == 'row' and type(value['table']) is str and value['table'] in TABLES)
    require(type(value['pk']) is list)
    policy = integer_text(value['policy_us'])
    require(type(value['row_sha256']) is str and re.fullmatch(r'[a-f0-9]{64}', value['row_sha256']) is not None)
    pk = value['pk']
    if value['table'] == TABLES[0]:
        require(len(pk) == 3 and type(pk[0]) is str and pk[0] in SOURCE_ORDER)
        require(type(pk[1]) is str and re.fullmatch(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', pk[1]) is not None)
        require(integer_text(pk[2]) == policy)
        order = (policy, SOURCE_ORDER[pk[0]], pk[1])
    else:
        require(len(pk) == 1)
        order = (policy, integer_text(pk[0], 1))
    return value['table'], Row(tuple(pk), policy, value['row_sha256'], order)


def read_snapshot(path):
    path = Path(path)
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_size <= MAX_BYTES, 'unsafe_monitor_snapshot_file')
    flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
    descriptor = os.open(str(path), flags)
    tables = {name: {} for name in TABLES}
    prefix, complete = hashlib.sha256(), hashlib.sha256()
    metadata = footer = None
    total = size = 0
    current_table = -1
    last_order = None
    with os.fdopen(descriptor, 'rb') as stream:
        opened = os.fstat(stream.fileno())
        require(stat.S_ISREG(opened.st_mode) and (opened.st_dev, opened.st_ino) == (before.st_dev, before.st_ino), 'changed_monitor_snapshot_file')
        while True:
            raw = stream.readline(MAX_LINE + 1)
            if not raw:
                break
            size += len(raw)
            require(size <= MAX_BYTES and len(raw) <= MAX_LINE, 'oversized_monitor_snapshot')
            require(raw.endswith(b'\n') and b'\r' not in raw, 'partial_monitor_snapshot')
            complete.update(raw)
            value = json.loads(raw.decode('utf-8'), object_pairs_hook=unique_object,
                               parse_constant=lambda _: (_ for _ in ()).throw(EvidenceError('invalid_monitor_snapshot')))
            require(type(value) is dict and 'kind' in value)
            if metadata is None:
                metadata = header(value)
                prefix.update(raw)
                continue
            if value['kind'] == 'footer':
                shape(value, ('kind', 'version', 'rows', 'counts', 'prefix_sha256'))
                require(type(value['version']) is int and value['version'] == 1 and type(value['rows']) is int and value['rows'] == total)
                shape(value['counts'], TABLES)
                require(all(type(value['counts'][name]) is int and value['counts'][name] == len(tables[name]) for name in TABLES))
                require(value['prefix_sha256'] == prefix.hexdigest(), 'monitor_snapshot_prefix_hash_mismatch')
                require(not stream.read(1), 'trailing_monitor_snapshot_data')
                footer = value
                break
            table, item = row(value)
            index = TABLES.index(table)
            require(index >= current_table, 'invalid_monitor_snapshot_order')
            if index != current_table:
                current_table, last_order = index, None
            require(last_order is None or item.order > last_order, 'invalid_monitor_snapshot_order')
            require(item.pk not in tables[table], 'duplicate_monitor_snapshot_pk')
            tables[table][item.pk] = item
            last_order = item.order
            total += 1
            require(total <= MAX_ROWS and len(tables[table]) <= POLICIES[table][1], 'monitor_snapshot_inventory_exceeds_policy_cap')
            prefix.update(raw)
        after, current = os.fstat(stream.fileno()), path.lstat()
        identity = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
        require(identity(before) == identity(opened) == identity(after) == identity(current) and stat.S_ISREG(current.st_mode), 'changed_monitor_snapshot_file')
    require(metadata is not None and footer is not None and size == before.st_size, 'partial_monitor_snapshot')
    return {'header': metadata, 'tables': tables, 'sha256': complete.hexdigest(), 'prefix_sha256': prefix.hexdigest(), 'rows': total}


def verify(before, after):
    left, right = before['header'], after['header']
    reasons = []
    for field in ('database', 'schema_version', 'server_version_num', 'postmaster_started_us', 'source_label'):
        if left[field] != right[field]:
            reasons.append('monitor_database_metadata_mismatch:' + field)
    start, end = int(left['snapshot_started_us']), int(right['snapshot_started_us'])
    if not (start < end and int(left['captured_us']) <= end and int(left['captured_us']) < int(right['captured_us']) and int(right['captured_us']) - start <= MAX_INTERVAL_US):
        reasons.append('monitor_snapshot_interval_invalid')
    report = {'version': 1, 'scope': 'monitor_original_rows_end_state_policy',
              'policy_required_originals_preserved': False, 'reasons': reasons, 'tables': {},
              'input_sha256': {'before': before['sha256'], 'after': after['sha256']},
              'source_label_is_authentication': False, 'source_authenticated': False,
              'actual_delete_cause_verified': False, 'actual_delete_timing_verified': False,
              'continuous_availability_verified': False, 'intermediate_new_rows_complete_verified': False,
              'conservative_endpoint_check': True, 'legitimate_backdated_or_reinserted_rows_may_be_rejected': True,
              'capacity_verified': False, 'latest_state_verified': False, 'recovery_verified': False}
    if reasons:
        return report
    report['interval_us'] = str(end - start)
    for table in TABLES:
        original, final = before['tables'][table], after['tables'][table]
        days, limit = POLICIES[table]
        cutoff = end - days * DAY_US
        union = dict(original)
        union.update((pk, item) for pk, item in final.items() if pk not in original)
        active = sorted((item for item in union.values() if item.policy_us >= cutoff), key=lambda item: item.order, reverse=True)
        required = {item.pk for item in active[:limit]}
        retained = changed = missing = expired = capacity = unexplained = 0
        prefix_ok, saw_retained = True, False
        for pk, item in original.items():
            if pk in final:
                retained += 1
                saw_retained = True
                found = final[pk]
                if found.row_sha256 != item.row_sha256 or found.policy_us != item.policy_us or found.order != item.order:
                    changed += 1
            else:
                missing += 1
                if saw_retained:
                    prefix_ok = False
                if item.policy_us < cutoff:
                    expired += 1
                elif pk not in required:
                    capacity += 1
                else:
                    unexplained += 1
        if changed:
            reasons.append('retained_original_changed:' + table)
        if not prefix_ok:
            reasons.append('missing_originals_not_oldest_prefix:' + table)
        if unexplained:
            reasons.append('policy_required_original_missing:' + table)
        report['tables'][table] = {'original_rows': len(original), 'final_rows': len(final),
                                  'retained_original_rows': retained, 'changed_original_rows': changed,
                                  'missing_original_rows': missing, 'missing_age_eligible': expired,
                                  'missing_capacity_eligible': capacity, 'unexplained_missing': unexplained,
                                  'missing_originals_oldest_prefix': prefix_ok, 'cutoff_us': str(cutoff),
                                  'retention_days': days, 'row_cap': limit}
    report['policy_required_originals_preserved'] = not reasons
    return report


def main(args):
    if len(args) != 4 or args[0] != 'verify':
        raise EvidenceError('usage_monitor_preservation_verify_before_after_new_report')
    before, after = read_snapshot(args[1]), read_snapshot(args[2])
    result = verify(before, after)
    descriptor = os.open(args[3], os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, 'w', encoding='utf-8', newline='\n') as output:
        json.dump(result, output, sort_keys=True, separators=(',', ':'))
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())
    print(json.dumps(result, sort_keys=True))
    return 0 if result['policy_required_originals_preserved'] else 1


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv[1:]))
    except EvidenceError as error:
        print(json.dumps({'code': str(error)}), file=sys.stderr)
        sys.exit(1)
    except (OSError, UnicodeError, ValueError, TypeError, OverflowError, RecursionError):
        print(json.dumps({'code': 'invalid_monitor_preservation_evidence'}), file=sys.stderr)
        sys.exit(1)
