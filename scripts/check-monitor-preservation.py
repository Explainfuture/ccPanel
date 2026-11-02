#!/usr/bin/env python3
"""Independent descriptor/counterexample fixtures for the offline v1 verifier.

These synthetic descriptor hashes are not a substitute for the real PG capture
suite; they test interpretation of complete, hashed JSONL evidence and its CLI.
"""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile

HELPER = Path(__file__).with_name('monitor-preservation.py').resolve()
spec = importlib.util.spec_from_file_location('monitor_preservation', HELPER)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
TABLES = ('operational_samples', 'operational_alert_history', 'pressure_signal_history', 'dlq_alert_history')
DAY = 86400 * 1000000
START = 20 * DAY
END = START + 2100 * 1000000
checks = []


def digest(label):
    return hashlib.sha256(label.encode()).hexdigest()


def header(at, **changes):
    value = {'kind': 'header', 'version': 1, 'database': {'name': 'fixture', 'oid': '16384'},
             'schema_version': 22, 'server_version_num': '170007', 'postmaster_started_us': '1',
             'snapshot_started_us': str(at), 'captured_us': str(at + 10000), 'source_label': 'synthetic_fixture'}
    value.update(changes)
    return value


def history(table, identity, policy, label=None):
    return {'kind': 'row', 'table': table, 'pk': [str(identity)], 'policy_us': str(policy),
            'row_sha256': digest(label or table + ':' + str(identity))}


def sample(identity, policy, source='api', label=None):
    return {'kind': 'row', 'table': TABLES[0], 'pk': [source, '00000000-0000-0000-0000-%012x' % identity, str(policy)],
            'policy_us': str(policy), 'row_sha256': digest(label or source + ':' + str(identity))}


def line(value):
    return (json.dumps(value, separators=(',', ':')) + '\n').encode()


def wire(meta, rows):
    prefix = line(meta) + b''.join(item if isinstance(item, bytes) else line(item) for item in rows)
    counts = {name: 0 for name in TABLES}
    for item in rows:
        value = json.loads(item) if isinstance(item, bytes) else item
        counts[value['table']] += 1
    footer = {'kind': 'footer', 'version': 1, 'rows': len(rows), 'counts': counts,
              'prefix_sha256': hashlib.sha256(prefix).hexdigest()}
    return prefix + line(footer)


with tempfile.TemporaryDirectory(prefix='ccpanel-monitor-preservation-') as temporary:
    root = Path(temporary)
    counter = 0

    def snapshot(meta, rows):
        global counter
        counter += 1
        path = root / ('snapshot-%d.jsonl' % counter)
        path.write_bytes(wire(meta, rows))
        return module.read_snapshot(path), path

    def compare(original, final, left=None, right=None):
        before, before_path = snapshot(left or header(START), original)
        after, after_path = snapshot(right or header(END), final)
        return module.verify(before, after), before_path, after_path

    def invalid(name, data):
        path = root / ('invalid-' + name + '.jsonl')
        path.write_bytes(data)
        try:
            module.read_snapshot(path)
            raise RuntimeError('invalid_snapshot_accepted:' + name)
        except (module.EvidenceError, ValueError, UnicodeError):
            pass
        checks.append(name)

    original = [sample(1, START)] + [history(name, 1, START) for name in TABLES[1:]]
    report, before_path, after_path = compare(original, original)
    assert report['policy_required_originals_preserved'] is True
    assert report['input_sha256']['before'] == hashlib.sha256(before_path.read_bytes()).hexdigest()
    assert all(report[name] is False for name in ('source_authenticated', 'actual_delete_cause_verified', 'actual_delete_timing_verified', 'continuous_availability_verified', 'intermediate_new_rows_complete_verified', 'capacity_verified', 'latest_state_verified', 'recovery_verified'))
    assert 'preserved' not in report
    checks.append('all_four_unchanged_scoped_only')

    cutoff = END - 7 * DAY
    for offset, passed in ((-1, True), (0, False), (1, False)):
        result, _, _ = compare([sample(1, cutoff + offset)], [])
        assert result['policy_required_originals_preserved'] is passed
        if offset == 0:
            assert result['tables'][TABLES[0]]['unexplained_missing'] == 1
    # captured_us is 10ms after the actual final snapshot start. It must not
    # expand the retention cutoff and license deletion at the exact boundary.
    checks.append('strict_age_cutoff_at_final_snapshot_start')

    old = sample(1, cutoff - 3)
    middle = sample(2, cutoff - 2)
    fresh = sample(3, START)
    result, _, _ = compare([old, middle, fresh], [old, fresh])
    assert not result['policy_required_originals_preserved']
    assert 'missing_originals_not_oldest_prefix:' + TABLES[0] in result['reasons']
    # The same endpoint can arise after an old key was removed and reinserted.
    # Endpoint evidence must not invent that intermediate sequence to pass.
    checks.append('expired_middle_hole_or_reinsertion_fails_closed')
    changed = {**old, 'row_sha256': digest('changed-received-at-or-payload')}
    result, _, _ = compare([old, fresh], [changed, fresh])
    assert result['tables'][TABLES[0]]['changed_original_rows'] == 1
    assert not result['policy_required_originals_preserved']
    checks.append('eligible_but_retained_original_hash_still_checked')

    large = [history(TABLES[1], n, START - 1000000 + n) for n in range(1, 5001)]
    new = history(TABLES[1], 5001, START + 1)
    result, large_before, large_after = compare(large, large[1:] + [new])
    assert result['policy_required_originals_preserved']
    assert result['tables'][TABLES[1]]['missing_capacity_eligible'] == 1
    assert result['tables'][TABLES[1]]['row_cap'] == 5000
    checks.append('actual_5000_row_cap_union')
    # A prior unseen insertion is not an available witness. With that row
    # absent from final evidence, the original loss cannot be justified.
    result, _, _ = compare(large, large[1:])
    assert not result['policy_required_originals_preserved']
    assert result['tables'][TABLES[1]]['unexplained_missing'] == 1
    checks.append('omitted_intermediate_new_row_is_not_invented')
    # Production prunes before INSERT: it may remove the current oldest
    # original, then insert an even older item. This legitimate backward
    # insertion need not equal the endpoint union's newest5000: reject it.
    backward = history(TABLES[1], 5001, int(large[0]['policy_us']) - 1)
    result, _, _ = compare(large, [backward] + large[1:])
    assert not result['policy_required_originals_preserved']
    assert result['tables'][TABLES[1]]['missing_originals_oldest_prefix']
    assert result['legitimate_backdated_or_reinserted_rows_may_be_rejected'] is True
    checks.append('before_insert_backward_timestamp_conservative_false_fail')

    # Even a row outside the union's protected set must be hashed if retained.
    changed_first = {**large[0], 'row_sha256': digest('changed-cap-eligible-present')}
    result, _, _ = compare(large, [changed_first] + large[2:] + [new])
    assert not result['policy_required_originals_preserved']
    assert result['tables'][TABLES[1]]['changed_original_rows'] == 1
    checks.append('capacity_eligible_present_hash_not_skipped')
    result, _, _ = compare([history(TABLES[1], 1, START), history(TABLES[1], 2, START + 1)], [history(TABLES[1], 2, START + 1)])
    assert not result['policy_required_originals_preserved']
    checks.append('noneligible_oldest_loss_rejected')

    tied = [history(TABLES[1], 9, START), history(TABLES[1], 10, START), history(TABLES[1], 9007199254740993, START), history(TABLES[1], 9223372036854775807, START)]
    assert compare(tied, tied)[0]['policy_required_originals_preserved']
    same_time = [sample(1, START, 'api'), sample(2, START, 'api'), sample(1, START, 'host'), sample(1, START, 'worker')]
    assert compare(same_time, same_time)[0]['policy_required_originals_preserved']
    ancient = sample(1, -(2 ** 63))
    assert compare([ancient], [])[0]['policy_required_originals_preserved']
    checks.append('exact_timestamp_source_uuid_and_int64_order')
    invalid('id_numeric_order', wire(header(START), [tied[1], tied[0]]))
    invalid('source_tie_order', wire(header(START), [same_time[2], same_time[0]]))
    invalid('timestamp_pk_mismatch', wire(header(START), [{**same_time[0], 'policy_us': str(START + 1)}]))
    invalid('duplicate_pk_with_different_time', wire(header(START), [tied[0], {**tied[0], 'policy_us': str(START + 1)}]))
    invalid('int64_overflow', wire(header(START), [history(TABLES[1], 9223372036854775808, START)]))
    invalid('noncanonical_id', wire(header(START), [history(TABLES[1], '01', START)]))
    invalid('noncanonical_epoch', wire(header(START), [{**same_time[0], 'policy_us': '-0'}]))
    invalid('unknown_row_field', wire(header(START), [{**same_time[0], 'payload': 'must-not-export'}]))
    invalid('unknown_header_field', wire({**header(START), 'authenticated': True}, []))
    invalid('policy_cap_overflow', wire(header(START), large + [new]))
    invalid('table_order', wire(header(START), [history(TABLES[2], 1, START), history(TABLES[1], 1, START)]))
    duplicate = line(original[0]).replace(b'"kind":"row"', b'"kind":"row","kind":"row"')
    invalid('duplicate_json_key', wire(header(START), [duplicate]))
    good_wire = wire(header(START), original)
    invalid('partial_footer', good_wire[:-1])
    invalid('missing_footer', line(header(START)) + line(original[0]))
    invalid('trailing_line', good_wire + b'\n')
    invalid('raw_prefix_hash_mismatch', good_wire.replace(b'synthetic_fixture', b'synthetic_CHANGED'))
    invalid('footer_count_mismatch', good_wire.replace(b'"rows":4', b'"rows":5'))
    invalid('oversized_line', b' ' * 4097 + b'\n')
    invalid('crlf', good_wire.replace(b'\n', b'\r\n'))
    oversized = root / 'oversized.jsonl'
    with oversized.open('wb') as file:
        file.truncate(32 * 1024 * 1024 + 1)
    try:
        module.read_snapshot(oversized)
        raise RuntimeError('oversized_snapshot_accepted')
    except module.EvidenceError:
        checks.append('bounded_file_size')

    for field, change in (
        ('database', {'name': 'other', 'oid': '16384'}), ('database', {'name': 'fixture', 'oid': '16385'}),
        ('server_version_num', '170008'), ('postmaster_started_us', '2'),
        ('snapshot_started_us', str(START)), ('captured_us', str(START + 3600 * 1000000 + 1)),
    ):
        right = header(END, **{field: change})
        result, _, _ = compare([], [], right=right)
        assert not result['policy_required_originals_preserved'] and not result['tables']
    checks.append('database_identity_restart_and_monotonic_bounded_interval')
    mixed, _, _ = compare([], [], right=header(END, source_label='another_window'))
    assert mixed['policy_required_originals_preserved'] is False and not mixed['tables']
    assert 'monitor_database_metadata_mismatch:source_label' in mixed['reasons']
    assert mixed['source_label_is_authentication'] is False
    checks.append('same_caller_label_required_without_authentication_claim')
    invalid('schema_version', wire(header(START, schema_version=23), []))
    invalid('clock_goes_backward_within_header', wire(header(START, captured_us=str(START - 1)), []))

    output = root / 'report.json'
    command = [sys.executable, str(HELPER), 'verify', str(large_before), str(large_after), str(output)]
    cli = subprocess.run(command, capture_output=True, text=True)
    assert cli.returncode == 0 and not cli.stderr and json.loads(cli.stdout)['policy_required_originals_preserved'] is True
    saved = output.read_bytes()
    collision = subprocess.run(command, capture_output=True, text=True)
    assert collision.returncode == 1 and output.read_bytes() == saved
    invalid_output = root / 'must-not-exist.json'
    rejected = subprocess.run([sys.executable, str(HELPER), 'verify', str(root / 'invalid-partial_footer.jsonl'), str(large_after), str(invalid_output)], capture_output=True, text=True)
    assert rejected.returncode == 1 and not invalid_output.exists() and not rejected.stdout
    assert str(root) not in rejected.stderr
    checks.append('real_cli_exclusive_report_and_reject_before_output')
    print(json.dumps({'passed': True, 'synthetic_descriptor_fixtures': True, 'checks': checks,
                      'policy_scope_only': True, 'actual_delete_cause_verified': False,
                      'capacity_verified': False, 'actual_pg_capture_verified': False}))
