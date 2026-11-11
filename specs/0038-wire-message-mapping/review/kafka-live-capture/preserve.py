from pathlib import Path
import hashlib
import json
import re
import shutil
import xml.etree.ElementTree as E

OUT = Path(__file__).parent
ROOT = Path('/Users/gabisonia/RiderProjects/Brighter')
REVIEW = ROOT / 'specs/0038-wire-message-mapping/review/kafka-live-capture'
PREVIOUS = Path('/private/tmp/brighter-4170-kafka-native-close-diagnostic')
REVIEW.mkdir(exist_ok=True)

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

trx = PREVIOUS / 'observed-full-net9.0.trx'
tree = E.parse(trx)
native = tree.findtext('.//{*}ResultSummary/{*}Output/{*}StdOut')
assert native
cases = []
for row in tree.findall('.//{*}UnitTestResult'):
    if 'KafkaMessageConsumerCommitRevokeConcurrency' not in row.attrib['testName']:
        continue
    output = row.findtext('.//{*}StdOut')
    match = re.search(r'NATIVE CLOSE DIAGNOSTIC strategy=(\S+) topic=(\S+) group=(\S+)', output)
    assert match
    strategy, topic, group = match.groups()
    clients = sorted(set(re.findall(r'%\d\|[^|]+\|[^|]+\|([^|]+)\|[^\n]*' + re.escape(group), native)))
    assert len(clients) == 2
    record = {'strategy': strategy, 'topic': topic, 'group': group, 'outcome': row.attrib['outcome'], 'consumers': []}
    for client in clients:
        lines = [line for line in native.splitlines() if '|' + client + '|' in line]
        starts = [line for line in lines if '|CLOSE|' in line and line.endswith('Closing consumer')]
        ends = [line for line in lines if '|CLOSE|' in line and line.endswith('Consumer closed')]
        assert len(starts) == 1 and len(ends) == 1
        duration_ms = (float(ends[0].split('|')[1]) - float(starts[0].split('|')[1])) * 1000
        safe = client.split('#')[-1]
        name = f'healthy-net9-{strategy}-{safe}.txt'
        (REVIEW / name).write_text('\n'.join(lines) + '\n')
        record['consumers'].append({'name': client, 'log': name, 'line_count': len(lines),
                                    'close_duration_ms_at_log_resolution': round(duration_ms, 3)})
    cases.append(record)
assert len(cases) == 2
report_path = OUT / 'validation/healthy-capture.crashreport.json'
threads = json.loads(report_path.read_text())['payload']['threads']
native_threads = [thread for thread in threads if any('rdkafka' in frame.get('native_module', '') for frame in thread.get('stack_frames', []))]
assert len(native_threads) > 0
summary = {'original_failure_artifacts': {'directory': '/private/tmp/brighter-4170-kafka-requeue-cancellation-recovery',
                                         'directory_present': Path('/private/tmp/brighter-4170-kafka-requeue-cancellation-recovery').exists(),
                                         'raw_stack_recovered': False},
           'prior_healthy_trx_sha256': sha(trx), 'prior_healthy_cases': cases,
           'live_capture_validation': json.loads((OUT / 'validation.json').read_text()),
           'healthy_stack_report': {'sha256': sha(report_path), 'threads': len(threads), 'threads_with_librdkafka_frames': len(native_threads),
                                    'internal_librdkafka_function_symbols_present': any(frame.get('unmanaged_name') for thread in native_threads for frame in thread['stack_frames'] if 'rdkafka' in frame.get('native_module', ''))},
           'source_sha256': {name: sha(OUT / name) for name in ['StartupHook.cs', 'NativeCapture.csproj', 'validate.py']},
           'validated_source_sha256': sha(OUT / 'StartupHook.validated.txt'),
           'validated_binary_sha256': sha(OUT / 'validated-NativeCapture.dll'),
           'post_validation_source_change': 'License header added and one explanatory comment moved; executable statements unchanged.',
           'final_diagnostic_binary_sha256': sha(OUT / 'bin/Release/net9.0/NativeCapture.dll')}
for name in ['StartupHook.cs', 'StartupHook.validated.txt', 'NativeCapture.csproj', 'validate.py', 'preserve.py', 'validation.json']:
    shutil.copy2(OUT / name, REVIEW / name)
for name in ['native-stderr-54.txt', 'healthy-capture.crashreport.json', 'createdump.txt', 'runner.txt', 'testhost-54.json', 'validation.trx', 'stop.txt']:
    shutil.copy2(OUT / 'validation' / name, REVIEW / name)
(REVIEW / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary, indent=2))
