from pathlib import Path
from collections import Counter
import hashlib
import json
import os
import subprocess
import sys
import time
import xml.etree.ElementTree as E
from capture import CloseWatch, collect

ROOT = Path('/Users/gabisonia/RiderProjects/Brighter')
BASE = Path(__file__).parent
PRIOR = Path('/private/tmp/brighter-4170-kafka-native-close-diagnostic')
HOOK = Path('/private/tmp/brighter-4170-kafka-live-capture/bin/Release/net9.0')
WORK = Path('/private/tmp/brighter-4170-rmq-scheduled-selection/workspace')
BROKER = 'brighter-4170-kafka-default-body-ownership'
MODE = sys.argv[1]
assert MODE in ('validate', 'full')
OUT = BASE / MODE
OUT.mkdir(exist_ok=False)
CONTAINER = 'brighter-4170-kafka-full-capture-' + MODE

def save(name, data):
    (OUT / (name + '.json')).write_text(json.dumps(data, indent=2) + '\n')

def command(args, label, timeout=90):
    with (OUT / (label + '.txt')).open('w') as log:
        return subprocess.run(args, stdout=log, stderr=subprocess.STDOUT, timeout=timeout).returncode

def manifest():
    paths = subprocess.check_output(['rg', '--files', 'src', 'tests', '-g', '*.cs'], cwd=ROOT, text=True).splitlines()
    paths += ['Directory.Build.props', 'Directory.Packages.props']
    return {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in paths}

before = manifest()
save('source-manifest', before)
binaries = {str(path.relative_to(PRIOR)): hashlib.sha256(path.read_bytes()).hexdigest() for path in (PRIOR / 'bin/Release/net9.0').rglob('*') if path.is_file()}
save('binaries', binaries)
inhibitor = subprocess.Popen(['caffeinate', '-i', '-s', '-w', str(os.getpid())])
summary = {'mode': MODE, 'captures': []}
process = None
started = False
watch = CloseWatch()
try:
    if MODE == 'full':
        validated = json.loads((BASE / 'validate/summary.json').read_text())
        assert validated['exit'] == 0 and validated['captures'][0]['elf_core'] and validated['captures'][0]['librdkafka_threads'] > 0
    for name in [BROKER, BROKER + '-schema']:
        assert subprocess.check_output(['docker', 'inspect', name, '--format', '{{.State.Running}}'], text=True).strip() == 'false'
    started = True
    assert command(['docker', 'start', BROKER, BROKER + '-schema'], 'start') == 0
    for _ in range(60):
        check = subprocess.run(['docker', 'exec', BROKER, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092', '--list'], capture_output=True, timeout=20)
        if check.returncode == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError('Broker readiness failed')
    args = ['docker', 'run', '--rm', '--pull', 'never', '--cap-add', 'SYS_PTRACE', '--ulimit', 'core=0', '--name', CONTAINER,
            '--network', 'container:' + BROKER, '-v', f'{BASE}:/capture', '-v', f'{HOOK}:/hook:ro',
            '-v', f'{PRIOR}/bin:/tests:ro', '-v', f'{WORK}:/workspace:ro', '-w', '/workspace',
            '-e', 'DOTNET_STARTUP_HOOKS=/hook/NativeCapture.dll', '-e', f'BRIGHTER_NATIVE_CAPTURE=/capture/{MODE}',
            'mcr.microsoft.com/dotnet/sdk:9.0', 'dotnet', 'vstest', '/tests/Release/net9.0/Paramore.Brighter.Kafka.Tests.dll',
            '--logger:trx;LogFileName=results.trx', f'--ResultsDirectory:/capture/{MODE}',
            '--Blame:CollectHangDump;TestTimeout=5m;HangDumpType=full']
    if MODE == 'validate':
        args += ['--TestCaseFilter:FullyQualifiedName~KafkaMessageConsumerCommitRevokeConcurrency']
    wall, mono = time.time(), time.monotonic()
    with (OUT / 'runner.txt').open('w') as log:
        process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
        offset = 0
        next_progress = mono + 30
        while process.poll() is None:
            now = time.monotonic()
            assert now - mono < 1800, 'Thirty-minute fixed diagnostic process budget exceeded'
            metadata = list(OUT.glob('testhost-*.json'))
            native = list(OUT.glob('native-stderr-*.txt'))
            if metadata and native:
                assert len(metadata) == 1 and len(native) == 1
                with native[0].open() as stream:
                    stream.seek(offset)
                    for line in stream:
                        watch.feed(line.rstrip('\n'), now)
                    offset = stream.tell()
                trigger = MODE == 'validate' and '|CGRP' in native[0].read_text()
                overdue = watch.overdue(now)
                if not summary['captures'] and (trigger or overdue):
                    host = json.loads(metadata[0].read_text())
                    reason = 'healthy-validation' if MODE == 'validate' else 'close-over-60-seconds'
                    save('capture-trigger', {'reason': reason, 'overdue': overdue, 'elapsed_seconds': now - mono})
                    print('Capturing:', reason, flush=True)
                    result = collect(CONTAINER, host, OUT, reason)
                    summary['captures'].append(result)
                    save('capture-results', summary['captures'])
                    assert result['dump_exit'] == 0 and result['elf_core'] and result['report_exists'], 'Native memory capture failed'
            if now >= next_progress:
                print(json.dumps({'mode': MODE, 'elapsed_seconds': round(now - mono), 'native_bytes_read': offset,
                                  'completed_native_closes': watch.completed, 'pending_closes': list(watch.pending)}), flush=True)
                next_progress = now + 30
            time.sleep(0.25)
        summary['exit'] = process.returncode
    summary.update(wall_seconds=time.time() - wall, monotonic_seconds=time.monotonic() - mono,
                   completed_native_closes=watch.completed, pending_closes=watch.pending)
    summary['clock_discontinuity'] = abs(summary['wall_seconds'] - summary['monotonic_seconds']) > 5
    rows = E.parse(OUT / 'results.trx').findall('.//{*}UnitTestResult') if (OUT / 'results.trx').exists() else []
    summary['counts'] = dict(Counter(row.attrib['outcome'] for row in rows))
    summary['failures'] = [{'name': row.attrib['testName'], 'message': row.findtext('.//{*}Message')} for row in rows if row.attrib['outcome'] != 'Passed']
    if MODE == 'full':
        baseline = E.parse(PRIOR / 'observed-full-net9.0.trx').findall('.//{*}UnitTestResult')
        summary['identities_match_prior_412'] = Counter(row.attrib['testName'] for row in rows) == Counter(row.attrib['testName'] for row in baseline)
    else:
        assert len(rows) == 2 and summary['counts'] == {'Passed': 2}
        assert len(summary['captures']) == 1 and summary['captures'][0]['librdkafka_threads'] > 0
finally:
    if process is not None and process.poll() is None:
        command(['docker', 'stop', CONTAINER], 'stop-test')
        process.wait(timeout=30)
    if started:
        command(['docker', 'logs', BROKER], 'broker')
        summary['fixture_stop_exit'] = command(['docker', 'stop', BROKER + '-schema', BROKER], 'stop')
    inhibitor.terminate()
    inhibitor.wait(timeout=10)
    summary['input_count'] = len(before)
    summary['unchanged_inputs'] = before == manifest()
    save('summary', summary)
    print(json.dumps(summary), flush=True)
    assert summary['unchanged_inputs']
