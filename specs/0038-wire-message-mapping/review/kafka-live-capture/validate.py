from pathlib import Path
import hashlib
import json
import os
import subprocess
import time
import xml.etree.ElementTree as E

OUT = Path(__file__).parent
ROOT = Path('/Users/gabisonia/RiderProjects/Brighter')
WORK = Path('/private/tmp/brighter-4170-rmq-scheduled-selection/workspace')
PREVIOUS = Path('/private/tmp/brighter-4170-kafka-native-close-diagnostic')
BROKER = 'brighter-4170-kafka-default-body-ownership'
TEST = 'brighter-4170-kafka-live-capture-validation'
ARTIFACTS = OUT / 'validation'
ARTIFACTS.mkdir(exist_ok=False)

def run(args, name, timeout=90):
    with (ARTIFACTS / (name + '.txt')).open('w') as log:
        return subprocess.run(args, stdout=log, stderr=subprocess.STDOUT, timeout=timeout).returncode

def snapshot():
    names = subprocess.check_output(['rg', '--files', 'src', 'tests', '-g', '*.cs'], cwd=ROOT, text=True).splitlines()
    names += ['Directory.Packages.props', 'Directory.Build.props']
    return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in names}

before = snapshot()
inhibitor = subprocess.Popen(['caffeinate', '-i', '-s', '-w', str(os.getpid())])
started = False
process = None
summary = {'purpose': 'Validate durable native stderr and nonterminating native/managed capture, not reproduce or clear the historical stall.'}
try:
    for name in [BROKER, BROKER + '-schema']:
        state = subprocess.check_output(['docker', 'inspect', name, '--format', '{{.State.Running}}'], text=True).strip()
        assert state == 'false', 'Do not take over a running fixture'
    assert run(['docker', 'start', BROKER, BROKER + '-schema'], 'start') == 0
    started = True
    for _ in range(60):
        ready = subprocess.run(['docker', 'exec', BROKER, '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092', '--list'], capture_output=True, timeout=20)
        if ready.returncode == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError('Broker readiness failed')
    args = ['docker', 'run', '--rm', '--pull', 'never', '--cap-add', 'SYS_PTRACE', '--ulimit', 'core=0', '--name', TEST,
            '--network', 'container:' + BROKER, '-v', f'{PREVIOUS}/bin:/tests:ro', '-v', f'{OUT}:/capture',
            '-v', f'{WORK}:/workspace:ro', '-w', '/workspace',
            '-e', 'DOTNET_STARTUP_HOOKS=/capture/bin/Release/net9.0/NativeCapture.dll',
            '-e', 'BRIGHTER_NATIVE_CAPTURE=/capture/validation', 'mcr.microsoft.com/dotnet/sdk:10.0',
            'dotnet', 'vstest', '/tests/Release/net10.0/Paramore.Brighter.Kafka.Tests.dll',
            '--TestCaseFilter:FullyQualifiedName~KafkaMessageConsumerCommitRevokeConcurrency',
            '--logger:trx;LogFileName=validation.trx', '--ResultsDirectory:/capture/validation',
            '--Blame:CollectHangDump;TestTimeout=5m;HangDumpType=full']
    with (ARTIFACTS / 'runner.txt').open('w') as log:
        process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            metadata = list(ARTIFACTS.glob('testhost-*.json'))
            logs = list(ARTIFACTS.glob('native-stderr-*.txt'))
            if metadata and logs and '|CGRP' in logs[0].read_text() and process.poll() is None:
                break
            if process.poll() is not None:
                raise RuntimeError('Test host ended before live native capture was observed')
            time.sleep(0.1)
        else:
            raise RuntimeError('No live native capture within readiness budget')
        assert len(metadata) == 1 and len(logs) == 1
        host = json.loads(metadata[0].read_text())
        summary['live_capture'] = {'pid': host['pid'], 'bytes_before_test_completion': logs[0].stat().st_size,
                                   'runner_still_running': process.poll() is None}
        # A healthy capture validates collection only. It is never labelled a hang.
        summary['createdump_exit'] = run(['docker', 'exec', TEST, host['runtime'] + 'createdump', '--crashreportonly',
                                         '--name', '/capture/validation/healthy-capture', str(host['pid'])], 'createdump')
        summary['exit'] = process.wait(timeout=420)
    report = ARTIFACTS / 'healthy-capture.crashreport.json'
    summary['crashreport_exists'] = report.exists()
    rows = E.parse(ARTIFACTS / 'validation.trx').findall('.//{*}UnitTestResult')
    summary['cases'] = [{key: row.attrib.get(key) for key in ['testName', 'outcome', 'duration']} for row in rows]
    assert summary['createdump_exit'] == 0 and report.exists()
    assert summary['exit'] == 0 and len(rows) == 2 and all(row.attrib['outcome'] == 'Passed' for row in rows)
finally:
    if process is not None and process.poll() is None:
        run(['docker', 'stop', TEST], 'stop-test')
        process.wait(timeout=30)
    if started:
        run(['docker', 'logs', BROKER], 'broker')
        summary['fixture_stop_exit'] = run(['docker', 'stop', BROKER + '-schema', BROKER], 'stop')
    inhibitor.terminate()
    inhibitor.wait(timeout=10)
    after = snapshot()
    summary['input_count'] = len(before)
    summary['unchanged_inputs'] = before == after
    (OUT / 'validation.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2), flush=True)
    assert before == after
