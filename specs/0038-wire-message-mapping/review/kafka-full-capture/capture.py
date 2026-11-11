from pathlib import Path
import json
import re
import subprocess

class CloseWatch:
    def __init__(self):
        self.pending = {}
        self.completed = 0

    def feed(self, line, now):
        match = re.match(r'%\d\|[^|]+\|CLOSE\|([^|]+)\|.*: (Closing consumer|Consumer closed)$', line)
        if not match:
            return
        client, event = match.groups()
        if event == 'Closing consumer':
            self.pending.setdefault(client, now)
        else:
            self.pending.pop(client, None)
            self.completed += 1

    def overdue(self, now):
        return {client: now - started for client, started in self.pending.items() if now - started >= 60}

def collect(container, host, output, label):
    pid = int(host['pid'])
    assert pid > 1
    metadata_script = f'''cat /proc/{pid}/maps
for task in /proc/{pid}/task/*; do
    printf '\\nTASK %s\\n' "$task"
    cat "$task/comm" "$task/status" "$task/wchan"
    cat "$task/stack" 2>/dev/null || true
done'''
    with (output / (label + '-proc.txt')).open('w') as log:
        metadata = subprocess.run(['docker', 'exec', container, 'sh', '-c', metadata_script], stdout=log, stderr=subprocess.STDOUT, timeout=30).returncode
    with (output / (label + '-createdump.txt')).open('w') as log:
        dump = subprocess.run(['docker', 'exec', container, host['runtime'] + 'createdump', '--full', '--crashreport',
                               '--name', f'/capture/{output.name}/{label}.core', str(pid)], stdout=log, stderr=subprocess.STDOUT, timeout=150).returncode
    core = output / (label + '.core')
    report = output / (label + '.core.crashreport.json')
    result = {'label': label, 'proc_exit': metadata, 'dump_exit': dump, 'core_bytes': core.stat().st_size if core.exists() else 0,
              'elf_core': core.exists() and core.open('rb').read(4) == b'\x7fELF', 'report_exists': report.exists()}
    if report.exists():
        threads = json.loads(report.read_text())['payload']['threads']
        result['threads'] = len(threads)
        result['librdkafka_threads'] = sum(any('rdkafka' in frame.get('native_module', '') for frame in thread.get('stack_frames', [])) for thread in threads)
    (output / (label + '-capture.json')).write_text(json.dumps(result, indent=2) + '\n')
    return result

if __name__ == '__main__':
    previous = Path('/Users/gabisonia/RiderProjects/Brighter/specs/0038-wire-message-mapping/review/kafka-live-capture')
    watch = CloseWatch()
    logs = list(previous.glob('healthy-net9-*.txt'))
    assert len(logs) == 4
    for path in logs:
        for line in path.read_text().splitlines():
            if line.startswith('%'):
                watch.feed(line, float(line.split('|')[1]))
    assert watch.completed == 4 and not watch.pending
    sample = '%7|0|CLOSE|diagnostic-control| [thrd:app]: '
    watch.feed(sample + 'Closing consumer', 0)
    assert not watch.overdue(59.9)
    assert watch.overdue(60) == {'diagnostic-control': 60}
    watch.feed(sample + 'Consumer closed', 61)
    assert not watch.overdue(120)
    print('Four healthy close sequences and the 60-second diagnostic trigger verified; no broker or product test run.')
