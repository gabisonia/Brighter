from pathlib import Path
import hashlib
import json
import os
import subprocess
import time

ROOT = Path('/Users/gabisonia/RiderProjects/Brighter')
OUT = Path(__file__).parent
SOURCE = ROOT / 'specs/0038-wire-message-mapping/review/gcp-ordered-nack-protocol-control'
IMAGES = {
    'baseline': 'gcr.io/google.com/cloudsdktool/cloud-sdk@sha256:3797a874fae6b75aa25260b5bd40fa0397c2f1102f18ae3ac7dc887732fbe449',
    'candidate': 'gcr.io/google.com/cloudsdktool/google-cloud-cli@sha256:faff8f5e8a44e70df68a2a916cefa187cac9e132c25350a8a0f27f36fd586676',
}

def save(name, data):
    (OUT / (name + '.json')).write_text(json.dumps(data, indent=2) + '\n')

def command(args, label, timeout=600):
    with (OUT / (label + '.log')).open('w') as log:
        result = subprocess.run(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
    return result.returncode

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def snapshot():
    paths = subprocess.check_output(['rg', '--files', 'src', 'tests', '-g', '*.cs'], cwd=ROOT, text=True).splitlines()
    paths += ['Directory.Build.props', 'Directory.Packages.props']
    paths += [str(path.relative_to(ROOT)) for path in SOURCE.glob('*') if path.suffix in ('.cs', '.csproj')]
    return {path: digest(ROOT / path) for path in sorted(paths)}

before = snapshot()
save('inputs-before', before)
assert all((OUT / f'bin/Release/{framework}/ProtocolControl.dll').exists() for framework in ['net9.0', 'net10.0'])
save('binaries', {str(path.relative_to(OUT)): digest(path) for path in sorted((OUT / 'bin').rglob('*')) if path.is_file()})
inhibitor = subprocess.Popen(['caffeinate', '-i', '-s', '-w', str(os.getpid())])
observations = []
try:
    for variant, image in IMAGES.items():
        assert command(['docker', 'image', 'inspect', image, '--format', '{{json .}}'], variant + '-image') == 0
        for framework in ['net9.0', 'net10.0']:
            label = variant + '-' + framework
            broker = 'brighter-4170-gcp-version-' + label.replace('.', '-')
            probe = broker + '-probe'
            exists = subprocess.run(['docker', 'inspect', broker], capture_output=True)
            assert exists.returncode != 0, 'Fresh owned fixture required: ' + broker
            started = False
            try:
                assert command(['docker', 'run', '-d', '--name', broker, '--memory', '2g', '--cpus', '2', '--pull', 'never', image,
                                'gcloud', 'beta', 'emulators', 'pubsub', 'start', '--host-port=0.0.0.0:8085', '--project=brighter-test'], label + '-start') == 0
                started = True
                python = 'python3' if variant == 'baseline' else '/google-cloud-sdk/platform/bundledpythonunix/bin/python3'
                readiness = "import urllib.request; assert urllib.request.urlopen('http://localhost:8085/v1/projects/brighter-test/topics', timeout=2).status == 200"
                for attempt in range(60):
                    ready = subprocess.run(['docker', 'exec', broker, python, '-c', readiness], capture_output=True, timeout=10)
                    if ready.returncode == 0:
                        break
                    time.sleep(1)
                else:
                    raise RuntimeError('Native emulator readiness failed: ' + ready.stderr.decode())
                jar_metadata = "import glob,hashlib,json; print(json.dumps([{'path':p,'sha256':hashlib.sha256(open(p,'rb').read()).hexdigest()} for p in glob.glob('/google-cloud-sdk/platform/pubsub-emulator/lib/*.jar')]))"
                assert command(['docker', 'exec', broker, python, '-c', jar_metadata], label + '-jar') == 0
                wall, monotonic = time.time(), time.monotonic()
                rc = command(['docker', 'run', '--rm', '--pull', 'never', '--name', probe, '--network', 'container:' + broker,
                              '-e', 'PUBSUB_EMULATOR_HOST=localhost:8085', '-e', 'GOOGLE_CLOUD_PROJECT=brighter-test',
                              '-v', f'{OUT}/bin:/probe:ro', 'mcr.microsoft.com/dotnet/sdk:' + framework[3:],
                              'dotnet', f'/probe/Release/{framework}/ProtocolControl.dll'], label)
                wall, monotonic = time.time() - wall, time.monotonic() - monotonic
                lines = (OUT / (label + '.log')).read_text().splitlines()
                result = {'variant': variant, 'framework': framework, 'image': image, 'exit': rc,
                          'wall_seconds': wall, 'monotonic_seconds': monotonic, 'clock_discontinuity': abs(wall - monotonic) > 5,
                          'results': [line for line in lines if line.startswith('RESULT')],
                          'recoveries': [line for line in lines if line.startswith('RECOVERY')],
                          'errors': [line for line in lines if 'PROBE ERROR' in line]}
                observations.append(result)
                save(label, result)
                print(json.dumps(result), flush=True)
                assert rc == 0 and len(result['results']) == 4 and not result['errors'] and not result['clock_discontinuity'], 'Incomplete or interrupted comparison'
            finally:
                if started:
                    subprocess.run(['docker', 'stop', probe], capture_output=True, timeout=30)
                    command(['docker', 'logs', broker], label + '-broker')
                    assert command(['docker', 'stop', broker], label + '-stop', timeout=30) == 0
                    assert command(['docker', 'inspect', broker, '--format', '{{json .State}}'], label + '-stopped') == 0
finally:
    inhibitor.terminate()
    inhibitor.wait(timeout=10)
    after = snapshot()
    save('verification', {'unchanged_inputs': before == after, 'input_count': len(before),
                         'changed': sorted(path for path in before.keys() | after.keys() if before.get(path) != after.get(path)),
                         'observations': observations})
    assert before == after, 'Source or dependency inputs changed during comparison'
