from pathlib import Path
import hashlib
import json
import shutil
import struct

BASE = Path(__file__).parent
ROOT = Path('/Users/gabisonia/RiderProjects/Brighter')
PRIOR = Path('/private/tmp/brighter-4170-kafka-native-close-diagnostic')
REVIEW = ROOT / 'specs/0038-wire-message-mapping/review/kafka-full-capture'
REVIEW.mkdir(exist_ok=True)

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

for mode in ['validate', 'full']:
    source = BASE / mode
    destination = REVIEW / mode
    destination.mkdir(exist_ok=True)
    summary = json.loads((source / 'summary.json').read_text())
    before = json.loads((source / 'source-manifest.json').read_text())
    assert all(sha(ROOT / name) == value for name, value in before.items())
    binaries = json.loads((source / 'binaries.json').read_text())
    assert all(sha(PRIOR / name) == value for name, value in binaries.items())
    for path in source.glob('*'):
        if path.is_file() and path.suffix in ('.txt', '.json', '.trx') and path.name != 'source-manifest.json':
            shutil.copy2(path, destination / path.name)
    (destination / 'preservation.json').write_text(json.dumps({'unchanged_source_inputs': len(before), 'unchanged_binary_inputs': len(binaries),
                                                             'artifact_hashes': {path.name: sha(path) for path in destination.glob('*') if path.is_file() and path.name != 'preservation.json'}}, indent=2) + '\n')
for name in ['capture.py', 'run.py', 'symbolicate.py', 'preserve.py']:
    shutil.copy2(BASE / name, REVIEW / name)
core = BASE / 'validate/healthy-validation.core'
with core.open('rb') as stream:
    header = stream.read(64)
    endian = '<' if header[5] == 1 else '>'
    assert header[:4] == b'\x7fELF' and struct.unpack_from(endian + 'H', header, 16)[0] == 4
(REVIEW / 'native-core.json').write_text(json.dumps({'path': str(core), 'bytes': core.stat().st_size, 'sha256': sha(core),
                                                    'purpose': 'Healthy full-memory capture validation; never a failing-hang dump.',
                                                    'copied_into_review': False}, indent=2) + '\n')
print('Both capture batches, unchanged source/binary inputs and the retained healthy native core verified.')
