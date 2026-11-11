"""Reconstruct acceptance from raw TRX; runner JSON is not authoritative."""
from pathlib import Path
from collections import Counter
import xml.etree.ElementTree as ET
import json, hashlib, struct, runpy

base = Path('/evidence')
ns = {'t': 'http://microsoft.com/schemas/VisualStudio/TeamTest/2010'}
def trx(path):
    root = ET.parse(path).getroot()
    rows = root.findall('t:Results/t:UnitTestResult', ns)
    return {'path': str(path.relative_to(base)), 'outcome': root.find('t:ResultSummary', ns).get('outcome'),
            'counts': dict(Counter(r.get('outcome') for r in rows)),
            'cases': [{'name': r.get('testName'), 'outcome': r.get('outcome'),
                       'message': r.findtext('t:Output/t:ErrorInfo/t:Message', default='', namespaces=ns)} for r in rows]}

results = {}
for fw in ['net9.0', 'net10.0']:
    asb = [trx(p) for p in sorted((base/'asb').glob(f'{fw}-*/**/*.trx'))]
    gcp = [trx(p) for p in sorted((base/'gcp'/fw).glob('*.trx'))]
    counts = Counter()
    for run in asb: counts.update(run['counts'])
    reference = ET.parse('/review/kafka-full-capture/full/results.trx').getroot()
    expected = {r.get('testName') for r in reference.findall('t:Results/t:UnitTestResult', ns)}
    full_dir = 'kafka-passive' if fw == 'net9.0' else 'kafka-passive-net10'
    runs = [trx(p) for d in [full_dir, f'kafka-remainder-{fw.split(".")[0]}'] for p in (base/d/fw).glob('results.trx')]
    passed = {r['name'] for run in runs for r in run['cases'] if r['outcome'] == 'Passed'}
    results[fw] = {'asb': {'counts': dict(counts), 'runs': asb}, 'gcp': gcp,
                   'kafka': {'referenceCount': len(expected), 'uniquePassed': len(passed),
                             'remaining': sorted(expected-passed), 'unexpected': sorted(passed-expected), 'runs': runs}}

cores = []
for path in Path('/cores').glob('*pending-close.core'):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        header = stream.read(64)
        assert header[:6] == b'\x7fELF\x02\x01'
        kind, machine = struct.unpack_from('<HH', header, 16)
        phoff = struct.unpack_from('<Q', header, 32)[0]
        phsize, phcount = struct.unpack_from('<HH', header, 54)
        stream.seek(phoff)
        segments = [struct.unpack('<IIQQQQQQ', stream.read(phsize)) for _ in range(phcount)]
        stream.seek(0)
        for block in iter(lambda: stream.read(8*1024*1024), b''): digest.update(block)
    length = path.stat().st_size
    cores.append({'name': path.name, 'bytes': length, 'sha256': digest.hexdigest(),
                  'elfType': kind, 'machine': machine, 'programHeaders': phcount,
                  'allSegmentsWithinFile': all(s[2]+s[5] <= length for s in segments),
                  'completeElfX64Core': kind == 4 and machine == 62 and all(s[2]+s[5] <= length for s in segments)})

for directory, fw in [('kafka-passive','net9.0'), ('kafka-passive-net10','net10.0')]:
    capture = base/directory/fw
    original = (base/'kafka/net9.0/symbolicate-x64.py').read_text()
    original = original.replace("base = Path('/capture')", f"base = Path('{capture}')")
    original = original.replace('bin/Release/net9.0/', f'bin/Release/{fw}/')
    original = original.replace('No DWARF variables, queue ownership, or complete core claim. Capture followed intrusive healthy-reference pause.', 'No DWARF variables or queue ownership claim. Passive full-suite failure preceded diagnostic capture; full core provenance recorded separately.')
    exec(compile(original, 'symbolicate-x64.py', 'exec'), {})

output = {'authority': 'Raw TRX identities/outcomes, not incomplete runner JSON. Aborted full suites remain failed acceptance even when remaining cases pass separately.',
          'frameworks': results, 'cores': cores}
(base/'acceptance-analysis.json').write_text(json.dumps(output, indent=2)+'\n')
print(json.dumps({'frameworks': {fw: {'asb':r['asb']['counts'], 'gcp': [x['counts'] for x in r['gcp']],
                                    'kafkaPassed':r['kafka']['uniquePassed'], 'kafkaRemaining':r['kafka']['remaining']} for fw,r in results.items()}, 'cores': cores}, indent=2))
