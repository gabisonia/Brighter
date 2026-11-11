from pathlib import Path
from datetime import datetime
import hashlib
import json
import re
import shutil

OUT = Path(__file__).parent
ROOT = Path('/Users/gabisonia/RiderProjects/Brighter')
REVIEW = ROOT / 'specs/0038-wire-message-mapping/review/gcp-emulator-comparison'
REVIEW.mkdir(exist_ok=True)
verification = json.loads((OUT / 'verification.json').read_text())
assert verification['unchanged_inputs'] and len(verification['observations']) == 4
summary = {'date': '2026-11-09', 'inputs': verification['input_count'], 'unchanged_inputs': True,
           'source_sha256': hashlib.sha256((ROOT / 'specs/0038-wire-message-mapping/review/gcp-ordered-nack-protocol-control/Program.cs').read_bytes()).hexdigest(),
           'images': {}, 'runs': [], 'build': {'errors': 0, 'warnings': 0}}
for variant in ['baseline', 'candidate']:
    image = json.loads((OUT / (variant + '-image.log')).read_text())
    jar = json.loads((OUT / (variant + '-net9.0-jar.log')).read_text())
    summary['images'][variant] = {key: image[key] for key in ['Id', 'Architecture', 'Os', 'Created']}
    summary['images'][variant]['jar'] = jar
    for framework in ['net9.0', 'net10.0']:
        label = variant + '-' + framework
        result = json.loads((OUT / (label + '.json')).read_text())
        state = json.loads((OUT / (label + '-stopped.log')).read_text())
        assert not state['Running']
        assert len(result['results']) == 4 and result['exit'] == 0 and not result['clock_discontinuity'] and not result['errors']
        result['fixture_status'] = state['Status']
        text = (OUT / (label + '.log')).read_text()
        case = text.split('CASE addStream=True acknowledgeSecondFirst=True\n')[1]
        deliveries = []
        for line in case.splitlines():
            match = re.match(r'(\S+) STREAM (\d+) DELIVERY (\{.*\})$', line)
            if match:
                deliveries.append({'utc': match[1], 'stream': int(match[2]), **json.loads(match[3])})
        original = next(d for d in deliveries if d['message']['data'] == 'Zmlyc3Q=')
        repeated = [d for d in deliveries if d['message']['messageId'] == original['message']['messageId'] and d['ackId'] != original['ackId']]
        result['two_stream_ack_first_evidence'] = {'original': original, 'redeliveries': repeated}
        if 'INTERVENTION:' in case:
            intervention = case.split('INTERVENTION:')[1]
            ack_success = re.search(r'(\S+) ACK SUCCESS ', intervention)[1]
            result['two_stream_ack_first_evidence']['repeated_ack_success_utc'] = ack_success
            if repeated:
                result['two_stream_ack_first_evidence']['recovery_ms_after_repeated_ack_success'] = (
                    datetime.fromisoformat(repeated[0]['utc']) - datetime.fromisoformat(ack_success)).total_seconds() * 1000
                assert repeated[0]['message']['data'] == original['message']['data']
                assert repeated[0]['stream'] == 1
        summary['runs'].append(result)
        for suffix in ['.log', '.json', '-jar.log', '-stopped.log']:
            destination = label + suffix.replace('.log', '.txt')
            shutil.copy2(OUT / (label + suffix), REVIEW / destination)
for name in ['run.py', 'summarize.py', 'verification.json', 'binaries.json']:
    shutil.copy2(OUT / name, REVIEW / name)
(OUT / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
shutil.copy2(OUT / 'summary.json', REVIEW / 'summary.json')
print(json.dumps(summary, indent=2))
