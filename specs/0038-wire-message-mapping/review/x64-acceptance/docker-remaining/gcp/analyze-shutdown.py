from pathlib import Path
from datetime import datetime
import json
import re
import xml.etree.ElementTree as ET

root = Path('/evidence')
observations = []
for framework in ('net9.0', 'net10.0'):
    directory = root / framework
    events = []
    for line in (directory / 'stream-native.log').read_text(encoding='utf-8-sig').splitlines():
        match = re.match(r'provider=(\w+) generation=(\d+) (\S+) (NATIVE DELIVERY|RPC (?:START|SUCCESS|FAILURE) \w+) (.*)', line)
        if not match:
            continue
        try:
            payload, _ = json.JSONDecoder().raw_decode(match[5])
        except json.JSONDecodeError:
            continue
        events.append({'provider': match[1], 'generation': int(match[2]), 'time': match[3],
                       'clock': datetime.fromisoformat(match[3]), 'event': match[4], 'payload': payload})
    rows = ET.parse(directory / 'results.trx').findall('.//{*}UnitTestResult')
    for row in rows:
        name = row.attrib['testName']
        if not any(fragment in name for fragment in (
            'When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message',
            'When_a_gcp_stream_channel_is_disposed_with_a_redelivery_buffered_after_requeue')):
            continue
        start, end = (datetime.fromisoformat(row.attrib[key]) for key in ('startTime', 'endTime'))
        relevant = [event for event in events if start <= event['clock'] <= end]
        providers = sorted({event['provider'] for event in relevant if event['event'] == 'NATIVE DELIVERY'})
        provider_results = []
        for provider in providers:
            correlated = [event for event in relevant if event['provider'] == provider]
            deliveries = [{'generation': event['generation'], 'time': event['time'],
                           'nativeId': event['payload']['message']['messageId'], 'ackId': event['payload']['ackId'],
                           'applicationId': event['payload']['message'].get('attributes', {}).get('ce-id')}
                          for event in correlated if event['event'] == 'NATIVE DELIVERY']
            calls = [{'generation': event['generation'], 'time': event['time'], 'event': event['event'],
                      'ackDeadlineSeconds': event['payload'].get('ackDeadlineSeconds', 0),
                      'ackIds': event['payload'].get('ackIds', [])}
                     for event in correlated if event['event'].endswith('ModifyAckDeadline')]
            provider_results.append({'provider': provider,
                'originalDeliveries': [delivery for delivery in deliveries if delivery['generation'] == 1],
                'replacementDeliveries': [delivery for delivery in deliveries if delivery['generation'] > 1],
                'deadlineRpcEvents': calls,
                'completedShutdownNacks': [call for call in calls if call['event'].startswith('RPC SUCCESS') and call['ackDeadlineSeconds'] == 0]})
        observations.append({'framework': framework, 'test': name, 'outcome': row.attrib['outcome'],
                             'start': row.attrib['startTime'], 'end': row.attrib['endTime'],
                             'assertion': row.findtext('.//{*}ErrorInfo/{*}Message'), 'providers': provider_results})
result = {'originalShutdownObservations': observations,
          'passed': sum(row['outcome'] == 'Passed' for row in observations),
          'failed': sum(row['outcome'] == 'Failed' for row in observations),
          'conclusion': 'The .NET 10 original unsettled async replacement failure is reproduced. Its observer sees original-generation redelivery and successful deadline-zero RPCs, but no replacement-generation native delivery in the bounded receive interval. Native-delivery-to-Brighter mapping loss is not demonstrated by this sample.',
          'limits': 'Client-side RPC success does not establish server execution order relative to the canceled 60-second extension. This observer does not explicitly log stream construction/open or terminal SubscriberClient.StartAsync task state. Exact SDK/emulator/caller-lifecycle cause remains unresolved; do not generalize the separately known ordered-nack cause.'}
(root / 'shutdown-analysis.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({'observations': len(observations), 'passed': result['passed'], 'failed': result['failed']}))
