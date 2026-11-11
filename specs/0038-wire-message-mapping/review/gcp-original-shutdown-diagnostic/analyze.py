from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
import json,re,hashlib,xml.etree.ElementTree as E

p=Path(__file__).parent
main=Path('/Users/gabisonia/RiderProjects/Brighter')
review=main/'specs/0038-wire-message-mapping/review/gcp-original-shutdown-diagnostic'
pattern=re.compile(r'^provider=(\w+) generation=(\d+) (\S+) (NATIVE DELIVERY|RPC SUCCESS \w+) (\{.*\})$')
reports=[]
for label,major in [('focus-net9.0',9),('focus-net10.0',10),('observed-full-net9.0',9)]:
    rows=E.parse(p/(label+'.trx')).findall('.//{*}UnitTestResult')
    events=[]
    lines=(p/f'original-shutdown-rpc-net{major}.log').read_text().splitlines()
    for line in lines:
        match=pattern.match(line)
        if not match:continue
        provider,generation,stamp,kind,payload=match.groups()
        events.append({'provider':provider,'generation':int(generation),'time':datetime.fromisoformat(stamp),'kind':kind,'payload':json.loads(payload)})
    for row in rows:
        name=row.attrib['testName']
        if not ('When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message' in name or 'When_a_gcp_stream_channel_is_disposed_with_a_redelivery_buffered_after_requeue' in name):continue
        start=datetime.fromisoformat(row.attrib['startTime']);end=datetime.fromisoformat(row.attrib['endTime'])
        selected=[x for x in events if start<=x['time']<=end]
        providers=sorted({x['provider'] for x in selected})
        result={'run':label,'test':name,'outcome':row.attrib['outcome'],'start':row.attrib['startTime'],'end':row.attrib['endTime'],'providers':[]}
        for provider in providers:
            entries=[x for x in selected if x['provider']==provider]
            delivered=[x for x in entries if x['kind']=='NATIVE DELIVERY']
            original=[x for x in delivered if x['generation']==1]
            replacement=[x for x in delivered if x['generation']==2]
            nacks=[x for x in entries if x['kind']=='RPC SUCCESS ModifyAckDeadline' and x['payload'].get('ackDeadlineSeconds',0)==0]
            result['providers'].append({'id':provider,
                'original_deliveries':[{'native_id':x['payload']['message']['messageId'],'ack_id':x['payload']['ackId'],'application_id':x['payload']['message'].get('attributes',{}).get('ce-id'),'time':x['time'].isoformat()} for x in original],
                'replacement_deliveries':[{'native_id':x['payload']['message']['messageId'],'ack_id':x['payload']['ackId'],'application_id':x['payload']['message'].get('attributes',{}).get('ce-id'),'time':x['time'].isoformat()} for x in replacement],
                'completed_nacks':[{'generation':x['generation'],'time':x['time'].isoformat(),'ack_ids':x['payload']['ackIds']} for x in nacks]})
        reports.append(result)
        keep='\n'.join(line for line in lines if any(line.startswith('provider='+provider+' ') for provider in providers))+'\n'
        (review/(label+'-'+str(len(reports))+'-native.log')).write_text(keep)
    (review/(label+'.json')).write_bytes((p/(label+'.json')).read_bytes())

baseline=E.parse('/private/tmp/brighter-4170-gcp-scheduled-publication/final-full-net9.0.trx').findall('.//{*}UnitTestResult')
current=E.parse(p/'observed-full-net9.0.trx').findall('.//{*}UnitTestResult')
before=Counter(x.attrib['testName'] for x in baseline);after=Counter(x.attrib['testName'] for x in current)
assert before==after
manifest=json.loads((p/'main-source-before.json').read_text())
changed=[f for f,h in manifest.items() if hashlib.sha256((main/f).read_bytes()).hexdigest()!=h]
assert not changed
data={'all_case_identities_retained':True,'full_case_count':len(current),'unchanged_main_source_count':len(manifest),'changed_main_sources':changed,
    'original_shutdown_observations':reports}
(p/'analysis.json').write_text(json.dumps(data,indent=2))
(review/'analysis.json').write_bytes((p/'analysis.json').read_bytes())
print(json.dumps({'full_cases':len(current),'shutdown_observations':len(reports),'main_sources_unchanged':len(manifest)}))
