from pathlib import Path
import subprocess,json
p=Path(__file__).parent
with (p/'negative-control.log').open('w') as log:rc=subprocess.run(['dotnet','msbuild',str(p/'negative-control.proj'),'-t:Check','-nologo','-v:minimal','-nodeReuse:false'],stdout=log,stderr=subprocess.STDOUT,timeout=180).returncode
text=(p/'negative-control.log').read_text()
result={'exit':rc,'missing_writer_detected':'error CP0001:' in text and 'IAmAWireMessageWriter' in text,'missing_reader_detected':'error CP0001:' in text and 'IAmAWireMessageReader' in text,'unresolved_references':text.count('Could not resolve reference')}
(p/'negative-control.json').write_text(json.dumps(result,indent=2));print(result)
assert rc!=0 and result['missing_writer_detected'] and result['missing_reader_detected'] and result['unresolved_references']==0
