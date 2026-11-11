from pathlib import Path
import subprocess,json,time,os,xml.etree.ElementTree as E
p=Path(__file__).parent;r=Path('/private/tmp/brighter-4170-rmq-scheduled-selection/workspace')
fixture='brighter-4170-gcp-original-shutdown'
image='gcr.io/google.com/cloudsdktool/cloud-sdk@sha256:3797a874fae6b75aa25260b5bd40fa0397c2f1102f18ae3ac7dc887732fbe449'
def command(args,label,timeout=600,cwd=None):
 with (p/(label+'.log')).open('w') as log:return subprocess.run(args,cwd=cwd,stdout=log,stderr=subprocess.STDOUT,timeout=timeout).returncode
def setup(name=fixture):
 found=subprocess.run(['docker','inspect',name],capture_output=True)
 if found.returncode:
  assert command(['docker','run','-d','--name',name,'--memory','2g','--cpus','2','--pull','never',image,'gcloud','beta','emulators','pubsub','start','--host-port=0.0.0.0:8085','--project=brighter-test'],name+'-start')==0
 else:assert command(['docker','start',name],name+'-start')==0
 probe="import urllib.request; assert urllib.request.urlopen('http://localhost:8085/v1/projects/brighter-test/topics', timeout=2).status==200"
 for _ in range(60):
  if subprocess.run(['docker','exec',name,'python3','-c',probe],capture_output=True).returncode==0:return
  time.sleep(1)
 raise RuntimeError('Native emulator readiness failed')
def build(label):
 assert command(['dotnet','restore','tests/Paramore.Brighter.Gcp.Tests','--locked-mode','-m:1','-nodeReuse:false'],label+'-restore',cwd=r)==0
 rc=command(['dotnet','build','tests/Paramore.Brighter.Gcp.Tests','-c','Release','--no-restore','-m:1','-nodeReuse:false','-t:Rebuild',f'-p:BaseOutputPath={p}/bin/'],label+'-build',cwd=r)
 print(label,'build',rc,flush=True);return rc
fullfilter='(FullyQualifiedName~Paramore.Brighter.Gcp.Tests.MessagingGateway|Category=GcpPubSubPurge)'
def run(label,framework,fil='FullyQualifiedName~GcpScheduledPublicationSelectionTests',broker=fixture,assembly_root='bin'):
 args=['docker','run','--rm','--ulimit','core=0','--name',fixture+'-'+label,'--network','container:'+broker,'-e','PUBSUB_EMULATOR_HOST=localhost:8085','-e','GOOGLE_CLOUD_PROJECT=brighter-test','-v',f'{r}:/workspace:ro','-v',f'{p}:/results','-w','/workspace','mcr.microsoft.com/dotnet/sdk:'+framework[3:],'dotnet','vstest',f'/results/{assembly_root}/Release/{framework}/Paramore.Brighter.Gcp.Tests.dll',f'--logger:trx;LogFileName={label}.trx','--ResultsDirectory:/results','--Blame:CollectHangDump;TestTimeout=5m;HangDumpType=mini']
 if fil:args.append('--TestCaseFilter:'+fil)
 wall,mono=time.time(),time.monotonic();rc=command(args,label,3600);wall,mono=time.time()-wall,time.monotonic()-mono
 rows=E.parse(p/(label+'.trx')).findall('.//{*}UnitTestResult') if (p/(label+'.trx')).exists() else []
 data={'exit':rc,'wall_seconds':wall,'monotonic_seconds':mono,'clock_discontinuity':abs(wall-mono)>5,'counts':{s:sum(x.attrib['outcome']==s for x in rows) for s in ['Passed','Failed','NotExecuted']},'failures':[{'name':x.attrib['testName'],'message':x.findtext('.//{*}Message'),'stack':x.findtext('.//{*}StackTrace')} for x in rows if x.attrib['outcome']=='Failed']}
 (p/(label+'.json')).write_text(json.dumps(data,indent=2));print(label,rc,data['counts'],flush=True);return data
