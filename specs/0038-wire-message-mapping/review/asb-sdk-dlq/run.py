from pathlib import Path
import subprocess, json, time, os, secrets, hashlib, ipaddress, xml.etree.ElementTree as E
p=Path(__file__).parent
root=Path('/Users/gabisonia/RiderProjects/Brighter')
config=root/'specs/0038-wire-message-mapping/review/asb-native-wire-writer/emulator-config.json'
asb='mcr.microsoft.com/azure-messaging/servicebus-emulator@sha256:5a96d893b245031740f7d46e0fe5ff282d24b78c4b7d761dd57590f3f010a9b3'
sql='mcr.microsoft.com/mssql/server@sha256:cf8f0ab2ebe61feec2eed2629061a07d247f791c2a8ddda74a4ac9f06a291718'
network='brighter-4170-asb-sdk-dlq'
password='Local-'+secrets.token_hex(16)+'!'
secrets_file=p/'fixture.env'
secrets_file.write_text('ACCEPT_EULA=Y\nMSSQL_SA_PASSWORD='+password+'\nSQL_SERVER=sql\nSQL_WAIT_INTERVAL=15\nMSSQL_MEMORY_LIMIT_MB=2048\nMSSQL_PID=Developer\n')
secrets_file.chmod(0o600)
config_data=json.loads(config.read_text());config_data['UserConfig']['Logging']['Type']='Console'
(p/'Config.json').write_text(json.dumps(config_data,indent=2)+'\n')
def command(args,timeout=60):
 return subprocess.run(args,capture_output=True,text=True,timeout=timeout)
def checked(args,timeout=60):
 r=command(args,timeout)
 if r.returncode: raise RuntimeError(r.stderr[:2000])
 return r.stdout
assert command(['docker','network','inspect',network]).returncode != 0,'Fixture network already exists; do not overwrite'
network_ids=checked(['docker','network','ls','-q']).split()
existing_networks=json.loads(checked(['docker','network','inspect',*network_ids]))
subnet=ipaddress.ip_network('10.254.242.0/28')
for item in existing_networks:
 for entry in item.get('IPAM',{}).get('Config') or []:
  if entry.get('Subnet'):
   allocated=ipaddress.ip_network(entry['Subnet'])
   assert allocated.version!=4 or not subnet.overlaps(allocated), 'Requested subnet overlaps existing Docker network'
(p/'network-preflight.json').write_text(json.dumps({'subnet':str(subnet),'existing_subnets':[c.get('Subnet') for x in existing_networks for c in x.get('IPAM',{}).get('Config') or []],'explicit_subnet':True},indent=2)+'\n')
checked(['docker','network','create','--subnet',str(subnet),network])
awake=subprocess.Popen(['caffeinate','-i','-s','-w',str(os.getpid())])
results=[]
try:
 for fw in ['net9.0','net10.0']:
  suffix=fw.replace('.','-'); db=network+'-sql-'+suffix; broker=network+'-broker-'+suffix
  outcome={'framework':fw,'sql_image':sql,'broker_image':asb,'host_architecture':'aarch64','sql_emulated':True,'experiment':'native SDK only; four topic and four queue cycles'}
  owned=[]
  try:
   checked(['docker','run','-d','--pull','never','--platform','linux/amd64','--name',db,'--network',network,'--network-alias','sql','--env-file',str(secrets_file),sql]);owned.append(db)
   start=time.monotonic()
   while time.monotonic()-start < 120:
    logs=command(['docker','logs',db]).stdout
    if 'SQL Server is now ready for client connections' in logs:break
    if checked(['docker','inspect',db,'--format','{{.State.Running}}']).strip()!='true': raise RuntimeError('SQL backend exited before readiness')
    time.sleep(2)
   else: raise RuntimeError('SQL backend not ready within 120 seconds')
   print(fw,'SQL backend ready',flush=True)
   checked(['docker','run','-d','--pull','never','--platform','linux/arm64','--name',broker,'--network',network,'--env-file',str(secrets_file),'-p','127.0.0.1::5672','-v',str(p/'Config.json')+':/ServiceBus_Emulator/ConfigFiles/Config.json:ro',asb]);owned.append(broker)
   start=time.monotonic()
   while time.monotonic()-start < 120:
    r=command(['docker','logs',broker]); logs=r.stdout+r.stderr
    if 'Emulator Service is Successfully Up' in logs or 'Emulator is running' in logs:break
    if checked(['docker','inspect',broker,'--format','{{.State.Running}}']).strip()!='true': raise RuntimeError('Service Bus emulator exited before readiness')
    time.sleep(2)
   else: raise RuntimeError('Emulator not ready within 120 seconds; inspect retained logs')
   print(fw,'Service Bus emulator ready',flush=True)
   port=checked(['docker','port',broker,'5672/tcp']).strip().rsplit(':',1)[1]
   env=os.environ.copy();env['BrighterTestsASBConnectionString']='Endpoint=sb://127.0.0.1:'+port+';SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=SAS_KEY_VALUE;UseDevelopmentEmulator=true;'
   env.pop('BrighterTestsASBNameSpace',None)
   assembly=p/'probe/bin/Release'/fw/'Probe.dll'
   outcome['assembly_sha256']=hashlib.sha256(assembly.read_bytes()).hexdigest()
   args=['dotnet',str(assembly),str(p/(fw+'-probe.json'))]
   wall,mono=time.time(),time.monotonic()
   with (p/(fw+'.log')).open('w') as log:
    run=subprocess.run(args,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,timeout=300)
   elapsed,monotonic=time.time()-wall,time.monotonic()-mono
   data=json.loads((p/(fw+'-probe.json')).read_text())
   outcome.update(exit_code=run.returncode,wall_seconds=elapsed,monotonic_seconds=monotonic,clock_discontinuity=abs(elapsed-monotonic)>5,
    counts={'Passed':len(data['results'])-data['failures'],'Failed':data['failures']},probe=data)
   print(fw,outcome['counts'],flush=True)
  except Exception as error:
   outcome['setup_or_execution_error']=str(error)
   print(fw,type(error).__name__,str(error),flush=True)
  finally:
   outcome['containers']=[]
   for name in reversed(owned):
    log=command(['docker','logs',name]);(p/(name+'.log')).write_text((log.stdout+log.stderr).replace(password, '[local fixture password redacted]'))
    state=json.loads(checked(['docker','inspect',name]))[0]['State']
    outcome['containers'].append({'name':name,'state_before_stop':state})
    stop=command(['docker','stop','-t','10',name],40)
    outcome['containers'][-1]['stop_exit_code']=stop.returncode
    outcome['containers'][-1]['running_after_stop']=checked(['docker','inspect',name,'--format','{{.State.Running}}']).strip()
   results.append(outcome);(p/'results.json').write_text(json.dumps(results,indent=2)+'\n')
  if outcome.get('setup_or_execution_error'):break
finally:
 awake.terminate();awake.wait(timeout=10)
print('Bounded SDK-only diagnostic finished; owned containers stopped and retained.',flush=True)
