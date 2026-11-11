from pathlib import Path
import json,subprocess,xml.etree.ElementTree as E,hashlib,time
p=Path(__file__).parent;main=Path('/Users/gabisonia/RiderProjects/Brighter');base=p/'baseline'
sdk=Path('/opt/homebrew/Cellar/dotnet/10.0.400/libexec/sdk/10.0.400')
scope=json.loads((p/'scope.json').read_text())
results=json.loads((p/'comparison.json').read_text()) if (p/'comparison.json').exists() else []
def references(root,project,framework):
 assets=json.loads((root/'src'/project/'obj/project.assets.json').read_text())
 refs=set()
 target=next(v for k,v in assets['targets'].items() if k==framework or k=='.NETStandard,Version=v2.0' and framework=='netstandard2.0')
 for name,entry in target.items():
  if entry['type']=='project':
   dep=name.split('/')[0]
   chosen=entry.get('framework',framework)
   if chosen.startswith('.NETStandard,Version=v'):chosen='netstandard'+chosen.split('=v')[1]
   elif chosen.startswith('.NETCoreApp,Version=v'):chosen='net'+chosen.split('=v')[1]
   elif chosen.startswith('.NETFramework,Version=v'):chosen='net'+chosen.split('=v')[1].replace('.','')
   file=root/'src'/dep/'bin/Release'/chosen/(dep+'.dll')
   if not file.exists():
    choices=list((root/'src'/dep/'bin/Release').glob('*/'+dep+'.dll'));assert len(choices)==1,(dep,framework,choices);file=choices[0]
   refs.add(str(file))
  else:
   lib=assets['libraries'][name]
   for rel in entry.get('compile',{}):
    if not rel.endswith('.dll'):continue
    choices=[Path(folder)/lib['path']/rel for folder in assets['packageFolders']]
    file=next((x for x in choices if x.exists()),None);assert file,(name,rel)
    refs.add(str(file))
 if framework=='netstandard2.0':
  for name,lib in assets['libraries'].items():
   if name.lower().startswith('netstandard.library/'):
    for folder in assets['packageFolders']:refs.update(str(x) for x in (Path(folder)/lib['path']/'build/netstandard2.0/ref').glob('*.dll'))
 if framework.startswith('net4'):
  for name,lib in assets['libraries'].items():
   if name.lower().startswith('microsoft.netframework.referenceassemblies.net'):
    for folder in assets['packageFolders']:
     refs.update(str(x) for x in (Path(folder)/lib['path']/'build/.NETFramework').rglob('*.dll'))
 info=assets['project']['frameworks'][framework]
 for dep in info.get('downloadDependencies',[]):
  if dep['name'].endswith('.Ref'):
   version=dep['version'].strip('[]').split(',')[0].strip()
   for folder in assets['packageFolders']:refs.update(str(x) for x in (Path(folder)/dep['name'].lower()/version/'ref'/framework).glob('*.dll'))
 for pack in info.get('frameworkReferences',{}):
  # Installed targeting packs cover net10; downloaded packs cover older targets.
  dirs=sorted((sdk.parent.parent/'packs'/(pack+'.Ref')).glob('*/ref/'+framework))
  if dirs:refs.update(str(x) for x in dirs[-1].glob('*.dll'))
 return sorted(refs)
for project in scope['projects']:
 for left in sorted((base/'src'/project/'bin/Release').glob('*/'+project+'.dll')):
  framework=left.parent.name;right=main/'src'/project/'bin/Release'/framework/left.name;assert right.exists(),right
  previous=next((x for x in results if x['project']==project and x['framework']==framework),None)
  if previous and previous['exit']==0:
   assert previous['baseline_sha256']==hashlib.sha256(left.read_bytes()).hexdigest() and previous['current_sha256']==hashlib.sha256(right.read_bytes()).hexdigest()
   continue
  target=E.Element('Project');E.SubElement(target,'UsingTask',TaskName='Microsoft.DotNet.ApiCompat.Task.ValidateAssembliesTask',AssemblyFile=str(sdk/'Sdks/Microsoft.NET.Sdk/tools/net10.0/Microsoft.DotNet.ApiCompat.Task.dll'))
  group=E.SubElement(target,'ItemGroup')
  for kind,paths in [('LeftRef',references(base,project,framework)),('RightRef',references(main,project,framework))]:
   for path in paths:E.SubElement(group,kind,Include=path)
  check=E.SubElement(target,'Target',Name='Check')
  E.SubElement(check,'Microsoft.DotNet.ApiCompat.Task.ValidateAssembliesTask',LeftAssemblies=str(left),RightAssemblies=str(right),LeftAssembliesReferences="@(LeftRef, ',')",RightAssembliesReferences="@(RightRef, ',')",RoslynAssembliesPath=str(sdk/'Roslyn/bincore'),RespectInternals='false',EnableRuleAttributesMustMatch='false',EnableRuleCannotChangeParameterName='true',EnableStrictMode='false',GenerateSuppressionFile='false')
  name=project+'-'+framework;proj=p/(name+'.proj');E.ElementTree(target).write(proj,encoding='unicode')
  with (p/(name+'.log')).open('w') as log:rc=subprocess.run(['dotnet','msbuild',str(proj),'-t:Check','-nologo','-v:minimal','-nodeReuse:false'],stdout=log,stderr=subprocess.STDOUT,timeout=180).returncode
  log_text=(p/(name+'.log')).read_text()
  unresolved=log_text.count('Could not resolve reference')
  result={'unresolved_references':unresolved,'project':project,'framework':framework,'exit':rc,'baseline_sha256':hashlib.sha256(left.read_bytes()).hexdigest(),'current_sha256':hashlib.sha256(right.read_bytes()).hexdigest()};results.append(result)
  (p/'comparison.json').write_text(json.dumps(results,indent=2));print(name,rc,flush=True)
  if rc:print('Diagnostics retained in',str(p/(name+'.log')),flush=True)
changed=[f for f,h in scope['source_hashes'].items() if hashlib.sha256((main/f).read_bytes()).hexdigest()!=h]
(p/'source-verification.json').write_text(json.dumps({'changed_sources':changed,'all_unchanged':not changed,'comparisons':len(results),'failed':sum(x['exit']!=0 or x.get('unresolved_references',0)>0 for x in results)},indent=2))
print('Finished',len(results),'compiled comparisons; failures',sum(x['exit']!=0 or x.get('unresolved_references',0)>0 for x in results),'changed sources',changed,flush=True)
