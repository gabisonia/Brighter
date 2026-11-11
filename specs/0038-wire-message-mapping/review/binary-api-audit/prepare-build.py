from pathlib import Path
import subprocess,tarfile,io,json,time,hashlib
p=Path(__file__).parent;main=Path('/Users/gabisonia/RiderProjects/Brighter');baseline=p/'baseline';baseline.mkdir(exist_ok=True)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=main,text=True).strip()
archive=subprocess.check_output(['git','archive','HEAD'],cwd=main)
with tarfile.open(fileobj=io.BytesIO(archive)) as tar:tar.extractall(baseline,filter='data')
files=subprocess.check_output(['git','diff','HEAD','--name-only','--','src'],cwd=main,text=True).splitlines()
projects=sorted({f.split('/')[1] for f in files if f.endswith('.cs')})
(p/'scope.json').write_text(json.dumps({'baseline':head,'projects':projects,'source_hashes':{f:hashlib.sha256((main/f).read_bytes()).hexdigest() for f in files if (main/f).is_file()},'sdk':'10.0.400'},indent=2))
results={}
for project in projects:
 wall,mono=time.time(),time.monotonic()
 path=baseline/'src'/project/(project+'.csproj')
 with (p/(project+'-build.log')).open('w') as log:
  rc=subprocess.run(['dotnet','build',str(path),'-c','Release','-m:1','-nodeReuse:false','-p:RestoreLockedMode=true'],cwd=baseline,stdout=log,stderr=subprocess.STDOUT,timeout=900).returncode
 results[project]={'exit':rc,'seconds':time.monotonic()-mono,'clock_discontinuity':abs((time.time()-wall)-(time.monotonic()-mono))>5}
 (p/'baseline-builds.json').write_text(json.dumps(results,indent=2))
 print(project,results[project],flush=True)
 if rc:raise RuntimeError('Baseline build failed; inspect retained log')
