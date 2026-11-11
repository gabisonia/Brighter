from work import *
import hashlib,difflib,atexit

keep_awake=subprocess.Popen(['caffeinate','-i','-s','-w',str(os.getpid())],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
def stop_keep_awake():
    keep_awake.terminate()
    keep_awake.wait(timeout=10)
atexit.register(stop_keep_awake)

main=Path('/Users/gabisonia/RiderProjects/Brighter')
review=main/'specs/0038-wire-message-mapping/review/gcp-disposal-result'
names=[
 'When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_should_return_promptly_async.cs',
 'When_a_gcp_stream_channel_is_disposed_with_a_redelivery_buffered_after_requeue_should_return_promptly_async.cs']
prefix=Path('tests/Paramore.Brighter.Gcp.Tests/MessagingGateway/Stream')
expected=[]
for name in names:
    relative=prefix/name
    before=(p/name).read_bytes()
    assert (main/relative).read_bytes()==before,'Main test changed during validation'
    after=(r/relative).read_bytes()
    target='            Assert.True(disposed, $"Disposing the stream channel did not return within {s_disposeBound}");'
    assert after.decode()==before.decode().replace(target,target+'\n            await disposing;')
    expected.append((relative,after))
production='src/Paramore.Brighter.MessagingGateway.GcpPubSub/GcpPubSubStreamMessageConsumer.cs'
assert (main/production).read_bytes()==(r/production).read_bytes()==(p/'GcpPubSubStreamMessageConsumer.cs').read_bytes()
assert (p/'regression-verification.json').exists()
for relative,after in expected:
    (main/relative).write_bytes(after)

args=['dotnet','build','tests/Paramore.Brighter.Gcp.Tests','-c','Release','--no-restore','-m:1','-nodeReuse:false','-t:Rebuild',f'-p:BaseOutputPath={p}/main-bin/']
assert command(args,'main-build',600,cwd=main)==0
filter='FullyQualifiedName~When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_async_should_return_promptly|FullyQualifiedName~When_a_gcp_stream_channel_is_disposed_with_a_redelivery_buffered_after_requeue_async_should_return_promptly'
for framework in ['net9.0','net10.0']:
    broker=fixture+'-broker-main-'+framework.replace('.','-')
    setup(broker)
    try:
        result=run('main-'+framework,framework,filter,broker,assembly_root='main-bin')
        assert result['counts']=={'Passed':2,'Failed':0,'NotExecuted':0},result
        assert not result['clock_discontinuity']
    finally:
        command(['docker','logs',broker],broker+'-broker',60)
        assert command(['docker','stop',broker],broker+'-stop',60)==0
manifest=json.loads((p/'source-before.json').read_text())
changed=[name for name,h in manifest.items() if hashlib.sha256((main/name).read_bytes()).hexdigest()!=h]
assert set(changed)=={str(x[0]) for x in expected},changed
data={'source_files_checked':len(manifest),'changed_sources':changed,'production_restored':True,'main_focused_passes_per_framework':2,'committed':False,'pushed':False}
(p/'final-verification.json').write_text(json.dumps(data,indent=2))
for pattern in ['*.json','*.py']:
    for source in p.glob(pattern):
        if source.name=='source-before.json':continue
        (review/source.name).write_bytes(source.read_bytes())
patch=''
for name in names:
    patch+=''.join(difflib.unified_diff((p/name).read_text().splitlines(keepends=True),(main/prefix/name).read_text().splitlines(keepends=True),fromfile='a/'+str(prefix/name),tofile='b/'+str(prefix/name)))
(review/'await-disposal.patch').write_text(patch)
print(json.dumps(data),flush=True)
