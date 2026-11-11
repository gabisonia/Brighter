from work import *
import hashlib

src=r/'tests/Paramore.Brighter.Gcp.Tests/MessagingGateway/GcpStreamMessageGatewayProvider.cs'
original=src.read_bytes()
(p/'original-provider.cs').write_bytes(original)
s=original.decode('utf-8-sig')
old='''            StreamConfiguration = cfg =>
            {
                cfg.EmulatorDetection = EmulatorDetection.EmulatorOrProduction;
            },'''
new='''            StreamConfiguration = cfg =>
            {
                cfg.EmulatorDetection = EmulatorDetection.EmulatorOrProduction;
                var generation = System.Threading.Interlocked.Increment(ref diagnosticGeneration);
                cfg.ApiSettings ??= new Google.Cloud.PubSub.V1.SubscriberServiceApiSettings();
                cfg.ApiSettings.Interceptor = new GcpStreamRpcObserver(line =>
                {
                    lock (typeof(GcpStreamMessageGatewayProvider))
                    {
                        System.IO.File.AppendAllText("/results/original-shutdown-rpc-net" + Environment.Version.Major + ".log",
                            "provider=" + diagnosticProvider + " generation=" + generation + " " + line + Environment.NewLine);
                    }
                });
            },'''
constructor='''    public GcpStreamMessageGatewayProvider()
    {'''
assert s.count(old)==1 and s.count(constructor)==1
try:
    s=s.replace(constructor,constructor+'''
        var diagnosticProvider = Guid.NewGuid().ToString("N");
        var diagnosticGeneration = 0;''').replace(old,new)
    src.write_text(s)
    (p/'observed-provider.cs').write_bytes(src.read_bytes())
    assert build('observed')==0
finally:
    src.write_bytes(original)
assert src.read_bytes()==original
(p/'source-restoration.json').write_text(json.dumps({'provider_restored_before_execution':True,'sha256':hashlib.sha256(original).hexdigest()},indent=2))
for framework in ['net9.0','net10.0']:
    broker=fixture+'-focus-'+framework.replace('.','-')
    setup(broker)
    try:
        result=run('focus-'+framework,framework,'FullyQualifiedName~When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message|FullyQualifiedName~When_a_gcp_stream_channel_is_disposed_with_a_redelivery_buffered_after_requeue',broker)
    finally:
        command(['docker','logs',broker],broker+'-broker',60)
        assert command(['docker','stop',broker],broker+'-stop',60)==0
    assert result['exit']==0 and result['counts']['Passed']==4
broker=fixture+'-full-net9'
setup(broker)
try:
    run('observed-full-net9.0','net9.0',fullfilter,broker)
finally:
    command(['docker','logs',broker],broker+'-broker',60)
    assert command(['docker','stop',broker],broker+'-stop',60)==0
assert src.read_bytes()==original
