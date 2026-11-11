from work import *

production=r/'src/Paramore.Brighter.MessagingGateway.GcpPubSub/GcpPubSubStreamMessageConsumer.cs'
testnames=[
 'When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_should_return_promptly_async.cs',
 'When_a_gcp_stream_channel_is_disposed_with_a_redelivery_buffered_after_requeue_should_return_promptly_async.cs']
tests=[r/'tests/Paramore.Brighter.Gcp.Tests/MessagingGateway/Stream'/name for name in testnames]
original=production.read_bytes()
filter='FullyQualifiedName~When_a_gcp_stream_channel_is_disposed_holding_an_unsettled_message_async_should_return_promptly|FullyQualifiedName~When_a_gcp_stream_channel_is_disposed_with_a_redelivery_buffered_after_requeue_async_should_return_promptly'

def observe(label,framework):
    broker=fixture+'-broker-'+label
    setup(broker)
    try:return run(label,framework,filter,broker)
    finally:
        command(['docker','logs',broker],broker+'-broker',60)
        assert command(['docker','stop',broker],broker+'-stop',60)==0

try:
    source=original.decode('utf-8-sig')
    target='        await _router.DisposeAsync();'
    assert source.count(target)==1
    field='    private int _disposed;'
    assert source.count(field)==1
    source=source.replace(field,field+'\n    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, bool> s_disposalMutation = new();')
    production.write_text(source.replace(target,target+'\n        if (s_disposalMutation.TryAdd(subscriptionName.ToString(), true))\n            throw new InvalidOperationException("disposal-result-mutation");'))
    assert build('original-with-mutation')==0
    result=observe('original-with-mutation-net9','net9.0')
    assert result['counts']=={'Passed':2,'Failed':0,'NotExecuted':0},result
    for test in tests:
        source=test.read_text(encoding='utf-8-sig')
        target='            Assert.True(disposed, $"Disposing the stream channel did not return within {s_disposeBound}");'
        assert source.count(target)==1
        test.write_text(source.replace(target,target+'\n            await disposing;'))
    assert build('awaited-with-mutation')==0
    for framework in ['net9.0','net10.0']:
        result=observe('awaited-with-mutation-'+framework.replace('.','-'),framework)
        assert result['counts']=={'Passed':0,'Failed':2,'NotExecuted':0},result
        assert all('disposal-result-mutation' in f['message'] for f in result['failures'])
finally:
    production.write_bytes(original)
assert production.read_bytes()==original
assert build('restored')==0
for framework in ['net9.0','net10.0']:
    result=observe('restored-'+framework.replace('.','-'),framework)
    assert result['counts']=={'Passed':2,'Failed':0,'NotExecuted':0},result
