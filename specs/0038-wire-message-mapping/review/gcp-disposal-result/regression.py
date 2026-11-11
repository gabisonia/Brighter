from work import *
from concurrent.futures import ThreadPoolExecutor
from collections import Counter

def matrix(framework):
    label='full-'+framework
    broker=fixture+'-broker-awake-'+label.replace('.','-')
    setup(broker)
    try:
        result=run(label,framework,fullfilter,broker)
        baseline=Path('/private/tmp/brighter-4170-gcp-scheduled-publication')/('final-full-'+framework+'.trx')
        previous=Counter(x.attrib['testName'] for x in E.parse(baseline).findall('.//{*}UnitTestResult'))
        current=Counter(x.attrib['testName'] for x in E.parse(p/(label+'.trx')).findall('.//{*}UnitTestResult'))
        assert current==previous,(framework,'Test inventory changed')
        assert not result['clock_discontinuity']
        return {'framework':framework,'all_512_identities_retained':True,'result':result}
    finally:
        command(['docker','logs',broker],broker+'-broker',60)
        assert command(['docker','stop',broker],broker+'-stop',60)==0

keep_awake=subprocess.Popen(['caffeinate','-i','-s'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
try:
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(matrix,['net9.0','net10.0']))
finally:
    keep_awake.terminate()
    keep_awake.wait(timeout=10)
(p/'regression-verification.json').write_text(json.dumps(results,indent=2))
