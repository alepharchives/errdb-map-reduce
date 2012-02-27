from disco.core import Job, result_iterator
from disco.func import chain_reader
from optparse import OptionParser
from os import getenv

wifi_ap_mapping = {
    'avail:pingok': (lambda v: v),
    'avail:pingtimeout': (lambda v: v),
    'avail:snmpok': (lambda v: v),
    'avail:snmptimeout': (lambda v: v),
    'assocnum:assocNum': (lambda v: v*3600),	#assocnum
    'assocnum:assocSuccNum': (lambda v: v*3600),#assocsuccnum
    'assocnum:reAssocNum': (lambda v: v*3600),	#reassocnum
    'assocnum:reAssocSuccNum': (lambda v: v*3600),
    'assocnum:assocRefusedNum': (lambda v: v*3600),
    'assocnum:deauthNum': (lambda v: v*3600),
    'wirelesstraffic:ifInOctets': ,  #rxbytestotalmax,rxbytestotal
    'wirelesstraffic:ifInPkts': ,
    'wirelesstraffic:ifInDiscards': ,
    'wirelesstraffic:ifInUcastPkts': ,
    'wirelesstraffic:ifInErrors': ,
    'wirelesstraffic:ifInAvgSignal': ,
    'wirelesstraffic:ifInHighSignal': ,
    'wirelesstraffic:ifInLowSignal': ,
    'wirelesstraffic:ifOutOctets': , #txbytestotal, txbytestotalmax
    'wirelesstraffic:ifOutPkts': ,
    'wirelesstraffic:ifOutDiscards': ,
    'wirelesstraffic:ifOutUcastPkts,': 
    'wirelesstraffic:ifOutErrors': ,
    'wirelesstraffic:ifFrameRetryRate': ,
    'assocnum:assocUserNum': (lambda v: v >= 100 ? 0 : v), #apassocnum
    'assocnum:authUserNum': (lambda v: v >= 100 ? 0 : v), #aponlinenum
	'wiredtraffic:ifInOctets': (lambda v: v*0.45), #ifinoctets, ifinoctetsmax,
	'wiredtraffic:ifInNUcastPkts': (lambda v: v*3600),  #ifinpkts,
	'wiredtraffic:ifInDiscards': (lambda v: v>100000 ? 0:v*0.45),   #ifindiscards,
	'wiredtraffic:ifInUcastPkts': (lambda v: v*0.45),  #ifinpkts, ifinucastpkts,
	'wiredtraffic:ifInErrors':(lambda v: v>100000 ? 0:v*0.45),  #ifinerrors,
	'wiredtraffic:ifOutOctets':(lambda v: v*0.45),  #ifoutoctets, ifoutoctetsmax,
	'wiredtraffic:ifOutNUcastPkt': (lambda v: v*0.45), #ifoutpkts,
	'wiredtraffic:ifOutDiscards': (lambda v: v>100000 ? 0:v*0.45),  #ifoutdiscards,
	'wiredtraffic:ifOutUcastPkts': (lambda v: v*0.45), #ifoutucastpkts,
	'wiredtraffic:ifOutErrors': (lambda v: v>100000 ? 0:v*0.45),  #ifouterrors,
    'delayper': (lambda v: v),
    'assocnum:cpuRTUsage': (lambda v: v), #cpuper,
    'assocnum:memRTUsage': (lambda v: v)#memper]).
}


"""
Input: "dn:grp@timestamp:metric=val,metric1=val1..."
Map: "dn:grp" -> {Metric: Val, Metric1: Val1, ...}
"""
def map1(line, params):
	key, line = tuple(line.split("@"))
	time, line = tuple(line.split(":"))
	tokens = line.split(",")
	metric = {}
	for token in tokens:
		k,v = tuple(token.split("="))
		metric[k] = float(v)
	yield key, metric

"""
Reduce: "Dn:Grp" -> {Metric: Avg=Val/Count, ...}
"""
def reduce1(iter, params):
	from disco.util import kvgroup
	for key, metrics in kvgroup(sorted(iter)):
		aggre = {}
		count = 0
		for metric in metrics:
			count = count + 1
			for name, val in metric.iteritems():
				if aggre.has_key(name):
					aggre[name] = aggre[name]+val
				else:
					aggre[name] = val
		for name, val in aggre.iteritems():
			aggre[name] = aggre[name] / count

		aggre["count"] = count

		yield key, aggre

"""
Input: dn:grp -> {metric: aggre, metric1: aggre1}
Map2: "Dn" -> {"Grp:Metric": Avg, ...}
"""
def map2((key, metric), params):
	dn, grp = tuple(key.rsplit(":", 1))
	metric1 = {}
	for name, val in metric.iteritems():
		metric1[grp+":"+name] = val
	yield dn, metric1

"""
TODO: Reduce2. "Dn" -> "V1|V2|V3..."
"""
def reduce2(iter, params):
	from disco.util import kvgroup
	for dn, metrics in kvgroup(sorted(iter)):
		dataset = []
		for metric in metrics:
			dataset += metric.items()
		yield dn, str(dict(dataset))

if __name__ == '__main__':
    parser = OptionParser(usage='%prog [options] hour')
    parser.add_option('--disco-master',
                      default=getenv('DISCO_MASTER'),
                      help='Disco master')

    (options, hour) = parser.parse_args()
    print hour
	
	#get time of now

"""
    job = Job().run(input=["http://localhost:9999/journal/25/1.journal",
						#"http://localhost:9999/journal/25/2.journal",
						#"http://localhost:9999/journal/25/3.journal",
						#"http://localhost:9999/journal/25/4.journal",
						#"http://localhost:9999/journal/25/5.journal",
						#"http://localhost:9999/journal/25/6.journal",
						#"http://localhost:9999/journal/25/7.journal",
						#"http://localhost:9999/journal/25/8.journal"
					],
					map=map1,
					reduce=reduce1,
					partitions=8,
					merge_partitions=False)
    job = Job().run(input=job.wait(show=True),
			map_reader=chain_reader,
			map=map2,
			partitions=4,
			reduce=reduce2,
			merge_partitions=False)
    out = file("out.txt", "w")
    for word, count in result_iterator(job.wait(show=True)): #
        print >>out, word, ":", count
    out.close()
"""


