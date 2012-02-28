
from disco.core import Job, result_iterator

from disco.func import chain_reader

from optparse import OptionParser

from os import getenv

#null = (lambda v: v)

#mbps = (lambda v: v / 100000)

#kbytes = (lambda v: v * 0.45)

#hourly = (lambda v: v * 3600)

#safehourly = (lambda v: v > 100000 and 0 or v*3600)

#less100 = (lambda v: v > 100 and 0 or v)

#percent = (lambda v: v / 100)

#(grp, raw_metric): (metric, formatter)

"""
Input: "dn:grp@timestamp:metric=val,metric1=val1..."
Map: "dn" -> {Metric: Val}
"""
def map(line, params):
	mapping = {
    ('avail', 'pingok'): ('pingok', (lambda v: v)),
    ('avail', 'pingtimeout'): ('pingtimeout', (lambda v: v)),
    ('avail', 'snmpok'): ('snmpok', (lambda v: v)),
    ('avail', 'snmptimeout'): ('snmptimeout', (lambda v: v)),

    ('ping', 'rtmax'): ('delaymax', (lambda v: v)),
    ('ping', 'rtmin'): ('delaymin', (lambda v: v)),
    ('ping', 'rta'): ('delayper', (lambda v: v)),
    ('ping', 'lostRate'): ('ping_loss', (lambda v: v / 100)),

    ('acperf', 'cpuRTUsage'): ('cpuper', (lambda v: v)),
    ('acperf', 'memRTUsage'): ('memper', (lambda v: v)),
    ('acperf', 'bandWidth'): ('uplink_bandwidth', (lambda v: v / 100000)
),
    ('acperf', 'upOctets'): ('ifinoctets', (lambda v: v * 0.45)),
    ('acperf', 'downOctets'): ('ifoutoctets', (lambda v: v * 0.45)),
    ('acperf', 'discardPkts'): ('ifindiscards', (lambda v: v * 3600)),
    ('acperf', 'upInPkts'): ('ifinucastpkts', (lambda v: v * 3600)),
    ('acperf', 'upOutPkts'): ('ifoutucastpkts', (lambda v: v * 3600)),
    ('acperf', 'dHCPReqTimes'): ('dhcpreqtimes', (lambda v: v * 3600)),
    ('acperf', 'dHCPReqSucTimes'): ('dhcpreqsuctimes', (lambda v: v * 3600)),
    ('acperf', 'onlineNum'): ('sessions', (lambda v: v)),
    ('acperf', 'authNum'): ('authtotals', (lambda v: v * 3600)),
    ('acperf', 'maxNum'): ('maxsessions', (lambda v: v)),
    ('acperf', 'normalNum'): ('normaldrops', (lambda v: v * 3600)),
    ('acperf', 'deauthNum'): ('abnormaldrops', (lambda v: v * 3600)),
    ('acperf', 'authReqNum'): ('authreqtotals', (lambda v: v * 3600)),
    ('acperf', 'authSucNum'): ('authsuctotals', (lambda v: v * 3600)),
    ('acperf', 'accReqNum'): ('accreqnum', (lambda v: v * 3600)),
    ('acperf', 'accSucNum'): ('accsucnum', (lambda v: v * 3600)),
    ('acperf', 'radiusReqPkts'): ('radiusreqpkts', (lambda v: v)),
    ('acperf', 'radiusReqPkts'): ('radiusreppkts', (lambda v: v)),
    ('acperf', 'leaveReqPkts'): ('leavereqpkts', (lambda v: v)),
    ('acperf', 'leaveReqPkts'): ('leavereppkts', (lambda v: v)),
    ('acperf', 'radiusAvgDelay'): ('radiuavgdelay', (lambda v: v)),
    ('acperf', 'portalChallengeReqCount'): ('challenge_total', (lambda v: v * 3600)),
    ('acperf', 'portalChallengeRespCount'): ('challenge_suc', (lambda v: v * 3600)),
    ('acperf', 'portalAuthReqCount'): ('login_total', (lambda v: v * 3600)),
    ('acperf', 'portalAuthRespCount'): ('login_suc', (lambda v: v * 3600)),
    ('acperf', 'leaveReqCount'): ('logout_total', (lambda v: v * 3600)),
    ('acperf', 'leaveRepCount'): ('logout_suc', (lambda v: v * 3600)),
    ('acperf', 'addressCount'): ('ip_using', (lambda v: v)),
    ('acperf', 'dHCPIpPoolUsage'): ('dhcp_rate', (lambda v: v)),
    ('acperf', 'flashMemRTUsage'): ('flashper', (lambda v: v)),
    ('acperf', 'flashMemFree'): ('flashmemfree', (lambda v: v)),
    ('acperf', 'flashMemTotal'): ('flashmemtotal', (lambda v: v)),

    ('assocnum', 'assocNum'): ('assocnum', (lambda v: v * 3600)),	
    ('assocnum', 'assocSuccNum'): ('assocsuccnum', (lambda v: v * 3600)),
    ('assocnum', 'assocFailNum'): ('assocfailnum', (lambda v: v * 3600)),
    ('assocnum', 'reAssocNum'): ('reassocnum', (lambda v: v * 3600)),
    ('assocnum', 'reAssocSuccNum'): ('reassocsuccnum', (lambda v: v * 3600)),
	('assocnum', 'reAssocFailNum'): ('reassocfailnum', (lambda v: v * 3600)),
    ('assocnum', 'assocRefusedNum'): ('deauthnum', (lambda v: v * 3600)),
    ('assocnum', 'deauthNum'): ('deauthnum', (lambda v: v * 3600)),
    ('assocnum', 'assocUserNum'): ('apassocnum', (lambda v: v > 100 and 0 or v)),
    ('assocnum', 'authUserNum'): ('aponlinenum', (lambda v: v > 100 and 0 or v)),
    ('assocnum', 'cpuRTUsage'): ('cpuper', (lambda v: v)),
    ('assocnum', 'memRTUsage'): ('memper', (lambda v: v)),

	('stausers', 'stausers'): ('stausers', (lambda v: v)),

    ('wirelesstraffic', 'ifInOctets'): (['rxbytestotalmax', 'rxbytestotal'], (lambda v: v * 0.45)),
    ('wirelesstraffic', 'ifInPkts'): ('rxpacketstotal', (lambda v: v * 3600)),
    ('wirelesstraffic', 'ifInDiscards'): ('rxpacketsdropped', (lambda v: v > 100000 and 0 or v*3600)),
    ('wirelesstraffic', 'ifInUcastPkts'): ('rxpacketsunicast', (lambda v: v * 3600)),
    ('wirelesstraffic', 'ifInErrors'): ('rxpacketserror', (lambda v: v > 100000 and 0 or v*3600)),
    ('wirelesstraffic', 'ifInAvgSignal'): ('ifinavgsignal', (lambda v: v)),
    ('wirelesstraffic', 'ifInHighSignal'): ('ifinhighsignal', (lambda v: v)),
    ('wirelesstraffic', 'ifInLowSignal'): ('ifinlowsignal', (lambda v: v)),
    ('wirelesstraffic', 'ifOutOctets'): (['txbytestotal', 'txbytestotalmax'], (lambda v: v * 0.45)), 
    ('wirelesstraffic', 'ifOutPkts'): ('txpacketstotal', (lambda v: v * 3600)),
    ('wirelesstraffic', 'ifOutDiscards'): ('txpacketsdropped', (lambda v: v > 100000 and 0 or v*3600)),
    ('wirelesstraffic', 'ifOutUcastPkts,'): ('txpacketsunicast', (lambda v: v * 3600)),
    ('wirelesstraffic', 'ifOutErrors'): ('txpacketserror', (lambda v: v > 100000 and 0 or v*3600)),
    ('wirelesstraffic', 'ifFrameRetryRate'): ('ifframeretryrate', (lambda v: v)),

    ('wiredtraffic', 'ifInOctets'): (['ifinoctetsmax', 'ifinoctets'], (lambda v: v * 0.45)),
    ('wiredtraffic', 'ifInNUcastPkts'): ('ifinnucastpkts', (lambda v: v * 3600)),  
    ('wiredtraffic', 'ifInDiscards'): ('ifindiscards', (lambda v: v > 100000 and 0 or v*3600)),
	#TODO: fixme later
    ('wiredtraffic', 'ifInPkts'): ('ifinpkts', (lambda v: v * 3600)),
    ('wiredtraffic', 'ifInUcastPkts'): (['ifinpkts', 'ifinucastpkts'], (lambda v: v * 3600)),
    ('wiredtraffic', 'ifInErrors'): ('ifinerrors', (lambda v: v > 100000 and 0 or v*3600)), 
    ('wiredtraffic', 'ifOutOctets'): (['ifoutoctetsmax', 'ifoutoctets'], (lambda v: v * 0.45)),
    ('wiredtraffic', 'ifOutNUcastPkts'): ('ifoutnucastpkts', (lambda v: v * 3600)),
    ('wiredtraffic', 'ifOutDiscards'): ('ifoutdiscards', (lambda v: v > 100000 and 0 or v*3600)),
	#TODO: fixme later
    ('wiredtraffic', 'ifOutPkts'): ('ifoutpkts', (lambda v: v * 3600)),
    ('wiredtraffic', 'ifOutUcastPkts'): (['ifoutpkts', 'ifoutucastpkts'], (lambda v: v * 3600)),
    ('wiredtraffic', 'ifOutErrors'): ('ifouterrors', (lambda v: v > 100000 and 0 or v*3600)),

    ('intftraffic', 'ifInOctets'): ('ifinoctets', (lambda v: v * 0.45)),
	('intftraffic', 'ifInNUcastPkts'): ('ifinnucastpkts', (lambda v: v * 3600)),
	('intftraffic', 'ifInDiscards'): ('ifindiscards', (lambda v: v * 3600)),
	#TODO: fixme later
	('intftraffic', 'ifInPkts'): ('ifInPkts', (lambda v: v * 3600)),
	('intftraffic', 'ifInUcastPkts'): ('ifinucastpkts', (lambda v: v * 3600)),
	('intftraffic', 'ifInErrors'): ('ifinerrors', (lambda v: v * 3600)),
	('intftraffic', 'ifOutOctets'): ('ifoutoctets', (lambda v: v * 0.45)),
	('intftraffic', 'ifOutNUcastPkts'): ('ifoutnucastpkts', (lambda v: v * 3600)),
	('intftraffic', 'ifOutDiscards'): ('ifoutdiscards', (lambda v: v * 3600)),
	#TODO: fixme later
	('intftraffic', 'ifOutPkts'): ('ifOutPkts', (lambda v: v * 3600)),
	('intftraffic', 'ifOutUcastPkts'): ('ifoutucastpkts', (lambda v: v * 3600)),
	('intftraffic', 'ifOutErrors'): ('ifouterrors', (lambda v: v * 3600)),
	('intftraffic', 'ifUpDwnTimes'): ('itfupdownnums', (lambda v: v * 3600)),

    ('swcpu', 'cpuLoad1'): (['cpuper', 'cpumax'], (lambda v: v)),
    ('swmem', 'memUsage'): (['memper', 'memmax'], (lambda v: v)),

	#TODO: FIXME later
	('dhcppool', 'dhcpUsage'): ('dhcpusage', (lambda v: v)),

    ('ssidwireless', 'ifInOctets'): ('rxbytestotal', (lambda v: v * 0.45)),
    ('ssidwireless', 'ifInPkts'): ('rxpacketstotal', (lambda v: v * 3600)),
    ('ssidwireless', 'ifOutOctets'): ('txbytestotal', (lambda v: v * 0.45)),
    ('ssidwireless', 'ifOutPkts'): ('txpacketstotal', (lambda v: v * 3600))}


	key, line = tuple(line.split("@"))
	dn, grp = tuple(key.rsplit(":", 1))
	time, line = tuple(line.split(":"))
	metrics = line.split(",")
	result = {}
	for metric in metrics:
	    raw_name, val = tuple(metric.split("="))
	    name, format = mapping[(grp, raw_name)]
	    names = type(name) == type('') and [name] or name
	    for name in names:
			result[name] = format(float(val)) 
	yield dn, result

"""
Reduce: "Dn" -> {Metric, Val}
"""
def reduce(iter, params):
	from disco.util import kvgroup
	for dn, metrics in kvgroup(sorted(iter)):
		dataset = {}
		for metric in metrics:
			for name, val in metric.iteritems():
				if dataset.has_key(name):
					dataset[name].append(val)
				else:
					dataset[name] = []
		yield dn, dataset

if __name__ == '__main__':
    parser = OptionParser(usage='%prog [options] hour')
    parser.add_option('--disco-master',
                      default=getenv('DISCO_MASTER'),
                      help='Disco master')

    (options, hour) = parser.parse_args()
	#get time of now
    job = Job().run(input=["http://localhost:9999/journal/25/1.journal",
			"http://localhost:9999/journal/25/2.journal",
			"http://localhost:9999/journal/25/3.journal",
			"http://localhost:9999/journal/25/4.journal",
			"http://localhost:9999/journal/25/5.journal",
			"http://localhost:9999/journal/25/6.journal",
			"http://localhost:9999/journal/25/7.journal",
			"http://localhost:9999/journal/25/8.journal",
			"http://localhost:9999/journal/25/12.journal",
			"http://localhost:9999/journal/25/11.journal"],
			map=map,
			reduce=reduce,
			partitions=16,
			merge_partitions=False)
    out = file("out.txt", "w")
    for dn, metrics in result_iterator(job.wait(show=True)): 
		list = [name + '=' + '|'.join([str(v) for v in values])
			for name, values in metrics.iteritems()]
		print >>out, dn, ":", ",".join(list)
    out.close()


