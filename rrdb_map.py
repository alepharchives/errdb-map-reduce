
from disco.core import Job, result_iterator
from disco.func import chain_reader
from optparse import OptionParser
from os import getenv

null = (lambda v: v)

mbps = (lambda v: v / 100000)

kbytes = (lambda v: v * 0.45)

hourly = (lambda v: v * 3600)

safehourly = (lambda v: v > 100000 and 0 or v*3600)

less100 = (lambda v: v > 100 and 0 or v)

#(grp, raw_metric): (metric, formatter)
metric_mapping = {
    ('avail', 'pingok'): ('pingok', null),
    ('avail', 'pingtimeout'): ('pingtimeout', null),
    ('avail', 'snmpok'): ('snmpok', null),
    ('avail', 'snmptimeout'): ('snmptimeout', null),

    ('ping', 'rtmax'): ('delaymax', null),
    ('ping', 'rtmin'): ('delaymin', null),
    ('ping', 'rta'): ('delayper', null),
    ('ping', 'lostRate'): ('ping_loss', percent),

    ('acperf', 'cpuRTUsage'): ('cpuper', null),
    ('acperf', 'memRTUsage'): ('memper', null),
    ('acperf', 'bandWidth'): ('uplink_bandwidth', mbps),
    ('acperf', 'upOctets'): ('ifinoctets', kbytes),
    ('acperf', 'downOctets'): ('ifoutoctets', kbytes),
    ('acperf', 'discardPkts'): ('ifindiscards', hourly),
    ('acperf', 'upInPkts'): ('ifinucastpkts', hourly),
    ('acperf', 'upOutPkts'): ('ifoutucastpkts', hourly),
    ('acperf', 'dHCPReqTimes'): ('dhcpreqtimes', hourly),
    ('acperf', 'dHCPReqSucTimes'): ('dhcpreqsuctimes', hourly),
    ('acperf', 'onlineNum'): ('sessions', null),
    ('acperf', 'authNum'): ('authtotals', hourly),
    ('acperf', 'maxNum'): ('maxsessions', null),
    ('acperf', 'normalNum'): ('normaldrops', hourly),
    ('acperf', 'deauthNum'): ('abnormaldrops', hourly),
    ('acperf', 'authReqNum'): ('authreqtotals', hourly),
    ('acperf', 'authSucNum'): ('authsuctotals', hourly),
    ('acperf', 'accReqNum'): ('accreqnum', hourly),
    ('acperf', 'accSucNum'): ('accsucnum', hourly),
    ('acperf', 'radiusReqPkts'): ('radiusreqpkts', null),
    ('acperf', 'radiusReqPkts'): ('radiusreppkts', null),
    ('acperf', 'leaveReqPkts'): ('leavereqpkts', null),
    ('acperf', 'leaveReqPkts'): ('leavereppkts', null),
    ('acperf', 'radiusAvgDelay'): ('radiuavgdelay', null),
    ('acperf', 'portalChallengeReqCount'): ('challenge_total', hourly),
    ('acperf', 'portalChallengeRespCount'): ('challenge_suc', hourly),
    ('acperf', 'portalAuthReqCount'): ('login_total', hourly),
    ('acperf', 'portalAuthRespCount'): ('login_suc', hourly),
    ('acperf', 'leaveReqCount'): ('logout_total', hourly),
    ('acperf', 'leaveRepCount'): ('logout_suc', hourly),
    ('acperf', 'addressCount'): ('ip_using', null),
    ('acperf', 'dHCPIpPoolUsage'): ('dhcp_rate', null),
    ('acperf', 'flashMemRTUsage'): ('flashper', null),

    ('assocnum', 'assocNum'): ('assocnum', hourly),	
    ('assocnum', 'assocSuccNum'): ('assocsuccnum', hourly),
    ('assocnum', 'reAssocNum'): ('reassocnum', hourly),
    ('assocnum', 'reAssocSuccNum'): ('reassocsuccnum', hourly),
    ('assocnum', 'assocRefusedNum'): ('deauthnum', hourly),
    ('assocnum', 'deauthNum'): ('deauthnum', hourly),
    ('assocnum', 'assocUserNum'): ('apassocnum', less100),
    ('assocnum', 'authUserNum'): ('aponlinenum', less100),
    ('assocnum', 'cpuRTUsage'): ('cpuper', null),
    ('assocnum', 'memRTUsage'): ('memper', null),

    ('wirelesstraffic', 'ifInOctets'): (['rxbytestotalmax', 'rxbytestotal'], kbytes),
    ('wirelesstraffic', 'ifInPkts'): ('rxpacketstotal', hourly),
    ('wirelesstraffic', 'ifInDiscards'): ('rxpacketsdropped', safehourly),
    ('wirelesstraffic', 'ifInUcastPkts'): ('rxpacketsunicast', hourly),
    ('wirelesstraffic', 'ifInErrors'): ('rxpacketserror', safehourly),
    ('wirelesstraffic', 'ifInAvgSignal'): ('ifinavgsignal', null),
    ('wirelesstraffic', 'ifInHighSignal'): ('ifinhighsignal', null),
    ('wirelesstraffic', 'ifInLowSignal'): ('ifinlowsignal', null),
    ('wirelesstraffic', 'ifOutOctets'): (['txbytestotal', 'txbytestotalmax'], kbytes), 
    ('wirelesstraffic', 'ifOutPkts'): ('txpacketstotal', hourly),
    ('wirelesstraffic', 'ifOutDiscards'): ('txpacketsdropped', safehourly),
    ('wirelesstraffic', 'ifOutUcastPkts,'): ('txpacketsunicast', hourly),
    ('wirelesstraffic', 'ifOutErrors'): ('txpacketserror', safehourly),
    ('wirelesstraffic', 'ifFrameRetryRate'): ('ifframeretryrate', null),

    ('wiredtraffic', 'ifInOctets'): (['ifinoctetsmax', 'ifinoctets'], kbytes),
    ('wiredtraffic', 'ifInNUcastPkts'): ('ifinnucastpkts', hourly),  
    ('wiredtraffic', 'ifInDiscards'): ('ifindiscards', safehourly),
    ('wiredtraffic', 'ifInUcastPkts'): (['ifinpkts', 'ifinucastpkts'], hourly),
    ('wiredtraffic', 'ifInErrors'): ('ifinerrors', safehourly), 
    ('wiredtraffic', 'ifOutOctets'): (['ifoutoctetsmax', 'ifoutoctets'], kbytes),
    ('wiredtraffic', 'ifOutNUcastPkt'): ('ifoutnucastpkts', hourly),
    ('wiredtraffic', 'ifOutDiscards'): ('ifoutdiscards', safehourly),
    ('wiredtraffic', 'ifOutUcastPkts'): (['ifoutpkts', 'ifoutucastpkts'], 'hourly'),
    ('wiredtraffic', 'ifOutErrors'): ('ifouterrors', safehourly),

    ('intftraffic', 'ifInOctets'): ('ifinoctets', kbytes),
	('intftraffic', 'ifInNUcastPkts'): ('ifinnucastpkts', hourly),
	('intftraffic', 'ifInDiscards'): ('ifindiscards', hourly),
	('intftraffic', 'ifInUcastPkts'): ('ifinucastpkts', hourly),
	('intftraffic', 'ifInErrors'): ('ifinerrors', hourly),
	('intftraffic', 'ifOutOctets'): ('ifoutoctets', kbytes),
	('intftraffic', 'ifOutNUcastPkts'): ('ifoutnucastpkts', hourly),
	('intftraffic', 'ifOutDiscards'): ('ifoutdiscards', hourly),
	('intftraffic', 'ifOutUcastPkts'): ('ifoutucastpkts', hourly),
	('intftraffic', 'ifOutErrors'): ('ifouterrors', hourly),
	('intftraffic', 'ifUpDwnTimes'): ('itfupdownnums', hourly),

    ('swcpu', 'cpuLoad1'): (['cpuper', 'cpumax'], null),
    ('swmem', 'memUsage'): (['memper', 'memmax'], null),

    ('ssidwireless', 'ifInOctets'): ('rxbytestotal', kbytes),
    ('ssidwireless', 'ifInPkts'): ('rxpacketstotal', hourly),
    ('ssidwireless', 'ifOutOctets'): ('txbytestotal', kbytes),
    ('ssidwireless', 'ifOutPkts'): ('txpacketstotal', hourly)
}

"""
Input: "dn:grp@timestamp:metric=val,metric1=val1..."
Map: "dn" -> {Metric: Val}
"""
def map(line, params):
	key, line = tuple(line.split("@"))
	dn, grp = tuple(key.rsplit(":", 1))
	time, line = tuple(line.split(":"))
	metrics = line.split(",")
	for metric in metric:
	    raw_name, val = tuple(metric.split("="))
		name, format = metric_mapping[(grp, raw_name)]
		names = type(name) == type('') and [name] or name
		for name in names:
			yield dn, {name: format(float(val))}

"""
Reduce: "Dn" -> {Metric, Val}
"""
def reduce(iter, params):
	from disco.util import kvgroup
	for dn, metrics in kvgroup(sorted(iter)):
		dataset = {}
		for metric in metrics:
			print dn, metric
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
    print hour
	#get time of now
    job = Job().run(input=["http://localhost:9999/journal/25/1.journal",
			"http://localhost:9999/journal/25/2.journal",
			"http://localhost:9999/journal/25/3.journal",
			"http://localhost:9999/journal/25/4.journal",
			"http://localhost:9999/journal/25/5.journal",
			"http://localhost:9999/journal/25/6.journal",
			"http://localhost:9999/journal/25/7.journal",
			"http://localhost:9999/journal/25/8.journal",
			"http://localhost:9999/journal/25/11.journal"],
			map=map1,
			reduce=reduce1,
			partitions=8,
			merge_partitions=False)
    out = file("out.txt", "w")
    for dn, metrics in result_iterator(job.wait(show=True)): #
        print >>out, dn, ":", str(metrics)
    out.close()

