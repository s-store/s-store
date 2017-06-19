#!/usr/bin/env python
import sys, argparse
import os, datetime
import re
import math
import time
import string
from array import *

def parseStatsFile(filename, partitioncol, outcols):
	result = {}
	cols = {}
	firstline = True

	f = open(filename, 'r')

	for line in f:
		if firstline:
			index = 0
			for col in line.split(","):
				col = col.replace("\"","").replace("\'", "")
				if col in outcols:
					cols[col] = index
				if col == partitioncol:
					cols[partitioncol] = index
				index+=1
			firstline = False
			continue

		spl = line.split(",")
		lnresult = {}
		lnname = spl[cols[partitioncol]]
		if '@' in lnname:
			continue
		for colname in outcols:
			val = spl[cols[colname]]
			val = val.replace("\"","").replace("\'", "")
			lnresult[colname] = int(val)
		result[lnname] = lnresult

	f.close()
	return result

def findStats(filename, partitioncol, outcols):
	endResult = parseStatsFile(filename, partitioncol, outcols)
	warmupfile = filename.replace(".csv", "-warmup.csv").replace(".txt","-warmup.txt")
	firstResult = parseStatsFile(warmupfile, partitioncol, outcols)
	result = {}
	for key in endResult.keys():
		lnresult = {}
		rowend = endResult[key]
		if key not in firstResult:
			lnresult = rowend
		else:
			rowbegin = firstResult[key]
			for col in outcols:
				lnresult[col] = rowend[col] - rowbegin[col]
		result[key] = lnresult

	return result


def generateReport(benchmark_result):
	anlyze_result = []

	#clearing to get pure json snippet
	benchmark_result = benchmark_result.replace("  [java] "," ")
	strbegin = "<json>"
	strend = "</json>"
	output  = re.compile('<json>(.*?)</json>', re.DOTALL |  re.IGNORECASE).findall(benchmark_result)

	try:
		jsonsnippet = str(output[0])
		jsonsnippet = jsonsnippet.replace("\n","")
		jsonsnippet = jsonsnippet.replace("\"","")
		
		# get 	THROUGHPUT
		output  = re.compile('TXNTOTALPERSECOND: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		THROUGHPUT = str(output[0])
		basic = " " + THROUGHPUT
		anlyze_result.append(THROUGHPUT)
		# get 	AVG LATENCY
		output  = re.compile('TOTALAVGLATENCY: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		AVGLATENCY = str(output[0])
		basic += " " + AVGLATENCY
		anlyze_result.append(AVGLATENCY)
		
		# total transaction count
		output  = re.compile('TXNTOTALCOUNT: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TXNTOTALCOUNT = str(output[0])
		basic += " " + TXNTOTALCOUNT
		anlyze_result.append(TXNTOTALCOUNT)
		# DTXNTOTALCOUNT
		output  = re.compile('DTXNTOTALCOUNT: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		DTXNTOTALCOUNT = str(output[0])
		basic += " " + DTXNTOTALCOUNT
		anlyze_result.append(DTXNTOTALCOUNT)
		# SPECEXECTOTALCOUNT
		output  = re.compile('SPECEXECTOTALCOUNT: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		SPECEXECTOTALCOUNT = str(output[0])
		basic += " " + SPECEXECTOTALCOUNT
		anlyze_result.append(SPECEXECTOTALCOUNT)

		# TXNMINPERSECOND 
		output  = re.compile('TXNMINPERSECOND: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TXNMINPERSECOND = str(output[0])
		basic += " " + TXNMINPERSECOND
		anlyze_result.append(TXNMINPERSECOND)
		# TXNMAXPERSECOND
		output  = re.compile('TXNMAXPERSECOND: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TXNMAXPERSECOND = str(output[0])
		basic += " " + TXNMAXPERSECOND
		anlyze_result.append(TXNMAXPERSECOND)
		# STDDEVTXNPERSECOND
		output  = re.compile('STDDEVTXNPERSECOND: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		STDDEVTXNPERSECOND = str(output[0])
		basic += " " + STDDEVTXNPERSECOND
		anlyze_result.append(STDDEVTXNPERSECOND)
		
		# TOTALMINLATENCY
		output  = re.compile('TOTALMINLATENCY: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TOTALMINLATENCY = str(output[0])
		basic += " " + TOTALMINLATENCY
		anlyze_result.append(TOTALMINLATENCY)
		# TOTALMAXLATENCY
		output  = re.compile('TOTALMAXLATENCY: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TOTALMAXLATENCY = str(output[0])
		basic += " " + TOTALMAXLATENCY
		anlyze_result.append(TOTALMAXLATENCY)
		# TOTALSTDEVLATENCY
		output  = re.compile('TOTALSTDEVLATENCY: (.*?),', re.DOTALL |  re.IGNORECASE).findall(jsonsnippet)
		TOTALSTDEVLATENCY = str(output[0])
		basic += " " + TOTALSTDEVLATENCY
		anlyze_result.append(TOTALSTDEVLATENCY)

		print basic

	except:
		print "Report Failed"
	
	
	#analyze the content
	#print "This benchmark result is :", jsonsnippet
	return anlyze_result
##enddef

def getReportFromList(list):
	report = ""
	for item in list:
		report += " " + item
	return report
##enddef

def getNumReportFromList(list):
	report = []
	for i in range(0,len(list)):
		report.append(float(list[i]))
	return report
##enddef

# get mean, std for array
def getMeanAndStd(a):
	n = len(a)
	mean = sum(a) / n
	std = math.sqrt(sum((x-mean)**2 for x in a) / n) 
	return mean, std
##enddef

# get the args from command line
# set default values for parameters needed by script
timestamp = datetime.datetime.now()
t = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, timestamp.second
defaultoutput = 'experiment' + '_'.join(str(i) for i in t)+ '.txt'
TXNFILE = "txncounters.csv"
TXNPARTITIONCOL = "PROCEDURE"
TXNOUTCOLS = ["RECEIVED", "REJECTED", "REDIRECTED", "EXECUTED", "COMPLETED"]
# get paramets from command line input

parser = argparse.ArgumentParser(description='This is a benchmark auto-run script, made by hawk.')
parser.add_argument('-p','--project', help='Benchmark name', default='tpcc')
parser.add_argument('-o','--output', help='output file', default=defaultoutput)
parser.add_argument('--stop', help='indicate if the threshold will be used to stop expeiments', action='store_true')
parser.add_argument('--blocking', help='indicate if system will blocking', action='store_true')
parser.add_argument('--log', help='indicate if system will run log', action='store_true')
parser.add_argument('--threads', help='threads per host', type=int, default=1)
parser.add_argument('--txnthreshold', help='percentage difference between txnrate and throughput', type=float, default='0.95')
#parser.add_argument('--tmax', help='max - thread per host', type=int, default=1)
#parser.add_argument('--tstep', help='starting step size - thread per host', type=int, default=5)
parser.add_argument('--rmin', help='min - txnrate', type=int, default=1000)
parser.add_argument('--rmax', help='max - txnrate', type=int, default=99999999)
parser.add_argument('--rstep', help='initial step size - txnrate', type=int, default=100)
parser.add_argument('--finalrmin', help='the absolute lowest txnrate to consider', type=int, default=1)
parser.add_argument('--finalrstep', help='final step size- txnrate', type=int, default=0)
parser.add_argument('--logtimeout', help='log timeout', type=int, default=10)
parser.add_argument('--groupcommit', help='log timeout', type=int, default=-1)
#parser.add_argument('--lmax', help='max - log timeout', type=int, default=10)
#parser.add_argument('--lstep', help='step - log timeout', type=int, default=10)
parser.add_argument('--warmup', help='warmup - time in ms', type=int, default=10000)
parser.add_argument('--numruns', help='number of experiment runs', type=int, default=3)
parser.add_argument('-e', '--expout', help='file that contains the final experiment results', default='expout.txt')
parser.add_argument('--winconfig', help='description of the window configuration', default='')
parser.add_argument('--debug', help='debug mode, only runs once', action='store_true')
parser.add_argument('--aries', help='turns on Aries logging', action='store_true')
parser.add_argument('--hscheduler', help='turns off S-Store scheduler', action='store_true')
parser.add_argument('--hstore', help='turns off S-Store', action='store_true')
parser.add_argument('--ftrigger_off', help='turns off frontend triggers', action='store_true')
parser.add_argument('--weakrecovery_off', help='sets strong or weak recovery', action='store_true')
parser.add_argument('--rejected_txns', help='changes the "quit" condition to whether there are rejected transactions', action='store_true')
parser.add_argument('--express', help='will store the final value (that failed) rather than the last successful', action='store_true')
parser.add_argument('--recordramp', help='will record the throughput of each step rather than only the final value', action='store_true')
parser.add_argument('--printheader', help='indicates whether header will be included for recordramp', action='store_true')
parser.add_argument('--duration', help='duration of each experiment', type=int, default=60000)
parser.add_argument('--workflows', help='records number of workflows instead of number of txns.', action='store_true')
parser.add_argument('--latency', help='use the latency rather than the throughput to determine when to finish', type=int, default=-1)
parser.add_argument('--customargs', help='additional arguments to override default configuration', default='')

args = parser.parse_args()

projectname = args.project
resultfile  = args.output
stopflag    = args.stop
blockingflag= args.blocking
threads	    = args.threads
txn_threshold = args.txnthreshold
#tmax	    = args.tmax
#tstep       = args.tstep
rmin	    = args.rmin
rmax	    = args.rmax
rstep       = args.rstep
orstep      = rstep
frstep      = args.finalrstep
frmin       = args.finalrmin
logtimeout  = args.logtimeout
groupcommit = args.groupcommit
#lmax	    = args.lmax
#lstep       = args.lstep
llog        = args.log
warmup      = args.warmup
numruns     = args.numruns
expout      = args.expout
expout_details = string.replace(expout,".txt","") + "_DETAILS.csv"
winconfig   = args.winconfig
debug       = args.debug
aries       = args.aries
scheduler   = not args.hscheduler
sstore      = not args.hstore
ftrigger    = not args.ftrigger_off
wrecovery   = not args.weakrecovery_off
rejected_txns = args.rejected_txns
express = args.express
recordramp = args.recordramp
printheader = args.printheader
dur = args.duration
DURATION = float(dur/1000)
workflows = args.workflows
latency = args.latency
customargs = args.customargs

if latency > 0:
	latencyflag = True
else:
	latencyflag = False

if blockingflag==True:
    strblocking = "true"
else:
    strblocking = "false"
#end if

if llog==True:
    strlogging = "true"
else:
    strlogging = "false"
#end if

if aries==True:
	straries = "true"
else:
	straries = "false"

if scheduler==True:
	strscheduler = "true"
else:
	strscheduler = "false"

if sstore==True:
	strsstore = "true"
else:
	strsstore = "false"

if ftrigger==True:
	strftrigger = "true"
else:
	strftrigger = "false"

if wrecovery==True:
	strwrecovery = "true"
else:
	strwrecovery = "false"

if workflows==True:
	strworkflows = "workflows"
else:
	strworkflows = "txns/sec"

print projectname, resultfile, stopflag, blockingflag, llog, threads, rmin, rmax, rstep, logtimeout

#exit(0)

file = open(resultfile, "w")

fieldsarray = ["THROUGHPUT(workflows/s)","AVGLATENCY(ms)","TotalTXN", "Distributed","SpecExec","THMIN","THMAX","THSTDDEV","LAMIN","LAMAX","LASTDDEV"];


#print fields
#####################JOHN fields = "client.theads_per_host client.txnrate site.commandlog_timeout THROUGHPUT(txn/s)";
fields = "client.theads_per_host client.txnrate site.commandlog_timeout";
for i in range(0,len(fieldsarray)):
	fields += " " + fieldsarray[i];


#fields = "client.threads_per_host " + "client.txnrate " + "site.commandlog_timeout " + "THROUGHPUT(txn/s) " + "AVGLATENCY(ms) " + "TotalTXN "
#fields += "Distributed " + "SpecExec " + "THMIN " + "THMAX " + "THSTDDEV " + "LAMIN " + "LAMAX " + "LASTDDEV"
file.write(fields + "\n")

idx_throughput = 0;
idx_avglatency = 1;
idx_thmin = 5;
idx_thmax = 6;
idx_txnrate = 11;
idx_latmax = 9;

max_values = [];
prev_perc = 0;

starttime = time.time()

#  make command line to execute benchmark with the indicated configuration

number_need_to_determine = 5
stdev_threshold = 0.03

resultlist =  list()

txn_stats = []
statsheader = []
rampstats = []
rampstats_workflows = []
rampstats_latency = []

client_threads_per_host = threads;
for rn in range(0, numruns):
	print "RUN NUMBER: " + "{0:d}".format(rn + 1)
	client_txnrate = rmin
	cur_values = []
	latest_stats = {}
	rstep = orstep
	rampstats_tmp = []
	rampstats_tmp_workflows = []
	rampstats_tmp_latency = []
	best_txnrate = 0
	firstrecordramp = True
	while client_txnrate <= rmax:
		if client_txnrate < frmin:
			client_txnrate += rstep
			continue

		site_commandlog_timeout	= logtimeout
		client_warmup = warmup
	
		print "no logging mechanism executed in system..."
		str_antcmd 			= "ant hstore-benchmark"
		str_project 			= " -Dproject=" + projectname
		str_client_output_results_json  = " -Dclient.output_results_json=" + "true"
		str_client_blocking    	        = " -Dclient.blocking=" + strblocking
		str_client_threads_per_host 	= " -Dclient.threads_per_host=" + "{0:d}".format(client_threads_per_host)
		str_client_txnrate		= " -Dclient.txnrate=" + "{0:d}".format(client_txnrate)
		str_client_warmup       = " -Dclient.warmup=" + "{0:d}".format(client_warmup)
		str_client_duration    = " -Dclient.duration=" + "{0:d}".format(dur)
		str_site_commandlog_timeout = " -Dsite.commandlog_timeout=" + "{0:d}".format(site_commandlog_timeout)
		str_site_commandlog_enable = " -Dsite.commandlog_enable=" + strlogging
		str_site_commandlog_groupcommit = " -Dsite.commandlog_groupcommit=" + "{0:d}".format(groupcommit)
		str_site_aries = " -Dsite.aries=" + straries
		str_global_scheduler = " -Dglobal.sstore_scheduler=" + strscheduler
		str_global_sstore = " -Dglobal.sstore=" + strsstore
		str_global_ftrigger = " -Dglobal.sstore_frontend_trigger=" + strftrigger
		str_global_wrecovery = " -Dglobal.weak_recovery=" + strwrecovery
		str_output_txn_counters = " -Dclient.output_txn_counters=" + TXNFILE
		str_output_warmup_stats = " -Dclient.output_warmup_stats=true"
		str_custom_args = " " + customargs
	
		basic = "{0:d}".format(client_threads_per_host) + " " + "{0:d}".format(client_txnrate) + " " +  "{0:d}".format(site_commandlog_timeout)
	
		runcmd = str_antcmd + str_project + str_client_blocking + str_client_output_results_json + str_client_threads_per_host + str_client_txnrate + str_client_warmup 
		runcmd += str_site_commandlog_enable + str_site_commandlog_groupcommit + str_site_aries + str_global_scheduler + str_global_sstore + str_global_ftrigger + str_global_wrecovery
		runcmd += str_output_txn_counters + str_output_warmup_stats + str_client_duration + str_custom_args
	
		print "running benchmark with following configuration:"
		print runcmd

		# run the benchmark by calling the runcmd with os
		f = os.popen( runcmd )
	
		# get the benchmark running result, and print it on screen
		result = f.read()
		#print "This benchmark result is :", result
		singlereport = generateReport(result)
		numreport = getNumReportFromList(singlereport)

		txnstatsreport = findStats(TXNFILE, TXNPARTITIONCOL, TXNOUTCOLS)

		############JOHN basic += txnstatsreport[]
		basic += getReportFromList(singlereport)
		file.write(basic+"\n")

		resultlist.append(singlereport)
		
		print "client_txnrate * txn_threshold: " + "{0:.2f}".format(client_txnrate * txn_threshold)
		print "throughput: " + "{0:.2f}".format(numreport[idx_throughput])

		if debug:
			numreport.append(float(client_txnrate))
			cur_values = numreport
			latest_stats = txnstatsreport
			break

		if rn == 0:
			statsheader.append(client_txnrate)
		totaltxns = 0
		totalworkflows = float(numreport[idx_throughput]*DURATION)
		for key in txnstatsreport.keys():
			totaltxns += txnstatsreport[key]["EXECUTED"]
		rampstats_tmp.append(totaltxns)
		rampstats_tmp_workflows.append(totalworkflows)
		rampstats_tmp_latency.append(numreport[idx_latmax])

		rejected = False
		update = False
		if rejected_txns:
			for key in txnstatsreport.keys():
				if txnstatsreport[key]["REJECTED"] > 0:
					rejected = True
					update = True

		if recordramp: #use this to instead record txnrate at each throughput
			if numreport[idx_throughput] > client_txnrate * txn_threshold or firstrecordramp:
				numreport.append(float(client_txnrate))
				cur_values = numreport
				latest_stats = txnstatsreport
				best_txnrate = client_txnrate
				firstrecordramp = False
			client_txnrate += rstep
			if client_txnrate > rmax and rstep > frstep:
				client_txnrate = best_txnrate
				rstep = frstep
				rmax = best_txnrate + (frstep * 12)
			continue

		if latencyflag:
			print("MAX LATENCY: " + str(numreport[idx_latmax]))
			if numreport[idx_latmax] > latency:
				update = True
		elif numreport[idx_throughput] <= client_txnrate * txn_threshold:
			update = True

		if update:
			if rstep > frstep:
				client_txnrate -= rstep
				rstep = rstep / 10
				prev_perc = 0.0
				continue
			elif ((numreport[idx_throughput] / client_txnrate <= prev_perc or rejected) and not latencyflag) or (latencyflag and numreport[idx_latmax] > latency):
				if not cur_values or express:
					numreport.append(float(client_txnrate))
					cur_values = numreport
					latest_stats = txnstatsreport
				break


		#run no matter what
		prev_perc = numreport[idx_throughput] / client_txnrate
		numreport.append(float(client_txnrate))
		cur_values = numreport
		client_txnrate += rstep
		latest_stats = txnstatsreport

	##endwhile
	#print "cur_values length: " + "{0:d}".format(len(cur_values))
	rampstats.append(rampstats_tmp)
	rampstats_workflows.append(rampstats_tmp_workflows)
	rampstats_latency.append(rampstats_tmp_latency)
	max_values.append(cur_values)
	txn_stats.append(latest_stats)

	if debug:
		break
##endfor

file.close()


expfile = open(expout_details, "a")
expfile.write("benchmark,config,threads,logging,timeout,groupcommit,blocking,scheduler,sstore,ftriggers,weakrecovery,units,runnum")
for txn in statsheader:
	expfile.write("," + str(txn))
expfile.write("\n")
avg = []
avg_workflow = []
avg_latency = []
for i in range(0,len(rampstats)):
	towrite = projectname + "," + winconfig + "," + str(client_threads_per_host) + "," + strlogging + "," + str(site_commandlog_timeout)
	towrite += "," + str(groupcommit) + "," + strblocking + "," + strscheduler + "," + strsstore + "," + strftrigger + "," + strwrecovery
	#workflows
	expfile.write(towrite + ",AVG_WORKFLOWS," + str(i))
	for stat in range(0,len(rampstats_workflows[i])):
		expfile.write("," + str(float(rampstats_workflows[i][stat])/DURATION))
		if i == 0:
			avg_workflow.append(float(rampstats_workflows[i][stat]))
		else:
			avg_workflow[stat] += float(rampstats_workflows[i][stat])
	expfile.write("\n")
	#txns
	expfile.write(towrite + ",AVG_TXNS," + str(i))
	for stat in range(0,len(rampstats[i])):
		expfile.write("," + str(float(rampstats[i][stat])/DURATION))
		if i == 0:
			avg.append(float(rampstats[i][stat]))
		else:
			avg[stat] += float(rampstats[i][stat])
	expfile.write("\n")
	###
	expfile.write(towrite + ",MAX_LATENCY," + str(i))
	for stat in range(0,len(rampstats_latency[i])):
		expfile.write("," + str(float(rampstats_latency[i][stat])))
		if i == 0:
			avg_latency.append(float(rampstats_latency[i][stat]))
		else:
			avg_latency[stat] += float(rampstats_latency[i][stat])
	expfile.write("\n")
	###
if numruns > 1:
	expfile.write(towrite + ",AVG_WORKFLOWS,avg")
	for a in avg_workflow:
		expfile.write("," + str(a/DURATION/numruns))
	expfile.write("\n" + towrite + ",AVG_TXNS,avg")
	for a in avg:
		expfile.write("," + str(a/DURATION/numruns))
	expfile.write("\n")
	expfile.write("\n" + towrite + ",MAX_LATENCY,avg")
	for a in avg_latency:
		expfile.write("," + str(a/numruns))
	expfile.write("\n")
expfile.write("-------------------------------------------------\n")
expfile.close()

str_comparemode = ""
if rejected_txns:
	str_comparemode = "stop on rejected"
else:
	str_comparemode = "stop on threshold"

str_express = ""
if rejected_txns:
	str_express = "express (results for failed)"

#append to the final experimental results file
expfile = open(expout, "a")
proj = projectname + " - " + winconfig + " (" + "{0:.2f}".format(txn_threshold) + " threshold)" + "  |  " + str_comparemode + "  |  " + str_express
config = "threads: " + "{0:d}".format(client_threads_per_host) + "  |  warmup: " + "{0:d}".format(client_warmup) + "  |  threshold: " + "{0:.2f}".format(txn_threshold)
config += "\nlogging: " +  strlogging + "  |  log timeout: " + "{0:d}".format(site_commandlog_timeout) + "  |  group commit: " + "{0:d}".format(groupcommit)
config += "\nblocking: " + strblocking +  "  |  aries: " + straries + "  |  scheduler: " + strscheduler + "  |  lat threshold: " + "{0:d}".format(latency)
config += "\nS-Store: " + strsstore + "  |  S-Store frontend trigger: " + strftrigger + "  |  weak recovery: " + strwrecovery 
expfile.write(proj + "\n");
expfile.write("--------------------------------------------------\n");
expfile.write(config + "\n");

avg_values = []
max_throughput = 0;
max_txnrate = 0;
min_throughput = 999999999;
min_txnrate = 0;
all_throughputs = []
all_txnrates = []
endtime = time.time()
runtime = endtime - starttime
runtimeformatted = str(datetime.timedelta(seconds=runtime))

#print "max_values length: " + "{0:d}".format(len(max_values))
for i in range(0, len(max_values)):
	for j in range(0, len(max_values[i])):
		if i == 0:
			avg_values.append(0.0);
		if j == idx_throughput:
			if max_values[i][j] > max_throughput:
				max_throughput = max_values[i][j]
				max_txnrate = max_values[i][idx_txnrate]
			if max_values[i][j] < min_throughput:
				min_throughput = max_values[i][j]
				min_txnrate = max_values[i][idx_txnrate]
			all_throughputs.append(max_values[i][j])
			all_txnrates.append(max_values[i][idx_txnrate])
		avg_values[j] += max_values[i][j]

totaltxns = 0
maxtxns = 0
mintxns = 0
avgthroughput = {}
totalstats = {}
maxstats = {}
minstats = {}
all_throughputs = []
all_proc_throughputs = []
all_proc_rejects = []
runcounter = 0
for run in txn_stats:
	runcounter += 1
	tmptotal = 0
	tmpmax = 0
	tmpmin = 0
	proc_throughputs = {}
	proc_rejects = {}
	for proc, stats in run.iteritems():
		proc_throughputs[proc] = float(stats["EXECUTED"]) / DURATION
		proc_rejects[proc] = stats["REJECTED"]
		if proc not in totalstats.keys():
			totalstats[proc] = stats
			maxstats[proc] = stats
			minstats[proc] = stats
		else:
			for stattype, stat in stats.iteritems():
				totalstats[proc][stattype] += stat
				if stat < minstats[proc][stattype]:
					minstats[proc][stattype] = stat
				if stat > maxstats[proc][stattype]:
					maxstats[proc][stattype] = stat
		tmptotal += stats["EXECUTED"]
	all_proc_throughputs.append(proc_throughputs)
	all_proc_rejects.append(proc_rejects)
	totaltxns += tmptotal
	all_throughputs.append(float(tmptotal)/DURATION)
	if tmptotal > maxtxns:
		maxtxns = tmptotal
	if tmptotal < mintxns:
		mintxns = tmptotal

for proc, stats in totalstats.iteritems():
	avgthroughput[proc] = float(stats["EXECUTED"]) / DURATION

min_throughput = float(mintxns) / DURATION
max_throughput = float(maxtxns) / DURATION
avg_throughput = float(totaltxns) / runcounter / DURATION

expfile.write("THROUGHPUT STATS\n")
for i in range(0, len(all_throughputs)):
	expfile.write("   " + "{0:d}".format(i+1) + ": " + "{0:.2f}".format(all_throughputs[i]) + " \n");
	for proc, throughput in all_proc_throughputs[i].iteritems():
		expfile.write("        " + proc + ": {0:.2f}".format(throughput) + " (" + "{0:d}".format(all_proc_rejects[i][proc]) + " txns rejected)\n")


expfile.write("    MIN: " + "{0:.2f}".format(min_throughput) + " (" + "{0:.2f}".format(min_txnrate) + " submitted)\n");
expfile.write("    MAX: " + "{0:.2f}".format(max_throughput) + " (" + "{0:.2f}".format(max_txnrate) + " submitted)\n");
#expfile.write("    AVG: " + "{0:.2f}".format(avg_values[idx_throughput]/numruns) + "\n")
expfile.write("    AVG: " + "{0:.2f}".format(avg_throughput) + "\n")
expfile.write("   ---   \n")
for i in range(0, len(avg_values) - 1):
	expfile.write(fieldsarray[i] + "(avg): " + "{0:.2f}".format(avg_values[i]/numruns) + "\n");
expfile.write("-----RUNTIME: " + runtimeformatted + " -----\n")
expfile.write("\n");
expfile.close()



##end script
