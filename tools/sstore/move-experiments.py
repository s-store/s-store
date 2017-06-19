#!/usr/bin/env python
import sys, argparse
import os, datetime, time
import pexpect
import re
from array import *
from subprocess import Popen
from subprocess import call



# get the args from command line
# set default values for parameters needed by script
timestamp = datetime.datetime.now()
t = timestamp.year, timestamp.month, timestamp.day, timestamp.hour, timestamp.minute, timestamp.second
#defaultoutput = 'experiment' + '_'.join(str(i) for i in t)+ '.txt'


# get paramets from command line input

parser = argparse.ArgumentParser(description='This is a benchmark auto-run script for optimizing throughput based on a latency bound')
parser.add_argument('-p','--project', help='Benchmark name', default='distributedmoveargs')
parser.add_argument('-o','--mode', help='3 = local, 4 = network', default='3')
parser.add_argument('-l','--maxlatency', help='Latency bound in ms', default='50')
parser.add_argument('-w','--warmup', help='Warmup time in ms', default='0')
parser.add_argument('-d','--duration', help='Duration time in ms', default='30000')

args = parser.parse_args()

projectname = args.project
tmpfile  = 'move-exp-tmp.txt'
movestatsfile = 'move-stats.txt'
duration = args.duration
warmup = args.warmup
mode = args.mode
commandlog = 'true'
mlat = int(args.maxlatency)
batchsizestotest = [1,10,20,50,100,500,1000]



def getLastLineOfFile(fileName):
    with open(fileName, 'rb') as fh:
        first = next(fh).decode()
        fh.seek(-32, 2)
        last = fh.readlines()[-1].decode()
        return last

def getFileContents(fileName):
    with open (fileName, "r") as myfile:
        data=myfile.read().replace('\n', '')
        return data


# Gets throughput and df latency based on fixed batch size and max latency
def getTPAndDFLatency(txnRate, batchSize):
    # Kill all existing s-store instances
    #pkill = Popen(["killall", "java"])
    #pkill.terminate()
    p = Popen('./tools/sstore/sstoremultisite.sh ' + projectname + ' ' + mode + ' ' + str(txnRate) + ' "-Dnoshutdown=true -Dglobal.weak_recovery=false -Dsite.commandlog_enable=' + commandlog + ' -Dclient.benchmark_param_0=' + str(batchSize) + ' -Dclient.warmup=' + warmup + ' -Dclient.duration=' + duration + ' -Dsite.jvm_asserts=false" > ' + tmpfile,shell=True)


    # Let p run until benchmark has completed
    hasended = False
    while not hasended:
        time.sleep(30)
        lastLine = getLastLineOfFile(tmpfile)
        #print "last line is '" + lastLine + "'"
        hasended = "r remaining online until killed\n" == lastLine

    # Experiment has ended. Now let's grab some stats before we kill p

    child = pexpect.spawnu(u'./hstore ' + projectname)
    try:
        child.expect(u'hstore>.*', timeout=15)
        child.sendline(u'exec CalculateStats')
        child.expect(u'Result #1 / 1.*')
        child.sendline(u'quit')
    except:
        return [0,999999]
    child.close()
    p.terminate()
    call(["killall", "java"])
    # If the bm is distributedmoveold, copy benchmark stats from remote partition
    if projectname == "distributedmoveold":
        childcopy = pexpect.spawnu(u'scp bsn07.cs.brown.edu:/data/cmath/s-store/move-stats.txt /data/cmath/s-store/move-stats.txt',timeout=15)
        childcopy.expect(u'move-stats.txt.*',timeout=10)
        childcopy.close()

    # Collect the data
    # First get the throughput and input latency
    bmout = getFileContents(tmpfile)
    matches = re.match(r".*\[java\] Throughput:\s+([0-9]+\.?[0-9]*) txn.*", bmout)
    foundTP = float(matches.group(1))
    matches = re.match(r".*\[java\] Latency:\s+([0-9]+\.?[0-9]*) ms.*", bmout)
    foundLatency = float(matches.group(1))
    # Then collect dataflow and network latency
    dmout = getFileContents(movestatsfile)
    matches = re.match(r".*?([0-9]*),([0-9]*),([0-9]*)$", dmout, re.M)
    foundTotalTuples = float(matches.group(1))
    foundDataFlowLatency = float(matches.group(2))
    foundNetworkLatency = float(matches.group(3))
    calcTP = foundTotalTuples/(float(duration)/1000)
    # print("TP: " + str(foundTP))
    # print("Actual TP: " + str(calcTP))
    # print("Latency: " + foundLatency)
    # print("Tuples: " + foundTotalTuples)
    # print("DataFlowLatency: " + str(foundDataFlowLatency))
    #print("NetworkLatency: " + foundNetworkLatency)


    return [calcTP, foundDataFlowLatency, foundNetworkLatency]


def getMaxTP(maxLatency, batchSize):
    mintx = 400
    maxtx = 70000
    maxtries = 13
    besttx = 0
    besttp = 0
    # Do binary search for max tp
    i = 0
    while i < maxtries:
        curtxnrate = (maxtx-mintx)/2+mintx
        print "trying txn rate " + str(curtxnrate)
        bmData = getTPAndDFLatency(curtxnrate, batchSize)
        latency = bmData[1]
        print "tuples per sec was: " + str(bmData[0]) + ", latency was " + str(latency)
        if latency < maxLatency:
            besttx = curtxnrate
            besttp = bmData[0]
            mintx = curtxnrate
            print "found new best: " + str(besttx) + " (lat: " + str(latency) + ")"
        else:
            maxtx = curtxnrate
        i += 1

    return [float(besttp), besttx]

def printVariousTPs(batchSize, minTxnRate, maxTxnRate):
    steps = 120
    stepsize = (maxTxnRate-minTxnRate)/steps

    curtxnrate = minTxnRate
    i = 0
    print "batchsize,latency,tp,txnrate,networklatency"
    while i < steps:
        bmData = getTPAndDFLatency(curtxnrate, batchSize)
        latency = bmData[1]
        tp = bmData[0]
        print str(batchSize) + "," + str(latency) + "," + str(tp) + "," + str(curtxnrate) + "," + str(bmData[2])
        curtxnrate += stepsize
        i += 1




i = 0

while i < len(batchsizestotest):
    curbatchsize = batchsizestotest[i]
    printVariousTPs(curbatchsize,400,50000)
    i += 1


#

# print ('latencybound,batchsize,throughput,txnrate')

#
# i = 0
#
# while i < len(batchsizestotest):
#     curbatchsize = batchsizestotest[i]
#     maxtp = getMaxTP(mlat, curbatchsize)
#     print(str(mlat) + ',' + str(curbatchsize) + ',' + str(maxtp[0]) + ',' + str(maxtp[1]) )
#     i += 1





##end script
