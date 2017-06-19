#!/bin/bash
if [ "$5" != "prepare=false" ] ; then
	ant clean-java build-java
	if [ "$2" = 1 ] ; then
		ant hstore-prepare -Dproject=$1 -Dhosts="localhost:0:0-5"
	fi
	if [ "$2" = 2 ] ; then
		ant hstore-prepare -Dproject=$1 -Dhosts="localhost:0:0-1"
	fi
	if [ "$2" = 3 ] ; then
		ant hstore-prepare -Dproject=$1 -Dhosts="localhost:0:0;localhost:1:1"
	fi
	if [ "$2" = 4 ] ; then
		ant hstore-prepare -Dproject=$1 -Dhosts="bsn06.cs.brown.edu:0:0;bsn07.cs.brown.edu:1:1"
	fi
	if [ "$2" = 5 ] ; then
		ant hstore-prepare -Dproject=$1 -Dhosts="localhost:0:0-4"
	fi
fi
if [ "$5" != "build=false" ] ; then
	ant hstore-benchmark -Dproject=$1 -Dsite.jvm_asserts=false -Dclient.threads_per_host=1 -Dglobal.sstore=true -Dglobal.sstore_scheduler=true -Dclient.blocking=false -Dclient.warmup=10000 -Dclient.duration=60000 -Dclient.txnrate=$3 $4
fi
