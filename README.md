# S-Store

S-Store is an experimental system for streaming data management with shared mutable state.  It is a hybrid streaming system with a main-memory OLTP data management system base.  It integrates transaction processing with push-based semantics.

More information and documentation is available at: <http://sstore.cs.brown.edu>

## Supported Platforms
S-Store is built on [H-Store]<http://hstore.cs.brown.edu>, and is known to work on the following platforms.
Please note that it will not compile on 32-bit systems.
+ Ubuntu Linux 9.10+ (64-bit)
+ Red Hat Enterprise Linux 5.5 (64-bit)

## Dependencies
+ [gcc +4.3](http://www.ubuntuupdates.org/gcc)
+ [openjdk 1.6 or 1.7](http://www.ubuntuupdates.org/openjdk-7-jdk) (1.7 is recommended)
+ [ant +1.7](http://www.ubuntuupdates.org/ant)
+ [python +2.7](http://www.ubuntuupdates.org/python)
+ [openssh-server](http://www.ubuntuupdates.org/openssh-server) (for automatic deployment)

## Quick Start
1. First build the entire distribution:

        ant build

2. Next make the project jar file for the target benchmark.
   S-Store includes several benchmarks that are built-in and ready to execute. A project jar contains all the of stored 
   procedures and statements for the target benchmark, as well as the cluster configuration for the database system.

        export SSTORE_BENCHMARK=votersstoreexample
        ant sstore-prepare -Dproject=$SSTORE_BENCHMARK

3. You can now execute the benchmark locally on your machine with two partitions

        ant sstore-benchmark -Dproject=$SSTORE_BENCHMARK

More information is available here: <http://sstore-doc.readthedocs.io/en/latest/deploy.html>
