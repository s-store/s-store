FROM ubuntu:14.04

# install required packages
RUN apt-get update && apt-get install -y build-essential gcc g++ \
git ant maven openssh-server subversion valgrind \
python2.7 openjdk-7-jdk

# ssh setup
RUN ssh-keygen -t dsa -q -P "" -f /root/.ssh/id_dsa && cat /root/.ssh/id_dsa.pub >> /root/.ssh/authorized_keys \
&& service ssh restart && ssh -o StrictHostKeyChecking=no localhost "date"

# download and compile
#RUN cd /root && git clone https://github.com/apavlo/h-store.git && cd h-store \
#&& ant build && ant hstore-prepare -Dproject=tpcc
#WORKDIR /root/h-store

# compile and prepare benchmark
COPY . /root/s-store
WORKDIR /root/s-store
RUN ant build && ant sstore-prepare -Dproject=votersstoreexample && ant sstore-prepare -Dproject=mimic2bigdawg

# run benchmark
CMD service ssh restart && ant sstore-benchmark -Dproject=votersstoreexample