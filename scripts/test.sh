#!/bin/bash

set -x

user="zhanhao"
#TODO@luoyu: read from ips.txt
ips=(10.10.10.235 10.10.10.227 10.10.10.226)
nodes=2 # replace with the size of ips
port=20010
workload="aws_perf_comparison.py"

# scp exec bin
#for id in $(seq 0 $nodes)
#    do 
#        echo "${ips[id]}"
#        scp ./bench_ycsb $user@${ips[id]}:/home/lyu/lefr/bench_ycsb
#    done

# create run.sh in nodes
#cd scripts
python distribute_script.py $port $workload
cd ..

# run.sh
for id in $(seq 0 $nodes)
    do
        # ssh $user@${ips[id]} "cd /data/zhanhao; ./run.sh &"
        ssh $user@${ips[id]} "source ~/.profile; cd /data/zhanhao; screen -dm bash run.sh"
        sleep 3s
    done

# wait for finishing
sleep 60s

# collect result
timestamp=`date "+%Y%m%d%H%M%S"`
resultDir="result-${timestamp}"
mkdir $resultDir


for id in $(seq 0 $nodes)
#TODO@luoyu: if the process has not finished yet, kill it before copy
    do
        echo "${ips[id]}"
        scp $user@${ips[id]}:/data/zhanhao/tps_node_${id} $resultDir/
        scp $user@${ips[id]}:/data/zhanhao/res_${id} $resultDir/
    done 
