import sys
import os
from time import sleep

cross_nodes = [8, 12, 16, 20, 24]
# cross_nodes = [20, 24]

# port = int(sys.argv[1]) 
port = 20010

for n in cross_nodes:
  os.system(" head -%d ips_all.txt >ips.txt" % (n))
  ips = [line.strip() for line in open("ips.txt", "r")]
  n = len(ips)

  ins = [line.split("\t")[0] for line in ips]
  outs = [line.split("\t")[1] for line in ips]

  # dispatch test script
  for i in range(n):
    os.system("python scale_comparison.py %d %d > run.sh" % (i, port))
    os.system("chmod u+x run.sh")
    os.system("scp run.sh zhanhao@%s:/data/zhanhao/run.sh" % outs[i])

  port = port + 10

  # run script
  for i in range(n):
    os.system("ssh zhanhao@%s 'source ~/.profile; cd /data/zhanhao; screen -dm bash run.sh'" % outs[i])

  # check whether finished
  pids = os.popen("ssh zhanhao@%s \"ps x | grep run.sh | grep -v grep | awk '{print \$1}'\"" % outs[0]).readlines()
  while len(pids) > 0:
    # print("sleeping")
    sleep(15)
    pids = os.popen("ssh zhanhao@%s \"ps x | grep run.sh | grep -v grep | awk '{print \$1}'\"" % outs[0]).readlines()
    print(len(pids))


  # for i in range(n):
  #   os.system("ssh zhanhao@%s \"ps aux|grep run.sh|awk '{print \$2}'|xargs kill -9\"" % outs[i])
  #   os.system("ssh zhanhao@%s \"ps aux|grep bench_tpcc|awk '{print \$2}'|xargs kill -9\"" % outs[i])
  #   os.system("ssh zhanhao@%s \"ps aux|grep bench_ycsb|awk '{print \$2}'|xargs kill -9\"" % outs[i])
