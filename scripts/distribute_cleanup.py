import sys
import os

ips = [line.strip() for line in open("ips_all.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

# port = int(sys.argv[1]) 
# script = sys.argv[2]

# dispatch test script
for i in range(n):
  os.system("ssh zhanhao@%s \"ps aux|grep run.sh|awk '{print \$2}'|xargs kill -9\"" % outs[i])
  os.system("ssh zhanhao@%s \"ps aux|grep bench_tpcc|awk '{print \$2}'|xargs kill -9\"" % outs[i])
  os.system("ssh zhanhao@%s \"ps aux|grep bench_ycsb|awk '{print \$2}'|xargs kill -9\"" % outs[i])
  # os.system("ssh zhanhao@%s 'rm -rf /data/zhanhao/*'" % outs[i])

