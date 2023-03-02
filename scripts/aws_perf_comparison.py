import sys

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

# protocols = ["LeFR"]
protocols = ["LeFR", "Tapir", "GPAC"]
# ratios = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
ratios = [0, 30, 60, 90, 100]
# ratios = [0, 30]
zipf = [0, 0.3, 0.6, 0.75, 0.9]
threads = [2, 4, 8, 12, 16]
rw_ratios = [0, 30, 60, 90, 100]
# scale = [3, 5, 7, 9, 11]

if 

def get_cmd(n, i):
  cmd = ""
  for j in range(n):
    if j > 0:
      cmd += ";"
    if id == j:
      cmd += ins[j] + ":" + str(port+i)
    else:
      cmd += outs[j] + ":" + str(port+i)
  return cmd

i = 0
for protocol in protocols: 
  for ratio in ratios:
    # for i in range(1):
      cmd = get_cmd(n, i)
      print('timeout 60s ~/zzh-LRFR/bench_ycsb  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --read_write_ratio=90 --cross_ratio=%d --batch_flush=200 > res_%d_cross_%d_%s 2>&1' % (id, cmd, protocol, 12*n, ratio, id, ratio, protocol))
      print('sleep 3')
      i += 1
      
#for protocol in protocols: 
#  for ratio in ratios:
#    for i in range(1):
#      cmd = get_cmd(n, i)
#      print('~/zzh-LRFR/bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --query=mixed --neworder_dist=%d --payment_dist=%d' % (id, cmd, protocol, 12*n, ratio, ratio))   

