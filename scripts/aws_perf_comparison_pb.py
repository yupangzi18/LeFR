import sys

ips = [line.strip() for line in open("ips.txt", "r")][:2]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

ratios = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]

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


for ratio in ratios:
  for i in range(3):
    cmd = get_cmd(n, i)
    print('./bench_ycsb  --id=%d --servers="%s" --protocol=SiloGC --partition_num=%d --threads=12 --partitioner=pb --read_write_ratio=90 --cross_ratio=%d --batch_flush=200' % (id, cmd, 48, ratio))
      
for ratio in ratios:
  for i in range(3):
    cmd = get_cmd(n, i)
    print('./bench_tpcc  --id=%d --servers="%s" --protocol=SiloGC --partition_num=%d --threads=12 --partitioner=pb --query=mixed --neworder_dist=%d --payment_dist=%d' % (id, cmd, 48, ratio, ratio))  
  
  
#for ratio in ratios:
#  for i in range(3):
#    cmd = get_cmd(n, i)
#    print('./bench_ycsb  --id=%d --servers="%s" --protocol=LeFR --partition_num=%d --threads=12 --partitioner=pb --read_write_ratio=90 --cross_ratio=%d' % (id, cmd, 48, ratio))
      
#for ratio in ratios:
#  for i in range(3):
#    cmd = get_cmd(n, i)
#    print('./bench_tpcc  --id=%d --servers="%s" --protocol=LeFR --partition_num=%d --threads=12 --partitioner=pb --query=mixed --neworder_dist=%d --payment_dist=%d' % (id, cmd, 48, ratio, ratio))  
      