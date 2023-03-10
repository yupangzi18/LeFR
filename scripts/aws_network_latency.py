import sys

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

protocols = ["Star", "SiloGC", "TwoPLGC"]
delays = [100, 200, 500, 1000, 2000, 5000, 10000]

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
      
      
for protocol in protocols: 
  for delay in delays:
    for i in range(3):
      cmd = get_cmd(n, i)
      print('./bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --group_time=100 --delay=%d --query=mixed --neworder_dist=10 --payment_dist=15' % (id, cmd, protocol, 12*n, delay))   
      