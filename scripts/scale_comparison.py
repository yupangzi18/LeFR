import sys

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 


ratio_default = 50
thread_default = 16
rw_ratio_default = 50
zipf_default = 0.50
partition_default = n*thread_default
replica_default = "hash5"
network_default = 0


# protocols = ["LeFR", "Tapir", "GPAC"]
# protocols = ["Tapir", "GPAC"]
protocols = ["LeFR"]

twopc = 'true'
flush_granularity = 'txn'

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

def make_cmd(protocol, thread, rw_ratio, ratio, zipf, replica, network):
  global i
  if protocol == "LeFR" and twopc == 'true':
    if flush_granularity == 'op':
      cmd = get_cmd(n, i)
      print('timeout 80s ~/LRFR/bench_ycsb  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --read_write_ratio=%d --cross_ratio=%d --zipf=%.2f --traditional_2pc=%s --flush_granularity=%s --network_latency=%d --batch_flush=200 > res_%d_tot_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_%s_net_%d_PPC 2>&1' % (id, cmd, protocol, thread*n, thread, replica, rw_ratio, ratio, zipf, twopc, flush_granularity, network, id, n, thread, rw_ratio, ratio, zipf, replica, network))
      print('sleep 10')
      i += 1
    cmd = get_cmd(n, i)
    print('timeout 80s ~/LRFR/bench_ycsb  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --read_write_ratio=%d --cross_ratio=%d --zipf=%.2f --traditional_2pc=%s --network_latency=%d --batch_flush=200 > res_%d_tot_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_%s_net_%d_2PC 2>&1' % (id, cmd, protocol, thread*n, thread, replica, rw_ratio, ratio, zipf, twopc, network, id, n, thread, rw_ratio, ratio, zipf, replica, network))
    print('sleep 10')
    i += 1
  cmd = get_cmd(n, i)
  print('timeout 80s ~/LRFR/bench_ycsb  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --read_write_ratio=%d --cross_ratio=%d --zipf=%.2f --network_latency=%d --batch_flush=200 > res_%d_tot_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_%s_net_%d_%s 2>&1' % (id, cmd, protocol, thread*n, thread, replica, rw_ratio, ratio, zipf, network, id, n, thread, rw_ratio, ratio, zipf, replica, network, protocol))
  print('sleep 10')
  i += 1


def make_cmd_tpcc(protocol, thread, ratio, parition, network):
  global i
  if protocol == "LeFR" and twopc == 'true':
    if flush_granularity == 'op':
      cmd = get_cmd(n, i)
      print('timeout 80s ~/LRFR/bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=hash5 --query=neworder --neworder_dist=%d --payment_dist=%d --traditional_2pc=%s --flush_granularity=%s --network_latency=%d --batch_flush=200> tpcc_%d_tot_%d_thr_%d_cross_%d_par_%d_net_%d_PPC 2>&1' % (id, cmd, protocol, parition, thread, ratio, ratio, twopc, flush_granularity, network, id, n, thread, ratio, parition, network))
      print('sleep 10')
      i += 1
    cmd = get_cmd(n, i)
    print('timeout 80s ~/LRFR/bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=hash5 --query=neworder --neworder_dist=%d --payment_dist=%d --traditional_2pc=%s --network_latency=%d --batch_flush=200> tpcc_%d_tot_%d_thr_%d_cross_%d_par_%d_net_%d_2PC 2>&1' % (id, cmd, protocol, parition, thread, ratio, ratio, twopc, network, id, n, thread, ratio, parition, network))
    print('sleep 10')
    i += 1
  cmd = get_cmd(n, i)
  print('timeout 80s ~/LRFR/bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=hash5 --query=neworder --neworder_dist=%d --payment_dist=%d --network_latency=%d --batch_flush=200> tpcc_%d_tot_%d_thr_%d_cross_%d_par_%d_net_%d_%s 2>&1' % (id, cmd, protocol, parition, thread, ratio, ratio, network, id, n, thread, ratio, parition, network, protocol))
  print('sleep 10')
  i += 1


for protocol in protocols: 
    make_cmd(protocol, thread_default, rw_ratio_default, ratio_default, zipf_default, replica_default, network_default)

for protocol in protocols: 
    make_cmd_tpcc(protocol, thread_default, ratio_default, partition_default, network_default)