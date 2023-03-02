import sys

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

ratio_default = 100
thread_default = 16
partition_default = n*thread_default
network_default = 0

protocols = ["LeFR", "Tapir", "GPAC"]
ratios = [0, 25, 50, 75, 100]
threads = [2, 4, 8, 12, 16]
# paritions = [20, 40, 60, 80, 100, 120]
# networks = [0, 1, 2, 3, 6, 9]

# protocols = ["Tapir"]
# ratios = [50]
# threads = [2]
networks = [0]


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

def make_cmd_tpcc(protocol, thread, ratio, parition, network):
  global i
  if protocol == "LeFR" and twopc == 'true':
    if flush_granularity == 'op':
      cmd = get_cmd(n, i)
      print('timeout 80s ~/LRFR/bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=hash5 --query=neworder --neworder_dist=%d --payment_dist=%d --traditional_2pc=%s --flush_granularity=%s --network_latency=%d --batch_flush=200> tpcc_%d_thr_%d_cross_%d_par_%d_net_%d_PPC 2>&1' % (id, cmd, protocol, parition, thread, ratio, ratio, twopc, flush_granularity, network, id, thread, ratio, parition, network))
      print('sleep 10')
      i += 1
    cmd = get_cmd(n, i)
    print('timeout 80s ~/LRFR/bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=hash5 --query=neworder --neworder_dist=%d --payment_dist=%d --traditional_2pc=%s --network_latency=%d --batch_flush=200> tpcc_%d_thr_%d_cross_%d_par_%d_net_%d_2PC 2>&1' % (id, cmd, protocol, parition, thread, ratio, ratio, twopc, network, id, thread, ratio, parition, network))
    print('sleep 10')
    i += 1
  cmd = get_cmd(n, i)
  print('timeout 80s ~/LRFR/bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=hash5 --query=neworder --neworder_dist=%d --payment_dist=%d --network_latency=%d --batch_flush=200> tpcc_%d_thr_%d_cross_%d_par_%d_net_%d_%s 2>&1' % (id, cmd, protocol, parition, thread, ratio, ratio, network, id, thread, ratio, parition, network, protocol))
  print('sleep 10')
  i += 1


# # ratio
# for protocol in protocols: 
#   for ratio in ratios:
#     make_cmd_tpcc(protocol, thread_default, ratio, partition_default, network_default)

# # thread
# for protocol in protocols: 
#   for thread in threads:
#     make_cmd_tpcc(protocol, thread, ratio_default, partition_default, network_default)


# network
for protocol in protocols: 
  for network in networks:
    make_cmd_tpcc(protocol, thread_default, ratio_default, partition_default, network)


# # partitions
# for protocol in protocols: 
#   for parition in paritions:
#     make_cmd_tpcc(protocol, thread_default, ratio_default, parition, network_default)






# tpcc
# for protocol in protocols: 
#  for ratio in ratios:
#    for i in range(1):
#      cmd = get_cmd(n, i)
#      print('~/zzh-LRFR/bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=12 --partitioner=hash2 --query=neworder --neworder_dist=%d --payment_dist=%d' % (id, cmd, protocol, 12*n, ratio, ratio))   
