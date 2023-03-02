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


protocols = ["LeFR", "Tapir", "GPAC"]
# ratios = [0, 25, 50, 75, 100]
# ratios = [0, 50, 100]
# ratios = [50]
threads = [2, 4, 8, 12, 16]
rw_ratios = [0, 25, 50, 75, 100]
# zipfs = [0.00, 0.25, 0.50, 0.75, 0.95]
zipfs = [0.50, 0.60, 0.70, 0.80, 0.90, 0.99]
# partitions = [10, 20, 30, 40]
replicas = ["hash3", "hash5", "hash7", "hash9"]
# networks = [0, 1, 2, 3, 6, 9]


# protocols = ["GPAC"]
ratios = [50]
# threads = [16]
# rw_ratios = [50]
# zipfs = [0.60]
# replicas = ["hash7"]
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

def make_cmd(protocol, thread, rw_ratio, ratio, zipf, replica, network):
  global i
  if protocol == "LeFR" and twopc == 'true':
    if flush_granularity == 'op':
      cmd = get_cmd(n, i)
      print('timeout 80s ~/LRFR/bench_ycsb  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --read_write_ratio=%d --cross_ratio=%d --zipf=%.2f --traditional_2pc=%s --flush_granularity=%s --network_latency=%d --batch_flush=200 > res_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_%s_net_%d_PPC 2>&1' % (id, cmd, protocol, thread*n, thread, replica, rw_ratio, ratio, zipf, twopc, flush_granularity, network, id, thread, rw_ratio, ratio, zipf, replica, network))
      print('sleep 10')
      i += 1
    cmd = get_cmd(n, i)
    print('timeout 80s ~/LRFR/bench_ycsb  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --read_write_ratio=%d --cross_ratio=%d --zipf=%.2f --traditional_2pc=%s --network_latency=%d --batch_flush=200 > res_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_%s_net_%d_2PC 2>&1' % (id, cmd, protocol, thread*n, thread, replica, rw_ratio, ratio, zipf, twopc, network, id, thread, rw_ratio, ratio, zipf, replica, network))
    print('sleep 10')
    i += 1
  cmd = get_cmd(n, i)
  print('timeout 80s ~/LRFR/bench_ycsb  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --read_write_ratio=%d --cross_ratio=%d --zipf=%.2f --network_latency=%d --batch_flush=200 > res_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_%s_net_%d_%s 2>&1' % (id, cmd, protocol, thread*n, thread, replica, rw_ratio, ratio, zipf, network, id, thread, rw_ratio, ratio, zipf, replica, network, protocol))
  print('sleep 10')
  i += 1



# # rwratio
# for protocol in protocols: 
#   for rw_ratio in rw_ratios:
#     make_cmd(protocol, thread_default, rw_ratio, ratio_default, zipf_default, replica_default, network_default)

# # thread
# for protocol in protocols: 
#   for thread in threads:
#     make_cmd(protocol, thread, rw_ratio_default, ratio_default, zipf_default, replica_default, network_default)





# # ratio
# for protocol in protocols: 
#   for ratio in ratios:
#     make_cmd(protocol, thread_default, rw_ratio_default, ratio, zipf_default, replica_default, network_default)

# zipf
# for protocol in protocols: 
#   for zipf in zipfs:
#     make_cmd(protocol, thread_default, rw_ratio_default, ratio_default, zipf, replica_default, network_default)


# replica
# for protocol in protocols: 
#   for replica in replicas:
#     make_cmd(protocol, thread_default, rw_ratio_default, ratio_default, zipf_default, replica, network_default)

# wide-area network
for protocol in protocols: 
  for network in networks:
    make_cmd(protocol, thread_default, rw_ratio_default, ratio_default, zipf_default, replica_default, network)