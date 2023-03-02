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
zipf_default = 0.60

# protocols = ["LeFR", "Tapir", "GPAC"]
# ratios = [0, 25, 50, 75, 100]
# threads = [2, 4, 8, 12, 16]
# rw_ratios = [0, 25, 50, 75, 100]
# zipfs = [0.00, 0.25, 0.50, 0.75, 0.95]

replica = 'hash3'

protocols = ["LeFR"]
ratios = [50]
threads = [4]
rw_ratios = [50]
zipfs = [0.50]


twopc = 'false'
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

def make_cmd_ycsb(protocol, thread, rw_ratio, ratio, zipf):
  global i
  if protocol == "LeFR" and twopc == 'true':
    if flush_granularity == 'op':
      cmd = get_cmd(n, i)
      print('timeout 700s ~/LRFR/bench_ycsb  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --read_write_ratio=%d --cross_ratio=%d --zipf=%.2f --traditional_2pc=%s --flush_granularity=%s --datanode_failure=true --batch_flush=200 > fault_ycsb_res_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_PPC 2>&1' % (id, cmd, protocol, thread*n, thread, replica, rw_ratio, ratio, zipf, twopc, flush_granularity, id, thread, rw_ratio, ratio, zipf))
      print('sleep 10')
      i += 1
    cmd = get_cmd(n, i)
    print('timeout 700s ~/LRFR/bench_ycsb  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --read_write_ratio=%d --cross_ratio=%d --zipf=%.2f --traditional_2pc=%s --datanode_failure=true --batch_flush=200 > fault_ycsb_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_2PC 2>&1' % (id, cmd, protocol, thread*n, thread, replica, rw_ratio, ratio, zipf, twopc, id, thread, rw_ratio, ratio, zipf))
    print('sleep 10')
    i += 1
  cmd = get_cmd(n, i)
  print('timeout 700s ~/LRFR/bench_ycsb  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --read_write_ratio=%d --cross_ratio=%d --zipf=%.2f --datanode_failure=true --batch_flush=200 > fault_ycsb_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_%s 2>&1' % (id, cmd, protocol, thread*n, thread, replica, rw_ratio, ratio, zipf, id, thread, rw_ratio, ratio, zipf, protocol))
  print('sleep 10')
  i += 1


def make_cmd_tpcc(protocol, thread, ratio):
  global i
  if protocol == "LeFR" and twopc == 'true':
    if flush_granularity == 'op':
      cmd = get_cmd(n, i)
      print('timeout 700s ~/LRFR/bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --query=neworder --neworder_dist=%d --payment_dist=%d --traditional_2pc=%s --flush_granularity=%s --datanode_failure=true --batch_flush=200> fault_tpcc_%d_thr_%d_cross_%d_PPC 2>&1' % (id, cmd, protocol, thread*n, replica, thread, ratio, ratio, twopc, flush_granularity, id, thread, ratio))
      print('sleep 10')
      i += 1
    cmd = get_cmd(n, i)
    print('timeout 700s ~/LRFR/bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --query=neworder --neworder_dist=%d --payment_dist=%d --traditional_2pc=%s --datanode_failure=true --batch_flush=200> fault_tpcc_%d_thr_%d_cross_%d_2PC 2>&1' % (id, cmd, protocol, thread*n, thread, replica, ratio, ratio, twopc, id, thread, ratio))
    print('sleep 10')
    i += 1
  cmd = get_cmd(n, i)
  print('timeout 700s ~/LRFR/bench_tpcc  --id=%d --servers="%s" --protocol=%s --partition_num=%d --threads=%d --partitioner=%s --query=neworder --neworder_dist=%d --payment_dist=%d --datanode_failure=true --batch_flush=200> fault_tpcc_%d_thr_%d_cross_%d_%s 2>&1' % (id, cmd, protocol, thread*n, thread, replica, ratio, ratio, id, thread, ratio, protocol))
  print('sleep 10')
  i += 1



# rwratio
for protocol in protocols: 
  make_cmd_ycsb(protocol, thread_default, rw_ratio_default, ratio_default, zipf_default)
  make_cmd_tpcc(protocol, thread_default, ratio_default)


