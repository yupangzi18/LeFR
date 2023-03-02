import sys
import os
import time

collection = int(sys.argv[1])

ips = [line.strip() for line in open("ips_all.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

if collection == 0:
    path = sys.argv[2]
else:
    path = 'result-' + time.strftime('%Y%m%d%H%M%S', time.localtime())
    os.makedirs(path)
    for i in range(n):
        os.system("scp zhanhao@%s:/data/zhanhao/res_%d_tot_* %s/" % (outs[i], i, path))
        os.system("scp zhanhao@%s:/data/zhanhao/tpcc_%d_tot_* %s/" % (outs[i], i, path))

ratio_default = 50
thread_default = 16
rw_ratio_default = 50
zipf_default = 0.50
replica_default = "hash5"
network_default = 0


cross_nodes = [8, 12, 16, 20, 24]
# cross_nodes = [8, 12, 16]
# protocols = ["LeFR", "Tapir", "GPAC", "2PC"]
# protocols = ["LeFR", "Tapir", "2PC"]
protocols = ["LeFR"]


def get_results(n, thread, rw_ratio, ratio, zipf, replica, network, protocol):
    for i in range(n):
        commit_temp=0.0; abort_temp = 0.0; avg_network_temp = 0.0; avg_latency_temp = 0.0
        raw_res_file_path = path + '/res_%d_tot_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_%s_net_%d_%s' % (i, n, thread, rw_ratio, ratio, zipf, replica, network, protocol)
        print(raw_res_file_path)
        raw_res = open(raw_res_file_path, "r")
        line = raw_res.readline()
        # is_first = 1
        while line:
            if "[summary]" in line:
                commit_temp = float(line.split('commit: ')[1].split(' ')[0])
                abort_temp = float(line.split('abort: ')[1].split(' ')[0])
                avg_network_temp = float(line.split('avg network size: ')[1].split(',')[0])
                avg_latency_temp = float(line.split('avg latency: ')[1].split(' ')[0])
                print(avg_latency_temp)
            line = raw_res.readline()

        commits.append(commit_temp) 
        aborts.append(abort_temp)
        avg_networks.append(avg_network_temp)
        avg_latencys.append(avg_latency_temp)
        raw_res.close()


def get_results_tpcc(n, thread, ratio, partition, network, protocol):
    for i in range(n):
        commit_temp=0.0; abort_temp = 0.0; avg_network_temp = 0.0; avg_latency_temp = 0.0
        raw_res_file_path = path + '/tpcc_%d_tot_%d_thr_%d_cross_%d_par_%d_net_%d_%s' % (i, n, thread, ratio, partition, network, protocol)
        print(raw_res_file_path)
        raw_res = open(raw_res_file_path, "r")
        line = raw_res.readline()
        is_first = 1
        while line:
            if "[summary]" in line:
                commit_temp = float(line.split('commit: ')[1].split(' ')[0])
                abort_temp = float(line.split('abort: ')[1].split(' ')[0])
                avg_network_temp = float(line.split('avg network size: ')[1].split(',')[0])
                avg_latency_temp = float(line.split('avg latency: ')[1].split(' ')[0])
            line = raw_res.readline()

        commits.append(commit_temp) 
        aborts.append(abort_temp)
        avg_networks.append(avg_network_temp)
        avg_latencys.append(avg_latency_temp)
        raw_res.close()



# ycsb
for protocol in protocols: 
    formatted_res_file_path = path + '/scale_%s' % (protocol)
    new_file = open(formatted_res_file_path, "a")

    for n in cross_nodes:

        os.system(" head -%d ips_all.txt >ips.txt" % (n))
        ips = [line.strip() for line in open("ips.txt", "r")]
        n = len(ips)
        
        ins = [line.split("\t")[0] for line in ips]
        outs = [line.split("\t")[1] for line in ips]
        
        partition_default = n*thread_default


        # if collection != 0: 
        #     for i in range(n):
        #         os.system("scp zhanhao@%s:/data/zhanhao/res_%d_tot_* %s/" % (outs[i], i, path))


        commits = []; aborts = []; avg_networks = []; avg_latencys = []
        commit_num=0.0; abort_num = 0.0; avg_network_num = 0.0; avg_latency_num = 0.0

        get_results(n, thread_default, rw_ratio_default, ratio_default, zipf_default, replica_default, network_default, protocol)
            
        for i in range(n):
            commit_num += commits[i]
            abort_num += aborts[i]
            avg_network_num += avg_networks[i]
            avg_latency_num += avg_latencys[i]
        avg_network_num /= n
        avg_latency_num /= n
        new_file.write('%s\t%.2f\t%.2f\t%.2f\t%.2f\n' % (n, commit_num, abort_num, avg_network_num, avg_latency_num))
    new_file.close()

#tpcc
for protocol in protocols: 
    formatted_res_file_path = path + '/tpcc_scale_%s' % (protocol)
    new_file = open(formatted_res_file_path, "a")

    for n in cross_nodes:

        os.system(" head -%d ips_all.txt >ips.txt" % (n))
        ips = [line.strip() for line in open("ips.txt", "r")]
        n = len(ips)
        
        ins = [line.split("\t")[0] for line in ips]
        outs = [line.split("\t")[1] for line in ips]
        
        partition_default = n*thread_default


        # if collection != 0: 
        #     for i in range(n):
        #         os.system("scp zhanhao@%s:/data/zhanhao/tpcc_%d_tot_* %s/" % (outs[i], i, path))


        commits = []; aborts = []; avg_networks = []; avg_latencys = []
        commit_num=0.0; abort_num = 0.0; avg_network_num = 0.0; avg_latency_num = 0.0

        get_results_tpcc(n, thread_default, ratio_default, partition_default, network_default, protocol)
        
        for i in range(n):
            commit_num += commits[i]
            abort_num += aborts[i]
            avg_network_num += avg_networks[i]
            avg_latency_num += avg_latencys[i]
        avg_network_num /= n
        avg_latency_num /= n
        new_file.write('%d\t%.2f\t%.2f\t%.2f\t%.2f\n' % (n, commit_num, abort_num, avg_network_num, avg_latency_num))
    new_file.close()
