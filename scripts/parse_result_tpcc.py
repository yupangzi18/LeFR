import sys
import os
import time

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

collection = int(sys.argv[1])

if collection == 0:
    path = sys.argv[2]
else:
    path = 'result-' + time.strftime('%Y%m%d%H%M%S', time.localtime())
    os.makedirs(path)
    for i in range(n):
        os.system("scp zhanhao@%s:/data/zhanhao/tpcc_* %s/" % (outs[i], path))


ratio_default = 100
thread_default = 16
partition_default = n*thread_default
network_default = 0

protocols = ["LeFR", "Tapir", "GPAC", "2PC"]
ratios = [0, 25, 50, 75, 100]
threads = [2, 4, 8, 12, 16]
partitions = [20, 40, 60, 80, 100, 120]
# networks = [0, 1, 2, 3, 6, 9]

# protocols = ["LeFR"]
# ratios = [30]
# threads = [4]
networks = [0]



def get_results(n, thread, ratio, partition, network, protocol):
    for i in range(n):
        commit_temp=0.0; abort_temp = 0.0; avg_network_temp = 0.0; avg_latency_temp = 0.0; rw_network_size_temp = 0.0; pr_network_size_temp = 0.0; com_network_size_temp = 0.0; rw_message_count_temp = 0.0; pr_message_count_temp = 0.0; com_message_count_temp = 0.0
        raw_res_file_path = path + '/tpcc_%d_thr_%d_cross_%d_par_%d_net_%d_%s' % (i, thread, ratio, partition, network, protocol)
        print(raw_res_file_path)
        raw_res = open(raw_res_file_path, "r")
        line = raw_res.readline()
        is_first = 1
        while line:
            # if "Coordinator.h:152]" in line and is_first == 1:
            #     commit_temp = float(line.split('commit: ')[1].split(' ')[0])
            #     abort_temp = float(line.split('abort: ')[1].split(' ')[0])
            #     avg_network_temp = float(line.split('avg network size: ')[1].split(',')[0])
            #     avg_latency_temp = float(line.split('avg latency: ')[1].split(' ')[0])
            #     is_first = 0
            if "[summary]" in line:
                # commit_temp_summary = float(line.split('commit: ')[1].split(' ')[0])
                # if (commit_temp_summary < commit_temp):
                #     break
                # else:
                commit_temp = float(line.split('commit: ')[1].split(' ')[0])
                abort_temp = float(line.split('abort: ')[1].split(' ')[0])
                avg_network_temp = float(line.split('avg network size: ')[1].split(',')[0])
                avg_latency_temp = float(line.split('avg latency: ')[1].split(' ')[0])
                rw_network_size_temp = float(line.split('avg rw network size: ')[1].split(',')[0])
                pr_network_size_temp = float(line.split('avg pr network size: ')[1].split(',')[0])
                com_network_size_temp = float(line.split('avg com network size: ')[1].split(',')[0])
                rw_message_count_temp = float(line.split('avg rw message count: ')[1].split(',')[0])
                pr_message_count_temp = float(line.split('avg pr message count: ')[1].split(',')[0])
                com_message_count_temp = float(line.split('avg com message count: ')[1].split(' ')[0])
            line = raw_res.readline()

        commits.append(commit_temp) 
        aborts.append(abort_temp)
        avg_networks.append(avg_network_temp)
        avg_latencys.append(avg_latency_temp)
        rw_network_size.append(rw_network_size_temp)
        pr_network_size.append(pr_network_size_temp)
        com_network_size.append(com_network_size_temp)
        rw_message_count.append(rw_message_count_temp)
        pr_message_count.append(pr_message_count_temp)
        com_message_count.append(com_message_count_temp)
        raw_res.close()


# # varying distributed transaction ratio
# for protocol in protocols: 
#     formatted_res_file_path = path + '/cross_%s' % (protocol)
#     new_file = open(formatted_res_file_path, "a")
#     for ratio in ratios:
#         commits = []; aborts = []; avg_networks = []; avg_latencys = []
#         commit_num=0.0; abort_num = 0.0; avg_network_num = 0.0; avg_latency_num = 0.0

#         get_results(n, thread_default, ratio, partition_default, network_default, protocol)

#         for i in range(n):
#             commit_num += commits[i]
#             abort_num += aborts[i]
#             avg_network_num += avg_networks[i]
#             avg_latency_num += avg_latencys[i]
#         avg_network_num /= n
#         avg_latency_num /= n
#         new_file.write('%d\t%.2f\t%.2f\t%.2f\t%.2f\n' % (ratio, commit_num, abort_num, avg_network_num, avg_latency_num))
#     new_file.close()



# # varying threads
# for protocol in protocols: 
#     formatted_res_file_path = path + '/thr_%s' % (protocol)
#     new_file = open(formatted_res_file_path, "a")
#     for thread in threads:
#         commits = []; aborts = []; avg_networks = []; avg_latencys = []
#         commit_num=0.0; abort_num = 0.0; avg_network_num = 0.0; avg_latency_num = 0.0

#         get_results(n, thread, ratio_default, partition_default, network_default, protocol)
        
#         for i in range(n):
#             commit_num += commits[i]
#             abort_num += aborts[i]
#             avg_network_num += avg_networks[i]
#             avg_latency_num += avg_latencys[i]
#         avg_network_num /= n
#         avg_latency_num /= n
#         new_file.write('%d\t%.2f\t%.2f\t%.2f\t%.2f\n' % (thread, commit_num, abort_num, avg_network_num, avg_latency_num))
#     new_file.close()

# varying network latency
for protocol in protocols: 
    formatted_res_file_path = path + '/network_%s' % (protocol)
    new_file = open(formatted_res_file_path, "a")
    for network in networks:
        commits = []; aborts = []; avg_networks = []; avg_latencys = []; rw_network_size = []; pr_network_size = []; com_network_size = []; rw_message_count = []; pr_message_count = []; com_message_count = []
        commit_num=0.0; abort_num = 0.0; avg_network_num = 0.0; avg_latency_num = 0.0; rw_network_size_num = 0.0; pr_network_size_num = 0.0; com_network_size_num = 0.0; rw_message_count_num = 0.0; pr_message_count_num = 0.0; com_message_count_num = 0.0

        get_results(n, thread_default, ratio_default, partition_default, network, protocol)
        
        for i in range(n):
            commit_num += commits[i]
            abort_num += aborts[i]
            avg_network_num += avg_networks[i]
            avg_latency_num += avg_latencys[i]
            rw_network_size_num += rw_network_size[i]
            pr_network_size_num += pr_network_size[i]
            com_network_size_num += com_network_size[i]
            rw_message_count_num += rw_message_count[i]
            pr_message_count_num += pr_message_count[i]
            com_message_count_num += com_message_count[i]
        avg_network_num /= n
        avg_latency_num /= n
        rw_network_size_num /= n
        pr_network_size_num /= n
        com_network_size_num /= n
        rw_message_count_num /= n
        pr_message_count_num /= n
        com_message_count_num /= n
        new_file.write('%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n' % (network, commit_num, abort_num, avg_network_num, avg_latency_num, rw_network_size_num, pr_network_size_num, com_network_size_num, rw_message_count_num, pr_message_count_num, com_message_count_num))
    new_file.close()



# # varying parititions
# for protocol in protocols: 
#     formatted_res_file_path = path + '/par_%s' % (protocol)
#     new_file = open(formatted_res_file_path, "a")
#     for partition in partitions:
#         commits = []; aborts = []; avg_networks = []; avg_latencys = []
#         commit_num=0.0; abort_num = 0.0; avg_network_num = 0.0; avg_latency_num = 0.0

#         get_results(n, thread, ratio_default, partition, network_default, protocol)
        
#         for i in range(n):
#             commit_num += commits[i]
#             abort_num += aborts[i]
#             avg_network_num += avg_networks[i]
#             avg_latency_num += avg_latencys[i]
#         avg_network_num /= n
#         avg_latency_num /= n
#         new_file.write('%d\t%.2f\t%.2f\t%.2f\t%.2f\n' % (partition, commit_num, abort_num, avg_network_num, avg_latency_num))
#     new_file.close()