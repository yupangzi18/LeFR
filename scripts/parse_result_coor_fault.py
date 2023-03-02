import sys
import os
import time

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)
time_to_run = 1240

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

collection = int(sys.argv[1])

if collection == 0:
    path = sys.argv[2]
else:
    path = 'result-' + time.strftime('%Y%m%d%H%M%S', time.localtime())
    os.makedirs(path)
    for i in range(n):
        os.system("scp zhanhao@%s:/data/zhanhao/coor_fault_* %s/" % (outs[i], path))

ratio_default = 50
thread_default = 16
rw_ratio_default = 50
zipf_default = 0.70

protocols = ["LeFR", "Tapir", "GPAC", "2PC"]

# protocols = ["Tapir", "GPAC"]


def get_results_ycsb(i, thread, rw_ratio, ratio, zipf, protocol):
    commits[i] = [0] * time_to_run; aborts[i] = [0] * time_to_run; avg_networks[i] = [0] * time_to_run; avg_latencys[i] = [0] * time_to_run
    commit_temp = 0.0; abort_temp = 0.0; avg_network_temp = 0.0; avg_latency_temp = 0.0
    raw_res_file_path = path + '/coor_fault_ycsb_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_%s' % (i, thread, rw_ratio, ratio, zipf, protocol)
    print(raw_res_file_path)
    raw_res = open(raw_res_file_path, "r")
    line = raw_res.readline()

    num = 0

    while line:
        if "Coordinator.h:175]" in line:
            commit_temp = float(line.split('commit: ')[1].split(' ')[0])
            abort_temp = float(line.split('abort: ')[1].split(' ')[0])
            avg_network_temp = float(line.split('avg network size: ')[1].split(',')[0])
            avg_latency_temp = float(line.split('avg latency: ')[1].split(' ')[0])
            # print('%.2f\t%.2f\t%.2f\t%.2f\n' % (commit_temp, abort_temp, avg_network_temp, avg_latency_temp))

            commits[i][num] = commit_temp
            aborts[i][num] = abort_temp
            avg_networks[i][num] = avg_network_temp
            avg_latencys[i][num] = avg_latency_temp
            num += 1
        line = raw_res.readline()
    
    raw_res.close()



def get_results_tpcc(i, thread, ratio, protocol):
    commits[i] = [0] * time_to_run; aborts[i] = [0] * time_to_run; avg_networks[i] = [0] * time_to_run; avg_latencys[i] = [0] * time_to_run
    commit_temp = 0.0; abort_temp = 0.0; avg_network_temp = 0.0; avg_latency_temp = 0.0
    raw_res_file_path = path + '/coor_fault_tpcc_%d_thr_%d_cross_%d_%s' % (i, thread, ratio, protocol)
    print(raw_res_file_path)
    raw_res = open(raw_res_file_path, "r")
    line = raw_res.readline()

    num = 0

    while line:
        if "Coordinator.h:175]" in line:
            # print(line)
            commit_temp = float(line.split('commit: ')[1].split(' ')[0])
            abort_temp = float(line.split('abort: ')[1].split(' ')[0])
            avg_network_temp = float(line.split('avg network size: ')[1].split(',')[0])
            avg_latency_temp = float(line.split('avg latency: ')[1].split(' ')[0])
            # print('%.2f\t%.2f\t%.2f\t%.2f\n' % (commit_temp, abort_temp, avg_network_temp, avg_latency_temp))

            commits[i][num] = commit_temp
            aborts[i][num] = abort_temp
            avg_networks[i][num] = avg_network_temp
            avg_latencys[i][num] = avg_latency_temp
            num += 1
        line = raw_res.readline()

    raw_res.close()



for protocol in protocols:

    commits = [[]] * n; aborts = [[]] * n; avg_networks = [[]] * n; avg_latencys = [[]] * n

    for i in range(1,n):
        get_results_ycsb(i, thread_default, rw_ratio_default, ratio_default, zipf_default, protocol)

    formatted_res_file_path = path + '/summary_coor_fault_ycsb_%s' % (protocol)
    new_file = open(formatted_res_file_path, "a")

    for j in range(time_to_run):
        commit_num=0.0; abort_num = 0.0; avg_network_num = 0.0; avg_latency_num = 0.0
        for i in range(1,n):
            commit_num += commits[i][j]
            abort_num += aborts[i][j]
            avg_network_num += avg_networks[i][j]
            avg_latency_num += avg_latencys[i][j]
        print (commit_num)
        new_file.write('%d\t%.2f\t%.2f\t%.2f\t%.2f\n' % (j, commit_num, abort_num, avg_network_num, avg_latency_num))

    new_file.close()




# for protocol in protocols:

#     commits = [[]] * n; aborts = [[]] * n; avg_networks = [[]] * n; avg_latencys = [[]] * n

#     for i in range(1,n):
#         get_results_tpcc(i, thread_default, ratio_default, protocol)

#     formatted_res_file_path = path + '/summary_coor_fault_tpcc_%s' % (protocol)
#     new_file = open(formatted_res_file_path, "a")

#     for j in range(time_to_run):
#         commit_num=0.0; abort_num = 0.0; avg_network_num = 0.0; avg_latency_num = 0.0
#         for i in range(1,n):
#             commit_num += commits[i][j]
#             abort_num += aborts[i][j]
#             avg_network_num += avg_networks[i][j]
#             avg_latency_num += avg_latencys[i][j]
#         print (commit_num)
#         new_file.write('%d\t%.2f\t%.2f\t%.2f\t%.2f\n' % (j, commit_num, abort_num, avg_network_num, avg_latency_num))

#     new_file.close()