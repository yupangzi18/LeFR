import sys
import os
import time

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)
time_to_run = 600

ins = [line.split("\t")[0] for line in ips]
outs = [line.split("\t")[1] for line in ips]

collection = int(sys.argv[1])

if collection == 0:
    path = sys.argv[2]
else:
    path = 'result-' + time.strftime('%Y%m%d%H%M%S', time.localtime())
    os.makedirs(path)
    for i in range(n):
        os.system("scp zhanhao@%s:/data/zhanhao/fault_* %s/" % (outs[i], path))

ratio_default = 50
thread_default = 32
rw_ratio_default = 50
zipf_default = 0.60

# protocols = ["LeFR", "Tapir", "GPAC", "2PC"]
# ratios = [0, 25, 50, 75, 100]
# threads = [2, 4, 8, 12, 16]
# rw_ratios = [0, 25, 50, 75, 100]
# zipfs = [0.00, 0.25, 0.50, 0.75, 0.95]

protocols = ["LeFR", "2PC"]
# ratios = [50]
# threads = [8]
# rw_ratios = [50]
# zipfs = [0.50]



def get_results_ycsb(i, thread, rw_ratio, ratio, zipf, protocol):
    # for i in range(n):

    commits[i] = [0] * 600; aborts[i] = [0] * 600; avg_networks[i] = [0] * 600; avg_latencys[i] = [0] * 600; normallongtxn[i] = [0] * 600; normallongtxn_latency[i] = [0] * 600; ourlongtxn[i] = [0] * 600; ourlongtxn_latency[i] = [0] * 600; alllongtxn[i] = [0] * 600; alllongtxn_latency[i] = [0] * 600
    commit_temp = 0.0; abort_temp = 0.0; avg_network_temp = 0.0; avg_latency_temp = 0.0; normallongtxn_temp = 0.0; normallongtxn_latency_temp = 0.0; ourlongtxn_temp = 0.0; ourlongtxn_latency_temp = 0.0; alllongtxn_temp = 0.0; alllongtxn_latency_temp = 0.0
    raw_res_file_path = path + '/fault_ycsb_%d_thr_%d_rw_%d_cross_%d_zipf_%.2f_%s' % (i, thread, rw_ratio, ratio, zipf, protocol)
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
            normallongtxn_temp = float(line.split('normal longtxn number: ')[1].split(',')[0])
            normallongtxn_latency_temp = float(line.split('normal longtxn latency: ')[1].split(' ')[0])
            ourlongtxn_temp = float(line.split('our recovering longtxn number: ')[1].split(',')[0])
            ourlongtxn_latency_temp = float(line.split('the our latency: ')[1].split(' ')[0])
            alllongtxn_temp = float(line.split('all redo longtxn number: ')[1].split(',')[0])
            alllongtxn_latency_temp = float(line.split('the all latency: ')[1].split(' ')[0])
            # print('%.2f\t%.2f\t%.2f\t%.2f\n' % (commit_temp, abort_temp, avg_network_temp, avg_latency_temp))

            commits[i][num] = commit_temp
            aborts[i][num] = abort_temp
            avg_networks[i][num] = avg_network_temp
            avg_latencys[i][num] = avg_latency_temp
            normallongtxn[i][num] = normallongtxn_temp
            normallongtxn_latency[i][num] = normallongtxn_latency_temp
            ourlongtxn[i][num] = ourlongtxn_temp
            ourlongtxn_latency[i][num] = ourlongtxn_latency_temp
            alllongtxn[i][num] = alllongtxn_temp
            alllongtxn_latency[i][num] = alllongtxn_latency_temp
            num += 1
        line = raw_res.readline()
    
    raw_res.close()



def get_results_tpcc(i, thread, ratio, protocol):
    # for i in range(n):

    commits[i] = [0] * 600; aborts[i] = [0] * 600; avg_networks[i] = [0] * 600; avg_latencys[i] = [0] * 600; normallongtxn[i] = [0] * 600; normallongtxn_latency[i] = [0] * 600; ourlongtxn[i] = [0] * 600; ourlongtxn_latency[i] = [0] * 600; alllongtxn[i] = [0] * 600; alllongtxn_latency[i] = [0] * 600
    commit_temp = 0.0; abort_temp = 0.0; avg_network_temp = 0.0; avg_latency_temp = 0.0; normallongtxn_temp = 0.0; normallongtxn_latency_temp = 0.0; ourlongtxn_temp = 0.0; ourlongtxn_latency_temp = 0.0; alllongtxn_temp = 0.0; alllongtxn_latency_temp = 0.0
    raw_res_file_path = path + '/fault_tpcc_%d_thr_%d_cross_%d_%s' % (i, thread, ratio, protocol)
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
            normallongtxn_temp = float(line.split('normal longtxn number: ')[1].split(',')[0])
            normallongtxn_latency_temp = float(line.split('normal longtxn latency: ')[1].split(' ')[0])
            ourlongtxn_temp = float(line.split('our recovering longtxn number: ')[1].split(',')[0])
            ourlongtxn_latency_temp = float(line.split('the our latency: ')[1].split(' ')[0])
            alllongtxn_temp = float(line.split('all redo longtxn number: ')[1].split(',')[0])
            alllongtxn_latency_temp = float(line.split('the all latency: ')[1].split(' ')[0])
            # print('%.2f\t%.2f\t%.2f\t%.2f\n' % (commit_temp, abort_temp, avg_network_temp, avg_latency_temp))

            commits[i][num] = commit_temp
            aborts[i][num] = abort_temp
            avg_networks[i][num] = avg_network_temp
            avg_latencys[i][num] = avg_latency_temp
            normallongtxn[i][num] = normallongtxn_temp
            normallongtxn_latency[i][num] = normallongtxn_latency_temp
            ourlongtxn[i][num] = ourlongtxn_temp
            ourlongtxn_latency[i][num] = ourlongtxn_latency_temp
            alllongtxn[i][num] = alllongtxn_temp
            alllongtxn_latency[i][num] = alllongtxn_latency_temp
            num += 1
        line = raw_res.readline()
    
    # commits_all[i].append(commit_temp) 
    # aborts[i].append(abort_temp)
    # avg_networks[i].append(avg_network_temp)
    # avg_latencys[i].append(avg_latency_temp)

    # new_file.write('%.2f\t%.2f\t%.2f\t%.2f\n' % (commit_num, abort_num, avg_network_num, avg_latency_num))
    raw_res.close()


for protocol in protocols:

    commits = [[]] * n; aborts = [[]] * n; avg_networks = [[]] * n; avg_latencys = [[]] * n; normallongtxn = [[]] * n; normallongtxn_latency = [[]] * n; ourlongtxn = [[]] * n; ourlongtxn_latency = [[]] * n; alllongtxn = [[]] * n; alllongtxn_latency = [[]] * n

    for i in range(n):
        get_results_ycsb(i, thread_default, rw_ratio_default, ratio_default, zipf_default, protocol)

    formatted_res_file_path = path + '/summary_fault_ycsb_%s' % (protocol)
    new_file = open(formatted_res_file_path, "a")

    for j in range(600):
        commit_num=0.0; abort_num = 0.0; avg_network_num = 0.0; avg_latency_num = 0.0; normallongtxn_num = 0.0; normallongtxn_latency_num = 0.0; ourlongtxn_num = 0.0; ourlongtxn_latency_num = 0.0; alllongtxn_num = 0.0; alllongtxn_latency_num = 0.0
        for i in range(n):
            commit_num += commits[i][j]
            abort_num += aborts[i][j]
            avg_network_num += avg_networks[i][j]
            avg_latency_num += avg_latencys[i][j]
            normallongtxn_num += normallongtxn[i][j]
            normallongtxn_latency_num += normallongtxn_latency[i][j]
            ourlongtxn_num += ourlongtxn[i][j]
            ourlongtxn_latency_num += ourlongtxn_latency[i][j]
            alllongtxn_num += alllongtxn[i][j]
            alllongtxn_latency_num += alllongtxn_latency[i][j]
        print (commit_num)
        new_file.write('%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n' % (j, commit_num, abort_num, avg_network_num, avg_latency_num, normallongtxn_num, normallongtxn_latency_num, ourlongtxn_num, ourlongtxn_latency_num, alllongtxn_num, alllongtxn_latency_num))

    new_file.close()

    # print(commits)
    # for i in range(n):
    #     commit_num += commits[i]
    #     abort_num += aborts[i]
    #     avg_network_num += avg_networks[i]
    #     avg_latency_num += avg_latencys[i]

    # avg_network_num /= n
    # avg_latency_num /= n
    # new_file.write('%d\t%.2f\t%.2f\t%.2f\t%.2f\n' % (i, commit_num, abort_num, avg_network_num, avg_latency_num))


for protocol in protocols:

    commits = [[]] * n; aborts = [[]] * n; avg_networks = [[]] * n; avg_latencys = [[]] * n; normallongtxn = [[]] * n; normallongtxn_latency = [[]] * n; ourlongtxn = [[]] * n; ourlongtxn_latency = [[]] * n; alllongtxn = [[]] * n; alllongtxn_latency = [[]] * n

    for i in range(n):
        get_results_tpcc(i, thread_default, ratio_default, protocol)

    formatted_res_file_path = path + '/summary_fault_tpcc_%s' % (protocol)
    new_file = open(formatted_res_file_path, "a")

    for j in range(600):
        commit_num=0.0; abort_num = 0.0; avg_network_num = 0.0; avg_latency_num = 0.0; normallongtxn_num = 0.0; normallongtxn_latency_num = 0.0; ourlongtxn_num = 0.0; ourlongtxn_latency_num = 0.0; alllongtxn_num = 0.0; alllongtxn_latency_num = 0.0
        for i in range(n):
            commit_num += commits[i][j]
            abort_num += aborts[i][j]
            avg_network_num += avg_networks[i][j]
            avg_latency_num += avg_latencys[i][j]
            normallongtxn_num += normallongtxn[i][j]
            normallongtxn_latency_num += normallongtxn_latency[i][j]
            ourlongtxn_num += ourlongtxn[i][j]
            ourlongtxn_latency_num += ourlongtxn_latency[i][j]
            alllongtxn_num += alllongtxn[i][j]
            alllongtxn_latency_num += alllongtxn_latency[i][j]
        print (commit_num)
        new_file.write('%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n' % (j, commit_num, abort_num, avg_network_num, avg_latency_num, normallongtxn_num, normallongtxn_latency_num, ourlongtxn_num, ourlongtxn_latency_num, alllongtxn_num, alllongtxn_latency_num))

    new_file.close()