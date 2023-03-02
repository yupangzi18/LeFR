#pragma once

#include <boost/algorithm/string/split.hpp>

#include "gflags/gflags.h"
#include "butil/logging.h"

DEFINE_string(servers,
              "127.0.0.1:20110;127.0.0.1:20111;127.0.0.1:20112",
              "semicolon-separated list of servers");
DEFINE_int32(id, 0, "coordinator id");
DEFINE_int32(threads, 16, "the number of threads");
DEFINE_int32(io, 1, "the number of i/o threads");
DEFINE_int32(partition_num, 15, "the number of partitions");
DEFINE_string(partitioner, "hash2", "database partitioner (hash, hash2, pb)");
DEFINE_bool(sleep_on_retry, true, "sleep when retry aborted transactions");
DEFINE_int32(batch_size, 100, "lefr or calvin batch size");
DEFINE_int32(group_time, 10, "group commit frequency");
DEFINE_string(flush_granularity, "txn", "the granularity of message flush");
DEFINE_string(workload_type, "short_txn", "long_txn,hybrid_txn,short_txn");
DEFINE_uint64(time_to_run, 120, "the running time when simulating");
DEFINE_int32(simulate_interval, 10, "count throughput per simulate_interval seconds");
DEFINE_int32(batch_flush, 50, "batch flush");
DEFINE_int32(batch_op_flush, 20, "batch operation flush");
DEFINE_int32(sleep_time, 1000, "retry sleep time");
DEFINE_string(protocol, "LeFR", "transaction protocol");
DEFINE_string(replica_group, "1,3", "calvin replica group");
DEFINE_string(lock_manager, "1,1", "calvin lock manager");
DEFINE_bool(read_on_replica, false, "read from replicas");
DEFINE_bool(local_validation, false, "local validation");
DEFINE_bool(rts_sync, false, "rts sync");
DEFINE_bool(star_sync, false, "synchronous write in the single-master phase");
DEFINE_bool(star_dynamic_batch_size, false, "dynamic batch size");
DEFINE_bool(plv, true, "parallel locking and validation");
DEFINE_bool(calvin_same_batch, false, "always run the same batch of txns.");
DEFINE_bool(kiva_read_only, true, "kiva read only optimization");
DEFINE_bool(kiva_reordering, true, "kiva reordering optimization");
DEFINE_bool(kiva_si, false, "kiva snapshot isolation");
DEFINE_int32(delay, 0, "delay time in us.");
DEFINE_string(cdf_path, "", "path to cdf");
DEFINE_string(log_path, "", "path to disk logging.");
DEFINE_bool(tcp_no_delay, true, "TCP Nagle algorithm, true: disable nagle");
DEFINE_bool(tcp_quick_ack, false, "TCP quick ack mode, true: enable quick ack");
DEFINE_bool(cpu_affinity, true, "pinning each thread to a separate core");
DEFINE_int32(cpu_core_id, 0, "cpu core id");
DEFINE_bool(traditional_2pc, false, "traditional 2pc");
DEFINE_bool(datanode_failure, false, "data node failure");
DEFINE_bool(coordinator_failure, false, "coordinator failure");
DEFINE_int32(failure_num, 0, "for 2f+1 replicas, the maximum number of failure replicas is f");
DEFINE_string(failure_nodes, "0", "list of failure data nodes");
DEFINE_int32(recovery_time, 2000, "coordinator recovery time");
DEFINE_int32(failure_interval, 60, "failure interval");
DEFINE_int32(failure_time, 10, "failure time");
DEFINE_int32(network_latency, 0, "simulate the network latency");
DEFINE_int32(max_cross_nodes, 20, "the max number of the nodes a txn processes");
DEFINE_bool(async_batch_replication, false, "replicate async in a batch");
DEFINE_int32(raft_port, 8100, "raft replication port");
DEFINE_string(raft_conf, "127.0.1.1:8100:0,127.0.1.1:8101:0,127.0.1.1:8102:0,", "raft service configuration");
DEFINE_int32(election_timeout_ms, 5000, "raft election time limitation");
DEFINE_int32(snapshot_interval, 30, "raft replication snapshot interval");
DEFINE_string(data_path, "./data", "log data path");
DEFINE_string(raft_group, "Replication0", "raft group id");

#define SETUP_CONTEXT(context)                                                                  \
    boost::algorithm::split(context.peers, FLAGS_servers, boost::is_any_of(";"));               \
    context.coordinator_num = context.peers.size();                                             \
    context.coordinator_id = FLAGS_id;                                                          \
    context.worker_num = FLAGS_threads;                                                         \
    context.io_thread_num = FLAGS_io;                                                           \
    context.partition_num = FLAGS_partition_num;                                                \
    context.partitioner = FLAGS_partitioner;                                                    \
    context.sleep_on_retry = FLAGS_sleep_on_retry;                                              \
    context.batch_size = FLAGS_batch_size;                                                      \
    context.group_time = FLAGS_group_time;                                                      \
    context.flush_granularity = FLAGS_flush_granularity;                                        \
    context.workload_type = FLAGS_workload_type;                                                \
    context.time_to_run = FLAGS_time_to_run;                                                    \
    context.batch_flush = FLAGS_batch_flush;                                                    \
    context.batch_op_flush = FLAGS_batch_op_flush;                                              \
    context.sleep_time = FLAGS_sleep_time;                                                      \
    context.protocol = FLAGS_protocol;                                                          \
    context.replica_group = FLAGS_replica_group;                                                \
    context.lock_manager = FLAGS_lock_manager;                                                  \
    context.read_on_replica = FLAGS_read_on_replica;                                            \
    context.local_validation = FLAGS_local_validation;                                          \
    context.rts_sync = FLAGS_rts_sync;                                                          \
    context.star_sync_in_single_master_phase = FLAGS_star_sync;                                 \
    context.star_dynamic_batch_size = FLAGS_star_dynamic_batch_size;                            \
    context.parallel_locking_and_validation = FLAGS_plv;                                        \
    context.calvin_same_batch = FLAGS_calvin_same_batch;                                        \
    context.kiva_read_only_optmization = FLAGS_kiva_read_only;                                  \
    context.kiva_reordering_optmization = FLAGS_kiva_reordering;                                \
    context.kiva_snapshot_isolation = FLAGS_kiva_si;                                            \
    context.delay_time = FLAGS_delay;                                                           \
    context.log_path = FLAGS_log_path;                                                          \
    context.cdf_path = FLAGS_cdf_path;                                                          \
    context.tcp_no_delay = FLAGS_tcp_no_delay;                                                  \
    context.tcp_quick_ack = FLAGS_tcp_quick_ack;                                                \
    context.cpu_affinity = FLAGS_cpu_affinity;                                                  \
    context.cpu_core_id = FLAGS_cpu_core_id;                                                    \
    context.traditional_2pc = FLAGS_traditional_2pc;                                            \
    context.datanode_failure = FLAGS_datanode_failure;                                          \
    context.coordinator_failure = FLAGS_coordinator_failure;                                    \
    boost::algorithm::split(context.failure_nodes, FLAGS_failure_nodes, boost::is_any_of(";")); \
    context.failure_num = context.failure_nodes.size();                                         \
    context.recovery_time = FLAGS_recovery_time;                                                \
    context.failure_interval = FLAGS_failure_interval;                                          \
    context.failure_time = FLAGS_failure_time;                                                  \
    context.network_latency = FLAGS_network_latency;                                            \
    context.max_cross_nodes = FLAGS_max_cross_nodes;                                            \
    context.async_batch_replication = FLAGS_async_batch_replication;                            \
    context.raft_port = FLAGS_raft_port;                                                        \
    context.raft_conf = FLAGS_raft_conf;                                                        \
    context.election_timeout_ms = FLAGS_election_timeout_ms;                                    \
    context.snapshot_interval = FLAGS_snapshot_interval;                                        \
    context.data_path = FLAGS_data_path;                                                        \
    context.raft_group = FLAGS_raft_group;                                                      \
    context.set_star_partitioner();
