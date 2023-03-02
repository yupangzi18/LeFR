#pragma once

#include <butil/logging.h>

#include <boost/algorithm/string.hpp>
#include <fstream>
#include <iostream>
#include <thread>
#include <vector>

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "common/Socket.h"
#include "core/ControlMessage.h"
#include "core/Dispatcher.h"
#include "core/Executor.h"
#include "core/Partitioner.h"
#include "core/Worker.h"
#include "core/factory/WorkerFactory.h"

namespace lefr {

class Coordinator {
public:
    template <class Database, class Context>
    Coordinator(std::size_t id, Database &db, const Context &context)
        : id(id),
          coordinator_num(context.peers.size()),
          peers(context.peers),
          context(context),
          partitioner(PartitionerFactory::create_partitioner(
              context.partitioner, id, context.coordinator_num, context.partition_num)) {
        workerStopFlag.store(false);
        ioStopFlag.store(false);
        workerFaultFlag.store(false);
        masterTransferFlag.store(false);
        masterRecoveryFlag.store(false);
        coordinatorFaultFlag.store(false);
        n_redo_txns.store(0);
        n_committing_txns.store(0);
        fault_partitions.clear();
        fault_partitionids.clear();
        LOG(INFO) << "Coordinator initializes " << context.worker_num << " workers.";
        LOG(INFO) << "is traditional_2pc " << context.traditional_2pc;
        choose_failure_nodes();
        workers = WorkerFactory::create_workers(
            id, db, context, *(partitioner.get()), workerStopFlag, workerFaultFlag,
            masterTransferFlag, masterRecoveryFlag, coordinatorFaultFlag, n_committing_txns,
            n_redo_txns, fault_partitions, faultNodes, faultMap, txnqueue);

        // init sockets vector
        inSockets.resize(context.io_thread_num);
        outSockets.resize(context.io_thread_num);

        for (auto i = 0u; i < context.io_thread_num; i++) {
            inSockets[i].resize(peers.size());
            outSockets[i].resize(peers.size());
        }
    }

    ~Coordinator() = default;

    void start() {
        // init dispatcher vector
        iDispatchers.resize(context.io_thread_num);
        oDispatchers.resize(context.io_thread_num);

        // start dispatcher threads

        std::vector<std::thread> iDispatcherThreads, oDispatcherThreads;

        for (auto i = 0u; i < context.io_thread_num; i++) {
            iDispatchers[i] = std::make_unique<IncomingDispatcher>(
                id, i, context.io_thread_num, inSockets[i], workers, in_queue, ioStopFlag);
            oDispatchers[i] = std::make_unique<OutgoingDispatcher>(
                id, i, context.io_thread_num, outSockets[i], workers, out_queue, ioStopFlag);

            iDispatcherThreads.emplace_back(&IncomingDispatcher::start, iDispatchers[i].get());
            oDispatcherThreads.emplace_back(&OutgoingDispatcher::start, oDispatchers[i].get());
            if (context.cpu_affinity) {
                pin_thread_to_core(iDispatcherThreads[i]);
                pin_thread_to_core(oDispatcherThreads[i]);
            }
        }

        std::vector<std::thread> threads;

        LOG(INFO) << "Coordinator starts to run " << workers.size() << " workers.";

        for (auto i = 0u; i < workers.size(); i++) {
            threads.emplace_back(&Worker::start, workers[i].get());
            if (context.cpu_affinity) {
                pin_thread_to_core(threads[i]);
            }
        }

        // run timeToRun seconds
        auto timeToRun = context.time_to_run, warmup = 10, cooldown = 5;
        auto startTime = std::chrono::steady_clock::now();

        uint64_t total_commit = 0, total_abort_no_retry = 0, total_abort_lock = 0,
                 total_abort_read_validation = 0, total_local = 0, total_si_in_serializable = 0,
                 total_network_size = 0, total_latency = 0, total_rw_network_size = 0,
                 total_pr_network_size = 0, total_com_network_size = 0, total_rw_message_count = 0,
                 total_pr_message_count = 0, total_com_message_count = 0;
        int count = 0;

        do {
            std::this_thread::sleep_for(std::chrono::seconds(1));

            uint64_t n_commit = 0, n_abort_no_retry = 0, n_abort_lock = 0,
                     n_abort_read_validation = 0, n_local = 0, n_si_in_serializable = 0,
                     n_network_size = 0, n_txn_latency = 0, n_normal_long_txn = 0,
                     n_normal_longtxn_latency = 0, n_allredo_long_txn = 0,
                     n_allredo_longtxn_latency = 0, n_during_fault = 0, n_fault_txn_latency = 0,
                     rw_network_size = 0, pr_network_size = 0, com_network_size = 0,
                     rw_message_count = 0, pr_message_count = 0, com_message_count = 0;

            for (auto i = 0u; i < workers.size(); i++) {
                n_commit += workers[i]->n_commit.load();
                workers[i]->n_commit.store(0);

                n_abort_no_retry += workers[i]->n_abort_no_retry.load();
                workers[i]->n_abort_no_retry.store(0);

                n_abort_lock += workers[i]->n_abort_lock.load();
                workers[i]->n_abort_lock.store(0);

                n_abort_read_validation += workers[i]->n_abort_read_validation.load();
                workers[i]->n_abort_read_validation.store(0);

                n_local += workers[i]->n_local.load();
                workers[i]->n_local.store(0);

                n_si_in_serializable += workers[i]->n_si_in_serializable.load();
                workers[i]->n_si_in_serializable.store(0);

                n_network_size += workers[i]->n_network_size.load();
                workers[i]->n_network_size.store(0);

                n_txn_latency += workers[i]->n_txn_latency.load();
                workers[i]->n_txn_latency.store(0);

                n_normal_long_txn += workers[i]->n_normal_long_txn.load();
                workers[i]->n_normal_long_txn.store(0);

                n_normal_longtxn_latency += workers[i]->n_normal_longtxn_latency.load();
                workers[i]->n_normal_longtxn_latency.store(0);

                n_allredo_long_txn += workers[i]->n_allredo_long_txn.load();
                workers[i]->n_allredo_long_txn.store(0);

                n_allredo_longtxn_latency += workers[i]->n_allredo_longtxn_latency.load();
                workers[i]->n_allredo_longtxn_latency.store(0);

                n_during_fault += workers[i]->n_during_fault.load();
                workers[i]->n_during_fault.store(0);

                n_fault_txn_latency += workers[i]->n_fault_txn_latency.load();
                workers[i]->n_fault_txn_latency.store(0);

                rw_network_size += workers[i]->rw_network_size.load();
                workers[i]->rw_network_size.store(0);

                pr_network_size += workers[i]->pr_network_size.load();
                workers[i]->pr_network_size.store(0);

                com_network_size += workers[i]->com_network_size.load();
                workers[i]->com_network_size.store(0);

                rw_message_count += workers[i]->rw_message_count.load();
                workers[i]->rw_message_count.store(0);

                pr_message_count += workers[i]->pr_message_count.load();
                workers[i]->pr_message_count.store(0);

                com_message_count += workers[i]->com_message_count.load();
                workers[i]->com_message_count.store(0);
            }

            LOG(INFO) << "commit: " << n_commit
                      << " abort: " << n_abort_no_retry + n_abort_lock + n_abort_read_validation
                      << " (" << n_abort_no_retry << "/" << n_abort_lock << "/"
                      << n_abort_read_validation << "), network size: " << n_network_size
                      << ", avg network size: " << 1.0 * n_network_size / n_commit
                      << ", si_in_serializable: " << n_si_in_serializable << " "
                      << 100.0 * n_si_in_serializable / n_commit << " %"
                      << ", local: " << 100.0 * n_local / n_commit << " %"
                      << ", avg latency: " << n_txn_latency << " us"
                      << ", normal longtxn number: " << n_normal_long_txn
                      << ", normal longtxn latency: " << n_normal_longtxn_latency << " us"
                      << ", our recovering longtxn number: " << n_during_fault
                      << ", the our latency: " << n_fault_txn_latency << " us"
                      << ", all redo longtxn number: " << n_allredo_long_txn
                      << ", the all latency: " << n_allredo_longtxn_latency << " us"
                      << ", the rw network size: " << rw_network_size
                      << ", the pr network size: " << pr_network_size
                      << ", the com network size: " << com_network_size
                      << ", the rw message count: " << rw_message_count
                      << ", the pr message count: " << pr_message_count
                      << ", the com message count: " << com_message_count;
            count++;
            if (count > warmup && count <= timeToRun - cooldown) {
                total_commit += n_commit;
                total_abort_no_retry += n_abort_no_retry;
                total_abort_lock += n_abort_lock;
                total_abort_read_validation += n_abort_read_validation;
                total_local += n_local;
                total_si_in_serializable += n_si_in_serializable;
                total_network_size += n_network_size;
                total_latency += n_txn_latency;
                total_rw_network_size += rw_network_size;
                total_pr_network_size += pr_network_size;
                total_com_network_size += com_network_size;
                total_rw_message_count += rw_message_count;
                total_pr_message_count += pr_message_count;
                total_com_message_count += com_message_count;
            }
            if (count % context.failure_interval == 0) {
                if (context.datanode_failure) {
                    if (faultNodes.count(id)) {
                        for (const auto node : faultNodes) {
                            for (auto i = 0u; i < context.worker_num; i++) {
                                fault_partitionids.insert(i * context.coordinator_num + node);
                            }
                        }
                        for (auto i = 0u; i < 10; i++) {
                            fault_partitions.insert(
                                std::pair<uint32_t, std::unordered_set<uint32_t>>(
                                    i, fault_partitionids));
                        }
                    }
                    if (id == *faultNodes.begin()) {
                        workerFaultFlag.store(true);
                        masterTransferFlag.store(true);
                        LOG(INFO) << "Node " << id << " set transferFlag!";
                    }
                }

                if (context.coordinator_failure) {
                    if (faultNodes.count(id)) {
                        coordinatorFaultFlag.store(true);
                        LOG(INFO) << "Node " << id << " set coordinatorFaultFlag!";
                    }
                }
            }

            if (count > context.failure_interval &&
                count % context.failure_interval == context.failure_time) {
                if (context.datanode_failure) {
                    if (faultNodes.count(id)) {
                        fault_partitions.clear();
                    }
                    if (id == *faultNodes.begin()) {
                        workerFaultFlag.store(false);
                    }
                }

                if (context.coordinator_failure) {
                    if (faultNodes.count(id)) {
                        coordinatorFaultFlag.store(false);
                        LOG(INFO) << "Node " << id << " unset coordinatorFaultFlag!";
                    }
                }
            }

            if (count > context.failure_interval &&
                count % context.failure_interval == context.failure_time) {
                if (context.datanode_failure) {
                    if (id == faultMap[*faultNodes.begin()]) {
                        masterRecoveryFlag.store(true);
                        LOG(INFO) << "Node " << id << " set recoveryFlag!";
                    }
                }
            }

        } while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() -
                                                                  startTime)
                     .count() < timeToRun);

        count = timeToRun - warmup - cooldown;

        LOG(INFO) << "[summary] average commit: " << 1.0 * total_commit / count << " abort: "
                  << 1.0 * (total_abort_no_retry + total_abort_lock + total_abort_read_validation) /
                         count
                  << " (" << 1.0 * total_abort_no_retry / count << "/"
                  << 1.0 * total_abort_lock / count << "/"
                  << 1.0 * total_abort_read_validation / count
                  << "), network size: " << total_network_size
                  << ", avg network size: " << 1.0 * total_network_size / total_commit
                  << ", si_in_serializable: " << total_si_in_serializable << " "
                  << 100.0 * total_si_in_serializable / total_commit << " %"
                  << ", local: " << 100.0 * total_local / total_commit << " %"
                  << ", avg latency: " << 1.0 * total_latency / total_commit << " "
                  << ", avg rw network size: " << 1.0 * total_rw_network_size / total_commit
                  << ", avg pr network size: " << 1.0 * total_pr_network_size / total_commit
                  << ", avg com network size: " << 1.0 * total_com_network_size / total_commit
                  << ", avg rw message count: " << 1.0 * total_rw_message_count / total_commit
                  << ", avg pr message count: " << 1.0 * total_pr_message_count / total_commit
                  << ", avg com message count: " << 1.0 * total_com_message_count / total_commit
                  << " ";

        workerStopFlag.store(true);

        for (auto i = 0u; i < threads.size(); i++) {
            workers[i]->onExit();
            threads[i].join();
        }

        // gather throughput
        double sum_commit = gather(1.0 * total_commit / count);
        if (id == 0) {
            LOG(INFO) << "total commit: " << sum_commit;
        }

        // make sure all messages are sent
        std::this_thread::sleep_for(std::chrono::seconds(1));

        ioStopFlag.store(true);

        for (auto i = 0u; i < context.io_thread_num; i++) {
            iDispatcherThreads[i].join();
            oDispatcherThreads[i].join();
        }

        close_sockets();

        LOG(INFO) << "Coordinator exits.";
    }

    void connectToPeers() {
        // single node test mode
        if (peers.size() == 1) {
            return;
        }

        auto getAddressPort = [](const std::string &addressPort) {
            std::vector<std::string> result;
            boost::algorithm::split(result, addressPort, boost::is_any_of(":"));
            return result;
        };

        // start some listener threads

        std::vector<std::thread> listenerThreads;

        for (auto i = 0u; i < context.io_thread_num; i++) {
            listenerThreads.emplace_back(
                [id = this->id, peers = this->peers, &inSockets = this->inSockets[i],
                 &getAddressPort, tcp_quick_ack = context.tcp_quick_ack](std::size_t listener_id) {
                    std::vector<std::string> addressPort = getAddressPort(peers[id]);

                    Listener l(addressPort[0].c_str(), atoi(addressPort[1].c_str()) + listener_id,
                               100);
                    LOG(INFO) << "Listener " << listener_id << " on coordinator " << id
                              << " listening on " << peers[id];

                    auto n = peers.size();

                    for (std::size_t i = 0; i < n - 1; i++) {
                        Socket socket = l.accept();
                        std::size_t c_id;
                        socket.read_number(c_id);
                        // set quick ack flag
                        socket.set_quick_ack_flag(tcp_quick_ack);
                        inSockets[c_id] = std::move(socket);
                    }

                    LOG(INFO) << "Listener " << listener_id << " on coordinator " << id
                              << " exits.";
                },
                i);
        }

        // connect to peers
        auto n = peers.size();
        constexpr std::size_t retryLimit = 50;

        // connect to multiple remote coordinators
        for (auto i = 0u; i < n; i++) {
            if (i == id) continue;
            std::vector<std::string> addressPort = getAddressPort(peers[i]);
            // connnect to multiple remote listeners
            for (auto listener_id = 0u; listener_id < context.io_thread_num; listener_id++) {
                for (auto k = 0u; k < retryLimit; k++) {
                    Socket socket;

                    int ret = socket.connect(addressPort[0].c_str(),
                                             atoi(addressPort[1].c_str()) + listener_id);
                    if (ret == -1) {
                        socket.close();
                        if (k == retryLimit - 1) {
                            LOG(FATAL) << "failed to connect to peers, exiting ...";
                            exit(1);
                        }

                        // listener on the other side has not been set up.
                        LOG(INFO) << "Coordinator " << id << " failed to connect " << i << "("
                                  << peers[i] << ")'s listener " << listener_id
                                  << ", retry in 5 seconds.";
                        std::this_thread::sleep_for(std::chrono::seconds(5));
                        continue;
                    }
                    if (context.tcp_no_delay) {
                        socket.disable_nagle_algorithm();
                    }

                    LOG(INFO) << "Coordinator " << id << " connected to " << i;
                    socket.write_number(id);
                    outSockets[listener_id][i] = std::move(socket);
                    break;
                }
            }
        }

        for (auto i = 0u; i < listenerThreads.size(); i++) {
            listenerThreads[i].join();
        }

        LOG(INFO) << "Coordinator " << id << " connected to all peers.";
    }

    double gather(double value) {
        auto init_message = [](Message *message, std::size_t coordinator_id,
                               std::size_t dest_node_id) {
            message->set_source_node_id(coordinator_id);
            message->set_dest_node_id(dest_node_id);
            message->set_worker_id(0);
        };

        double sum = value;

        if (id == 0) {
            for (std::size_t i = 0; i < coordinator_num - 1; i++) {
                in_queue.wait_till_non_empty();
                std::unique_ptr<Message> message(in_queue.front());
                bool ok = in_queue.pop();
                CHECK(ok);
                CHECK(message->get_message_count() == 1);

                MessagePiece messagePiece = *(message->begin());

                CHECK(messagePiece.get_message_type() ==
                      static_cast<uint32_t>(ControlMessage::STATISTICS));
                CHECK(messagePiece.get_message_length() ==
                      MessagePiece::get_header_size() + sizeof(double));
                Decoder dec(messagePiece.toStringPiece());
                double v;
                dec >> v;
                sum += v;
            }

        } else {
            auto message = std::make_unique<Message>();
            init_message(message.get(), id, 0);
            ControlMessageFactory::new_statistics_message(*message, value);
            out_queue.push(message.release());
        }
        return sum;
    }

private:
    void close_sockets() {
        for (auto i = 0u; i < inSockets.size(); i++) {
            for (auto j = 0u; j < inSockets[i].size(); j++) {
                inSockets[i][j].close();
            }
        }
        for (auto i = 0u; i < outSockets.size(); i++) {
            for (auto j = 0u; j < outSockets[i].size(); j++) {
                outSockets[i][j].close();
            }
        }
    }

    void pin_thread_to_core(std::thread &t) {
#ifndef __APPLE__
        static std::size_t core_id = context.cpu_core_id;
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id++, &cpuset);
        int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
        CHECK(rc == 0);
#endif
    }

    void choose_failure_nodes() {
        LOG(INFO) << "replica_num: " << partitioner->replica_num()
                  << ", failure_num: " << context.failure_num;
        DCHECK(context.failure_num == 0 ||
               context.failure_num <= (partitioner->replica_num() + 1) / 2 - 1);

        LOG(INFO) << "failure_num: " << context.failure_num;
        for (auto i = 0u; i < context.failure_num; i++) {
            std::string id = context.failure_nodes[i];
            faultNodes.insert(stoul(id));
        }

        for (const auto faultNode : faultNodes) {
            bool retry;
            uint32_t recoveryNode = (faultNode + 1) % context.coordinator_num;
            do {
                retry = false;

                if (faultNodes.count(recoveryNode)) {
                    retry = true;
                    recoveryNode = (recoveryNode + 1) % context.coordinator_num;
                } else {
                    faultMap.insert(std::pair<uint32_t, uint32_t>(faultNode, recoveryNode));
                    LOG(INFO) << "faultNode: " << faultNode
                              << ", recoveryNode: " << faultMap[faultNode];
                }
            } while (retry);
        }
    }

private:
    /*
     * A coordinator may have multilpe inSockets and outSockets, connected to one
     * remote coordinator to fully utilize the network
     *
     * inSockets[0][i] receives w_id % io_threads from coordinator i
     */

    std::size_t id, coordinator_num;
    const std::vector<std::string> &peers;
    const Context &context;
    std::vector<std::vector<Socket>> inSockets, outSockets;
    std::atomic<bool> workerStopFlag, ioStopFlag;
    std::atomic<bool> workerFaultFlag, masterTransferFlag, masterRecoveryFlag;
    std::atomic<bool> coordinatorFaultFlag;
    std::atomic<uint32_t> n_committing_txns;
    std::atomic<uint32_t> n_redo_txns;
    std::vector<std::shared_ptr<Worker>> workers;
    std::vector<std::unique_ptr<IncomingDispatcher>> iDispatchers;
    std::vector<std::unique_ptr<OutgoingDispatcher>> oDispatchers;
    LockfreeQueue<Message *> in_queue, out_queue;
    std::unique_ptr<Partitioner> partitioner;
    std::unordered_map<uint32_t, std::unordered_set<uint32_t>>
        fault_partitions;  // the first is the tableid; the second is partitionid.
    std::unordered_set<uint32_t> fault_partitionids;
    std::unordered_set<uint32_t> faultNodes;
    std::unordered_map<uint32_t, uint32_t> faultMap;
    std::queue<uint64_t> txnqueue;
};
}  // namespace lefr
