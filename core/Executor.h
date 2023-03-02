#pragma once

#include <chrono>
#include <thread>
#include <unordered_set>

#include "common/Percentile.h"
#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Delay.h"
#include "core/Partitioner.h"
#include "core/Worker.h"
#include "butil/logging.h"

#include <bthread/bthread.h>
#include <brpc/channel.h>
#include <braft/route_table.h>
#include "common/RaftService.h"

namespace lefr {

template <class Workload, class Protocol>
class Executor : public Worker {
public:
    using WorkloadType = Workload;
    using ProtocolType = Protocol;
    using DatabaseType = typename WorkloadType::DatabaseType;
    using TransactionType = typename WorkloadType::TransactionType;
    using ContextType = typename DatabaseType::ContextType;
    using RandomType = typename DatabaseType::RandomType;
    using MessageType = typename ProtocolType::MessageType;
    using MessageFactoryType = typename ProtocolType::MessageFactoryType;
    using MessageHandlerType = typename ProtocolType::MessageHandlerType;

    using StorageType = typename WorkloadType::StorageType;

    Executor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
             const ContextType &context, Partitioner &partitioner,
             std::atomic<uint32_t> &worker_status, std::atomic<uint32_t> &n_complete_workers,
             std::atomic<uint32_t> &n_started_workers, std::atomic<bool> &faultFlag,
             std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
             std::atomic<bool> &coordinatorFaultFlag, std::atomic<uint32_t> &n_committing_txns,
             std::atomic<uint32_t> &n_redo_txns,
             std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions,
             std::unordered_map<uint32_t, uint32_t> &faultMap, 
             std::queue<uint64_t> &txnqueue)
        : Worker(coordinator_id, id),
          db(db),
          context(context),
          partitioner(partitioner),
          worker_status(worker_status),
          n_complete_workers(n_complete_workers),
          n_started_workers(n_started_workers),
          faultFlag(faultFlag),
          transferFlag(transferFlag),
          recoveryFlag(recoveryFlag),
          coordinatorFaultFlag(coordinatorFaultFlag),
          n_committing_txns(n_committing_txns),
          n_redo_txns(n_redo_txns),
          isscanreplica(false),
          random(reinterpret_cast<uint64_t>(this)),
          fault_seed(0),
          protocol(db, context, partitioner, transferFlag, recoveryFlag, coordinatorFaultFlag),
          workload(coordinator_id, db, random, partitioner),
          delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num,
                                            context.delay_time)),
          fault_partitions(fault_partitions),
          faultMap(faultMap), 
          txnqueue(txnqueue) {
        for (auto i = 0u; i < context.coordinator_num; i++) {
            messages.emplace_back(std::make_unique<Message>());
            init_message(messages[i].get(), i);

            async_messages.emplace_back(std::make_unique<Message>());
            init_message(async_messages[i].get(), i);

            StorageType storage;

            replicate_randoms.emplace_back(RandomType());
            sub_randoms.emplace_back(RandomType());
            replicate_storages.push_back(storage);
            sub_storages.push_back(storage);
            replicate_transactions.emplace_back(nullptr);
            redo_transactions.emplace_back(nullptr);
            sub_transactions.emplace_back(nullptr);
        }

        messageHandlers = MessageHandlerType::get_message_handlers();

        message_stats.resize(messageHandlers.size(), 0);
        message_sizes.resize(messageHandlers.size(), 0);

        init_replicate_transaction = [this](uint32_t type, std::size_t coordinator_id,
                                            std::size_t partition_id, uint64_t seed,
                                            uint64_t startTime) {
            init_transaction(type, coordinator_id, partition_id, seed, startTime);
        };
        init_sub_transaction = [this](std::size_t coordinator_id, std::size_t partition_id) {
            init_sub_txn(coordinator_id, partition_id);
        };
    }

    ~Executor() = default;

    void start() override {
        LOG(INFO) << "Executor " << id << " starts.";

        StorageType storage;
        uint64_t last_seed = 0;
        uint64_t replicate_seed = 0;

        ExecutorStatus status;

        while ((status = static_cast<ExecutorStatus>(worker_status.load())) !=
               ExecutorStatus::START) {
            std::this_thread::yield();
        }

        workerStartTime = std::chrono::steady_clock::now();
        protocol.set_worker_start_time(workerStartTime);

        n_started_workers.fetch_add(1);
        int batch_count = 0;

        bool retry_transaction = false;
        int max_retry = 0;
        int retry_count = 0;

        if (context.protocol == "LeFR") {
            max_retry = 20;
        } else {
            max_retry = 5;
        }
        
        std::string raft_conf = context.raft_conf;
        std::string raft_group = context.raft_group;
        if (braft::rtb::update_configuration(raft_group, raft_conf) != 0) {
            LOG(ERROR)  << "Fail to register configuration " << raft_conf
                        << " of group " << raft_group;
        }

        do {
            process_request();

            if (!partitioner.is_backup()) {
                // backup node stands by for replication
                last_seed = random.get_seed();

                if (retry_transaction) {
                    if (transaction->isredo) {
                        transaction->reset();
                    } else {
                        if (retry_count >= max_retry) {
                            retry_count = 0;
                            auto temptime = transaction->startTimeTxn;
                            auto partition_id = get_partition_id();
                            replicate_seed = random.get_seed();
                            transaction = workload.next_transaction(context, partition_id, storage,
                                                                    transferFlag, recoveryFlag,
                                                                    fault_partitions);
                            transaction->startTimeTxn = temptime;
                            setupHandlers(*transaction);
                        } else {
                            retry_count++;
                            auto temptime = transaction->startTimeTxn;
                            transaction->reset();
                            transaction->startTimeTxn = temptime;
                            setupHandlers(*transaction);
                        }
                    }
                } else {
                    auto partition_id = get_partition_id();
                    replicate_seed = random.get_seed();
                    transaction =
                        workload.next_transaction(context, partition_id, storage, transferFlag,
                                                  recoveryFlag, fault_partitions);
                    setupHandlers(*transaction);
                }

                transaction->txn_seed = replicate_seed;
                txnqueue.push(transaction->cur_tid);
                auto result = transaction->execute(id, messages, faultFlag, n_committing_txns,
                                                   workerStartTime, n_redo_txns);

                if (result == TransactionResult::REDO) {
                    if (context.flush_granularity == "txn") {
                        n_network_size.fetch_add(transaction->network_size);
                        retry_transaction = true;
                        transaction->isredo = true;
                        continue;
                    }
                }
                if (result == TransactionResult::READY_TO_COMMIT) {
                    bool commit =
                        protocol.commit(*transaction, messages, n_committing_txns, async_messages);
                    n_network_size.fetch_add(transaction->network_size);
                    rw_network_size.fetch_add(transaction->rw_network_size);
                    pr_network_size.fetch_add(transaction->pr_network_size);
                    com_network_size.fetch_add(transaction->com_network_size);
                    rw_message_count.fetch_add(transaction->rw_message_count);
                    pr_message_count.fetch_add(transaction->pr_message_count);
                    com_message_count.fetch_add(transaction->com_message_count);
                    if (commit) {
                        retry_count = 0;
                        batch_count++;
                        n_commit.fetch_add(1);
                        if (transaction->si_in_serializable) {
                            n_si_in_serializable.fetch_add(1);
                        }
                        if (transaction->local_validated) {
                            n_local.fetch_add(1);
                        }
                        retry_transaction = false;
                        auto latency =
                            (std::chrono::steady_clock::now().time_since_epoch().count() -
                             transaction->startTimeTxn) /
                            1000;
                        if (transaction->is_longtxn == true) {
                            n_normal_long_txn.fetch_add(1);
                            n_normal_longtxn_latency.fetch_add(static_cast<uint64_t>(latency));
                        }
                        if (transaction->isredo && transaction->is_longtxn == true) {
                            n_allredo_long_txn.fetch_add(1);
                            n_allredo_longtxn_latency.fetch_add(static_cast<uint64_t>(latency));
                        }
                        percentile.add(latency);
                        n_txn_latency.fetch_add(static_cast<uint64_t>(latency));
                        if (transaction->during_fault == true && transaction->is_longtxn) {
                            n_during_fault.fetch_add(1);
                            n_fault_txn_latency.fetch_add(static_cast<uint64_t>(latency));
                        }
                        if (transaction->distributed_transaction) {
                            dist_latency.add(latency);
                        } else {
                            local_latency.add(latency);
                        }
                    } else {
                        if (transaction->abort_lock) {
                            n_abort_lock.fetch_add(1);
                        } else if (transaction->abort_read_validation) {
                            n_abort_read_validation.fetch_add(1);
                        }

                        if (context.sleep_on_retry) {
                            std::this_thread::sleep_for(std::chrono::microseconds(
                                random.uniform_dist(30, context.sleep_time)));
                        }
                        retry_transaction = true;
                        if (retry_count < max_retry) {
                            random.set_seed(last_seed);
                        }
                    }
                } else {
                    protocol.abort_all(*transaction, messages);
                    n_abort_no_retry.fetch_add(1);
                }
            }
            if (batch_count % context.batch_flush == 0) {
                flush_async_messages();
            }

            simulate_coordinator_failure();

            if (fault_seed != 0 && retry_transaction == false) {
                random.set_seed(fault_seed);
                fault_seed = 0;
            }

            retry_transaction = redo_replicated_txn();

            status = static_cast<ExecutorStatus>(worker_status.load());
        } while (status != ExecutorStatus::STOP);
        flush_async_messages();

        // LOG(INFO)<< "worker "<< id << " prepare to stop!!!";

        n_complete_workers.fetch_add(1);

        // once all workers are stop, we need to process the replication
        // requests

        while (static_cast<ExecutorStatus>(worker_status.load()) != ExecutorStatus::CLEANUP) {
            process_request();
        }

        process_request();
        n_complete_workers.fetch_add(1);

        LOG(INFO) << "Executor " << id << " exits.";
    }

    void onExit() override {
        LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50) << " us (50%) "
                  << percentile.nth(75) << " us (75%) " << percentile.nth(95) << " us (95%) "
                  << percentile.nth(99) << " us (99%). dist txn latency: " << dist_latency.nth(50)
                  << " us (50%) " << dist_latency.nth(75) << " us (75%) " << dist_latency.nth(95)
                  << " us (95%) " << dist_latency.nth(99)
                  << " us (99%). local txn latency: " << local_latency.nth(50) << " us (50%) "
                  << local_latency.nth(75) << " us (75%) " << local_latency.nth(95) << " us (95%) "
                  << local_latency.nth(99) << " us (99%).";

        if (id == 0) {
            for (auto i = 0u; i < message_stats.size(); i++) {
                LOG(INFO) << "message stats, type: " << i << " count: " << message_stats[i]
                          << " total size: " << message_sizes[i];
            }
            percentile.save_cdf(context.cdf_path);
        }
    }

    void sender(TransactionType &txn) {
        braft::PeerId leader;
        // Select leader of the target group from RouteTable
        if (braft::rtb::select_leader(context.raft_group,&leader) != 0) {
            // Leader is unknown in RouteTable. Ask RouteTable to refresh leader
            // by sending RPCs.
            butil::Status st = braft::rtb::refresh_leader(
                        context.raft_group, 500);
            if (!st.ok()) {
                // Not sure about the leader, sleep for a while and the ask again.
                bthread_usleep(50 * 1000L);
            }
            // continue;
            return;
        }
        // Now we known who is the leader, construct Stub and then sending
        // rpc
        brpc::Channel channel;
        if (channel.Init(leader.addr, NULL) != 0) {
            LOG(ERROR) << "Fail to init channel to " << leader;
            bthread_usleep(50 * 1000L);
            // continue;
            return;
        }
        ReplicationService_Stub stub(&channel);
        brpc::Controller cntl;
        cntl.set_timeout_ms(50);
        ReplicationResponse response;
        ReplicationRequest request;
        request.set_tid(txn.txn_seed);
        for (auto i = 0u; i < txn.readSet.size(); i++) {
            auto &readKey = txn.readSet[i];
            auto tableId = readKey.get_table_id();
            auto partitionId = readKey.get_partition_id();
            auto table = db.find_table(tableId, partitionId);
            auto key = readKey.get_key();
            auto tid = readKey.get_tid();
            auto row = table->search(key);
            std::string value = table->value_to_string(std::get<1>(row));
            OpLog* log;
            log = request.add_logs();
            log->set_type(OpLog_Type::OpLog_Type_READ);
            log->set_tid(tid);
            log->set_value(value);
        }
        for (auto i = 0u; i < txn.writeSet.size(); i++) {
            auto &writeKey = txn.writeSet[i];
            auto tableId = writeKey.get_table_id();
            auto partitionId = writeKey.get_partition_id();
            auto table = db.find_table(tableId, partitionId);
            auto key = writeKey.get_key();
            auto tid = writeKey.get_tid();
            auto row = table->search(key);
            std::string value = table->value_to_string(std::get<1>(row));
            OpLog* log;
            log = request.add_logs();
            log->set_type(OpLog_Type::OpLog_Type_WRITE);
            log->set_tid(tid);
            log->set_value(value);
        }
        stub.init_txn(&cntl, &request, &response, NULL);
        if (cntl.Failed()) {
            // LOG(WARNING) << "Fail to send request to " << leader
            //         << " : " << cntl.ErrorText();
            // Clear leadership since this RPC failed.
            braft::rtb::update_leader(context.raft_group, braft::PeerId());
            bthread_usleep(50 * 1000L);
            // continue;
            return;
        }
        if (!response.success()) {
            LOG(WARNING) << "Fail to send request to " << leader
                    << ", redirecting to "
                    << (response.has_redirect() 
                            ? response.redirect() : "nowhere");
            // Update route table since we have redirect information
            braft::rtb::update_leader(context.raft_group, response.redirect());
            // continue;
            return;
        }
        return ;
    }

    std::size_t get_partition_id() {
        std::size_t partition_id;

        if (context.partitioner == "pb") {
            partition_id = random.uniform_dist(0, context.partition_num - 1);
        } else {
            auto partition_num_per_node = context.partition_num / context.coordinator_num;
            partition_id =
                random.uniform_dist(0, partition_num_per_node - 1) * context.coordinator_num +
                coordinator_id;
        }
        // CHECK(partitioner.has_master_partition(partition_id));
        return partition_id;
    }

    void push_message(Message *message) override { in_queue.push(message); }

    Message *pop_message() override {
        if (out_queue.empty()) return nullptr;

        Message *message = out_queue.front();

        if (delay->delay_enabled()) {
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::microseconds>(now - message->time).count() <
                delay->message_delay()) {
                return nullptr;
            }
        }

        bool ok = out_queue.pop();
        CHECK(ok);

        return message;
    }

    std::size_t process_request() {
        std::size_t size = 0;

        while (!in_queue.empty()) {
            std::unique_ptr<Message> message(in_queue.front());
            bool ok = in_queue.pop();
            CHECK(ok);

            std::size_t source_node_id = message->get_source_node_id();

            for (auto it = message->begin(); it != message->end(); it++) {
                MessagePiece messagePiece = *it;
                auto type = messagePiece.get_message_type();
                DCHECK(type < messageHandlers.size());
                ITable *table =
                    db.find_table(messagePiece.get_table_id(), messagePiece.get_partition_id());

                messageHandlers[type](
                    messagePiece, *messages[source_node_id], *table, transaction.get(),
                    replicate_transactions[source_node_id].get(),
                    sub_transactions[source_node_id].get(), init_replicate_transaction,
                    init_scanreplica_transaction, init_sub_transaction, partitioner);
                message_stats[type]++;
                message_sizes[type] += messagePiece.get_message_length();
            }

            size += message->get_message_count();
            flush_messages();
        }
        return size;
    }

    virtual void setupHandlers(TransactionType &txn) = 0;

protected:
    void flush_messages() {
        DCHECK(messages[coordinator_id]->get_message_count() == 0);
        for (auto i = 0u; i < messages.size(); i++) {
            if (i == coordinator_id) {
                continue;
            }

            if (messages[i]->get_message_count() == 0) {
                continue;
            }

            auto message = messages[i].release();

            out_queue.push(message);
            messages[i] = std::make_unique<Message>();
            init_message(messages[i].get(), i);
        }
    }

    void flush_async_messages() {
        DCHECK(async_messages[coordinator_id]->get_message_count() == 0);
        for (auto i = 0u; i < async_messages.size(); i++) {
            if (i == coordinator_id) {
                continue;
            }

            if (async_messages[i]->get_message_count() == 0) {
                continue;
            }

            auto message = async_messages[i].release();

            out_queue.push(message);
            async_messages[i] = std::make_unique<Message>();
            init_message(async_messages[i].get(), i);
        }
    }

    void init_message(Message *message, std::size_t dest_node_id) {
        message->set_source_node_id(coordinator_id);
        message->set_dest_node_id(dest_node_id);
        message->set_worker_id(id);
    }

    void init_transaction(uint32_t type, std::size_t coordinator_id, std::size_t partition_id,
                          uint64_t seed, uint64_t startTime) {
        uint64_t last_seed = seed;
        replicate_randoms[coordinator_id].set_seed(seed);
        WorkloadType workload(coordinator_id, db, replicate_randoms[coordinator_id], partitioner);

        replicate_transactions[coordinator_id] = workload.get_replicate_transaction(
            type, context, partition_id, replicate_storages[coordinator_id], transferFlag,
            recoveryFlag, fault_partitions);
        replicate_transactions[coordinator_id]->coordinator_id = context.coordinator_id;
        replicate_transactions[coordinator_id]->txn_seed = last_seed;
        replicate_transactions[coordinator_id]->readHandled = true;
        replicate_transactions[coordinator_id]->startTimeTxn = startTime;
        replicate_transactions[coordinator_id]->redo = 0;
    }

    void init_sub_txn(std::size_t coordinator_id, std::size_t partition_id) {
        sub_randoms[coordinator_id].set_seed(0);
        WorkloadType workload(coordinator_id, db, sub_randoms[coordinator_id], partitioner);
        sub_transactions[coordinator_id] =
            workload.next_sub_transaction(context, partition_id, sub_storages[coordinator_id],
                                          transferFlag, recoveryFlag, fault_partitions);
    }

    bool redo_replicated_txn() {
        bool is_warmup = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::steady_clock::now() - workerStartTime)
                             .count() < 10;
        for (auto i = 0u; i < context.coordinator_num; i++) {
            if (i != context.coordinator_id) {
                if (faultMap.count(i) && faultMap[i] == context.coordinator_id &&
                    replicate_transactions[i].get() != nullptr &&
                    replicate_randoms[i].get_seed() != 0) {
                    bool is_failure =
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - replicate_transactions[i]->startTime)
                            .count() > 1000;
                    if (!is_warmup && is_failure) {
                        if (replicate_transactions[i]->status == TransactionStatus::COMMIT &&
                            context.protocol != "Tapir") {
                            transaction = std::move(replicate_transactions[i]);
                            uint64_t commit_tid = transaction->cur_tid;
                            setupHandlers(*transaction);
                            protocol.write_and_replicate_all(*transaction, commit_tid, messages,
                                                             async_messages);
                            return false;
                        } else {
                            fault_seed = random.get_seed();
                            random.set_seed(replicate_transactions[i]->txn_seed);
                            transaction = std::move(replicate_transactions[i]);
                            replicate_randoms[i].set_seed(0);
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    void simulate_coordinator_failure() {
        while (coordinatorFaultFlag.load()) {
            process_request();
            std::this_thread::yield();
        }
    }

protected:
    DatabaseType &db;
    std::chrono::steady_clock::time_point workerStartTime;
    const ContextType &context;
    ContextType redo_context;
    Partitioner &partitioner;
    std::atomic<uint32_t> &worker_status;
    std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
    std::atomic<bool> &faultFlag;
    std::atomic<bool> &transferFlag;
    std::atomic<bool> &recoveryFlag;
    std::atomic<bool> &coordinatorFaultFlag;
    std::atomic<uint32_t> &n_committing_txns;
    std::atomic<uint32_t> &n_redo_txns;
    
    std::atomic<bool> isscanreplica;
    RandomType random;
    uint64_t fault_seed;
    StorageType replicate_storage;
    ProtocolType protocol;
    WorkloadType workload;
    std::unique_ptr<Delay> delay;
    Percentile<int64_t> percentile, dist_latency, local_latency;
    std::unique_ptr<TransactionType> transaction;
    std::unique_ptr<TransactionType> redoTransaction;
    std::vector<std::unique_ptr<Message>> messages, async_messages;
    std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions;
    std::unordered_map<uint32_t, uint32_t> &faultMap;
    std::queue<uint64_t> &txnqueue;
    std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
        init_replicate_transaction;
    std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction;
    std::function<void(std::size_t, std::size_t)> init_sub_transaction;
    std::vector<std::function<void(
        MessagePiece, Message &, ITable &, TransactionType *, TransactionType *, TransactionType *,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>,
        std::function<void(uint64_t, uint32_t)>, std::function<void(std::size_t, std::size_t)>,
        Partitioner &)>>
        messageHandlers;
    std::vector<std::size_t> message_stats, message_sizes;
    LockfreeQueue<Message *> in_queue, out_queue;
    std::vector<RandomType> replicate_randoms;
    std::vector<StorageType> replicate_storages;
    std::vector<RandomType> sub_randoms;
    std::vector<StorageType> sub_storages;
    std::vector<std::unique_ptr<TransactionType>> replicate_transactions;
    std::vector<std::unique_ptr<TransactionType>> redo_transactions;
    std::vector<std::unique_ptr<TransactionType>> sub_transactions;
};
}  // namespace lefr