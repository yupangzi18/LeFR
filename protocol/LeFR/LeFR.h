#pragma once

#include <butil/logging.h>

#include <algorithm>
#include <atomic>
#include <set>
#include <thread>

#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/LeFR/LeFRMessage.h"
#include "protocol/LeFR/LeFRTransaction.h"
#include "protocol/LeFR/SiloHelper.h"

namespace lefr {

template <class Database>
class LeFR {
public:
    using DatabaseType = Database;
    using MetaDataType = std::atomic<uint64_t>;
    using ContextType = typename DatabaseType::ContextType;
    using MessageType = LeFRMessage;
    using TransactionType = LeFRTransaction<DatabaseType>;

    using MessageFactoryType = LeFRMessageFactory;
    using MessageHandlerType = LeFRMessageHandler<DatabaseType>;

    LeFR(DatabaseType &db, const ContextType &context, Partitioner &partitioner,
         std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
         std::atomic<bool> &coordinatorFaultFlag)
        : db(db),
          context(context),
          partitioner(partitioner),
          transferFlag(transferFlag),
          recoveryFlag(recoveryFlag),
          coordinatorFaultFlag(coordinatorFaultFlag) {}

    uint64_t search(std::size_t table_id, std::size_t partition_id, const void *key,
                    void *value) const {
        ITable *table = db.find_table(table_id, partition_id);
        auto value_bytes = table->value_size();
        auto row = table->search(key);
        return SiloHelper::read(row, value, value_bytes);
    }

    void abort_all(TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages) {
        auto &writeSet = txn.writeSet;

        // unlock locked records
        for (auto i = 0u; i < partitioner.total_coordinators(); i++) {
            if (i == context.coordinator_id) {
                for (auto j = 0u; j < writeSet.size(); j++) {
                    auto &writeKey = writeSet[j];
                    if (!writeKey.get_write_lock_bit()) {
                        continue;
                    }
                    auto tableId = writeKey.get_table_id();
                    auto partitionId = writeKey.get_partition_id();
                    auto table = db.find_table(tableId, partitionId);

                    if (partitioner.has_master_partition(partitionId) ||
                        partitionId % partitioner.get_partition_num() == context.coordinator_id) {
                        auto key = writeKey.get_key();
                        std::atomic<uint64_t> &tid = table->search_metadata(key);
                        SiloHelper::unlock(tid);
                    }
                }
            } else if (txn.nodeSet.count(i)) {
                txn.network_size += MessageFactoryType::new_remote_abort_message(*messages[i], i);
                txn.pr_message_count++;
            }
        }

        sync_messages(txn, false);
    }

    void write_and_replicate_all(TransactionType &txn, uint64_t commit_tid,
                                 std::vector<std::unique_ptr<Message>> &messages,
                                 std::vector<std::unique_ptr<Message>> &async_messages) {
        auto &readSet = txn.readSet;
        auto &writeSet = txn.writeSet;
        uint32_t round_count = 1;
        std::size_t masterid = partitioner.master_coordinator(0);
        std::set<std::size_t> rep_coor;
        auto temp_network_size = 0u;

        // which protocol in the silomessage
        if (context.traditional_2pc == true) {
            round_count = 1;
        } else {
            round_count = 0;
        }

        // write
        for (auto i = 0u; i < partitioner.total_coordinators(); i++) {
            if (i == context.coordinator_id) {
                for (auto j = 0u; j < writeSet.size(); j++) {
                    auto &writeKey = writeSet[j];
                    auto tableId = writeKey.get_table_id();
                    auto partitionId = writeKey.get_partition_id();
                    auto table = db.find_table(tableId, partitionId);
                    for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
                        if (partitioner.is_partition_replicated_on(partitionId, k) &&
                            rep_coor.count(k) == 0 && k != context.coordinator_id) {
                            rep_coor.insert(k);
                        }
                    }

                    if (partitioner.has_master_partition(partitionId)) {
                        auto key = writeKey.get_key();
                        auto value = writeKey.get_value();
                        table->update(key, value);
                    }
                }
            } else if (txn.nodeSet.count(i)) {
                txn.pendingResponses++;
                txn.remote_write[i] = true;
                temp_network_size = MessageFactoryType::new_remote_write_message(
                    *messages[i], i, round_count, context.coordinator_id);
                txn.network_size += temp_network_size;
                txn.com_message_count++;
            }
        }

        // replicate

        for (auto i = 0u; i < writeSet.size(); i++) {
            auto &writeKey = writeSet[i];
            auto tableId = writeKey.get_table_id();
            auto partitionId = writeKey.get_partition_id();
            auto table = db.find_table(tableId, partitionId);

            for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
                if (!partitioner.is_partition_replicated_on(partitionId, k)) {
                    continue;
                }

                if (k == partitioner.master_coordinator(partitionId)) {
                    continue;
                }

                // local replicate
                if (k == context.coordinator_id) {
                    auto key = writeKey.get_key();
                    auto value = writeKey.get_value();
                    std::atomic<uint64_t> &tid = table->search_metadata(key);

                    table->update(key, value);
                    SiloHelper::unlock(tid, commit_tid);
                } else {
                    if (context.async_batch_replication == false) {
                        txn.pendingResponses++;
                        auto coordinatorID = k;
                        temp_network_size = MessageFactoryType::new_replication_message(
                            *messages[coordinatorID], *table, writeKey.get_key(),
                            writeKey.get_value(), commit_tid, round_count, context.coordinator_id,
                            i);
                        if (context.traditional_2pc == false) {
                            txn.network_size += temp_network_size;
                            txn.com_message_count++;
                        }
                    } else {
                        auto coordinatorID = k;
                        temp_network_size = MessageFactoryType::new_async_replication_message(
                            *async_messages[coordinatorID], *table, writeKey.get_key(),
                            writeKey.get_value(), commit_tid, round_count, context.coordinator_id,
                            i);
                        if (context.traditional_2pc == false) {
                            txn.network_size += temp_network_size;
                            txn.com_message_count++;
                        }
                    }
                }
            }
        }

        simulate_network_latency(txn);
        while (txn.pendingResponses > 0) {
            txn.remote_request_handler();
        }

        txn.process_message_write.clear();
        txn.process_message_write_2.clear();
        txn.last_coordinator_write.clear();
        txn.last_coordinator_write_2.clear();
    }

    bool commit(TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages,
                std::atomic<uint32_t> &n_committing_txns,
                std::vector<std::unique_ptr<Message>> &async_messages) {
        // 2PC
        // Phase 1 - Prepare
        // Backup ops across coordinators
        // Lock and validate

        if (context.protocol == "LeFR" && context.traditional_2pc == true) {
            backup_redo_logs(txn, messages);
        }

        if (lock_all_write_set(txn, messages)) {
            abort_all(txn, messages);
            txn.status = TransactionStatus::ABORT;
            txn.pr_network_size = txn.network_size - txn.rw_network_size;
            return false;
        }

        txn.status = TransactionStatus::COMMITTING;
        n_committing_txns.fetch_add(1);

        bool locked = false;
        if (context.traditional_2pc) {
            if (transferFlag.load() || recoveryFlag.load()) {
                if (transferFlag.load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                }
                abort_all(txn, messages);
                txn.status = TransactionStatus::ABORT;
                txn.isredo = true;
                n_committing_txns.fetch_sub(1);
                return false;
            }
        } else {
            if (!locked) {
                if (transferFlag.load()) {
                    txn.during_fault = true;
                    while (!partitioner.finished_transfer()) {
                        txn.remote_request_handler();
                        std::this_thread::yield();
                    }

                    if (restart_in_new_master(txn, messages)) {
                        abort_all(txn, messages);
                        txn.status = TransactionStatus::ABORT;
                        n_committing_txns.fetch_sub(1);
                        return false;
                    }
                    locked = true;
                }

                if (recoveryFlag.load()) {
                    txn.during_fault = true;
                    while (!partitioner.finished_recovery()) {
                        txn.remote_request_handler();
                        std::this_thread::yield();
                    }

                    if (restart_in_new_master(txn, messages)) {
                        abort_all(txn, messages);
                        txn.status = TransactionStatus::ABORT;
                        n_committing_txns.fetch_sub(1);
                        LOG(INFO) << "Relock failed after lock!";
                        return false;
                    }
                    locked = true;
                }
            }
        }

        // read validation
        if (!validate_all_read_set(txn, messages)) {
            abort_all(txn, messages);
            txn.status = TransactionStatus::ABORT;
            n_committing_txns.fetch_sub(1);
            txn.pr_network_size = txn.network_size - txn.rw_network_size;
            return false;
        }

        if (context.traditional_2pc) {
            if (transferFlag.load() || recoveryFlag.load()) {
                if (transferFlag.load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                }
                LOG(INFO) << "Abort due to transfer";
                abort_all(txn, messages);
                txn.status = TransactionStatus::ABORT;
                txn.isredo = true;
                n_committing_txns.fetch_sub(1);
                return false;
            }
        } else {
            if (!locked) {
                if (transferFlag.load()) {
                    txn.during_fault = true;
                    while (!partitioner.finished_transfer()) {
                        txn.remote_request_handler();
                        std::this_thread::yield();
                    }

                    if (restart_in_new_master(txn, messages)) {
                        abort_all(txn, messages);
                        txn.status = TransactionStatus::ABORT;
                        n_committing_txns.fetch_sub(1);
                        return false;
                    }
                    locked = true;
                }

                if (recoveryFlag.load()) {
                    txn.during_fault = true;
                    while (!partitioner.finished_recovery()) {
                        txn.remote_request_handler();
                        std::this_thread::yield();
                    }

                    if (restart_in_new_master(txn, messages)) {
                        abort_all(txn, messages);
                        txn.status = TransactionStatus::ABORT;
                        n_committing_txns.fetch_sub(1);
                        return false;
                    }
                    locked = true;
                }
            }
        }

        // generate tid
        uint64_t commit_tid = generate_tid(txn);

        bool coordinatorFault = false;
        auto startTime = std::chrono::steady_clock::now();
        while (coordinatorFaultFlag.load()) {
            if (coordinatorFault == false) {
                coordinatorFault = true;
            }
            txn.remote_request_handler();
            if (std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - startTime)
                    .count() > context.recovery_time) {
                LOG(INFO) << "Coordinated by the new coordinator: " << txn.txn_seed;
                abort_all(txn, messages);
                break;
            }
        }
        if (coordinatorFault) {
            LOG(INFO) << "Abort due to coordinatot failure!";
            n_committing_txns.fetch_sub(1);
            return true;
        }

        if (context.traditional_2pc) {
            backup_decision(txn, commit_tid, messages);
        }
        // record the network size of thr preparaiton
        txn.pr_network_size = txn.network_size - txn.rw_network_size;

        // if (coordinatorFaultFlag.load()) {
        //   LOG(INFO) << "Coordinator failure after commit: " << txn.txn_seed;
        //   return true;
        // }

        // write and replicate
        write_and_replicate_all(txn, commit_tid, messages, async_messages);

        if (transferFlag.load() || recoveryFlag.load()) {
            if (context.traditional_2pc == false) {
                txn.during_fault = true;
            }
            // LOG(INFO) << "Before write and replicate";
        }

        txn.status = TransactionStatus::COMMIT;
        n_committing_txns.fetch_sub(1);

        // release locks
        release_all_lock(txn, commit_tid, messages, async_messages);
        txn.com_network_size = txn.network_size - txn.rw_network_size - txn.pr_network_size;

        return true;
    }

    void set_worker_start_time(std::chrono::steady_clock::time_point workerStartTime) {
        this->workerStartTime = workerStartTime;
    }

private:
    void backup_redo_logs(TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages) {
        DCHECK(txn.pendingResponses == 0);

        std::size_t messageCount = 0;
        std::size_t temp_network_size = 0;

        auto &readSet = txn.readSet;
        auto &writeSet = txn.writeSet;
        std::set<std::size_t> cross_node;
        for (auto node : txn.nodeSet) {
            for (auto i = 0u; i < partitioner.replica_num(); i++) {
                auto dest_node_id = (node.first + i) % context.coordinator_num;
                if (!cross_node.count(dest_node_id)) {
                    cross_node.insert(dest_node_id);
                }
            }
        }

        for (auto dest_node_id : cross_node) {
            if (dest_node_id == context.coordinator_id) {
                continue;
            }
            txn.pendingResponses++;
            txn.network_size += MessageFactoryType::new_init_txn_message(
                *messages[dest_node_id], 0, txn.coordinator_id, txn.partition_id, txn.txn_seed,
                txn.startTimeTxn);
            txn.pr_message_count++;
        }

        for (auto i = 0u; i < writeSet.size(); i++) {
            auto &writeKey = writeSet[i];
            auto tableId = writeKey.get_table_id();
            auto partitionId = writeKey.get_partition_id();
            auto table = db.find_table(tableId, partitionId);

            for (auto dest_node_id : cross_node) {
                if (dest_node_id == context.coordinator_id) {
                    continue;
                }
                txn.pendingResponses++;
                temp_network_size = MessageFactoryType::new_write_replication_message(
                    *messages[dest_node_id], *table, writeKey.get_key(), writeKey.get_value());
                txn.network_size += temp_network_size;
                txn.pr_message_count++;

                messageCount++;
                if (messageCount % 2000 == 0) {
                    txn.remote_request_handler();
                    txn.message_flusher();
                }
            }
        }

        simulate_network_latency(txn);
        while (txn.pendingResponses > 0) {
            txn.remote_request_handler();
        }
    }

    void backup_decision(TransactionType &txn, uint64_t commit_tid,
                         std::vector<std::unique_ptr<Message>> &messages) {
        for (auto i = 0u; i < partitioner.total_coordinators(); i++) {
            if (i == context.coordinator_id) {
                continue;
            }

            txn.pendingResponses++;
            txn.network_size += MessageFactoryType::new_sync_decision_message(
                *messages[i], txn.coordinator_id, commit_tid);
            txn.pr_message_count++;
        }

        simulate_network_latency(txn);
        while (txn.pendingResponses > 0) {
            txn.remote_request_handler();
        }
    }

    bool lock_all_write_set(TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages) {
        auto &writeSet = txn.writeSet;
        std::size_t message_count_end = 0;
        uint32_t round_count = 1;
        std::size_t masterid = partitioner.master_coordinator(0);

        for (auto i = 0u; i < partitioner.total_coordinators(); i++) {
            if (i == context.coordinator_id) {
                for (auto j = 0u; j < writeSet.size(); j++) {
                    auto &writeKey = writeSet[j];
                    auto tableId = writeKey.get_table_id();
                    auto partitionId = writeKey.get_partition_id();
                    auto table = db.find_table(tableId, partitionId);

                    if (masterid != partitioner.master_coordinator(0) && masterid == 0) {
                        for (auto k = 0u; k < j; k++) {
                            auto &unlockwriteKey = writeSet[k];
                            auto tableId = unlockwriteKey.get_table_id();
                            auto partitionId = unlockwriteKey.get_partition_id();
                            auto table = db.find_table(tableId, partitionId);

                            if (context.coordinator_id == 0 &&
                                partitionId % partitioner.get_partition_num() == 0) {
                                if (!unlockwriteKey.get_write_lock_bit()) {
                                    continue;
                                }
                                auto key = unlockwriteKey.get_key();
                                std::atomic<uint64_t> &tid = table->search_metadata(key);
                                SiloHelper::unlock(tid);
                            } else if (context.coordinator_id != 0 &&
                                       partitionId % partitioner.get_partition_num() == 0) {
                                txn.network_size += MessageFactoryType::new_release_lock_message(
                                    *messages[0], *table, unlockwriteKey.get_key(), 0);
                            }
                        }
                    }

                    if (masterid != partitioner.master_coordinator(0) && masterid == 1) {
                        for (auto k = 0u; k < j; k++) {
                            auto &unlockwriteKey = writeSet[k];
                            auto tableId = unlockwriteKey.get_table_id();
                            auto partitionId = unlockwriteKey.get_partition_id();
                            auto table = db.find_table(tableId, partitionId);

                            if (context.coordinator_id == 1 &&
                                partitionId % partitioner.get_partition_num() == 0) {
                                if (!unlockwriteKey.get_write_lock_bit()) {
                                    continue;
                                }
                                auto key = unlockwriteKey.get_key();
                                std::atomic<uint64_t> &tid = table->search_metadata(key);
                                SiloHelper::unlock(tid);
                            } else if (context.coordinator_id != 1 &&
                                       partitionId % partitioner.get_partition_num() == 0) {
                                txn.network_size += MessageFactoryType::new_release_lock_message(
                                    *messages[1], *table, unlockwriteKey.get_key(), 0);
                            }
                        }
                    }

                    // lock local records
                    if (partitioner.has_master_partition(partitionId)) {
                        auto key = writeKey.get_key();
                        std::atomic<uint64_t> &tid = table->search_metadata(key);
                        bool success;
                        uint64_t latestTid = SiloHelper::lock(tid, success);

                        if (!success) {
                            txn.abort_lock = true;
                            break;
                        }

                        writeKey.set_write_lock_bit();
                        writeKey.set_tid(latestTid);

                        auto readKeyPtr = txn.get_read_key(key);
                        if (readKeyPtr == nullptr) {
                            readKeyPtr = txn.get_read_key(key, table);
                        }

                        // assume no blind write
                        DCHECK(readKeyPtr != nullptr);
                        uint64_t tidOnRead = readKeyPtr->get_tid();
                        if (latestTid != tidOnRead) {
                            txn.abort_lock = true;
                            break;
                        }
                    }
                }
            } else if (txn.nodeSet.count(i)) {
                txn.pendingResponses++;
                txn.lock_remote[i] = true;
                txn.network_size += MessageFactoryType::new_remote_lock_message(
                    *messages[i], i, round_count, context.coordinator_id);
                txn.pr_message_count++;
            }
        }

        txn.message_flusher();
        while (txn.pendingResponses > 0) {
            txn.remote_request_handler();
        }

        txn.process_message_write.clear();
        txn.last_coordinator_write.clear();
        return txn.abort_lock;
    }

    bool restart_in_new_master(TransactionType &txn,
                               std::vector<std::unique_ptr<Message>> &messages) {
        DCHECK(txn.pendingResponses == 0);
        std::set<std::size_t> relockNode;

        // create sub txn in new master
        for (auto i = 0u; i < txn.readSet.size(); i++) {
            std::size_t partition_id = txn.readSet[i].get_partition_id();
            if (!partitioner.has_master_partition(partition_id) &&
                partitioner.is_partition_relock(partition_id)) {
                auto coordinator_id = partitioner.master_coordinator(partition_id);
                if (0 == txn.nodeSet.count(coordinator_id)) {
                    txn.nodeSet.insert(std::pair<std::size_t, bool>(coordinator_id, false));
                }
                relockNode.insert(coordinator_id);
            }
        }
        for (auto i = 0u; i < txn.writeSet.size(); i++) {
            std::size_t partition_id = txn.writeSet[i].get_partition_id();
            if (!partitioner.has_master_partition(partition_id) &&
                partitioner.is_partition_relock(partition_id)) {
                auto coordinator_id = partitioner.master_coordinator(partition_id);
                if (0 == txn.nodeSet.count(coordinator_id)) {
                    txn.nodeSet.insert(std::pair<std::size_t, bool>(coordinator_id, false));
                }
                relockNode.insert(coordinator_id);
            }
        }

        for (const auto &node : txn.nodeSet) {
            if (!node.second) {
                txn.network_size += MessageFactoryType::new_create_sub_txn_message(
                    *messages[node.first], txn.coordinator_id, node.first, txn.partition_id, 1);
                txn.pendingResponses++;
                txn.distributed_transaction = true;
            }
        }

        // re-construct rw set of sub txn
        for (auto i = 0u; i < txn.readSet.size(); i++) {
            std::size_t table_id = txn.readSet[i].get_table_id();
            std::size_t partition_id = txn.readSet[i].get_partition_id();
            ITable *table = this->db.find_table(table_id, partition_id);
            auto coordinator_id = partitioner.master_coordinator(partition_id);

            if (partitioner.is_partition_relock(partition_id)) {
                if (partitioner.has_master_partition(partition_id)) {
                    this->search(table_id, partition_id, txn.readSet[i].get_key(),
                                 txn.readSet[i].get_value());
                } else {
                    txn.network_size += MessageFactoryType::new_search_message(
                        *messages[coordinator_id], *table, txn.readSet[i].get_key(), i, 1,
                        context.coordinator_id);
                    txn.pendingResponses++;
                    txn.distributed_transaction = true;
                }
            }
        }
        for (auto i = 0u; i < txn.writeSet.size(); i++) {
            std::size_t table_id = txn.writeSet[i].get_table_id();
            std::size_t partition_id = txn.writeSet[i].get_partition_id();
            ITable *table = this->db.find_table(table_id, partition_id);
            auto coordinator_id = partitioner.master_coordinator(partition_id);

            if (partitioner.is_partition_relock(partition_id)) {
                if (!partitioner.has_master_partition(partition_id)) {
                    txn.network_size += MessageFactoryType::new_add_remote_wset_message(
                        *messages[coordinator_id], *table, txn.writeSet[i].get_key(),
                        txn.writeSet[i].get_value(), i, 1, context.coordinator_id);
                    txn.pendingResponses++;
                    txn.distributed_transaction = true;
                }
            }
        }
        sync_messages(txn);
        txn.process_message_write.clear();
        txn.process_message_read.clear();

        // try to relock
        for (auto i = 0u; i < partitioner.total_coordinators(); i++) {
            if (i == context.coordinator_id) {
                for (auto j = 0u; j < txn.writeSet.size(); j++) {
                    auto &writeKey = txn.writeSet[j];
                    auto tableId = writeKey.get_table_id();
                    auto partitionId = writeKey.get_partition_id();
                    auto table = db.find_table(tableId, partitionId);

                    // lock local records
                    if (partitioner.has_master_partition(partitionId) &&
                        partitioner.is_partition_relock(partitionId)) {
                        auto key = writeKey.get_key();
                        std::atomic<uint64_t> &tid = table->search_metadata(key);
                        bool success;
                        uint64_t latestTid = SiloHelper::lock(tid, success);

                        if (!success) {
                            txn.abort_lock = true;
                            break;
                        }

                        writeKey.set_write_lock_bit();
                        writeKey.set_tid(latestTid);

                        auto readKeyPtr = txn.get_read_key(key);
                        // for redo txn while some coordinator failed
                        if (readKeyPtr == nullptr) {
                            readKeyPtr = txn.get_read_key(key, table);
                        }
                        // assume no blind write
                        DCHECK(readKeyPtr != nullptr);
                        uint64_t tidOnRead = readKeyPtr->get_tid();
                        if (latestTid != tidOnRead) {
                            txn.abort_lock = true;
                            break;
                        }
                    }
                }
            } else if (relockNode.count(i)) {
                txn.pendingResponses++;
                txn.network_size += MessageFactoryType::new_remote_lock_message(
                    *messages[i], i, 1, context.coordinator_id);
            }
        }

        sync_messages(txn);
        txn.process_message_write.clear();

        for (auto i = 0u; i < context.partition_num; i++) {
            partitioner.finish_partition_relock(i);
        }

        return txn.abort_lock;
    }

    bool validate_all_read_set(TransactionType &txn,
                               std::vector<std::unique_ptr<Message>> &messages) {
        DCHECK(txn.pendingResponses == 0);
        auto &readSet = txn.readSet;
        auto &writeSet = txn.writeSet;
        uint32_t round_count = 1;
        uint32_t message_count_end = 0;
        std::size_t masterid = partitioner.master_coordinator(0);

        auto isKeyInWriteSet = [&writeSet](const void *key) {
            for (auto &writeKey : writeSet) {
                if (writeKey.get_key() == key) {
                    return true;
                }
            }
            return false;
        };

        for (auto i = 0u; i < partitioner.total_coordinators(); i++) {
            if (i == context.coordinator_id) {
                for (auto j = 0u; j < readSet.size(); j++) {
                    auto &readKey = readSet[j];

                    if (readKey.get_local_index_read_bit()) {
                        continue;  // read only index does not need to validate
                    }

                    bool in_write_set = isKeyInWriteSet(readKey.get_key());
                    if (in_write_set) {
                        continue;  // already validated in lock write set
                    }

                    auto tableId = readKey.get_table_id();
                    auto partitionId = readKey.get_partition_id();
                    auto table = db.find_table(tableId, partitionId);
                    auto key = readKey.get_key();
                    auto tid = readKey.get_tid();

                    if (partitioner.has_master_partition(partitionId)) {
                        uint64_t latest_tid = table->search_metadata(key).load();
                        if (SiloHelper::remove_lock_bit(latest_tid) != tid) {
                            txn.abort_read_validation = true;
                            break;
                        }

                        if (SiloHelper::is_locked(latest_tid)) {  // must be locked by others
                            txn.abort_read_validation = true;
                            break;
                        }
                    }
                }
            } else if (txn.nodeSet.count(i)) {
                txn.pendingResponses++;
                txn.read_remote_validation[i] = true;
                txn.network_size += MessageFactoryType::new_remote_read_validation_message(
                    *messages[i], i, round_count, context.coordinator_id);
                txn.pr_message_count++;
            }
        }

        if (txn.pendingResponses == 0) {
            txn.local_validated = true;
        }

        txn.message_flusher();
        while (txn.pendingResponses > 0) {
            txn.remote_request_handler();
        }

        txn.process_message_read.clear();
        txn.last_coordinator_read.clear();

        return !txn.abort_read_validation;
    }

    uint64_t generate_tid(TransactionType &txn) {
        auto &readSet = txn.readSet;
        auto &writeSet = txn.writeSet;

        uint64_t next_tid = 0;

        /*
         *  A timestamp is a 64-bit word.
         *  The most significant bit is the lock bit.
         *  The lower 63 bits are for transaction sequence id.
         *  [  lock bit (1)  |  id (63) ]
         */

        // larger than the TID of any record read or written by the transaction
        for (std::size_t i = 0; i < readSet.size(); i++) {
            next_tid = std::max(next_tid, readSet[i].get_tid());
        }
        for (std::size_t i = 0; i < writeSet.size(); i++) {
            next_tid = std::max(next_tid, writeSet[i].get_tid());
        }

        // larger than remote max tid
        next_tid = std::max(next_tid, txn.max_tid);

        // larger than the worker's most recent chosen TID
        next_tid = std::max(next_tid, max_tid);

        // increment
        next_tid++;

        // update worker's most recent chosen TID
        max_tid = next_tid;

        return next_tid;
    }

    void release_all_lock(TransactionType &txn, uint64_t commit_tid,
                          std::vector<std::unique_ptr<Message>> &messages,
                          std::vector<std::unique_ptr<Message>> &async_messages) {
        auto &readSet = txn.readSet;
        auto &writeSet = txn.writeSet;
        std::size_t masterid = partitioner.master_coordinator(0);

        for (auto i = 0u; i < partitioner.total_coordinators(); i++) {
            if (i == context.coordinator_id) {
                for (auto j = 0u; j < writeSet.size(); j++) {
                    auto &writeKey = writeSet[j];
                    auto tableId = writeKey.get_table_id();
                    auto partitionId = writeKey.get_partition_id();
                    auto table = db.find_table(tableId, partitionId);

                    if (partitioner.has_master_partition(partitionId)) {
                        auto key = writeKey.get_key();
                        auto value = writeKey.get_value();
                        std::atomic<uint64_t> &tid = table->search_metadata(key);
                        table->update(key, value);
                        SiloHelper::unlock(tid, commit_tid);
                    } else if ((partitionId % context.coordinator_num == context.coordinator_id) &&
                               masterid != partitioner.master_coordinator(0) && masterid == 0) {
                        auto key = writeKey.get_key();
                        auto value = writeKey.get_value();
                        std::atomic<uint64_t> &tid = table->search_metadata(key);
                        table->update(key, value);
                        SiloHelper::unlock(tid, commit_tid);
                    } else if ((partitionId % context.coordinator_num ==
                                context.coordinator_id - 1) &&
                               masterid != partitioner.master_coordinator(0) && masterid == 1) {
                        auto key = writeKey.get_key();
                        auto value = writeKey.get_value();
                        std::atomic<uint64_t> &tid = table->search_metadata(key);
                        table->update(key, value);
                        SiloHelper::unlock(tid, commit_tid);
                    }
                }
            } else if (txn.nodeSet.count(i)) {
                txn.network_size += MessageFactoryType::new_remote_release_lock_message(
                    *messages[i], i, commit_tid);
                txn.com_message_count++;
            }
        }
        txn.message_flusher();
    }

    void sync_messages(TransactionType &txn, bool wait_response = true) {
        txn.message_flusher();
        if (wait_response) {
            while (txn.pendingResponses > 0) {
                txn.remote_request_handler();
            }
        }
    }

    void simulate_network_latency(TransactionType &txn) {
        auto starttime = std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - starttime)
                   .count() < context.network_latency) {
            txn.remote_request_handler();
        }
        txn.message_flusher();
    }

private:
    DatabaseType &db;
    const ContextType &context;
    Partitioner &partitioner;
    std::atomic<bool> &transferFlag;
    std::atomic<bool> &recoveryFlag;
    std::atomic<bool> &coordinatorFaultFlag;
    std::chrono::steady_clock::time_point workerStartTime;
    uint64_t max_tid = 0;
    bool locked = false;
};

}  // namespace lefr
