#pragma once

#include "butil/logging.h"

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/LeFR/SiloHelper.h"
#include "protocol/Tapir/TapirMessage.h"
#include "protocol/Tapir/TapirTransaction.h"

namespace lefr {

template <class Database>
class Tapir {
public:
    using DatabaseType = Database;
    using MetaDataType = std::atomic<uint64_t>;
    using ContextType = typename DatabaseType::ContextType;
    using MessageType = TapirMessage;
    using TransactionType = TapirTransaction<DatabaseType>;

    using MessageFactoryType = TapirMessageFactory;
    using MessageHandlerType = TapirMessageHandler<DatabaseType>;

    Tapir(DatabaseType &db, const ContextType &context, Partitioner &partitioner,
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

    uint32_t fast_passnumber() {
        uint32_t result;
        if (partitioner.replica_num() <= 4) {
            result = partitioner.replica_num();
        } else if (partitioner.replica_num() == 5) {
            result = partitioner.replica_num() - 1;
        } else if (partitioner.replica_num() == 7) {
            result = partitioner.replica_num() - 1;
        } else if (partitioner.replica_num() == 9) {
            result = partitioner.replica_num() - 2;
        } else if (partitioner.replica_num() == 11) {
            result = partitioner.replica_num() - 2;
        }
        return result;
    }
    uint32_t slow_passnumber() {
        uint32_t result;
        if (partitioner.replica_num() <= 2) {
            result = partitioner.replica_num();
        } else if (partitioner.replica_num() == 3) {
            result = 2;
        } else if (partitioner.replica_num() == 4) {
            result = 3;
        } else if (partitioner.replica_num() == 5) {
            result = 3;
        } else if (partitioner.replica_num() == 7) {
            result = 4;
        } else if (partitioner.replica_num() == 9) {
            result = 5;
        } else if (partitioner.replica_num() == 11) {
            result = 6;
        }
        return result;
    }

    void clear_partitionsets(TransactionType &txn) {
        for (auto &partition : txn.lock_partitionSet) {
            partition.second = 0;
        }
        for (auto &partition : txn.validation_partitionSet) {
            partition.second = 0;
        }
        for (auto &partition : txn.commit_partitionSet) {
            partition.second = 0;
        }
        for (auto &partition : txn.ft_partitionSet) {
            partition.second = 0;
        }
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

                    if (partitioner.is_partition_replicated_on(partitionId,
                                                               context.coordinator_id)) {
                        auto key = writeKey.get_key();
                        std::atomic<uint64_t> &tid = table->search_metadata(key);
                        SiloHelper::unlock(tid);
                    }
                }
            } else if (txn.nodeSet.count(i)) {
                auto temp_network_size =
                    MessageFactoryType::new_remote_abort_message(*messages[i], i);
                txn.network_size += temp_network_size;
                txn.pr_network_size += temp_network_size;
                txn.pr_message_count++;
            }
        }

        sync_messages(txn, false);
    }

    bool commit(TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages,
                std::atomic<uint32_t> &n_committing_txns,
                std::vector<std::unique_ptr<Message>> &async_messages) {
        clear_partitionsets(txn);

        // 2PC
        // Phase 1 - Prepare
        // Lock and validate

        // lock write set
        auto lock_pathtype = PathType::FAST;
        if (!lock_all_write_set(txn, messages)) {
            lock_pathtype = PathType::SLOW;
            for (auto partitionset : txn.lock_partitionSet) {
                if (partitionset.second < slow_passnumber()) {
                    lock_pathtype = PathType::ABORT;
                    break;
                }
            }
            if (lock_pathtype == PathType::ABORT) {
                abort_all(txn, messages);
                txn.status = TransactionStatus::ABORT;
                txn.abort_lock = true;
                return false;
            }
        }

        txn.status = TransactionStatus::COMMITTING;

        bool coordinatorFault = false;
        auto startTime = std::chrono::steady_clock::now();
        while (coordinatorFaultFlag.load()) {
            if (coordinatorFault == false) {
                coordinatorFault = true;
            }
            txn.remote_request_handler();
            if (std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - startTime)
                    .count() > (int32_t)context.recovery_time) {
                abort_all(txn, messages);
                break;
            }
        }
        if (coordinatorFault) {
            n_committing_txns.fetch_sub(1);
            return true;
        }

        // commit phase 2, read validation
        auto validate_pathtype = PathType::FAST;
        if (!validate_all_read_set(txn, messages)) {
            validate_pathtype = PathType::SLOW;
            for (auto partitionset : txn.validation_partitionSet) {
                if (partitionset.second < slow_passnumber()) {
                    validate_pathtype = PathType::ABORT;
                    break;
                }
            }
            if (validate_pathtype == PathType::ABORT) {
                abort_all(txn, messages);
                txn.status = TransactionStatus::ABORT;
                txn.abort_read_validation = true;
                return false;
            }
        }

        // generate tid
        uint64_t commit_tid = generate_tid(txn);

        if (lock_pathtype == PathType::SLOW) {
            backup_decision(txn, commit_tid, messages);
        }

        // write and replicate
        write_and_replicate_all(txn, commit_tid, messages, async_messages);

        txn.status = TransactionStatus::COMMIT;

        return true;
    }

    void set_worker_start_time(std::chrono::steady_clock::time_point workerStartTime) {
        this->workerStartTime = workerStartTime;
    }

private:
    void backup_decision(TransactionType &txn, uint64_t commit_tid,
                         std::vector<std::unique_ptr<Message>> &messages) {
        auto partitionNum = txn.partitioner.get_partition_num();
        uint32_t ftCount = 0;
        for (auto i = 0u; i < partitioner.total_coordinators(); i++) {
            if (i == context.coordinator_id) {
                for (auto k = 0u; k < partitionNum; k++) {
                    if (partitioner.is_partition_replicated_on(k, context.coordinator_id)) {
                        txn.ft_partitionSet[i] = 1;
                    }
                }
            } else if (txn.nodeSet.count(i)) {
                txn.pendingResponses++;
                auto temp_network_size = MessageFactoryType::new_sync_decision_message(
                    *messages[i], txn.coordinator_id, commit_tid);
                txn.network_size += temp_network_size;
                txn.pr_network_size += temp_network_size;
                txn.pr_message_count++;
            }
        }

        simulate_network_latency(txn);
        while (txn.pendingResponses > 0) {
            txn.remote_request_handler();
            ftCount = 0;
            for (auto partition : txn.ft_partitionSet) {
                if (partition.second >= slow_passnumber()) {
                    ftCount += 1;
                }
            }
            if (ftCount == txn.ft_partitionSet.size()) {
                break;
            }
        }
    }

    bool lock_all_write_set(TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages) {
        auto &writeSet = txn.writeSet;
        std::unordered_map<uint32_t, bool> lock_result;
        std::size_t message_count_end = 0;
        uint32_t round_count = 1;

        for (auto i = 0u; i < partitioner.total_coordinators(); i++) {
            if (i == context.coordinator_id) {
                for (auto j = 0u; j < writeSet.size(); j++) {
                    auto &writeKey = writeSet[j];
                    auto tableId = writeKey.get_table_id();
                    auto partitionId = writeKey.get_partition_id();
                    DCHECK(0 != txn.lock_partitionSet.count(partitionId));
                    auto table = db.find_table(tableId, partitionId);

                    // lock local records
                    if (partitioner.is_partition_replicated_on(partitionId,
                                                               context.coordinator_id)) {
                        if (0 == lock_result.count(partitionId)) {
                            lock_result.insert(std::pair<uint32_t, uint32_t>(partitionId, true));
                        } else if (lock_result[partitionId] == false) {
                            continue;
                        }

                        // simulate fault
                        if (0 != txn.fault_partitions.count(tableId)) {
                            if (0 != txn.fault_partitions[tableId].count(partitionId)) {
                                lock_result[partitionId] = false;
                                continue;
                            }
                        }

                        auto key = writeKey.get_key();
                        std::atomic<uint64_t> &tid = table->search_metadata(key);
                        bool success;
                        uint64_t latestTid = SiloHelper::lock(tid, success);

                        if (!success) {
                            lock_result[partitionId] = false;
                            continue;
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
                            lock_result[partitionId] = false;
                            continue;
                        }
                    }
                }
                for (auto &result : lock_result) {
                    if (result.second == true) {
                        txn.lock_partitionSet[result.first] = 1;
                    } else {
                        txn.lock_partitionSet[result.first] = 0;
                    }
                }
            } else if (txn.nodeSet.count(i)) {
                txn.pendingResponses++;
                auto temp_network_size = MessageFactoryType::new_remote_lock_message(
                    *messages[i], i, round_count, context.coordinator_id);
                txn.network_size += temp_network_size;
                txn.pr_network_size += temp_network_size;
                txn.pr_message_count++;
            }
        }

        bool pass;
        txn.message_flusher();
        while (txn.pendingResponses > 0) {
            txn.remote_request_handler();
            pass = true;
            for (auto partition : txn.lock_partitionSet) {
                if (partition.second >= fast_passnumber()) {
                    continue;
                } else {
                    pass = false;
                    break;
                }
            }
            if (pass == true) break;
        }

        return pass;
    }

    bool validate_all_read_set(TransactionType &txn,
                               std::vector<std::unique_ptr<Message>> &messages) {
        auto &readSet = txn.readSet;
        auto &writeSet = txn.writeSet;
        std::unordered_map<uint32_t, bool> validate_result;
        uint32_t round_count = 1;
        uint32_t message_count_end = 0;

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
                    auto partitionId = readKey.get_partition_id();
                    DCHECK(0 != txn.validation_partitionSet.count(partitionId));
                    auto tableId = readKey.get_table_id();
                    auto table = db.find_table(tableId, partitionId);
                    auto key = readKey.get_key();
                    auto tid = readKey.get_tid();

                    if (readKey.get_local_index_read_bit()) {
                        continue;  // read only index does not need to validate
                    }

                    if (partitioner.is_partition_replicated_on(partitionId,
                                                               context.coordinator_id)) {
                        if (0 == validate_result.count(partitionId)) {
                            validate_result.insert(
                                std::pair<uint32_t, uint32_t>(partitionId, true));
                        } else if (validate_result[partitionId] == false) {
                            continue;
                        }

                        bool in_write_set = isKeyInWriteSet(readKey.get_key());
                        if (in_write_set) {
                            continue;  // already validated in lock write set
                        }

                        // simulate fault
                        if (0 != txn.fault_partitions.count(tableId)) {
                            if (0 != txn.fault_partitions[tableId].count(partitionId)) {
                                validate_result[partitionId] = false;
                                continue;
                            }
                        }

                        uint64_t latest_tid = table->search_metadata(key).load();
                        // if validation is failed, the key's partition should be added to the set.
                        if (SiloHelper::remove_lock_bit(latest_tid) != tid) {
                            validate_result[partitionId] = false;
                            continue;
                        }

                        if (SiloHelper::is_locked(latest_tid)) {  // must be locked by others
                            validate_result[partitionId] = false;
                            continue;
                        }
                    }
                }
                // record the local validationresult
                for (auto &result : validate_result) {
                    if (result.second == true) {
                        txn.validation_partitionSet[result.first] = 1;
                    } else {
                        txn.validation_partitionSet[result.first] = 0;
                    }
                }
            } else if (txn.nodeSet.count(i)) {  // sub txns validate
                txn.pendingResponses++;
                auto temp_network_size = MessageFactoryType::new_remote_read_validation_message(
                    *messages[i], i, round_count, context.coordinator_id);
                txn.network_size += temp_network_size;
                txn.pr_network_size += temp_network_size;
                txn.pr_message_count++;
            }
        }

        bool pass;
        txn.message_flusher();
        while (txn.pendingResponses > 0) {
            pass = true;
            txn.remote_request_handler();
            for (auto &partition : txn.validation_partitionSet) {
                if (partition.second >= fast_passnumber()) {
                    continue;
                } else {
                    pass = false;
                    break;
                }
            }
            if (pass == true) break;
        }

        auto starttime = std::chrono::steady_clock::now();
        while (std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - starttime)
                   .count() < context.network_latency) {
            txn.remote_request_handler();
        }

        return pass;
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

public:
    void write_and_replicate_all(TransactionType &txn, uint64_t commit_tid,
                                 std::vector<std::unique_ptr<Message>> &messages,
                                 std::vector<std::unique_ptr<Message>> &async_messages) {
        auto &readSet = txn.readSet;
        auto &writeSet = txn.writeSet;
        std::unordered_map<uint32_t, bool> write_result;
        uint32_t round_count = 1;
        std::size_t masterid = partitioner.master_coordinator(0);

        // write
        for (auto i = 0u; i < partitioner.total_coordinators(); i++) {
            if (i == context.coordinator_id) {
                for (auto j = 0u; j < writeSet.size(); j++) {
                    auto &writeKey = writeSet[j];
                    auto tableId = writeKey.get_table_id();
                    auto partitionId = writeKey.get_partition_id();
                    DCHECK(0 != txn.commit_partitionSet.count(partitionId));
                    auto table = db.find_table(tableId, partitionId);

                    if (partitioner.is_partition_replicated_on(partitionId,
                                                               context.coordinator_id)) {
                        if (0 == write_result.count(partitionId)) {
                            write_result.insert(std::pair<uint32_t, uint32_t>(partitionId, true));
                        }
                        auto key = writeKey.get_key();
                        auto value = writeKey.get_value();
                        std::atomic<uint64_t> &tid = table->search_metadata(key);
                        table->update(key, value);
                        SiloHelper::unlock(tid, commit_tid);
                    }
                }
                for (auto &result : write_result) {
                    if (result.second == true) {
                        txn.commit_partitionSet[result.first] = 1;
                    } else {
                        txn.commit_partitionSet[result.first] = 0;
                    }
                }
            } else if (txn.nodeSet.count(i)) {
                txn.pendingResponses++;
                auto temp_network_size = MessageFactoryType::new_remote_write_message(
                    *messages[i], i, round_count, context.coordinator_id, commit_tid);
                txn.network_size += temp_network_size;
                txn.com_network_size += temp_network_size;
                txn.com_message_count++;
            }
        }

        simulate_network_latency(txn);
        while (txn.pendingResponses > 0) {
            txn.remote_request_handler();

            for (auto &partition : txn.commit_partitionSet) {
                if (partition.second >= fast_passnumber()) {
                    continue;
                }
            }
        }
        return;
    }

private:
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
