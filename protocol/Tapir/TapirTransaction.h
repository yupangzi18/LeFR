#pragma once

#include <butil/logging.h>

#include <chrono>
#include <list>
#include <unordered_map>
#include <vector>

#include "common/Message.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/LeFR/SiloHelper.h"
#include "protocol/LeFR/SiloRWKey.h"

namespace lefr {

template <class Database>
class TapirTransaction {
public:
    using MetaDataType = std::atomic<uint64_t>;
    using DatabaseType = Database;

    TapirTransaction(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db,
                     Partitioner &partitioner, std::atomic<bool> &transferFlag,
                     std::atomic<bool> &recoveryFlag,
                     std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions)
        : coordinator_id(coordinator_id),
          partition_id(partition_id),
          db(db),
          startTime(std::chrono::steady_clock::now()),
          partitioner(partitioner),
          transferFlag(transferFlag),
          recoveryFlag(recoveryFlag),
          fault_partitions(fault_partitions) {
        reset();
        redo = 0;
        scanreplica = 2;
        during_fault = false;
        isredo = false;
        startTimeTxn = startTime.time_since_epoch().count();
    }

    virtual ~TapirTransaction() = default;

    void reset() {
        txn_seed = 0;
        status = TransactionStatus::BEGIN;
        pendingResponses = 0;
        abort_pendingResponses = 0;
        network_size = 0;
        abort_lock = false;
        abort_read_validation = false;
        local_validated = false;
        si_in_serializable = false;
        distributed_transaction = false;
        execution_phase = true;
        operation.clear();
        readSet.clear();
        writeSet.clear();
        nodeSet.clear();
        lock_partitionSet.clear();
        validation_partitionSet.clear();
        commit_partitionSet.clear();
        ft_partitionSet.clear();
        stringPieces.clear();
        max_tid = 0;
        cur_tid = 0;
        operation_count = 0;
        readHandled = false;
        rw_network_size = 0;
        pr_network_size = 0;
        com_network_size = 0;
        rw_message_count = 0;
        pr_message_count = 0;
        com_message_count = 0;
    }

    virtual TransactionResult execute(std::size_t worker_id,
                                      std::vector<std::unique_ptr<Message>> &messages,
                                      std::atomic<bool> &faultFlag,
                                      std::atomic<uint32_t> &n_committing_txns,
                                      std::chrono::steady_clock::time_point workerStartTime,
                                      std::atomic<uint32_t> &n_redo_txns) = 0;

    virtual void reset_query() = 0;

    template <class KeyType, class ValueType>
    void search_local_index(std::size_t table_id, std::size_t partition_id, const KeyType &key,
                            ValueType &value) {
        SiloRWKey readKey;

        readKey.set_table_id(table_id);
        readKey.set_partition_id(partition_id);

        readKey.set_key(&key);
        readKey.set_value(&value);

        readKey.set_local_index_read_bit();
        readKey.set_read_request_bit();

        add_to_read_set(readKey);

        for (std::size_t k = 0; k < partitioner.total_coordinators(); k++) {
            if (partitioner.is_partition_replicated_on(partition_id, k)) {
                if (coordinator_id != k) {
                    add_to_node_set(k);
                }
            }
        }
    }

    template <class KeyType, class ValueType>
    void search_for_read(std::size_t table_id, std::size_t partition_id, const KeyType &key,
                         ValueType &value) {
        SiloRWKey readKey;

        readKey.set_table_id(table_id);
        readKey.set_partition_id(partition_id);

        readKey.set_key(&key);
        readKey.set_value(&value);

        readKey.set_read_request_bit();

        add_to_read_set(readKey);

        for (std::size_t k = 0; k < partitioner.total_coordinators(); k++) {
            if (partitioner.is_partition_replicated_on(partition_id, k)) {
                if (coordinator_id != k) {
                    add_to_node_set(k);
                }
            }
        }
    }

    template <class KeyType, class ValueType>
    void search_for_update(std::size_t table_id, std::size_t partition_id, const KeyType &key,
                           ValueType &value) {
        SiloRWKey readKey;

        readKey.set_table_id(table_id);
        readKey.set_partition_id(partition_id);

        readKey.set_key(&key);
        readKey.set_value(&value);

        readKey.set_read_request_bit();

        add_to_read_set(readKey);

        for (std::size_t k = 0; k < partitioner.total_coordinators(); k++) {
            if (partitioner.is_partition_replicated_on(partition_id, k)) {
                if (coordinator_id != k) {
                    add_to_node_set(k);
                }
            }
        }
    }

    template <class KeyType, class ValueType>
    void update(std::size_t table_id, std::size_t partition_id, const KeyType &key,
                const ValueType &value) {
        SiloRWKey writeKey;

        writeKey.set_table_id(table_id);
        writeKey.set_partition_id(partition_id);

        writeKey.set_key(&key);
        writeKey.set_value(const_cast<ValueType *>(&value));

        writeKey.set_write_request_bit();

        add_to_write_set(writeKey);

        for (std::size_t k = 0; k < partitioner.total_coordinators(); k++) {
            if (partitioner.is_partition_replicated_on(partition_id, k)) {
                if (coordinator_id != k) {
                    add_to_node_set(k);
                }
            }
        }
    }

    void replicate_transaction(uint32_t type, uint64_t seed, std::size_t batch_op_flush,
                               uint64_t startTime,
                               std::vector<std::unique_ptr<Message>> &messages) {
        std::size_t replicate_count = 0;

        for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
            // k does not have this partition
            if (!partitioner.is_partition_replicated_on(partition_id, k)) {
                continue;
            }

            // already write
            if (k == partitioner.master_coordinator(partition_id) && redo == 0) {
                continue;
            }

            // remote replicate
            if (k != coordinator_id) {
                replicate_count++;
                replicate_transaction_info(*messages[k], type, coordinator_id, partition_id, seed,
                                           startTime);
            }
        }

        DCHECK(replicate_count == partitioner.replica_num() - 1);

        operation_count++;
        if (operation_count % batch_op_flush == 0) {
            sync_messages();
        }
    }

    void replicate_read(std::size_t table_id, std::size_t partition_id, DatabaseType &db,
                        std::size_t batch_op_flush,
                        std::vector<std::unique_ptr<Message>> &messages) {
        auto &readKey = readSet.back();
        auto table = db.find_table(table_id, partition_id);
        auto key = readKey.get_key();
        auto value = readKey.get_value();
        auto tid = readKey.get_tid();

        std::size_t replicate_count = 0;

        // value replicate
        for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
            // k does not have this partition
            if (!partitioner.is_partition_replicated_on(this->partition_id, k)) {
                continue;
            }

            // already write
            if (k == partitioner.master_coordinator(this->partition_id) && redo == 0) {
                continue;
            }

            // remote replicate
            if (k != coordinator_id) {
                replicate_count++;
                replicate_read_operation(*messages[k], *table, key, value, tid);
            }
        }

        operation_count++;
        if (operation_count % batch_op_flush == 0) {
            sync_messages();
        }
    }

    void replicate_write(std::size_t table_id, std::size_t partition_id, DatabaseType &db,
                         std::size_t batch_op_flush,
                         std::vector<std::unique_ptr<Message>> &messages) {
        auto &writeKey = writeSet.back();
        auto table = db.find_table(table_id, partition_id);
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();

        std::size_t replicate_count = 0;

        // value replicate
        for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
            // k does not have this partition
            if (!partitioner.is_partition_replicated_on(this->partition_id, k)) {
                continue;
            }

            // already write
            if (k == partitioner.master_coordinator(this->partition_id) && redo == 0) {
                continue;
            }

            // remote replicate
            if (k != coordinator_id) {
                replicate_count++;
                replicate_write_operation(*messages[k], *table, key, value);
            }
        }

        operation_count++;
        if (operation_count % batch_op_flush == 0) {
            sync_messages();
        }
    }

    void signal_redo(uint32_t redo, std::vector<std::unique_ptr<Message>> &messages) {
        for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
            if (!partitioner.is_partition_replicated_on(partition_id, k)) {
                continue;
            }

            if (k == partitioner.master_coordinator(partition_id) && redo == 0) {
                continue;
            }

            if (k != coordinator_id) {
                send_redo_signal(*messages[k], redo);
                break;
            }
        }

        message_flusher();
    }

    bool process_requests(std::size_t worker_id) {
        uint32_t round_count = 1;
        std::size_t last_pendingresponses = pendingResponses;

        // create sub txns
        for (auto &node : nodeSet) {
            if (!node.second) {
                create_sub_txn(node.first, coordinator_id, partition_id, round_count);
                node.second = true;
            }
        }

        // cannot use unsigned type in reverse iteration
        for (int i = int(readSet.size()) - 1; i >= 0; i--) {
            // early return
            if (!readSet[i].get_read_request_bit()) {
                break;
            }

            const SiloRWKey &readKey = readSet[i];
            if (0 == validation_partitionSet.count(readKey.get_partition_id())) {
                validation_partitionSet.insert(
                    std::pair<uint32_t, uint32_t>(readKey.get_partition_id(), 0));
            }
            auto tid = readRequestHandler(readKey.get_table_id(), readKey.get_partition_id(), i,
                                          readKey.get_key(), readKey.get_value(),
                                          readKey.get_local_index_read_bit(), round_count);
            readSet[i].clear_read_request_bit();
            readSet[i].set_tid(tid);
        }
        // cannot use unsigned type in reverse iteration
        for (int i = int(writeSet.size()) - 1; i >= 0; i--) {
            // early return
            if (!writeSet[i].get_write_request_bit()) {
                break;
            }

            const SiloRWKey &writeKey = writeSet[i];
            if (0 == lock_partitionSet.count(writeKey.get_partition_id())) {
                lock_partitionSet.insert(
                    std::pair<uint32_t, uint32_t>(writeKey.get_partition_id(), 0));
                commit_partitionSet.insert(
                    std::pair<uint32_t, uint32_t>(writeKey.get_partition_id(), 0));
                ft_partitionSet.insert(
                    std::pair<uint32_t, uint32_t>(writeKey.get_partition_id(), 0));
            }

            writeRequestHandler(writeKey.get_table_id(), writeKey.get_partition_id(), i,
                                writeKey.get_key(), writeKey.get_value(), round_count);

            writeSet[i].clear_write_request_bit();
        }

        if (pendingResponses > 0) {
            message_flusher();
            while (pendingResponses > 0) {
                remote_request_handler();
            }
        }
        return false;
    }

    SiloRWKey *get_read_key(const void *key) {
        for (auto i = 0u; i < readSet.size(); i++) {
            if (readSet[i].get_key() == key) {
                return &readSet[i];
            }
        }

        return nullptr;
    }

    SiloRWKey *get_read_key(const void *key, ITable *table) {
        auto key_size = table->key_size();
        for (auto i = 0u; i < readSet.size(); i++) {
            if (table->search_value(readSet[i].get_key()) == table->search_value(key)) {
                return &readSet[i];
            }
        }

        return nullptr;
    }

    SiloRWKey *get_write_key(const void *key, ITable *table) {
        auto key_size = table->key_size();
        for (auto i = 0u; i < writeSet.size(); i++) {
            if (table->search_value(writeSet[i].get_key()) == table->search_value(key)) {
                return &writeSet[i];
            }
        }

        return nullptr;
    }

    std::size_t add_to_read_set(const SiloRWKey &key) {
        readSet.push_back(key);
        return readSet.size() - 1;
    }

    std::size_t add_to_write_set(const SiloRWKey &key) {
        writeSet.push_back(key);
        return writeSet.size() - 1;
    }

    std::size_t add_to_node_set(std::size_t id) {
        if (0 == nodeSet.count(id)) {
            nodeSet.insert(std::pair<std::size_t, bool>(id, false));
        }
        return nodeSet.size();
    }

    std::size_t add_to_node_writeset(std::size_t id) {
        if (0 == nodewriteSet.count(id)) {
            nodewriteSet.insert(std::pair<std::size_t, bool>(id, false));
        }
        return nodewriteSet.size();
    }

    void lock_write_set() {
        std::unordered_map<uint32_t, bool> lock_result;
        for (auto i = 0u; i < writeSet.size(); i++) {
            auto &writeKey = writeSet[i];
            auto tableId = writeKey.get_table_id();
            auto partitionId = writeKey.get_partition_id();
            DCHECK(0 != lock_partitionSet.count(partitionId));

            auto table = db.find_table(tableId, partitionId);

            if (0 == lock_result.count(partitionId)) {
                lock_result.insert(std::pair<uint32_t, uint32_t>(partitionId, true));
            } else if (lock_result[partitionId] == false) {
                continue;
            }

            if (0 != fault_partitions.count(tableId)) {
                if (0 != fault_partitions[tableId].count(partitionId)) {
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

            max_tid = std::max(max_tid, latestTid);

            auto readKeyPtr = get_read_key(key, table);
            DCHECK(readKeyPtr != nullptr);

            uint64_t tidOnRead = readKeyPtr->get_tid();
            if (latestTid != tidOnRead) {
                lock_result[partitionId] = false;
                continue;
            }
        }
        for (auto &result : lock_result) {
            if (result.second == true) {
                lock_partitionSet[result.first] = 1;
            } else {
                lock_partitionSet[result.first] = 0;
            }
        }

        return;
    }

    void validate_read_set() {
        std::unordered_map<uint32_t, bool> validate_result;
        auto writeSet = this->writeSet;
        auto isKeyInWriteSet = [&writeSet](const void *key, ITable *table) {
            for (auto &writeKey : writeSet) {
                if (table->search_value(writeKey.get_key()) == table->search_value(key)) {
                    return true;
                }
            }
            return false;
        };

        for (auto i = 0u; i < readSet.size(); i++) {
            auto &readKey = readSet[i];
            auto tableId = readKey.get_table_id();
            auto partitionId = readKey.get_partition_id();
            DCHECK(0 != validation_partitionSet.count(partitionId));

            if (0 == validate_result.count(partitionId)) {
                validate_result.insert(std::pair<uint32_t, uint32_t>(partitionId, true));
            } else if (validate_result[partitionId] == false) {
                continue;
            }

            auto table = db.find_table(tableId, partitionId);
            auto key = readKey.get_key();
            auto tid = readKey.get_tid();

            if (readKey.get_local_index_read_bit()) {
                continue;  // read only index does not need to validate
            }

            bool in_write_set = isKeyInWriteSet(readKey.get_key(), table);
            if (in_write_set) {
                continue;  // already validated in lock write set
            }

            if (0 != fault_partitions.count(tableId)) {
                if (0 != fault_partitions[tableId].count(partitionId)) {
                    validate_result[partitionId] = false;
                    continue;
                }
            }
            uint64_t latest_tid = table->search_metadata(key).load();
            if (SiloHelper::remove_lock_bit(latest_tid) != tid) {
                validate_result[partitionId] = false;
                continue;
            }
            if (SiloHelper::is_locked(latest_tid)) {  // must be locked by others
                validate_result[partitionId] = false;
                continue;
            }
        }
        for (auto &result : validate_result) {
            if (result.second == true) {
                validation_partitionSet[result.first] = 1;
            } else {
                validation_partitionSet[result.first] = 0;
            }
        }

        return;
    }

    void write_local(uint64_t commit_tid) {
        for (auto i = 0u; i < writeSet.size(); i++) {
            auto &writeKey = writeSet[i];
            auto tableId = writeKey.get_table_id();
            auto partitionId = writeKey.get_partition_id();
            DCHECK(0 != commit_partitionSet.count(partitionId));
            auto table = db.find_table(tableId, partitionId);

            commit_partitionSet[partitionId] = 1;
            auto key = writeKey.get_key();
            auto value = writeKey.get_value();
            std::atomic<uint64_t> &tid = table->search_metadata(key);
            table->update(key, value);
            SiloHelper::unlock(tid, commit_tid);
        }
    }

    void release_lock(uint64_t commit_tid) {
        for (auto i = 0u; i < writeSet.size(); i++) {
            auto &writeKey = writeSet[i];
            auto tableId = writeKey.get_table_id();
            auto partitionId = writeKey.get_partition_id();
            auto table = db.find_table(tableId, partitionId);

            auto key = writeKey.get_key();
            auto value = writeKey.get_value();
            std::atomic<uint64_t> &tid = table->search_metadata(key);
            table->update(key, value);
            SiloHelper::unlock(tid, commit_tid);
        }
    }

    void abort() {
        for (auto i = 0u; i < writeSet.size(); i++) {
            auto &writeKey = writeSet[i];
            // only unlock locked records
            if (!writeKey.get_write_lock_bit()) {
                continue;
            }
            auto tableId = writeKey.get_table_id();
            auto partitionId = writeKey.get_partition_id();
            auto table = db.find_table(tableId, partitionId);
            auto key = writeKey.get_key();
            std::atomic<uint64_t> &tid = table->search_metadata(key);
            SiloHelper::unlock(tid);
        }
    }

    void replicate_transaction_txn(uint32_t type, std::size_t coordinator_id,
                                   std::size_t partition_id, uint64_t seed, uint64_t startTime,
                                   std::vector<std::unique_ptr<Message>> &syncMessages) {
        for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
            // k does not have this partition
            if (!partitioner.is_partition_replicated_on(partition_id, k)) {
                continue;
            }

            // local
            if (k == coordinator_id) {
                continue;
            }

            pendingResponses++;
            replicate_transaction_info(*syncMessages[k], type, coordinator_id, partition_id, seed,
                                       startTime);
        }

        sync_messages();
    }

    void replicate_scanreplica_txn(std::vector<std::unique_ptr<Message>> &syncMessages,
                                   uint32_t scanreplica, uint64_t startTime) {
        send_scanreplica_signal(*syncMessages[1], scanreplica, startTime);
        message_flusher();
    }

    void sync_messages() {
        if (pendingResponses > 0) {
            message_flusher();
            while (pendingResponses > 0) {
                std::this_thread::yield();
                remote_request_handler();
            }
        }
    }

public:
    std::size_t coordinator_id, partition_id;
    uint64_t txn_seed;
    DatabaseType &db;
    TransactionStatus status;
    std::chrono::steady_clock::time_point startTime;
    std::chrono::steady_clock::time_point workerStartTime;
    std::size_t pendingResponses;
    std::size_t abort_pendingResponses;
    std::size_t network_size;
    bool abort_lock, abort_read_validation, local_validated, si_in_serializable;
    bool distributed_transaction;
    bool execution_phase;
    std::function<void(std::size_t, std::size_t, std::size_t, uint32_t)> create_sub_txn;
    std::function<uint64_t(std::size_t, std::size_t, uint32_t, const void *, void *, bool,
                           uint32_t)>
        readRequestHandler;
    std::function<void(std::size_t, std::size_t, uint32_t, const void *, void *, uint32_t)>
        writeRequestHandler;
    std::function<std::size_t(void)> remote_request_handler;
    std::function<void(Message &, uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
        replicate_transaction_info;
    std::function<void()> message_flusher;

    std::function<void(Message &, ITable &, const void *key, const void *value, uint64_t)>
        replicate_read_operation;
    std::function<void(Message &, ITable &, const void *key, const void *value)>
        replicate_write_operation;
    std::function<void(Message &, uint32_t)> send_redo_signal;
    std::function<void(Message &, uint32_t, uint64_t)> send_scanreplica_signal;

    Partitioner &partitioner;
    std::atomic<bool> &transferFlag;
    std::atomic<bool> &recoveryFlag;
    Operation operation;
    std::vector<SiloRWKey> readSet, writeSet;
    std::unordered_map<std::size_t, bool> nodeSet, nodewriteSet;
    std::unordered_map<uint32_t, uint32_t> lock_partitionSet;
    std::unordered_map<uint32_t, uint32_t> validation_partitionSet;
    std::unordered_map<uint32_t, uint32_t> commit_partitionSet;
    std::unordered_map<uint32_t, uint32_t> ft_partitionSet;
    std::list<StringPiece> stringPieces;
    uint64_t max_tid, cur_tid;
    std::size_t operation_count;
    bool readHandled;
    uint32_t redo;
    uint32_t scanreplica;
    uint64_t startTimeTxn;
    std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions;

    bool during_fault;
    bool is_longtxn;
    bool isredo;
    std::size_t rw_network_size;
    std::size_t pr_network_size;
    std::size_t com_network_size;
    std::size_t rw_message_count;
    std::size_t pr_message_count;
    std::size_t com_message_count;
};

}  // namespace lefr
