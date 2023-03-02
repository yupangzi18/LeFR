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
class LeFRTransaction {
public:
    using MetaDataType = std::atomic<uint64_t>;
    using DatabaseType = Database;

    LeFRTransaction(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db,
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
        for (auto i = 1u; i < partitioner.replica_num(); i++) {
            auto dest_node_id = (coordinator_id + i) % partitioner.total_coordinators();
            replicateSet.insert(std::pair<std::size_t, bool>(dest_node_id, false));
        }
        for (auto i = 0u; i < partitioner.total_coordinators(); i++) {
            read_remote_validation.emplace_back(false);
            lock_remote.emplace_back(false);
            is_create_sub.emplace_back(false);
            remote_write.emplace_back(false);
        }
    }

    virtual ~LeFRTransaction() = default;

    void reset() {
        txn_seed = 0;
        status = TransactionStatus::BEGIN;
        pendingResponses = 0;
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
        replicateSet.clear();
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
        std::size_t node_id = partitioner.master_coordinator(partition_id);
        if (coordinator_id != node_id) {
            LOG(INFO) << coordinator_id << "-" << node_id;
            add_to_node_set(node_id);
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
        std::size_t node_id = partitioner.master_coordinator(partition_id);
        if (coordinator_id != node_id) {
            add_to_node_set(node_id);
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
        std::size_t node_id = partitioner.master_coordinator(partition_id);
        if (coordinator_id != node_id) {
            add_to_node_set(node_id);
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
        std::size_t node_id = partitioner.master_coordinator(partition_id);
        if (coordinator_id != node_id) {
            add_to_node_set(node_id);
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

        // create sub txns
        for (const auto &node : nodeSet) {
            if (!node.second) {
                create_sub_txn(node.first, coordinator_id, partition_id, round_count);
            }
        }

        // cannot use unsigned type in reverse iteration
        for (int i = int(readSet.size()) - 1; i >= 0; i--) {
            // early return
            if (!readSet[i].get_read_request_bit()) {
                break;
            }

            const SiloRWKey &readKey = readSet[i];
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
        raft_replicate_txn();
        process_message_write.clear();
        process_message_read.clear();
        last_coordinator_read.clear();
        last_coordinator_write.clear();
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

    void set_process_message_read(std::size_t index, std::size_t value) {
        process_message_read[index] = value;
        return;
    }

    int get_index_process_message_read(std::size_t value) {
        std::size_t index = 0;
        for (auto i = 0u; i < process_message_read.size(); i++) {
            if (process_message_read[i] == value) {
                index = i;
                return index;
            }
        }
        return -1;
    }

    void set_process_message_write(std::size_t index, std::size_t value) {
        process_message_write[index] = value;
        return;
    }

    int get_index_process_message_write(std::size_t value) {
        std::size_t index = 0;
        for (auto i = 0u; i < process_message_write.size(); i++) {
            if (process_message_write[i] == value) {
                index = i;
                return index;
            }
        }
        return -1;
    }

    void set_process_message_write_2(std::size_t index, std::size_t value) {
        process_message_write_2[index] = value;
        return;
    }

    int get_index_process_message_write_2(std::size_t value) {
        std::size_t index = 0;
        for (auto i = 0u; i < process_message_write_2.size(); i++) {
            if (process_message_write_2[i] == value) {
                index = i;
                return index;
            }
        }
        return -1;
    }

    bool lock_write_set() {
        for (auto i = 0u; i < writeSet.size(); i++) {
            auto &writeKey = writeSet[i];
            auto tableId = writeKey.get_table_id();
            auto partitionId = writeKey.get_partition_id();
            auto table = db.find_table(tableId, partitionId);

            auto key = writeKey.get_key();
            std::atomic<uint64_t> &tid = table->search_metadata(key);
            bool success;
            uint64_t latestTid = SiloHelper::lock(tid, success);

            if (!success) {
                abort_lock = true;
                break;
            }

            writeKey.set_write_lock_bit();
            writeKey.set_tid(latestTid);

            max_tid = std::max(max_tid, latestTid);

            auto readKeyPtr = get_read_key(key, table);
            if (readKeyPtr != nullptr) {
                uint64_t tidOnRead = readKeyPtr->get_tid();
                if (latestTid != tidOnRead) {
                    abort_lock = true;
                    break;
                }
            }
        }
        return abort_lock;
    }

    bool validate_read_set() {
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

            uint64_t latest_tid = table->search_metadata(key).load();
            if (SiloHelper::remove_lock_bit(latest_tid) != tid) {
                abort_read_validation = true;
                break;
            }
            if (SiloHelper::is_locked(latest_tid)) {  // must be locked by others
                abort_read_validation = true;
                break;
            }
        }

        return abort_read_validation;
    }

    void write_local() {
        for (auto i = 0u; i < writeSet.size(); i++) {
            auto &writeKey = writeSet[i];
            auto tableId = writeKey.get_table_id();
            auto partitionId = writeKey.get_partition_id();
            auto table = db.find_table(tableId, partitionId);

            // local write
            if (partitioner.has_master_partition(partitionId)) {
                auto key = writeKey.get_key();
                auto value = writeKey.get_value();
                table->update(key, value);
            }
        }
    }

    void release_lock(uint64_t commit_tid) {
        for (auto i = 0u; i < writeSet.size(); i++) {
            auto &writeKey = writeSet[i];
            auto tableId = writeKey.get_table_id();
            auto partitionId = writeKey.get_partition_id();
            auto table = db.find_table(tableId, partitionId);

            // write
            if (partitioner.has_master_partition(partitionId)) {
                auto key = writeKey.get_key();
                auto value = writeKey.get_value();
                std::atomic<uint64_t> &tid = table->search_metadata(key);
                table->update(key, value);
                SiloHelper::unlock(tid, commit_tid);
            }
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
            if (partitioner.has_master_partition(partitionId)) {
                auto key = writeKey.get_key();
                std::atomic<uint64_t> &tid = table->search_metadata(key);
                SiloHelper::unlock(tid);
            }
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
    std::size_t network_size;
    bool abort_lock, abort_read_validation, local_validated, si_in_serializable;
    bool distributed_transaction;
    bool execution_phase;
    std::function<void(std::size_t, std::size_t, std::size_t, uint32_t)> create_sub_txn;
    std::function<void(std::size_t, std::size_t, std::size_t, uint64_t, uint64_t)>
        create_replicate_txn;
    std::function<void(std::size_t, std::size_t, const void *, void *, uint64_t)>
        readset_replicate_txn;
    std::function<void()> raft_replicate_txn;        
    std::function<void(std::size_t, std::size_t, const void *, void *)> writeset_replicate_txn;
    // table id, partition id, key, value, local index read?
    std::function<uint64_t(std::size_t, std::size_t, uint32_t, const void *, void *, bool,
                           uint32_t)>
        readRequestHandler;
    std::function<void(std::size_t, std::size_t, uint32_t, const void *, void *, uint32_t)>
        writeRequestHandler;
    // processed a request?
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
    std::unordered_map<std::size_t, bool> nodeSet;
    std::unordered_map<std::size_t, bool> replicateSet;
    std::list<StringPiece> stringPieces;
    uint64_t max_tid, cur_tid;
    std::size_t operation_count;
    bool readHandled;
    uint32_t redo;
    uint32_t scanreplica;
    uint64_t startTimeTxn;
    std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions;
    std::vector<std::size_t> process_message_read;  // the number is the key_offset/
    std::vector<std::size_t> process_message_write;
    std::vector<std::size_t> process_message_write_2;
    std::vector<std::size_t> last_coordinator_read;  // the last destination of a message.
    std::vector<std::size_t> last_coordinator_write;
    std::vector<std::size_t> last_coordinator_write_2;
    std::vector<bool> read_remote_validation;
    std::vector<bool> lock_remote;
    std::vector<bool> is_create_sub;
    std::vector<bool> remote_write;
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
