#pragma once

#include "core/Executor.h"
#include "protocol/GPAC/GPAC.h"

namespace lefr {
template <class Workload>
class GPACExecutor : public Executor<Workload, GPAC<typename Workload::DatabaseType>>

{
public:
    using base_type = Executor<Workload, GPAC<typename Workload::DatabaseType>>;

    using WorkloadType = Workload;
    using ProtocolType = GPAC<typename Workload::DatabaseType>;
    using DatabaseType = typename WorkloadType::DatabaseType;
    using TransactionType = typename WorkloadType::TransactionType;
    using ContextType = typename DatabaseType::ContextType;
    using RandomType = typename DatabaseType::RandomType;
    using MessageType = typename ProtocolType::MessageType;
    using MessageFactoryType = typename ProtocolType::MessageFactoryType;
    using MessageHandlerType = typename ProtocolType::MessageHandlerType;

    using StorageType = typename WorkloadType::StorageType;

    GPACExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
                 const ContextType &context, Partitioner &partitioner,
                 std::atomic<uint32_t> &worker_status, std::atomic<uint32_t> &n_complete_workers,
                 std::atomic<uint32_t> &n_started_workers, std::atomic<bool> &faultFlag,
                 std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
                 std::atomic<bool> &coordinatorFaultFlag, std::atomic<uint32_t> &n_committing_txns,
                 std::atomic<uint32_t> &n_redo_txns,
                 std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions,
                 std::unordered_map<uint32_t, uint32_t> &faultMap, 
                 std::queue<uint64_t> &txnqueue)
        : base_type(coordinator_id, id, db, context, partitioner, worker_status, n_complete_workers,
                    n_started_workers, faultFlag, transferFlag, recoveryFlag, coordinatorFaultFlag,
                    n_committing_txns, n_redo_txns, fault_partitions, faultMap, txnqueue) {}

    ~GPACExecutor() = default;

    void setupHandlers(TransactionType &txn) override {
        txn.create_sub_txn = [this, &txn](std::size_t dest_node_id, std::size_t coordinator_id,
                                          std::size_t partition_id, uint32_t round_count) {
            auto temp_network_size = MessageFactoryType::new_create_sub_txn_message(
                *(this->messages[dest_node_id]), coordinator_id, dest_node_id, partition_id,
                round_count);
            txn.network_size += temp_network_size;
            txn.rw_network_size += temp_network_size;
            txn.rw_message_count++;
            txn.distributed_transaction = true;
        };

        txn.readRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id,
                                              uint32_t key_offset, const void *key, void *value,
                                              bool local_index_read,
                                              uint32_t round_count) -> uint64_t {
            bool local_read = false;
            uint64_t result_tid = 0;
            auto temp_network_size = 0u;

            if (this->partitioner.is_partition_replicated_on(partition_id, this->coordinator_id)) {
                local_read = true;
                result_tid = this->protocol.search(table_id, partition_id, key, value);
            }
            ITable *table = this->db.find_table(table_id, partition_id);
            for (auto k = 0u; k < this->context.coordinator_num; k++) {
                if (this->partitioner.is_partition_replicated_on(partition_id, k) &&
                    k != this->coordinator_id) {
                    temp_network_size = MessageFactoryType::new_search_message(
                        *(this->messages[k]), *table, key, key_offset, round_count,
                        this->coordinator_id);
                    txn.network_size += temp_network_size;
                    txn.pr_network_size += 2 * temp_network_size;
                    txn.pr_message_count += 2;
                }
            }

            if (local_index_read || local_read) {
                return result_tid;
            } else {
                txn.rw_network_size += temp_network_size;
                txn.rw_message_count++;
            }

            return 0;
        };

        txn.writeRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id,
                                               uint32_t key_offset, const void *key, void *value,
                                               uint32_t round_count) {
            ITable *table = this->db.find_table(table_id, partition_id);
            for (auto k = 0u; k < this->context.coordinator_num; k++) {
                if (this->partitioner.is_partition_replicated_on(partition_id, k) &&
                    k != this->coordinator_id) {
                    auto temp_network_size = MessageFactoryType::new_add_remote_wset_message(
                        *(this->messages[k]), *table, key, value, key_offset, round_count,
                        this->coordinator_id);
                    txn.network_size += temp_network_size;
                    txn.pr_network_size += temp_network_size;
                    txn.pr_message_count++;
                }
            }
        };

        txn.remote_request_handler = [this]() { return this->process_request(); };
        txn.message_flusher = [this]() { this->flush_messages(); };

        txn.replicate_transaction_info =
            [this, &txn](Message &message, uint32_t type, std::size_t coordinator_id,
                         std::size_t partition_id, uint64_t seed, uint64_t startTime) {
                txn.pendingResponses++;
                txn.network_size += MessageFactoryType::new_init_txn_message(
                    message, type, coordinator_id, partition_id, seed, startTime);
            };

        txn.replicate_read_operation = [&txn](Message &message, ITable &table, const void *key,
                                              const void *value, uint64_t tid) {
            txn.pendingResponses++;
            txn.network_size +=
                MessageFactoryType::new_read_replication_message(message, table, key, value, tid);
        };

        txn.replicate_write_operation = [&txn](Message &message, ITable &table, const void *key,
                                               const void *value) {
            txn.pendingResponses++;
            txn.network_size +=
                MessageFactoryType::new_write_replication_message(message, table, key, value);
        };

        txn.send_redo_signal = [&txn](Message &message, uint32_t redo) {
            txn.network_size += MessageFactoryType::new_redo_message(message, redo);
        };

        txn.send_scanreplica_signal = [&txn](Message &message, uint32_t scanreplica,
                                             uint64_t startTime) {
            txn.network_size +=
                MessageFactoryType::new_scanreplica_message(message, scanreplica, startTime);
        };
    };
};
}  // namespace lefr
