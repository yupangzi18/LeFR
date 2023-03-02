#pragma once

#include "core/Executor.h"
#include "protocol/LeFR/LeFR.h"

namespace lefr {
template <class Workload>
class LeFRExecutor : public Executor<Workload, LeFR<typename Workload::DatabaseType>>

{
public:
    using base_type = Executor<Workload, LeFR<typename Workload::DatabaseType>>;

    using WorkloadType = Workload;
    using ProtocolType = LeFR<typename Workload::DatabaseType>;
    using DatabaseType = typename WorkloadType::DatabaseType;
    using TransactionType = typename WorkloadType::TransactionType;
    using ContextType = typename DatabaseType::ContextType;
    using RandomType = typename DatabaseType::RandomType;
    using MessageType = typename ProtocolType::MessageType;
    using MessageFactoryType = typename ProtocolType::MessageFactoryType;
    using MessageHandlerType = typename ProtocolType::MessageHandlerType;

    using StorageType = typename WorkloadType::StorageType;

    LeFRExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db,
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

    ~LeFRExecutor() = default;

    void setupHandlers(TransactionType &txn) override {
        txn.create_sub_txn = [this, &txn](std::size_t dest_node_id, std::size_t coordinator_id,
                                          std::size_t partition_id, uint32_t round_count) {
            txn.network_size += MessageFactoryType::new_create_sub_txn_message(
                *(this->messages[dest_node_id]), coordinator_id, dest_node_id, partition_id,
                round_count);
            txn.pendingResponses++;
            txn.rw_message_count++;
            txn.distributed_transaction = true;
        };

        txn.create_replicate_txn =
            [this, &txn](std::size_t dest_node_id, std::size_t coordinator_id,
                         std::size_t partition_id, uint64_t txnseed, uint64_t txnstarttime) {
                if (this->context.protocol == "LeFR" && this->context.traditional_2pc == false) {
                    auto temp_network_size = MessageFactoryType::async_new_init_txn_message(
                        *(this->messages[dest_node_id]), 0, coordinator_id, partition_id, txnseed,
                        txnstarttime);
                    txn.network_size += 2 * temp_network_size;
                    txn.rw_message_count++;
                }
            };

        txn.readRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id,
                                              uint32_t key_offset, const void *key, void *value,
                                              bool local_index_read,
                                              uint32_t round_count) -> uint64_t {
            bool local_read = false;

            if (this->partitioner.has_master_partition(partition_id) ||
                (this->partitioner.is_partition_replicated_on(partition_id, this->coordinator_id) &&
                 this->context.read_on_replica)) {
                local_read = true;
            }

            if (local_index_read || local_read) {
                return this->protocol.search(table_id, partition_id, key, value);
            } else {
                ITable *table = this->db.find_table(table_id, partition_id);
                auto coordinatorID = this->partitioner.master_coordinator(partition_id);
                if (coordinatorID == this->partitioner.get_coordinator_id()) {
                    return 0;
                }
                txn.network_size += MessageFactoryType::new_search_message(
                    *(this->messages[coordinatorID]), *table, key, key_offset, round_count,
                    this->coordinator_id);
                txn.pendingResponses++;
                txn.rw_message_count++;
                txn.distributed_transaction = true;
                return 0;
            }
        };

        txn.readset_replicate_txn = [this, &txn](std::size_t table_id, std::size_t partition_id,
                                                 const void *key, void *value, uint64_t tid) {
            if (this->context.protocol == "LeFR" && this->context.traditional_2pc == false) {
                ITable *table = this->db.find_table(table_id, partition_id);
                for (auto i = 1u; i < txn.partitioner.replica_num(); i++) {
                    auto dest_node_id =
                        (txn.coordinator_id + i) % txn.partitioner.total_coordinators();
                    auto temp_network_size = MessageFactoryType::async_new_read_replication_message(
                        *(this->messages[dest_node_id]), *table, key, value, tid);
                    txn.network_size += 2 * temp_network_size;
                    txn.rw_message_count++;
                }
            }
        };
        txn.raft_replicate_txn = [this, &txn](){
            if (this->context.protocol == "LeFR" && this->context.traditional_2pc == false) {
                this->sender(txn);
            }
        };

        txn.writeRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id,
                                               uint32_t key_offset, const void *key, void *value,
                                               uint32_t round_count) {
            bool local_write = false;
            if (this->partitioner.has_master_partition(partition_id)) {
                local_write = true;
            }
            if (!local_write) {
                ITable *table = this->db.find_table(table_id, partition_id);
                auto coordinatorID = this->partitioner.master_coordinator(partition_id);
                if (coordinatorID == this->partitioner.get_coordinator_id()) {
                    return;
                }
                txn.network_size += MessageFactoryType::new_add_remote_wset_message(
                    *(this->messages[coordinatorID]), *table, key, value, key_offset, round_count,
                    this->coordinator_id);
                txn.pendingResponses++;
                txn.rw_message_count++;
                txn.distributed_transaction = true;
            }
        };

        txn.writeset_replicate_txn = [this, &txn](std::size_t table_id, std::size_t partition_id,
                                                  const void *key, void *value) {
            if (this->context.protocol == "LeFR" && this->context.traditional_2pc == false) {
                ITable *table = this->db.find_table(table_id, partition_id);
                for (auto i = 1u; i < txn.partitioner.replica_num(); i++) {
                    auto dest_node_id =
                        (txn.coordinator_id + i) % txn.partitioner.total_coordinators();
                    auto temp_network_size =
                        MessageFactoryType::async_new_write_replication_message(
                            *(this->messages[dest_node_id]), *table, key, value);
                    txn.network_size += 2 * temp_network_size;
                    txn.rw_message_count += 2;
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
