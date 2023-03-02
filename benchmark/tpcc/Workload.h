#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Storage.h"
#include "benchmark/tpcc/Transaction.h"
#include "core/Partitioner.h"

namespace lefr {

namespace tpcc {

template <class Transaction>
class Workload {
public:
    using TransactionType = Transaction;
    using DatabaseType = Database;
    using ContextType = Context;
    using RandomType = Random;
    using StorageType = Storage;

    Workload(std::size_t coordinator_id, DatabaseType &db, RandomType &random,
             Partitioner &partitioner)
        : coordinator_id(coordinator_id), db(db), random(random), partitioner(partitioner) {}

    std::unique_ptr<TransactionType> next_transaction(
        const ContextType &context, std::size_t partition_id, StorageType &storage,
        std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
        std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions) {
        int x = random.uniform_dist(1, 100);
        std::unique_ptr<TransactionType> p;

        if (context.workload_type == "hybrid_txn") {
            if (x <= 98) {
                p = std::make_unique<NewOrder<Transaction>>(
                    coordinator_id, partition_id, db, context, random, partitioner, storage,
                    transferFlag, recoveryFlag, fault_partitions);
            } else {
                p = std::make_unique<ScanOrder<Transaction>>(
                    coordinator_id, partition_id, db, context, random, partitioner, storage,
                    transferFlag, recoveryFlag, fault_partitions);
            }
        } else if (context.workload_type == "short_txn") {
            if (context.workloadType == TPCCWorkloadType::NEW_ORDER_ONLY) {
                p = std::make_unique<NewOrder<Transaction>>(
                    coordinator_id, partition_id, db, context, random, partitioner, storage,
                    transferFlag, recoveryFlag, fault_partitions);
            } else if (context.workloadType == TPCCWorkloadType::PAYMENT_ONLY) {
                p = std::make_unique<Payment<Transaction>>(
                    coordinator_id, partition_id, db, context, random, partitioner, storage,
                    transferFlag, recoveryFlag, fault_partitions);
            }
        } else {
            p = std::make_unique<ScanOrder<Transaction>>(coordinator_id, partition_id, db, context,
                                                         random, partitioner, storage, transferFlag,
                                                         recoveryFlag, fault_partitions);
        }

        return p;
    }

    std::unique_ptr<TransactionType> get_replicate_transaction(
        uint32_t type, const ContextType &context, std::size_t partition_id, StorageType &storage,
        std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
        std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions) {
        std::unique_ptr<TransactionType> p;

        if (type == static_cast<uint32_t>(TPCCTransactionType::NEW_ORDER)) {
            p = std::make_unique<NewOrder<Transaction>>(coordinator_id, partition_id, db, context,
                                                        random, partitioner, storage, transferFlag,
                                                        recoveryFlag, fault_partitions);
        } else if (type == static_cast<uint32_t>(TPCCTransactionType::PAYMENT)) {
            p = std::make_unique<Payment<Transaction>>(coordinator_id, partition_id, db, context,
                                                       random, partitioner, storage, transferFlag,
                                                       recoveryFlag, fault_partitions);
        }

        return p;
    }

    std::unique_ptr<TransactionType> get_scanReplica_transaction(
        const ContextType &context, std::size_t partition_id, StorageType &storage,
        std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
        std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions) {
        std::unique_ptr<TransactionType> p = std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner, storage, transferFlag,
            recoveryFlag, fault_partitions);

        return p;
    }
    std::unique_ptr<TransactionType> get_scanAll_transaction(
        const ContextType &context, std::size_t partition_id, StorageType &storage,
        std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
        std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions) {
        std::unique_ptr<TransactionType> p = std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner, storage, transferFlag,
            recoveryFlag, fault_partitions);

        return p;
    }

    std::unique_ptr<TransactionType> next_sub_transaction(
        const ContextType &context, std::size_t partition_id, StorageType &storage,
        std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
        std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions) {
        std::unique_ptr<TransactionType> p = std::make_unique<NewOrder<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner, storage, transferFlag,
            recoveryFlag, fault_partitions);
        return p;
    }

private:
    std::size_t coordinator_id;
    DatabaseType &db;
    RandomType &random;
    Partitioner &partitioner;
};

}  // namespace tpcc
}  // namespace lefr
