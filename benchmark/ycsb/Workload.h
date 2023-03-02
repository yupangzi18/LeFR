#pragma once

#include <stdlib.h>

#include "benchmark/tpcc/Context.h"
#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Storage.h"
#include "benchmark/ycsb/Transaction.h"
#include "core/Partitioner.h"

namespace lefr {

namespace ycsb {

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
        if (context.workload_type == "long_txn") {
            std::unique_ptr<TransactionType> p = std::make_unique<ScanAll<Transaction>>(
                coordinator_id, partition_id, db, context, random, partitioner, storage,
                transferFlag, recoveryFlag, fault_partitions);
            return p;

        } else if (context.workload_type == "hybrid_txn") {
            if (rand() % 20 + 1 == 1) {
                std::unique_ptr<TransactionType> p = std::make_unique<ScanAll<Transaction>>(
                    coordinator_id, partition_id, db, context, random, partitioner, storage,
                    transferFlag, recoveryFlag, fault_partitions);
                return p;
            } else {
                std::unique_ptr<TransactionType> p = std::make_unique<ReadModifyWrite<Transaction>>(
                    coordinator_id, partition_id, db, context, random, partitioner, storage,
                    transferFlag, recoveryFlag, fault_partitions);
                return p;
            }

        } else {
            std::unique_ptr<TransactionType> p = std::make_unique<ReadModifyWrite<Transaction>>(
                coordinator_id, partition_id, db, context, random, partitioner, storage,
                transferFlag, recoveryFlag, fault_partitions);
            return p;
        }
    }

    std::unique_ptr<TransactionType> get_replicate_transaction(
        uint32_t type, const ContextType &context, std::size_t partition_id, StorageType &storage,
        std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
        std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions) {
        std::unique_ptr<TransactionType> p = std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner, storage, transferFlag,
            recoveryFlag, fault_partitions);

        return p;
    }

    std::unique_ptr<TransactionType> get_scanReplica_transaction(
        const ContextType &context, std::size_t partition_id, StorageType &storage,
        std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
        std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions) {
        std::unique_ptr<TransactionType> p = std::make_unique<ScanReplica<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner, storage, transferFlag,
            recoveryFlag, fault_partitions);

        return p;
    }

    std::unique_ptr<TransactionType> get_scanAll_transaction(
        const ContextType &context, std::size_t partition_id, StorageType &storage,
        std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
        std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions) {
        std::unique_ptr<TransactionType> p = std::make_unique<ScanAll<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner, storage, transferFlag,
            recoveryFlag, fault_partitions);

        return p;
    }

    std::unique_ptr<TransactionType> next_sub_transaction(
        const ContextType &context, std::size_t partition_id, StorageType &storage,
        std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
        std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions) {
        std::unique_ptr<TransactionType> p = std::make_unique<ReadModifyWrite<Transaction>>(
            coordinator_id, partition_id, db, context, random, partitioner, storage, transferFlag,
            recoveryFlag, fault_partitions);
        return p;
    }

private:
    std::size_t coordinator_id;
    DatabaseType &db;
    RandomType &random;
    Partitioner &partitioner;
    bool readonly;
};

}  // namespace ycsb
}  // namespace lefr
