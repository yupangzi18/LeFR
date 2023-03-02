#pragma once

#include <unordered_map>
#include <unordered_set>

#include "benchmark/tpcc/Workload.h"
#include "benchmark/ycsb/Workload.h"
#include "core/Checker.h"
#include "core/Rafter.h"
#include "core/Defs.h"
#include "core/Executor.h"
#include "core/Manager.h"
#include "core/group_commit/Executor.h"
#include "core/group_commit/Manager.h"
#include "protocol/GPAC/GPAC.h"
#include "protocol/GPAC/GPACExecutor.h"
#include "protocol/LeFR/LeFR.h"
#include "protocol/LeFR/LeFRExecutor.h"
#include "protocol/Tapir/Tapir.h"
#include "protocol/Tapir/TapirExecutor.h"

namespace lefr {

template <class Context>
class InferType {};

template <>
class InferType<lefr::tpcc::Context> {
public:
    template <class Transaction>
    using WorkloadType = lefr::tpcc::Workload<Transaction>;
};

template <>
class InferType<lefr::ycsb::Context> {
public:
    template <class Transaction>
    using WorkloadType = lefr::ycsb::Workload<Transaction>;
};

class WorkerFactory {
public:
    template <class Database, class Context>
    static std::vector<std::shared_ptr<Worker>> create_workers(
        std::size_t coordinator_id, Database &db, const Context &context, Partitioner &partitioner,
        std::atomic<bool> &stop_flag, std::atomic<bool> &fault_flag,
        std::atomic<bool> &transfer_flag, std::atomic<bool> &recovery_flag,
        std::atomic<bool> &coordinator_fault_flag, std::atomic<uint32_t> &n_committing_txns,
        std::atomic<uint32_t> &n_redo_txns,
        std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions,
        std::unordered_set<uint32_t> &faultNodes,
        std::unordered_map<uint32_t, uint32_t> &faultMap, 
        std::queue<uint64_t> &txnqueue) {
        std::unordered_set<std::string> protocols = {"LeFR", "Tapir", "GPAC"};

        CHECK(protocols.count(context.protocol) == 1);

        std::vector<std::shared_ptr<Worker>> workers;

        if (context.protocol == "LeFR") {
            using TransactionType = lefr::LeFRTransaction<Database>;
            using WorkloadType =
                typename InferType<Context>::template WorkloadType<TransactionType>;

            auto manager =
                std::make_shared<Manager>(coordinator_id, context.worker_num, context, stop_flag);

            auto checker = std::make_shared<Checker>(
                coordinator_id, context.worker_num + 1, context, partitioner, stop_flag,
                transfer_flag, recovery_flag, n_committing_txns, faultNodes, faultMap);
            
            auto rafter = 
                std::make_shared<Rafter>(coordinator_id, context.worker_num, context, stop_flag, txnqueue, 0);

            for (auto i = 0u; i < context.worker_num; i++) {
                workers.push_back(std::make_shared<LeFRExecutor<WorkloadType>>(
                    coordinator_id, i, db, context, partitioner, manager->worker_status,
                    manager->n_completed_workers, manager->n_started_workers, fault_flag,
                    transfer_flag, recovery_flag, coordinator_fault_flag, n_committing_txns,
                    n_redo_txns, fault_partitions, faultMap, txnqueue));
            }

            

            workers.push_back(manager);
            workers.push_back(rafter);
            workers.push_back(checker);

        } else if (context.protocol == "Tapir") {
            using TransactionType = lefr::TapirTransaction<Database>;
            using WorkloadType =
                typename InferType<Context>::template WorkloadType<TransactionType>;

            auto manager =
                std::make_shared<Manager>(coordinator_id, context.worker_num, context, stop_flag);

            for (auto i = 0u; i < context.worker_num; i++) {
                workers.push_back(std::make_shared<TapirExecutor<WorkloadType>>(
                    coordinator_id, i, db, context, partitioner, manager->worker_status,
                    manager->n_completed_workers, manager->n_started_workers, fault_flag,
                    transfer_flag, recovery_flag, coordinator_fault_flag, n_committing_txns,
                    n_redo_txns, fault_partitions, faultMap, txnqueue));
            }

            workers.push_back(manager);
        } else if (context.protocol == "GPAC") {
            using TransactionType = lefr::GPACTransaction<Database>;
            using WorkloadType =
                typename InferType<Context>::template WorkloadType<TransactionType>;

            auto manager =
                std::make_shared<Manager>(coordinator_id, context.worker_num, context, stop_flag);

            for (auto i = 0u; i < context.worker_num; i++) {
                workers.push_back(std::make_shared<GPACExecutor<WorkloadType>>(
                    coordinator_id, i, db, context, partitioner, manager->worker_status,
                    manager->n_completed_workers, manager->n_started_workers, fault_flag,
                    transfer_flag, recovery_flag, coordinator_fault_flag, n_committing_txns,
                    n_redo_txns, fault_partitions, faultMap, txnqueue));
            }

            workers.push_back(manager);
        }

        return workers;
    }
};
}  // namespace lefr