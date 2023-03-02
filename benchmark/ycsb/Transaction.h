#pragma once

#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Query.h"
#include "benchmark/ycsb/Schema.h"
#include "benchmark/ycsb/Storage.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "butil/logging.h"

namespace lefr {
namespace ycsb {

enum class YCSBTransactionType { RMW, SCAN_ALL, NFIELDS };

template <class Transaction>
class ReadModifyWrite : public Transaction {
public:
    using DatabaseType = Database;
    using ContextType = typename DatabaseType::ContextType;
    using RandomType = typename DatabaseType::RandomType;
    using StorageType = Storage;

    static constexpr std::size_t keys_num = 10;

    ReadModifyWrite(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db,
                    const ContextType &context, RandomType &random, Partitioner &partitioner,
                    Storage &storage, std::atomic<bool> &transferFlag,
                    std::atomic<bool> &recoveryFlag,
                    std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions)
        : Transaction(coordinator_id, partition_id, db, partitioner, transferFlag, recoveryFlag,
                      fault_partitions),
          context(context),
          random(random),
          storage(storage),
          partition_id(partition_id),
          query(makeYCSBQuery<keys_num>()(context, partition_id, random)) {}

    virtual ~ReadModifyWrite() override = default;

    TransactionResult execute(std::size_t worker_id,
                              std::vector<std::unique_ptr<Message>> &messages,
                              std::atomic<bool> &faultFlag,
                              std::atomic<uint32_t> &n_committing_txns,
                              std::chrono::steady_clock::time_point workerStartTime,
                              std::atomic<uint32_t> &n_redo_txns) override {
        this->workerStartTime = workerStartTime;
        this->is_longtxn = false;

        // block new txns before finishing redo in new master
        if (context.protocol == "LeFR") {
            while (this->transferFlag.load()) {
                if (context.traditional_2pc == false) {
                    this->during_fault = true;
                }
                this->remote_request_handler();
            }
            while (this->recoveryFlag.load()) {
                if (context.traditional_2pc == false) {
                    this->during_fault = true;
                }
                this->remote_request_handler();
            }
        }
        this->status = TransactionStatus::INPROGRESS;

        DCHECK(context.keysPerTransaction == keys_num);

        int ycsbTableID = ycsb::tableID;

        for (auto i = 0u; i < keys_num; i++) {
            auto key = query.Y_KEY[i];
            storage.ycsb_keys[i].Y_KEY = key;
            if (query.UPDATE[i]) {
                this->search_for_update(ycsbTableID, context.getPartitionID(key),
                                        storage.ycsb_keys[i], storage.ycsb_values[i]);
            } else {
                this->search_for_read(ycsbTableID, context.getPartitionID(key),
                                      storage.ycsb_keys[i], storage.ycsb_values[i]);
            }
        }

        for (auto i = 0u; i < keys_num; i++) {
            auto key = query.Y_KEY[i];
            if (query.UPDATE[i]) {
                if (this->execution_phase) {
                    RandomType local_random;
                    storage.ycsb_values[i].Y_F01.assign(
                        local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                    storage.ycsb_values[i].Y_F02.assign(
                        local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                    storage.ycsb_values[i].Y_F03.assign(
                        local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                    storage.ycsb_values[i].Y_F04.assign(
                        local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                    storage.ycsb_values[i].Y_F05.assign(
                        local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                    storage.ycsb_values[i].Y_F06.assign(
                        local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                    storage.ycsb_values[i].Y_F07.assign(
                        local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                    storage.ycsb_values[i].Y_F08.assign(
                        local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                    storage.ycsb_values[i].Y_F09.assign(
                        local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                    storage.ycsb_values[i].Y_F10.assign(
                        local_random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
                }
                this->update(ycsbTableID, context.getPartitionID(key), storage.ycsb_keys[i],
                             storage.ycsb_values[i]);
            }
        }

        if (this->execution_phase && context.nop_prob > 0) {
            auto x = random.uniform_dist(1, 10000);
            if (x <= context.nop_prob) {
                for (auto i = 0u; i < context.n_nop; i++) {
                    asm("nop");
                }
            }
        }
        if (this->process_requests(worker_id)) {
            LOG(INFO) << "Abort";
            return TransactionResult::ABORT;
        }
        this->readHandled = true;

        if (context.protocol == "LeFR") {
            if (this->transferFlag.load() || this->recoveryFlag.load()) {
                while (this->transferFlag.load()) {
                    if (context.traditional_2pc == false) {
                        this->during_fault = true;
                    }
                    this->remote_request_handler();
                }
                while (this->recoveryFlag.load()) {
                    if (context.traditional_2pc == false) {
                        this->during_fault = true;
                    }
                    this->remote_request_handler();
                }
                if (context.traditional_2pc == true) {
                    return TransactionResult::REDO;
                }
            }
        }

        this->status = TransactionStatus::READY_TO_COMMIT;
        return TransactionResult::READY_TO_COMMIT;
    }

    void reset_query() override {
        query = makeYCSBQuery<keys_num>()(context, partition_id, random);
    }

private:
    const ContextType &context;
    RandomType &random;
    Storage &storage;
    std::size_t partition_id;
    YCSBQuery<keys_num> query;
};

template <class Transaction>
class ScanAll : public Transaction {
public:
    using DatabaseType = Database;
    using ContextType = typename DatabaseType::ContextType;
    using RandomType = typename DatabaseType::RandomType;
    using StorageType = Storage;

    static constexpr std::size_t keys_num = 10;

    ScanAll(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db,
            const ContextType &context, RandomType &random, Partitioner &partitioner,
            Storage &storage, std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
            std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions)
        : Transaction(coordinator_id, partition_id, db, partitioner, transferFlag, recoveryFlag,
                      fault_partitions),
          db(db),
          context(context),
          random(random),
          storage(storage),
          partition_id(partition_id),
          query(makeYCSBQuery<keys_num>()(context, partition_id, random)) {}

    virtual ~ScanAll() override = default;

    TransactionResult execute(std::size_t worker_id,
                              std::vector<std::unique_ptr<Message>> &messages,
                              std::atomic<bool> &faultFlag,
                              std::atomic<uint32_t> &n_committing_txns,
                              std::chrono::steady_clock::time_point workerStartTime,
                              std::atomic<uint32_t> &n_redo_txns) override {
        this->workerStartTime = workerStartTime;
        this->is_longtxn = true;

        // block new txns before finishing redo in new master
        if (context.protocol == "LeFR") {
            while (this->transferFlag.load()) {
                if (context.traditional_2pc == false) {
                    this->during_fault = true;
                }
                this->remote_request_handler();
            }
            while (this->recoveryFlag.load()) {
                if (context.traditional_2pc == false) {
                    this->during_fault = true;
                }
                this->remote_request_handler();
            }
        }
        this->status = TransactionStatus::INPROGRESS;

        int ycsbTableID = ycsb::tableID;

        std::size_t message_count = 0;
        for (auto k = 0u; k < 100; k++) {
            for (auto i = 0u; i < keys_num; i++) {
                auto key = query.Y_KEY[i];
                storage.ycsb_keys[i].Y_KEY = key;
                this->search_for_update(ycsbTableID, context.getPartitionID(key),
                                        storage.ycsb_keys[i], storage.ycsb_values[i]);
                message_count++;

                if (message_count % 1000 == 0) {
                    if (this->process_requests(worker_id)) {
                        return TransactionResult::ABORT;
                    }
                }
                if (message_count % 100 == 0) {
                    if (context.protocol == "LeFR") {
                        if (this->transferFlag.load() || this->recoveryFlag.load()) {
                            while (this->transferFlag.load()) {
                                if (context.traditional_2pc == false) {
                                    this->during_fault = true;
                                }
                                this->remote_request_handler();
                            }
                            while (this->recoveryFlag.load()) {
                                if (context.traditional_2pc == false) {
                                    this->during_fault = true;
                                }
                                this->remote_request_handler();
                            }
                            if (context.traditional_2pc == true) {
                                this->process_requests(worker_id);
                                return TransactionResult::REDO;
                            }
                        }
                    }
                }
            }

            if (this->process_requests(worker_id)) {
                return TransactionResult::ABORT;
            }
        }

        this->status = TransactionStatus::READY_TO_COMMIT;
        return TransactionResult::READY_TO_COMMIT;
    }

    void reset_query() override {
        query = makeYCSBQuery<keys_num>()(context, partition_id, random);
    }

private:
    DatabaseType &db;
    const ContextType &context;
    RandomType &random;
    Storage &storage;
    std::size_t partition_id;
    YCSBQuery<keys_num> query;
};

template <class Transaction>
class ScanReplica : public Transaction {
public:
    using DatabaseType = Database;
    using ContextType = typename DatabaseType::ContextType;
    using RandomType = typename DatabaseType::RandomType;
    using StorageType = Storage;

    static constexpr std::size_t keys_num = 10;

    ScanReplica(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db,
                const ContextType &context, RandomType &random, Partitioner &partitioner,
                Storage &storage, std::atomic<bool> &transferFlag, std::atomic<bool> &recoveryFlag,
                std::unordered_map<uint32_t, std::unordered_set<uint32_t>> &fault_partitions)
        : Transaction(coordinator_id, partition_id, db, partitioner, transferFlag, recoveryFlag,
                      fault_partitions),
          context(context),
          random(random),
          storage(storage),
          partition_id(partition_id),
          query(makeYCSBQuery<keys_num>()(context, partition_id, random)) {}

    virtual ~ScanReplica() override = default;

    TransactionResult execute(std::size_t worker_id,
                              std::vector<std::unique_ptr<Message>> &messages,
                              std::atomic<bool> &faultFlag,
                              std::atomic<uint32_t> &n_committing_txns,
                              std::chrono::steady_clock::time_point workerStartTime,
                              std::atomic<uint32_t> &n_redo_txns) override {
        this->workerStartTime = workerStartTime;

        // block new txns before finishing redo in new master
        if (context.protocol == "LeFR") {
            while (this->transferFlag.load()) {
                if (context.traditional_2pc == false) {
                    this->during_fault = true;
                }
                this->remote_request_handler();
            }
            while (this->recoveryFlag.load()) {
                if (context.traditional_2pc == false) {
                    this->during_fault = true;
                }
                this->remote_request_handler();
            }
        }
        this->status = TransactionStatus::INPROGRESS;

        int ycsbTableID = ycsb::tableID;
        std::size_t keysPerPartition = context.keysPerPartition;
        std::size_t partitionNum = context.partition_num;
        std::size_t totalKeys = keysPerPartition * partitionNum;
        std::size_t message_count = 0;

        for (std::size_t partitionID = 0; partitionID < partitionNum; partitionID++) {
            if (partitionID % context.coordinator_num == context.coordinator_id - 1) {
                ITable *table = this->db.find_table(ycsbTableID, partitionID);

                for (auto i = partitionID * keysPerPartition;
                     i < (partitionID + 1) * keysPerPartition; i++) {
                    DCHECK(context.getPartitionID(i) == partitionID);

                    ycsb::key key(i);
                    ycsb::value value;

                    this->search_for_read(ycsbTableID, context.getPartitionID(i), key, value);
                    message_count++;
                    if (message_count % 1000 == 0) {
                        if (this->process_requests(worker_id)) {
                            return TransactionResult::ABORT;
                        }
                    }
                }
                if (this->process_requests(worker_id)) {
                    return TransactionResult::ABORT;
                }
            }
        }

        this->status = TransactionStatus::READY_TO_COMMIT;
        return TransactionResult::READY_TO_COMMIT;
    }

    void reset_query() override {
        query = makeYCSBQuery<keys_num>()(context, partition_id, random);
    }

private:
    const ContextType &context;
    RandomType &random;
    Storage &storage;
    std::size_t partition_id;
    YCSBQuery<keys_num> query;
};

}  // namespace ycsb

}  // namespace lefr
