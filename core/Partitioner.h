#pragma once

#include <butil/logging.h>

#include <memory>
#include <numeric>
#include <string>

namespace lefr {

class Partitioner {
public:
    Partitioner(std::size_t coordinator_id, std::size_t coordinator_num,
                std::size_t partition_num) {
        DCHECK(coordinator_id < coordinator_num);
        this->coordinator_id = coordinator_id;
        this->coordinator_num = coordinator_num;
        this->partition_num = partition_num;
    }

    virtual ~Partitioner() = default;

    std::size_t total_coordinators() const { return coordinator_num; }

    virtual std::size_t replica_num() const = 0;

    virtual bool is_replicated() const = 0;

    virtual bool has_master_partition(std::size_t partition_id) const = 0;

    virtual std::size_t master_coordinator(std::size_t partition_id) const = 0;

    virtual bool is_partition_replicated_on(std::size_t partition_id,
                                            std::size_t coordinator_id) const = 0;

    bool is_partition_replicated_on_me(std::size_t partition_id) const {
        return is_partition_replicated_on(partition_id, coordinator_id);
    }

    virtual bool is_backup() const = 0;

    virtual void transfer_master_coordinator(std::size_t partition_id, std::size_t coordinator_id) {
    }

    virtual void transfer_master_coordinators(std::size_t new_coordinator) {}

    virtual void recovery_master_coordinators() {}

    void set_coordinator_fault(std::size_t coordinator_id) {
        coordinator_falut[coordinator_id] = true;
    }

    void set_coordinator_recovery(std::size_t coordinator_id) {
        if (coordinator_falut[coordinator_id]) {
            coordinator_falut[coordinator_id] = false;
        }
    }

    bool get_coordinator_fault(std::size_t coordinator_id) {
        return coordinator_falut[coordinator_id];
    }

    std::size_t get_coordinator_id() { return coordinator_id; }

    bool has_any_master_partition() {
        for (auto partition_id = 0u; partition_id < partition_num; partition_id++) {
            if (has_master_partition(partition_id)) {
                return true;
            }
        }
        return false;
    }

    std::size_t get_partition_num() { return partition_num; }

    bool get_coordinator_fault_id(std::size_t coordinator_id) {
        return coordinator_falut[coordinator_id];
    }

    bool finished_transfer() {
        for (auto partition_id = 0u; partition_id < partition_num; partition_id++) {
            if (coordinator_falut[coordinator_id] &&
                master_coordinator(partition_id) == coordinator_id) {
                return false;
            }
        }
        return true;
    }

    bool finished_recovery() {
        for (auto partition_id = 0u; partition_id < partition_num; partition_id++) {
            if (partition_id % coordinator_num != master_coordinator(partition_id)) {
                return false;
            }
        }
        return true;
    }

    bool is_partition_relock(std::size_t partition_id) { return partition_relock[partition_id]; }

    void finish_partition_relock(std::size_t partition_id) {
        partition_relock[partition_id] = false;
    }

protected:
    std::size_t coordinator_id;
    std::size_t coordinator_num;
    std::size_t partition_num;
    std::vector<std::size_t> master_coordinators;
    std::vector<bool> coordinator_falut;
    std::vector<bool> partition_relock;
};

/*
 * N is the total number of replicas.
 * N is always larger than 0.
 * The N coordinators from the master coordinator have the replication for a
 * given partition.
 */

template <std::size_t N>
class HashReplicatedPartitioner : public Partitioner {
public:
    HashReplicatedPartitioner(std::size_t coordinator_id, std::size_t coordinator_num,
                              std::size_t partition_num)
        : Partitioner(coordinator_id, coordinator_num, partition_num) {
        CHECK(N > 0 && N <= coordinator_num);
        for (auto partition_id = 0u; partition_id < partition_num; partition_id++) {
            master_coordinators.push_back(partition_id % coordinator_num);
            partition_relock.push_back(false);
        }
        for (auto coordinator_id = 0u; coordinator_id < coordinator_num; coordinator_id++) {
            coordinator_falut.push_back(false);
        }
    }

    ~HashReplicatedPartitioner() override = default;

    std::size_t replica_num() const override { return N; }

    bool is_replicated() const override { return N > 1; }

    bool has_master_partition(std::size_t partition_id) const override {
        return master_coordinator(partition_id) == coordinator_id;
    }

    std::size_t master_coordinator(std::size_t partition_id) const override {
        return master_coordinators[partition_id];
    }

    bool is_partition_replicated_on(std::size_t partition_id,
                                    std::size_t coordinator_id) const override {
        DCHECK(coordinator_id < coordinator_num);

        std::size_t first_replica = partition_id % coordinator_num;
        std::size_t last_replica = (first_replica + N - 1) % coordinator_num;

        if (last_replica >= first_replica) {
            return first_replica <= coordinator_id && coordinator_id <= last_replica;
        } else {
            return coordinator_id >= first_replica || coordinator_id <= last_replica;
        }
    }

    bool is_backup() const override { return false; }

    void transfer_master_coordinator(std::size_t partition_id,
                                     std::size_t coordinator_id) override {
        LOG(INFO) << "partition " << partition_id << ": master from "
                  << master_coordinators[partition_id] << " to " << coordinator_id;
        master_coordinators[partition_id] = coordinator_id;
        partition_relock[partition_id] = true;
    }

    void transfer_master_coordinators(std::size_t new_coordinator) override {
        for (auto partition_id = 0u; partition_id < partition_num; partition_id++) {
            if (coordinator_falut[partition_id % coordinator_num]) {
                transfer_master_coordinator(partition_id, new_coordinator);
            }
        }
    }

    void recovery_master_coordinators() override {
        for (auto partition_id = 0u; partition_id < partition_num; partition_id++) {
            if (master_coordinator(partition_id) != partition_id % coordinator_num) {
                auto new_coordinator = partition_id % coordinator_num;
                transfer_master_coordinator(partition_id, new_coordinator);
            }
        }
    }
};

using HashPartitioner = HashReplicatedPartitioner<1>;

class PrimaryBackupPartitioner : public Partitioner {
public:
    PrimaryBackupPartitioner(std::size_t coordinator_id, std::size_t coordinator_num,
                             std::size_t partition_num)
        : Partitioner(coordinator_id, coordinator_num, partition_num) {
        CHECK(coordinator_num == 2);
    }

    ~PrimaryBackupPartitioner() override = default;

    std::size_t replica_num() const override { return 2; }

    bool is_replicated() const override { return true; }

    bool has_master_partition(std::size_t partition_id) const override {
        return coordinator_id == 0;
    }

    std::size_t master_coordinator(std::size_t partition_id) const override { return 0; }

    bool is_partition_replicated_on(std::size_t partition_id,
                                    std::size_t coordinator_id) const override {
        DCHECK(coordinator_id < coordinator_num);
        return true;
    }

    bool is_backup() const override { return coordinator_id == 1; }
};

/*
 * There are 2 replicas in the system with N coordinators.
 * Coordinator 0 has a full replica.
 * The other replica is partitioned across coordinator 1 and coordinator N - 1
 *
 *
 * The master partition is partition id % N.
 *
 * case 1
 * If the master partition is from coordinator 1 to coordinator N - 1,
 * the secondary partition is on coordinator 0.
 *
 * case 2
 * If the master partition is on coordinator 0,
 * the secondary partition is from coordinator 1 to coordinator N - 1.
 *
 */

class StarSPartitioner : public Partitioner {
public:
    StarSPartitioner(std::size_t coordinator_id, std::size_t coordinator_num,
                     std::size_t partition_num)
        : Partitioner(coordinator_id, coordinator_num, partition_num) {
        CHECK(coordinator_num >= 2);
    }

    ~StarSPartitioner() override = default;

    std::size_t replica_num() const override { return 2; }

    bool is_replicated() const override { return true; }

    bool has_master_partition(std::size_t partition_id) const override {
        return master_coordinator(partition_id) == coordinator_id;
    }

    std::size_t master_coordinator(std::size_t partition_id) const override {
        return partition_id % coordinator_num;
    }

    bool is_partition_replicated_on(std::size_t partition_id,
                                    std::size_t coordinator_id) const override {
        DCHECK(coordinator_id < coordinator_num);

        auto master_id = master_coordinator(partition_id);
        auto secondary_id = 0u;  // case 1
        if (master_id == 0) {
            secondary_id = partition_id % (coordinator_num - 1) + 1;  // case 2
        }
        return coordinator_id == master_id || coordinator_id == secondary_id;
    }

    bool is_backup() const override { return false; }
};

class StarCPartitioner : public Partitioner {
public:
    StarCPartitioner(std::size_t coordinator_id, std::size_t coordinator_num,
                     std::size_t partition_num)
        : Partitioner(coordinator_id, coordinator_num, partition_num) {
        CHECK(coordinator_num >= 2);
    }

    ~StarCPartitioner() override = default;

    std::size_t replica_num() const override { return 2; }

    bool is_replicated() const override { return true; }

    bool has_master_partition(std::size_t partition_id) const override {
        return coordinator_id == 0;
    }

    std::size_t master_coordinator(std::size_t partition_id) const override { return 0; }

    bool is_partition_replicated_on(std::size_t partition_id,
                                    std::size_t coordinator_id) const override {
        DCHECK(coordinator_id < coordinator_num);

        if (coordinator_id == 0) return true;

        return coordinator_id == (partition_id % (coordinator_num - 1)) + 1;
    }

    bool is_backup() const override { return coordinator_id != 0; }
};

class CalvinPartitioner : public Partitioner {
public:
    CalvinPartitioner(std::size_t coordinator_id, std::size_t coordinator_num,
                      std::size_t partition_num, std::vector<std::size_t> replica_group_sizes)
        : Partitioner(coordinator_id, coordinator_num, partition_num) {
        std::size_t size = 0;
        for (auto i = 0u; i < replica_group_sizes.size(); i++) {
            CHECK(replica_group_sizes[i] > 0);
            size += replica_group_sizes[i];

            if (coordinator_id < size) {
                coordinator_start_id = size - replica_group_sizes[i];
                replica_group_id = i;
                replica_group_size = replica_group_sizes[i];
                break;
            }
        }
        CHECK(std::accumulate(replica_group_sizes.begin(), replica_group_sizes.end(), 0u) ==
              coordinator_num);
    }

    ~CalvinPartitioner() override = default;

    std::size_t replica_num() const override { return replica_group_size; }

    bool is_replicated() const override {
        // replica group in calvin is independent
        return false;
    }

    bool has_master_partition(std::size_t partition_id) const override {
        return master_coordinator(partition_id) == coordinator_id;
    }

    std::size_t master_coordinator(std::size_t partition_id) const override {
        return partition_id % replica_group_size + coordinator_start_id;
    }

    bool is_partition_replicated_on(std::size_t partition_id,
                                    std::size_t coordinator_id) const override {
        // replica group in calvin is independent
        return false;
    }

    bool is_backup() const override { return false; }

public:
    std::size_t replica_group_id;
    std::size_t replica_group_size;

private:
    // the first coordinator in this replica group
    std::size_t coordinator_start_id;
};

class PartitionerFactory {
public:
    static std::unique_ptr<Partitioner> create_partitioner(const std::string &part,
                                                           std::size_t coordinator_id,
                                                           std::size_t coordinator_num,
                                                           std::size_t partition_num) {
        if (part == "hash") {
            return std::make_unique<HashPartitioner>(coordinator_id, coordinator_num,
                                                     partition_num);
        } else if (part == "hash2") {
            return std::make_unique<HashReplicatedPartitioner<2>>(coordinator_id, coordinator_num,
                                                                  partition_num);
        } else if (part == "hash3") {
            return std::make_unique<HashReplicatedPartitioner<3>>(coordinator_id, coordinator_num,
                                                                  partition_num);
        } else if (part == "hash4") {
            return std::make_unique<HashReplicatedPartitioner<4>>(coordinator_id, coordinator_num,
                                                                  partition_num);
        } else if (part == "hash5") {
            return std::make_unique<HashReplicatedPartitioner<5>>(coordinator_id, coordinator_num,
                                                                  partition_num);
        } else if (part == "hash6") {
            return std::make_unique<HashReplicatedPartitioner<6>>(coordinator_id, coordinator_num,
                                                                  partition_num);
        } else if (part == "hash7") {
            return std::make_unique<HashReplicatedPartitioner<7>>(coordinator_id, coordinator_num,
                                                                  partition_num);
        } else if (part == "hash9") {
            return std::make_unique<HashReplicatedPartitioner<9>>(coordinator_id, coordinator_num,
                                                                  partition_num);
        } else if (part == "pb") {
            return std::make_unique<PrimaryBackupPartitioner>(coordinator_id, coordinator_num,
                                                              partition_num);
        } else if (part == "StarS") {
            return std::make_unique<StarSPartitioner>(coordinator_id, coordinator_num,
                                                      partition_num);
        } else if (part == "StarC") {
            return std::make_unique<StarCPartitioner>(coordinator_id, coordinator_num,
                                                      partition_num);
        } else {
            CHECK(false);
            return nullptr;
        }
    }
};

}  // namespace lefr