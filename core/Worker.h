#pragma once

#include <butil/logging.h>

#include <atomic>
#include <queue>

#include "common/LockfreeQueue.h"
#include "common/Message.h"

namespace lefr {

class Worker {
public:
    Worker(std::size_t coordinator_id, std::size_t id) : coordinator_id(coordinator_id), id(id) {
        n_commit.store(0);
        n_abort_no_retry.store(0);
        n_abort_lock.store(0);
        n_abort_read_validation.store(0);
        n_local.store(0);
        n_si_in_serializable.store(0);
        n_network_size.store(0);
        n_txn_latency.store(0);
        n_during_fault.store(0);
        n_fault_txn_latency.store(0);
        n_normal_long_txn.store(0);
        n_normal_longtxn_latency.store(0);
        n_allredo_long_txn.store(0);
        n_allredo_longtxn_latency.store(0);
        rw_network_size.store(0);
        pr_network_size.store(0);
        com_network_size.store(0);
        rw_message_count.store(0);
        pr_message_count.store(0);
        com_message_count.store(0);
    }

    virtual ~Worker() = default;

    virtual void start() = 0;

    virtual void onExit() {}

    virtual void push_message(Message *message) = 0;

    virtual Message *pop_message() = 0;

public:
    std::size_t coordinator_id;
    std::size_t id;
    std::atomic<uint64_t> n_commit, n_abort_no_retry, n_abort_lock, n_abort_read_validation,
        n_local, n_si_in_serializable, n_network_size, n_txn_latency, n_during_fault,
        n_fault_txn_latency, n_normal_long_txn, n_normal_longtxn_latency, n_allredo_long_txn,
        n_allredo_longtxn_latency, rw_network_size, pr_network_size, com_network_size,
        rw_message_count, pr_message_count, com_message_count;
};

}  // namespace lefr
