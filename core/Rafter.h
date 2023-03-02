#pragma once

#include <thread>

#include "core/Context.h"
#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "common/RaftService.h"

namespace lefr {

class Rafter : public Worker {
public:
    Rafter(std::size_t coordinator_id, std::size_t id, const Context &context,
            std::atomic<bool> &stopFlag, std::queue<uint64_t> &txnqueue, std::size_t raft_id)
        : Worker(coordinator_id, id),
          context(context),
          stopFlag(stopFlag),
          txnqueue(txnqueue),
          raft_id(raft_id), 
          delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num,
                                            context.delay_time)) {
        for (auto i = 0u; i < context.coordinator_num; i++) {
            messages.emplace_back(std::make_unique<Message>());
            init_message(messages[i].get(), i);
        }

        worker_status.store(static_cast<uint32_t>(ExecutorStatus::STOP));
    }

    void set_worker_status(ExecutorStatus status) {
        worker_status.store(static_cast<uint32_t>(status));
    }

    void start() override {
        brpc::Server server;
        struct RaftNodeOptions raftnode = {0,"",0,0,"",false,"",true};
        raftnode.FLAGS_port = context.raft_port + coordinator_id;
        raftnode.FLAGS_conf = context.raft_conf;
        raftnode.FLAGS_election_timeout_ms = context.election_timeout_ms;
        raftnode.FLAGS_snapshot_interval = context.snapshot_interval;
        raftnode.FLAGS_data_path = context.data_path;
        raftnode.FLAGS_group = context.raft_group;
        RaftService raftservice(raftnode);
        ReplicationServiceImpl service(&raftservice);
        

        if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(ERROR) << "Fail to add service";
            return;
        }

        if (braft::add_service(&server, raftnode.FLAGS_port) != 0) {        
            LOG(ERROR) << "Fail to add raft services";
            return;
        }

        if (server.Start(raftnode.FLAGS_port, NULL) != 0) {
            LOG(ERROR) << "Fail to start server";
            return;
        }

        if (raftservice.start() != 0) {
            LOG(ERROR) << "Fail to start Replication";
            return;
        }
        while (!stopFlag.load()){
            // wait for stopFlag
        }
        raftservice.shutdown();
        server.Stop(0);

        raftservice.join();
        server.Join(); 
        return;
    }

    void push_message(Message *message) override {return;}

    Message *pop_message() override {
        if (out_queue.empty()) return nullptr;

        Message *message = out_queue.front();

        if (delay->delay_enabled()) {
            auto now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::microseconds>(now - message->time).count() <
                delay->message_delay()) {
                return nullptr;
            }
        }

        bool ok = out_queue.pop();
        CHECK(ok);

        return message;
    }
protected:
    void init_message(Message *message, std::size_t dest_node_id) {
        message->set_source_node_id(coordinator_id);
        message->set_dest_node_id(dest_node_id);
        message->set_worker_id(id);
    }


protected:
    const Context &context;
    std::atomic<bool> &stopFlag;
    LockfreeQueue<Message *> ack_in_queue, signal_in_queue, stop_in_queue, out_queue;
    std::vector<std::unique_ptr<Message>> messages;

public:
    std::atomic<uint32_t> worker_status;
    std::atomic<uint32_t> n_completed_workers;
    std::atomic<uint32_t> n_started_workers;
    std::queue<uint64_t> &txnqueue;
    std::size_t raft_id;

    std::unique_ptr<Delay> delay;
};

}  // namespace lefr
