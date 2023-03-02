#pragma once

#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "core/Context.h"
#include "core/ControlMessage.h"
#include "core/Delay.h"
#include "core/Partitioner.h"
#include "core/Worker.h"

namespace lefr {
class Checker : public Worker {
public:
    Checker(std::size_t coordinator_id, std::size_t id, const Context &context,
            Partitioner &partitioner, std::atomic<bool> &stopFlag, std::atomic<bool> &transferFlag,
            std::atomic<bool> &recoveryFlag, std::atomic<uint32_t> &n_committing_txns,
            std::unordered_set<uint32_t> &faultNodes,
            std::unordered_map<uint32_t, uint32_t> &faultMap)
        : Worker(coordinator_id, id),
          context(context),
          partitioner(partitioner),
          stopFlag(stopFlag),
          transferFlag(transferFlag),
          recoveryFlag(recoveryFlag),
          n_committing_txns(n_committing_txns),
          faultNodes(faultNodes),
          faultMap(faultMap),
          delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num,
                                            context.delay_time)) {
        for (auto i = 0u; i < context.coordinator_num; i++) {
            messages.emplace_back(std::make_unique<Message>());
            init_message(messages[i].get(), i);
        }
        for (const auto it : faultMap) {
            recoveryNodes.insert(it.second);
            LOG(INFO) << "recoveryNode: " << it.second;
        }
        pendingResponses = 0;
    }

    ~Checker() = default;

    void start() override {
        LOG(INFO) << recoveryNodes.size();
        while (!stopFlag.load()) {
            if (transferFlag.load()) {
                if (coordinator_id == *faultNodes.begin()) {
                    for (const auto faultNode : faultNodes) {
                        partitioner.set_coordinator_fault(faultNode);
                    }
                    send_transfer_message();
                    for (const auto faultNode : faultNodes) {
                        partitioner.transfer_master_coordinators(faultMap[faultNode]);
                    }
                    wait4_ack();

                    while (n_committing_txns.load() > 0 || !partitioner.finished_transfer()) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(200));
                        process_request();
                    }
                    transferFlag.store(false);
                    LOG(INFO) << "transferFlag: " << transferFlag.load();
                }
            } else if (recoveryFlag.load()) {
                if (coordinator_id == faultMap[*faultNodes.begin()]) {
                    for (const auto recoveryNode : recoveryNodes) {
                        partitioner.set_coordinator_recovery(recoveryNode);
                    }
                    send_recovery_message();
                    partitioner.recovery_master_coordinators();
                    wait4_ack();

                    while (n_committing_txns.load() > 0 || !partitioner.finished_recovery()) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(200));
                        process_request();
                    }
                    recoveryFlag.store(false);
                    LOG(INFO) << "recoveryFlag: " << recoveryFlag.load();
                }
            } else {
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                process_request();
            }
        }
    }

    void push_message(Message *message) override { in_queue.push(message); }

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

    void wait4_ack() {
        while (pendingResponses > 0) {
            in_queue.wait_till_non_empty();
            std::unique_ptr<Message> message(in_queue.front());
            bool ok = in_queue.pop();
            CHECK(ok);

            for (auto it = message->begin(); it != message->end(); it++) {
                MessagePiece messagePiece = *it;
                auto type = static_cast<ControlMessage>(messagePiece.get_message_type());
                CHECK(type == ControlMessage::ACK);
                pendingResponses--;
            }
        }
    }

    std::size_t process_request() {
        std::size_t size = 0;
        while (!in_queue.empty()) {
            std::unique_ptr<Message> message(in_queue.front());
            bool ok = in_queue.pop();
            CHECK(ok);

            std::size_t source_node_id = message->get_source_node_id();

            if (!transferFlag.load() && source_node_id == *faultNodes.begin()) {
                transferFlag.store(true);
                std::this_thread::sleep_for(std::chrono::milliseconds(500));
                LOG(INFO) << "Coordinator " << context.coordinator_id << " set transferFlag!";
            }

            if (!recoveryFlag.load() && source_node_id == faultMap[*faultNodes.begin()]) {
                recoveryFlag.store(true);
                LOG(INFO) << "Coordinator " << context.coordinator_id << " set recoveryFlag!";
            }

            for (auto it = message->begin(); it != message->end(); it++) {
                MessagePiece messagePiece = *it;
                auto type = static_cast<ControlMessage>(messagePiece.get_message_type());

                CHECK(type == ControlMessage::TRANSFER);

                std::size_t partition_id, coordinator_id;
                StringPiece stringPiece = messagePiece.toStringPiece();
                Decoder dec(stringPiece);
                dec >> partition_id;
                dec >> coordinator_id;

                if (source_node_id == *faultNodes.begin()) {
                    for (const auto faultNode : faultNodes) {
                        if (!partitioner.get_coordinator_fault(faultNode)) {
                            partitioner.set_coordinator_fault(faultNode);
                        }
                    }
                }

                if (source_node_id == faultMap[*faultNodes.begin()]) {
                    for (const auto recoveryNode : recoveryNodes) {
                        if (partitioner.get_coordinator_fault(recoveryNode)) {
                            partitioner.set_coordinator_recovery(recoveryNode);
                        }
                    }
                }

                partitioner.transfer_master_coordinator(partition_id, coordinator_id);
                ControlMessageFactory::new_ack_message(*messages[source_node_id]);
            }

            size += message->get_message_count();
            flush_messages();

            while (n_committing_txns.load() > 0) {
                std::this_thread::yield();
            }

            if (transferFlag.load() && source_node_id == *faultNodes.begin()) {
                transferFlag.store(false);
                LOG(INFO) << "transferFlag: " << transferFlag.load();
            }
            if (recoveryFlag.load() && source_node_id == faultMap[*faultNodes.begin()]) {
                recoveryFlag.store(false);
                LOG(INFO) << "recoveryFlag: " << recoveryFlag.load();
            }
        }

        return size;
    }

protected:
    void flush_messages() {
        for (auto i = 0u; i < messages.size(); i++) {
            if (i == coordinator_id) {
                continue;
            }

            if (messages[i]->get_message_count() == 0) {
                continue;
            }

            auto message = messages[i].release();

            out_queue.push(message);
            messages[i] = std::make_unique<Message>();
            init_message(messages[i].get(), i);
        }
    }

    void init_message(Message *message, std::size_t dest_node_id) {
        message->set_source_node_id(coordinator_id);
        message->set_dest_node_id(dest_node_id);
        message->set_worker_id(id);
    }

    void send_transfer_message() {
        for (auto partition_id = 0u; partition_id < context.partition_num; partition_id++) {
            auto faultNode = partition_id % context.coordinator_num;
            if (partitioner.get_coordinator_fault(faultNode)) {
                std::size_t new_master = faultMap[faultNode];
                for (auto node_id = 0u; node_id < context.coordinator_num; node_id++) {
                    if (node_id != coordinator_id) {
                        pendingResponses++;
                        LOG(INFO) << "node " << node_id << ", partition: " << partition_id
                                  << ", new master: " << new_master;
                        ControlMessageFactory::new_transfer_message(*messages[node_id],
                                                                    partition_id, new_master);
                    }
                }
            }
        }
        flush_messages();
    }

    void send_recovery_message() {
        for (auto partition_id = 0u; partition_id < context.partition_num; partition_id++) {
            if (partitioner.master_coordinator(partition_id) !=
                partition_id % context.coordinator_num) {
                std::size_t new_master = partition_id % context.coordinator_num;
                for (auto node_id = 0u; node_id < context.coordinator_num; node_id++) {
                    if (node_id != coordinator_id) {
                        LOG(INFO) << "node " << node_id << ", partition: " << partition_id
                                  << ", new master: " << new_master;
                        ControlMessageFactory::new_transfer_message(*messages[node_id],
                                                                    partition_id, new_master);
                        pendingResponses++;
                    }
                }
            }
        }
        flush_messages();
    }

protected:
    const Context &context;
    Partitioner &partitioner;
    std::atomic<bool> &stopFlag;
    std::atomic<bool> &transferFlag;
    std::atomic<bool> &recoveryFlag;
    std::atomic<uint32_t> &n_committing_txns;
    std::unordered_set<uint32_t> &faultNodes;
    std::unordered_map<uint32_t, uint32_t> &faultMap;
    std::unordered_set<uint32_t> recoveryNodes;
    std::vector<std::unique_ptr<Message>> messages;
    LockfreeQueue<Message *> in_queue, out_queue;
    std::size_t pendingResponses;
    bool transferFinished = false;
    bool recoveryFinished = false;

public:
    std::unique_ptr<Delay> delay;
};
}  // namespace lefr