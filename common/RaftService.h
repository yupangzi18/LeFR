#pragma once

#include <gflags/gflags.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <braft/raft.h>
#include <braft/storage.h> 
#include <braft/util.h>
#include <braft/protobuf_file.h>
#include "common/replication.pb.h"

// DEFINE_bool(check_term, true, "Check if the leader changed to another term");
// DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
// DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
// DEFINE_int32(election_timeout_ms, 5000, 
//             "Start election in such milliseconds if disconnect with the leader");
// DEFINE_int32(port, 8100, "Listen port of this peer");
// DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
// DEFINE_string(conf, "", "Initial configuration of the replication group");
// DEFINE_string(data_path, "./data", "Path of data stored on");
// DEFINE_string(group, "Counter", "Id of the replication group");

namespace lefr {
class RaftService;

class RaftServiceClosure : public braft::Closure {
public:
    RaftServiceClosure(RaftService* raftService,
                       const ReplicationRequest* request,
                       ReplicationResponse* response,
                       google::protobuf::Closure* done)
        : _raftService(raftService)
        , _request(request)
        , _response(response)
        , _done(done) {}

    ~RaftServiceClosure() {}

    const ReplicationRequest* request() const {return _request;}
    ReplicationResponse* response() const { return _response; }
    void Run();

private:
    RaftService* _raftService;
    const ReplicationRequest* _request;
    ReplicationResponse* _response;
    google::protobuf::Closure* _done;
};

struct RaftNodeOptions {
    int32_t FLAGS_port;
    std::string FLAGS_conf;
    int32_t FLAGS_election_timeout_ms;
    int32_t FLAGS_snapshot_interval;
    std::string FLAGS_data_path;
    bool FLAGS_disable_cli;
    std::string FLAGS_group;
    bool FLAGS_check_term;
};


class RaftService : public braft::StateMachine {
public:
    RaftService(RaftNodeOptions options)
        : _options(options)
        , _node(NULL)
        , _leader_term(-1)
        , _value(0) {}

    ~RaftService() {
        delete _node;
    }

    int start() {
        butil::EndPoint addr(butil::my_ip(), _options.FLAGS_port);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(_options.FLAGS_conf) != 0) {
            LOG(ERROR) << "Fail to parse configuration `" << _options.FLAGS_conf << '\'';
            return -1;
        }
        node_options.election_timeout_ms = _options.FLAGS_election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = _options.FLAGS_snapshot_interval;
        std::string prefix = "local://" + _options.FLAGS_data_path;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = _options.FLAGS_disable_cli;
        braft::Node* node = new braft::Node(_options.FLAGS_group, braft::PeerId(addr));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        return 0;
    }

    // Implements Service method
    void init_txn(const ReplicationRequest* request,
                  ReplicationResponse* response,
                  google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        // Serialize request to the replicated write-ahead-log so that all the
        // peers in the group receive this request as well.
        // Notice that _value can't be modified in this routine otherwise it
        // will be inconsistent with others in this group.

        // Serialize request to IOBuf
        const int64_t term = _leader_term.load(butil::memory_order_relaxed);
        if (term < 0) {
            return redirect(response);
        }
        butil::IOBuf log;
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to serialize request";
            response->set_success(false);
            return;
        }

        // Apply this log as a braft::Task
        braft::Task task;
        task.data = &log;
        // This callback would be iovoked when the task actually excuted or fail
        task.done = new RaftServiceClosure(this, request, response, done_guard.release());

        // if (FLAGS_check_term) {
        //     // ABA problem can be avoid if expected_term is set
        //     task.expected_term = term;
        // }
        // ABA problem can be avoid if expected_term is set
        task.expected_term = term;
        // Now the task is applied to the group, waiting for the result.
        return _node->apply(task);
    }

    bool is_leader() const { 
        return _leader_term.load(butil::memory_order_acquire) > 0;
    }

    // Shut this node down.
    void shutdown() {
        if (_node) {
            _node->shutdown(NULL);
        }
    }

    // Blocking this thread until the node is eventually down.
    void join() {
        if (_node) {
            _node->join();
        }
    }

private:
friend class RaftServiceClosure;

    void redirect(ReplicationResponse* response) {
        response->set_success(false);
        if (_node) {
            braft::PeerId leader = _node->leader_id();
            if (!leader.is_empty()) {
                response->set_redirect(leader.to_string());
            }
        }
    }

    // @braft::StateMachine
    void on_apply(braft::Iterator& iter) {
        // A batch of tasks are committed, which must be processed through 
        // |iter|
        for (; iter.valid(); iter.next()) {
            int64_t tid = 0;
            int64_t lnum = 0;
            ReplicationResponse* response = NULL;
            // This guard helps invoke iter.done()->Run() asynchronously to
            // avoid that callback blocks the StateMachine.
            braft::AsyncClosureGuard closure_guard(iter.done());
            if (iter.done()) {
                // This task is applied by this node, get value from this
                // closure to avoid additional parsing.
                RaftServiceClosure* c = dynamic_cast<RaftServiceClosure*>(iter.done());
                response = c->response();
                tid = c->request()->tid();
                lnum = c->request()->logs_size();
                for (int i = 0; i < lnum; i++) {
                    _logs.push_back(c->request()->logs(i));
                }
            } else {
                // Have to parse ReplicationRequest from this log.
                butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
                ReplicationRequest request;
                CHECK(request.ParseFromZeroCopyStream(&wrapper));
                tid = request.tid();
                lnum = request.logs_size();
                for (int i = 0; i < lnum; i++) {
                    _logs.push_back(request.logs(i));
                }
            }

            // Set tid for specific transaction.
            _value.store(tid);

            if (response) {
                response->set_success(true);
            }

        }
    }

    struct SnapshotClosure {
        int64_t value;
        std::vector<OpLog> logs;
        braft::SnapshotWriter* writer;
        braft::Closure* done;
    };

    static void *save_snapshot(void* arg) {
        SnapshotClosure* sc = (SnapshotClosure*) arg;
        std::unique_ptr<SnapshotClosure> arg_guard(sc);
        // Serialize StateMachine to the snapshot
        brpc::ClosureGuard done_guard(sc->done);
        std::string snapshot_path = sc->writer->get_path() + "/data";
        LOG(INFO) << "Saving snapshot to " << snapshot_path;
        // Use protobuf to store the snapshot for backward compatibility.
        // Snapshot s;
        // s.set_value(sc->value);
        // braft::ProtoBufFile pb_file(snapshot_path);
        // if (pb_file.save(&s, true) != 0)  {
        //     sc->done->status().set_error(EIO, "Fail to save pb_file");
        //     return NULL;
        // }
        // // Snapshot is a set of files in raft. Add the only file into the
        // // writer here.
        // if (sc->writer->add_file("data") != 0) {
        //     sc->done->status().set_error(EIO, "Fail to add file to writer");
        //     return NULL;
        // }
        std::ofstream os(snapshot_path.c_str());
        os << sc->value << '\n';
        for (size_t i = 0; i < sc->logs.size(); ++i) {
            os << sc->logs[i].type() << ' ' << sc->logs[i].tid() << ' ' << sc->logs[i].value() << '\n';
        }
        CHECK_EQ(0, sc->writer->add_file("data"));
        return NULL;
    }

    void on_snapshot_save(braft::SnapshotWriter* writer, braft::Closure* done) {
        // Save current StateMachine in memory and starts a new bthread to avoid
        // blocking StateMachine since it's a bit slow to write data to disk
        // file.
        SnapshotClosure* sc = new SnapshotClosure;
        sc->value = _value.load(butil::memory_order_relaxed);
        for (const auto log : _logs) {
            sc->logs.push_back(log);
        }
        sc->writer = writer;
        sc->done = done;
        bthread_t tid;
        bthread_start_urgent(&tid, NULL, save_snapshot, sc);
    }

    int on_snapshot_load(braft::SnapshotReader* reader) {
        // Load snasphot from reader, replacing the running StateMachine
        CHECK(!is_leader()) << "Leader is not supposed to load snapshot";
        _logs.clear();
        std::string snapshot_path = reader->get_path();
        snapshot_path.append("/data");
        std::ifstream is(snapshot_path.c_str());
        int64_t type;
        int64_t tid;
        std::string value;
        while (is >> type >> tid >> value) {
            OpLog log;
            log.set_type(static_cast<OpLog_Type>(type));
            log.set_tid(tid);
            log.set_value(value);
            _logs.push_back(log);
        }
        return 0;
    }

    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }
    void on_leader_stop(const butil::Status& status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }

    void on_shutdown() {
        LOG(INFO) << "This node is down";
    }
    void on_error(const ::braft::Error& e) {
        LOG(ERROR) << "Met raft error " << e;
    }
    void on_configuration_committed(const ::braft::Configuration& conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }
    void on_stop_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }
    void on_start_following(const ::braft::LeaderChangeContext& ctx) {
        LOG(INFO) << "Node start following " << ctx;
    }
    // end of @braft::StateMachine

private:
    RaftNodeOptions _options;
    braft::Node* volatile _node;
    butil::atomic<int64_t> _leader_term;
    butil::atomic<int64_t> _value;
    std::vector<OpLog> _logs;
};

void RaftServiceClosure::Run() {
    // Auto delete this after Run()
    std::unique_ptr<RaftServiceClosure> self_guard(this);
    // Repsond this RPC.
    brpc::ClosureGuard done_guard(_done);
    if (status().ok()) {
        return;
    }
    // Try redirect if this request failed.
    _raftService->redirect(_response);
}

// Implements lefr::ReplicationService if you are using brpc.
class ReplicationServiceImpl : public ReplicationService {
public:
    explicit ReplicationServiceImpl(RaftService* raftService) : _raft_service(raftService) {}
    void init_txn(::google::protobuf::RpcController* controller,
                  const ::lefr::ReplicationRequest* request,
                  ::lefr::ReplicationResponse* response,
                  ::google::protobuf::Closure* done) {
        return _raft_service->init_txn(request, response, done);
    }
private:
    RaftService* _raft_service;
};

} // namespace lefr