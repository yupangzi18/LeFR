#include "benchmark/ycsb/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"

DEFINE_int32(read_write_ratio, 50, "read write ratio");
DEFINE_int32(read_only_ratio, 0, "read only transaction ratio");
DEFINE_int32(cross_ratio, 100, "cross partition transaction ratio");
DEFINE_int32(keys, 200000, "keys in a partition.");
DEFINE_double(zipf, 0, "skew factor");

DEFINE_int32(nop_prob, 0, "prob of transactions having nop, out of 10000");
DEFINE_int64(n_nop, 0, "total number of nop");

// ./main  --id=1 --servers="127.0.0.1:10010;127.0.0.1:10011"
// cmake -DCMAKE_BUILD_TYPE=Release

int main(int argc, char *argv[]) {
    // google::InitGoogleLogging(argv[0]);
    // google::InstallFailureSignalHandler();
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    lefr::ycsb::Context context;
    SETUP_CONTEXT(context);

    context.readWriteRatio = FLAGS_read_write_ratio;
    context.readOnlyTransaction = FLAGS_read_only_ratio;
    context.crossPartitionProbability = FLAGS_cross_ratio;
    context.keysPerPartition = FLAGS_keys;

    context.nop_prob = FLAGS_nop_prob;
    context.n_nop = FLAGS_n_nop;

    if (FLAGS_zipf > 0) {
        context.isUniform = false;
        lefr::Zipf::globalZipf().init(context.keysPerPartition, FLAGS_zipf);
    }

    lefr::ycsb::Database db;
    db.initialize(context);

    lefr::Coordinator c(FLAGS_id, db, context);
    LOG(INFO) << "The granularity of message flush: " << context.flush_granularity;
    c.connectToPeers();
    c.start();
    return 0;
}