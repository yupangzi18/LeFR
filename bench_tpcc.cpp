#include "benchmark/tpcc/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"

DEFINE_bool(operation_replication, false, "use operation replication");
DEFINE_string(query, "neworder", "tpcc query, neworder, payment");
DEFINE_int32(neworder_dist, 100, "new order distributed.");
DEFINE_int32(payment_dist, 15, "payment distributed.");

// ./main  --id=1 --servers="127.0.0.1:10010;127.0.0.1:10011"
// cmake -DCMAKE_BUILD_TYPE=Release

int main(int argc, char *argv[]) {
    // google::InitGoogleLogging(argv[0]);
    // google::InstallFailureSignalHandler();
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    lefr::tpcc::Context context;
    SETUP_CONTEXT(context);

    context.operation_replication = FLAGS_operation_replication;

    if (FLAGS_query == "neworder") {
        context.workloadType = lefr::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY;
    } else if (FLAGS_query == "payment") {
        context.workloadType = lefr::tpcc::TPCCWorkloadType::PAYMENT_ONLY;
    } else {
        CHECK(false);
    }

    context.newOrderCrossPartitionProbability = FLAGS_neworder_dist;
    context.paymentCrossPartitionProbability = FLAGS_payment_dist;

    lefr::tpcc::Database db;
    db.initialize(context);

    lefr::Coordinator c(FLAGS_id, db, context);
    c.connectToPeers();
    c.start();
    return 0;
}