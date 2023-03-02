#pragma once

namespace lefr {

enum class ExecutorStatus {
    START,
    CLEANUP,
    C_PHASE,
    S_PHASE,
    Analysis,
    Execute,
    Kiva_READ,
    Kiva_COMMIT,
    STOP,
    EXIT
};

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY, REDO };
enum class TransactionStatus { BEGIN, INPROGRESS, READY_TO_COMMIT, COMMITTING, COMMIT, ABORT };
enum class FaultFlag { HAPPEN, NORMAL };
enum class PathType { FAST, SLOW, ABORT };
enum class CommitResult { SUCCESS, REXECUTE, ABORT };

}  // namespace lefr
