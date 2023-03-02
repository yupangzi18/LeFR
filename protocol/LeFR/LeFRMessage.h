//
// Created by Yi Lu on 8/31/18.
//

#pragma once

#include <cstring>
#include <string>

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/LeFR/LeFRTransaction.h"
#include "protocol/LeFR/SiloHelper.h"
#include "protocol/LeFR/SiloRWKey.h"

namespace lefr {

enum class LeFRMessage {
    INIT_TXN_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
    INIT_TXN_RESPONSE,

    REDO_REQUEST,
    SCANREPLICA_REQUEST,

    READ_REPLICATION_REQUEST,
    READ_REPLICATION_RESPONSE,

    WRITE_REPLICATION_REQUEST,
    WRITE_REPLICATION_RESPONSE,

    SEARCH_REQUEST,
    SEARCH_RESPONSE,

    LOCK_REQUEST,
    LOCK_RESPONSE,

    READ_VALIDATION_REQUEST,
    READ_VALIDATION_RESPONSE,

    ABORT_REQUEST,

    WRITE_REQUEST,
    WRITE_RESPONSE,

    REPLICATION_REQUEST,
    REPLICATION_RESPONSE,

    RELEASE_LOCK_REQUEST,

    SYNC_TRANSACTION_INFO_REPLICATION_REQUEST,
    SYNC_REPLICATION_PER_TRANSACTION_RESPONSE,

    CREATE_SUB_TXN_REQUEST,
    CREATE_SUB_TXN_RESPONSE,

    ADD_REMOTE_WSET_REQUEST,
    ADD_REMOTE_WSET_RESPONSE,

    REMOTE_LOCK_REQUEST,
    REMOTE_LOCK_RESPONSE,

    REMOTE_READ_VALIDATION_REQUEST,
    REMOTE_READ_VALIDATION_RESPONSE,

    REMOTE_WRITE_REQUEST,
    REMOTE_WRITE_RESPONSE,

    REMOTE_RELEASE_LOCK_REQUEST,
    REMOTE_ABORT_REQUEST,

    ASYNC_INIT_TXN_REQUEST,
    ASYNC_READ_REPLICATION_REQUEST,
    ASYNC_WRITE_REPLICATION_REQUEST,
    ASYNC_END_REQUEST,
    ASYNC_REPLICATION_REQUEST,

    SYNC_DECISION_REQUEST,
    SYNC_DECISION_RESPONSE,
    NFIELDS
};

class LeFRMessageFactory {
public:
    static std::size_t new_init_txn_message(Message &message, uint32_t type,
                                            std::size_t coordinator_id, std::size_t partition_id,
                                            uint64_t seed, uint64_t startTime) {
        /*
         * The structure of a init txn request: (type, coordinator id, partition id, seed,
         * startTIme)
         */

        auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t) +
                            sizeof(std::size_t) + sizeof(std::size_t) + sizeof(uint64_t) +
                            sizeof(uint64_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::INIT_TXN_REQUEST), message_size, 0, 0);
        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << type;
        encoder << coordinator_id;
        encoder << partition_id;
        encoder << seed;
        encoder << startTime;
        message.flush();
        return message_size;
    }

    static std::size_t new_redo_message(Message &message, uint32_t redo) {
        /*
         * The structure of a redo request: (redo)
         */

        auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::REDO_REQUEST), message_size, 0, 0);
        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << redo;
        message.flush();
        return message_size;
    }

    static std::size_t new_scanreplica_message(Message &message, uint32_t scanreplica,
                                               uint64_t startTime) {
        // The structure of a scanreplica request: (scanreplica, startTime)

        auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t) + sizeof(uint64_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::SCANREPLICA_REQUEST), message_size, 0, 0);
        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << scanreplica;
        encoder << startTime;
        message.flush();
        return message_size;
    }

    static std::size_t new_read_replication_message(Message &message, ITable &table,
                                                    const void *key, const void *value,
                                                    uint64_t tid) {
        /*
         * The structure of a read replication request: (primary key, field value, tid)
         */

        auto key_size = table.key_size();
        auto field_size = table.field_size();

        auto message_size =
            MessagePiece::get_header_size() + key_size + field_size + sizeof(uint64_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::READ_REPLICATION_REQUEST), message_size,
            table.tableID(), table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        table.serialize_value(encoder, value);
        encoder << tid;
        message.flush();
        return message_size;
    }

    static std::size_t new_write_replication_message(Message &message, ITable &table,
                                                     const void *key, const void *value) {
        /*
         * The structure of a write replication request: (primary key, field value)
         */

        auto key_size = table.key_size();
        auto field_size = table.field_size();

        auto message_size = MessagePiece::get_header_size() + key_size + field_size;
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::WRITE_REPLICATION_REQUEST), message_size,
            table.tableID(), table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        table.serialize_value(encoder, value);
        message.flush();
        return message_size;
    }

    static std::size_t new_search_message(Message &message, ITable &table, const void *key,
                                          uint32_t key_offset, uint32_t round_count,
                                          std::size_t coordinator_id) {
        /*
         * The structure of a search request: (primary key, read key offset, round count,
         * coordinator id)
         */

        auto key_size = table.key_size();

        auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset) +
                            sizeof(round_count) + sizeof(coordinator_id);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::SEARCH_REQUEST), message_size, table.tableID(),
            table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        encoder << key_offset;
        encoder << round_count;
        encoder << coordinator_id;
        message.flush();
        return message_size;
    }

    static std::size_t new_lock_message(Message &message, ITable &table, const void *key,
                                        uint32_t key_offset, uint32_t round_count,
                                        std::size_t coordinator_id) {
        /*
         * The structure of a lock request: (primary key, write key offset, round count, coordinator
         * id)
         */

        auto key_size = table.key_size();

        auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset) +
                            sizeof(round_count) + sizeof(coordinator_id);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::LOCK_REQUEST), message_size, table.tableID(),
            table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        encoder << key_offset << round_count << coordinator_id;
        message.flush();
        return message_size;
    }

    static std::size_t new_read_validation_message(Message &message, ITable &table, const void *key,
                                                   uint32_t key_offset, uint64_t tid,
                                                   uint32_t round_count,
                                                   std::size_t coordinator_id) {
        /*
         * The structure of a read validation request: (primary key, read key offset, tid, round
         * count, coordinator id)
         */

        auto key_size = table.key_size();

        auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset) +
                            sizeof(tid) + sizeof(round_count) + sizeof(coordinator_id);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::READ_VALIDATION_REQUEST), message_size,
            table.tableID(), table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        encoder << key_offset << tid << round_count << coordinator_id;
        message.flush();
        return message_size;
    }

    static std::size_t new_abort_message(Message &message, ITable &table, const void *key) {
        /*
         * The structure of an abort request: (primary key)
         */

        auto key_size = table.key_size();

        auto message_size = MessagePiece::get_header_size() + key_size;
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::ABORT_REQUEST), message_size, table.tableID(),
            table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        message.flush();
        return message_size;
    }

    static std::size_t new_write_message(Message &message, ITable &table, const void *key,
                                         const void *value, uint32_t round_count,
                                         std::size_t coordinator_id, std::size_t key_offset) {
        /*
         * The structure of a write request: (primary key, field value, round count, coordinator id,
         * write key offset)
         */

        auto key_size = table.key_size();
        auto field_size = table.field_size();

        auto message_size = MessagePiece::get_header_size() + key_size + field_size +
                            sizeof(round_count) + sizeof(std::size_t) + sizeof(key_offset);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::WRITE_REQUEST), message_size, table.tableID(),
            table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        table.serialize_value(encoder, value);
        encoder << round_count << coordinator_id << key_offset;
        message.flush();
        return message_size;
    }

    static std::size_t new_replication_message(Message &message, ITable &table, const void *key,
                                               const void *value, uint64_t commit_tid,
                                               uint32_t round_count, std::size_t coordinator_id,
                                               std::size_t key_offset) {
        /*
         * The structure of a replication request: (primary key, field value, commit_tid
         * round count, coordinator id. key offset)
         */

        auto key_size = table.key_size();
        auto field_size = table.field_size();

        auto message_size = MessagePiece::get_header_size() + key_size + field_size +
                            sizeof(commit_tid) + sizeof(round_count) + sizeof(coordinator_id) +
                            sizeof(key_offset);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::REPLICATION_REQUEST), message_size, table.tableID(),
            table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        table.serialize_value(encoder, value);
        encoder << commit_tid << round_count << coordinator_id << key_offset;
        message.flush();
        return message_size;
    }

    static std::size_t new_release_lock_message(Message &message, ITable &table, const void *key,
                                                uint64_t commit_tid) {
        /*
         * The structure of a release lock request: (primary key, commit tid)
         */

        auto key_size = table.key_size();

        auto message_size = MessagePiece::get_header_size() + key_size + sizeof(commit_tid);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::RELEASE_LOCK_REQUEST), message_size, table.tableID(),
            table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        encoder << commit_tid;
        message.flush();
        return message_size;
    }

    static std::size_t new_sync_transaction_info_replication_message(Message &message,
                                                                     uint32_t type,
                                                                     std::size_t coordinator_id,
                                                                     std::size_t partition_id,
                                                                     uint64_t seed) {
        /*
         * The structure of a sync transaction info replication request: (coordinator id, partition
         * id, seed)
         */

        auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t) +
                            sizeof(std::size_t) + sizeof(std::size_t) + sizeof(uint64_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::SYNC_TRANSACTION_INFO_REPLICATION_REQUEST),
            message_size, 0, 0);
        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << type;
        encoder << coordinator_id;
        encoder << partition_id;
        encoder << seed;
        message.flush();
        return message_size;
    }

    static std::size_t new_create_sub_txn_message(Message &message, std::size_t coordinator_id,
                                                  std::size_t dest_node_id,
                                                  std::size_t partition_id, uint32_t round_count) {
        /*
         * The structure of a create sub txn request: (coordinator id, partition id, round count)
         */

        auto message_size = MessagePiece::get_header_size() + sizeof(std::size_t) +
                            sizeof(std::size_t) + sizeof(std::size_t) + sizeof(uint32_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::CREATE_SUB_TXN_REQUEST), message_size, 0, 0);
        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << coordinator_id;
        encoder << dest_node_id;
        encoder << partition_id;
        encoder << round_count;
        message.flush();
        return message_size;
    }

    static std::size_t new_add_remote_wset_message(Message &message, ITable &table, const void *key,
                                                   const void *value, uint32_t key_offset,
                                                   uint32_t round_count,
                                                   std::size_t coordinator_id) {
        /*
         * The structure of an add to remote writeset request: (primary key, field value, key
         * offset, round count, coordinator id)
         */

        auto key_size = table.key_size();
        auto field_size = table.field_size();

        auto message_size = MessagePiece::get_header_size() + key_size + field_size +
                            sizeof(key_offset) + sizeof(round_count) + sizeof(coordinator_id);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::ADD_REMOTE_WSET_REQUEST), message_size,
            table.tableID(), table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        table.serialize_value(encoder, value);
        encoder << key_offset;
        encoder << round_count;
        encoder << coordinator_id;
        message.flush();
        return message_size;
    }

    static std::size_t new_remote_lock_message(Message &message, std::size_t coordinator_id,
                                               uint32_t round_count,
                                               std::size_t source_coordinator) {
        /*
         * The structure of a remote lock request: (dest coordinator id, round count, source
         * coordinator id)
         */
        auto message_size = MessagePiece::get_header_size() + sizeof(std::size_t) +
                            sizeof(round_count) + sizeof(source_coordinator);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::REMOTE_LOCK_REQUEST), message_size, 0, 0);

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << coordinator_id;
        encoder << round_count;
        encoder << source_coordinator;
        message.flush();
        return message_size;
    }

    static std::size_t new_remote_read_validation_message(Message &message,
                                                          std::size_t coordinator_id,
                                                          uint32_t round_count,
                                                          std::size_t source_coordinator) {
        /*
         * The structure of a remote read validate request: (dest coordinator id, round count,
         * source coordinator)
         */
        auto message_size = MessagePiece::get_header_size() + sizeof(std::size_t) +
                            sizeof(round_count) + sizeof(source_coordinator);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::REMOTE_READ_VALIDATION_REQUEST), message_size, 0, 0);

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << coordinator_id;
        encoder << round_count;
        encoder << source_coordinator;
        message.flush();
        return message_size;
    }

    static std::size_t new_remote_write_message(Message &message, std::size_t coordinator_id,
                                                uint32_t round_count,
                                                std::size_t source_coordinator_id) {
        /*
         * The structure of a remote write request: (dest coordinator id, round count, source
         * coordinator id)
         */
        auto message_size = MessagePiece::get_header_size() + sizeof(std::size_t) +
                            sizeof(round_count) + sizeof(coordinator_id);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::REMOTE_WRITE_REQUEST), message_size, 0, 0);

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << coordinator_id;
        encoder << round_count;
        encoder << source_coordinator_id;
        message.flush();
        return message_size;
    }

    static std::size_t new_remote_release_lock_message(Message &message, std::size_t coordinator_id,
                                                       uint64_t commit_tid) {
        /*
         * The structure of a remote release lock request: (coordinator id, commit tid)
         */
        auto message_size =
            MessagePiece::get_header_size() + sizeof(std::size_t) + sizeof(uint64_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::REMOTE_RELEASE_LOCK_REQUEST), message_size, 0, 0);

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << coordinator_id;
        encoder << commit_tid;
        message.flush();
        return message_size;
    }

    static std::size_t new_remote_abort_message(Message &message, std::size_t coordinator_id) {
        /*
         * The structure of a remote abort request: (coordinator id)
         */
        auto message_size = MessagePiece::get_header_size() + sizeof(std::size_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::REMOTE_ABORT_REQUEST), message_size, 0, 0);

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << coordinator_id;
        message.flush();
        return message_size;
    }

    static std::size_t async_new_init_txn_message(Message &message, uint32_t type,
                                                  std::size_t coordinator_id,
                                                  std::size_t partition_id, uint64_t seed,
                                                  uint64_t startTime) {
        /*
         * The structure of an async init txn request: (coordinator id, partition id, seed, start
         * time)
         */
        auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t) +
                            sizeof(std::size_t) + sizeof(std::size_t) + sizeof(uint64_t) +
                            sizeof(uint64_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::ASYNC_INIT_TXN_REQUEST), message_size, 0, 0);
        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << type;
        encoder << coordinator_id;
        encoder << partition_id;
        encoder << seed;
        encoder << startTime;
        message.flush();
        return message_size;
    }

    static std::size_t async_new_read_replication_message(Message &message, ITable &table,
                                                          const void *key, const void *value,
                                                          uint64_t tid) {
        /*
         * The structure of a read replication request: (primary key, field value)
         */

        auto key_size = table.key_size();
        auto field_size = table.field_size();

        auto message_size =
            MessagePiece::get_header_size() + key_size + field_size + sizeof(uint64_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::ASYNC_READ_REPLICATION_REQUEST), message_size,
            table.tableID(), table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        table.serialize_value(encoder, value);
        encoder << tid;
        message.flush();
        return message_size;
    }

    static std::size_t async_new_write_replication_message(Message &message, ITable &table,
                                                           const void *key, const void *value) {
        /*
         * The structure of an async write replications request: (primary key, field value)
         */

        auto key_size = table.key_size();
        auto field_size = table.field_size();

        auto message_size = MessagePiece::get_header_size() + key_size + field_size;
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::ASYNC_WRITE_REPLICATION_REQUEST), message_size,
            table.tableID(), table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        table.serialize_value(encoder, value);
        message.flush();
        return message_size;
    }

    static std::size_t async_end_message(Message &message, std::size_t coordinator_id) {
        /*
         * The structure of an end request for backing up operations: (coordinator id)
         */

        auto message_size = MessagePiece::get_header_size() + sizeof(coordinator_id);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::ASYNC_END_REQUEST), message_size, 0, 0);

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << coordinator_id;
        message.flush();
        return message_size;
    }

    static std::size_t new_async_replication_message(Message &message, ITable &table,
                                                     const void *key, const void *value,
                                                     uint64_t commit_tid, uint32_t round_count,
                                                     std::size_t coordinator_id,
                                                     std::size_t key_offset) {
        /*
         * The structure of an async replication request: (primary key, field value,
         * commit_tid, round count, coordinator, key offset)
         */

        auto key_size = table.key_size();
        auto field_size = table.field_size();

        auto message_size = MessagePiece::get_header_size() + key_size + field_size +
                            sizeof(commit_tid) + sizeof(round_count) + sizeof(coordinator_id) +
                            sizeof(key_offset);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::ASYNC_REPLICATION_REQUEST), message_size,
            table.tableID(), table.partitionID());

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder.write_n_bytes(key, key_size);
        table.serialize_value(encoder, value);
        encoder << commit_tid << round_count << coordinator_id << key_offset;
        message.flush();
        return message_size;
    }

    static std::size_t new_sync_decision_message(Message &message, std::size_t coordinator_id,
                                                 uint64_t tid) {
        /*
         * The structure of a sync decision request: (coordinator id, tid)
         */
        auto message_size =
            MessagePiece::get_header_size() + sizeof(std::size_t) + sizeof(uint64_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::SYNC_DECISION_REQUEST), message_size, 0, 0);
        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << coordinator_id;
        encoder << tid;
        message.flush();
        return message_size;
    }
};

template <class Database>
class LeFRMessageHandler {
    using Transaction = LeFRTransaction<Database>;

public:
    static void init_txn_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::INIT_TXN_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        uint32_t type;
        std::size_t coordinator_id;
        std::size_t partition_id;
        uint64_t seed;
        uint64_t startTime;

        Decoder dec(stringPiece);
        dec >> type;
        dec >> coordinator_id;
        dec >> partition_id;
        dec >> seed;
        dec >> startTime;

        DCHECK(dec.size() == 0);

        init_replicate_transaction(type, coordinator_id, partition_id, seed, startTime);

        // prepare response message header
        auto message_size = MessagePiece::get_header_size();
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::INIT_TXN_RESPONSE), message_size, 0, 0);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        responseMessage.flush();
    }

    static void init_txn_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::INIT_TXN_RESPONSE));

        /*
         * The structure of a sync value replication response: ()
         */
        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        DCHECK(txn->pendingResponses >= 0);
        txn->network_size += inputPiece.get_message_length();
        txn->pr_message_count++;
    }

    static void redo_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(LeFRMessage::REDO_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        uint32_t redo;

        Decoder dec(stringPiece);
        dec >> redo;

        DCHECK(dec.size() == 0);

        replicateTxn->redo = redo;
    }

    static void scanreplica_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::SCANREPLICA_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        uint32_t scanreplica;
        uint64_t startTime;

        Decoder dec(stringPiece);
        dec >> scanreplica;
        dec >> startTime;

        DCHECK(dec.size() == 0);

        init_scanreplica_transaction(startTime, scanreplica);
    }

    static void read_replication_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::READ_REPLICATION_REQUEST));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();
        auto field_size = table.field_size();

        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + key_size + field_size + sizeof(uint64_t));

        auto stringPiece = inputPiece.toStringPiece();
        uint64_t tid;

        replicateTxn->stringPieces.push_back(stringPiece);
        char *data = new char[stringPiece.length()];
        memcpy(data, stringPiece.data(), stringPiece.length());
        replicateTxn->stringPieces.back().set(data);

        const void *key = replicateTxn->stringPieces.back().data();
        stringPiece.remove_prefix(key_size);
        auto valueStringPiece = stringPiece;

        Decoder dec(valueStringPiece);
        void *value = table.deserialize_value(dec, key);
        dec >> tid;

        SiloRWKey readKey;

        readKey.set_table_id(table_id);
        readKey.set_partition_id(partition_id);
        readKey.set_tid(tid);

        readKey.set_key(key);
        readKey.set_value(value);

        replicateTxn->add_to_read_set(readKey);

        // prepare response message header
        auto message_size = MessagePiece::get_header_size();
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::READ_REPLICATION_RESPONSE), message_size, table_id,
            partition_id);

        Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        responseMessage.flush();
    }

    static void read_replication_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::READ_REPLICATION_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        /*
         * The structure of a replication response: ()
         */
        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
        txn->pr_message_count++;
    }

    static void write_replication_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::WRITE_REPLICATION_REQUEST));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();
        auto field_size = table.field_size();

        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + key_size + field_size);

        auto stringPiece = inputPiece.toStringPiece();
        replicateTxn->stringPieces.push_back(stringPiece);
        char *data = new char[stringPiece.length()];
        memcpy(data, stringPiece.data(), stringPiece.length());
        replicateTxn->stringPieces.back().set(data);

        const void *key = replicateTxn->stringPieces.back().data();
        stringPiece.remove_prefix(key_size);
        auto valueStringPiece = stringPiece;

        Decoder dec(valueStringPiece);
        void *value = table.deserialize_value(dec, key);

        SiloRWKey writeKey;

        writeKey.set_table_id(table_id);
        writeKey.set_partition_id(partition_id);

        writeKey.set_key(key);
        writeKey.set_value(value);

        replicateTxn->add_to_write_set(writeKey);

        // prepare response message header
        auto message_size = MessagePiece::get_header_size();
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::WRITE_REPLICATION_RESPONSE), message_size, table_id,
            partition_id);

        Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        responseMessage.flush();
    }

    static void write_replication_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::WRITE_REPLICATION_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        /*
         * The structure of a replication response: ()
         */
        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
        txn->pr_message_count++;
    }

    static void search_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(LeFRMessage::SEARCH_REQUEST));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();
        auto value_size = table.value_size();

        auto stringPiece = inputPiece.toStringPiece();
        auto stringPiece_ = inputPiece.toStringPiece();

        uint32_t key_offset;
        uint32_t round_count;
        std::size_t source_coordinator_id;

        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size +
                                                      sizeof(key_offset) + sizeof(round_count) +
                                                      sizeof(source_coordinator_id));

        stringPiece.remove_prefix(key_size);
        lefr::Decoder dec(stringPiece);
        dec >> key_offset;
        dec >> round_count;
        dec >> source_coordinator_id;

        DCHECK(dec.size() == 0);

        if (round_count > 1) {
            uint64_t tid = 1000000;
            auto message_size = MessagePiece::get_header_size() + value_size + sizeof(uint64_t) +
                                sizeof(key_offset) + sizeof(round_count);
            auto message_piece_header = MessagePiece::construct_message_piece_header(
                static_cast<uint32_t>(LeFRMessage::SEARCH_RESPONSE), message_size, table_id,
                partition_id);

            lefr::Encoder encoder(responseMessage.data);
            encoder << message_piece_header;
            responseMessage.data.append(value_size, 0);
            encoder << tid << key_offset << round_count;

            responseMessage.flush();

            return;
        }

        subTxn->stringPieces.push_back(stringPiece_);
        char *data = new char[stringPiece_.length()];
        memcpy(data, stringPiece_.data(), stringPiece_.length());
        subTxn->stringPieces.back().set(data);

        // get row and offset
        const void *key = subTxn->stringPieces.back().data();
        auto row = table.search(key);

        SiloRWKey readKey;
        readKey.set_table_id(table_id);
        readKey.set_partition_id(partition_id);

        readKey.set_key(key);
        readKey.set_value(&std::get<1>(row));
        auto &readSet = subTxn->readSet;

        if (subTxn->status == TransactionStatus::BEGIN) {
            subTxn->status = TransactionStatus::INPROGRESS;
        }

        // prepare response message header
        auto message_size = MessagePiece::get_header_size() + value_size + sizeof(uint64_t) +
                            sizeof(key_offset) + sizeof(round_count);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::SEARCH_RESPONSE), message_size, table_id,
            partition_id);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;

        // reserve size for read
        responseMessage.data.append(value_size, 0);
        void *dest = &responseMessage.data[0] + responseMessage.data.size() - value_size;
        // read to message buffer
        auto tid = SiloHelper::read(row, dest, value_size);

        readKey.set_tid(tid);
        readSet.insert(readSet.begin(), readKey);

        encoder << tid << key_offset << round_count;

        responseMessage.flush();
    }

    static void search_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::SEARCH_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();
        auto value_size = table.value_size();

        uint64_t tid;
        uint32_t key_offset;
        uint32_t round_count;

        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + value_size +
                                                      sizeof(tid) + sizeof(key_offset) +
                                                      sizeof(round_count));

        StringPiece stringPiece = inputPiece.toStringPiece();
        stringPiece.remove_prefix(value_size);
        Decoder dec(stringPiece);
        dec >> tid >> key_offset >> round_count;

        if (round_count > 1) {
            DCHECK(txn->pendingResponses > 0);
            if (txn->pendingResponses != 0) {
                txn->pendingResponses--;
            }
            txn->network_size += inputPiece.get_message_length();
            return;
        }

        int index = txn->get_index_process_message_read(key_offset);

        if (index != -1) {
            txn->set_process_message_read(index, 1000000);
        }

        SiloRWKey &readKey = txn->readSet[key_offset];
        dec = Decoder(inputPiece.toStringPiece());
        dec.read_n_bytes(readKey.get_value(), value_size);
        readKey.set_tid(tid);
        readKey.clear_read_request_bit();
        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
        txn->rw_message_count++;
    }

    static void lock_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(LeFRMessage::LOCK_REQUEST));
        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        auto stringPiece = inputPiece.toStringPiece();

        uint32_t key_offset;
        uint32_t round_count;
        std::size_t coordinator_id;

        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size +
                                                      sizeof(key_offset) + sizeof(round_count) +
                                                      sizeof(coordinator_id));

        if (round_count > 1) {
            auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint64_t) +
                                sizeof(uint32_t) + sizeof(round_count);
            auto message_piece_header = MessagePiece::construct_message_piece_header(
                static_cast<uint32_t>(LeFRMessage::LOCK_RESPONSE), message_size, table_id,
                partition_id);

            bool success = false;
            uint64_t latest_tid = 1000000;

            lefr::Encoder encoder(responseMessage.data);
            encoder << message_piece_header;
            encoder << success << latest_tid << key_offset << round_count;
            responseMessage.flush();

            return;
        }

        const void *key = stringPiece.data();
        std::atomic<uint64_t> &tid = table.search_metadata(key);

        bool success;
        uint64_t latest_tid = SiloHelper::lock(tid, success);

        stringPiece.remove_prefix(key_size);
        lefr::Decoder dec(stringPiece);
        dec >> key_offset >> round_count >> coordinator_id;

        DCHECK(dec.size() == 0);

        // prepare response message header
        auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint64_t) +
                            sizeof(uint32_t) + sizeof(round_count);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::LOCK_RESPONSE), message_size, table_id,
            partition_id);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        encoder << success << latest_tid << key_offset << round_count;
        responseMessage.flush();
    }

    static void lock_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(LeFRMessage::LOCK_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        bool success;
        uint64_t latest_tid;
        uint32_t key_offset;
        uint32_t round_count;

        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                      sizeof(success) + sizeof(latest_tid) +
                                                      sizeof(key_offset) + sizeof(round_count));

        StringPiece stringPiece = inputPiece.toStringPiece();
        Decoder dec(stringPiece);
        dec >> success >> latest_tid >> key_offset >> round_count;

        if (round_count > 1) {
            DCHECK(txn->pendingResponses > 0);
            if (txn->pendingResponses != 0) {
                txn->pendingResponses--;
            }
            txn->network_size += inputPiece.get_message_length();

            return;
        }

        DCHECK(dec.size() == 0);

        SiloRWKey &writeKey = txn->writeSet[key_offset];

        bool tid_changed = false;

        if (success) {
            SiloRWKey *readKey = txn->get_read_key(writeKey.get_key());

            DCHECK(readKey != nullptr);

            uint64_t tid_on_read = readKey->get_tid();
            if (latest_tid != tid_on_read) {
                tid_changed = true;
            }
            writeKey.set_tid(latest_tid);
            writeKey.set_write_lock_bit();
        }

        int index = txn->get_index_process_message_write(key_offset);
        if (index != -1) {
            txn->set_process_message_write(index, 1000000);
        }

        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();

        if (!success || tid_changed) {
            txn->abort_lock = true;
        }
    }

    static void read_validation_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::READ_VALIDATION_REQUEST));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size +
                                                      sizeof(uint32_t) + sizeof(uint64_t) +
                                                      sizeof(uint32_t) + sizeof(std::size_t));

        auto stringPiece = inputPiece.toStringPiece();
        const void *key = stringPiece.data();
        auto latest_tid = table.search_metadata(key).load();
        stringPiece.remove_prefix(key_size);

        uint32_t key_offset;
        uint64_t tid;
        uint32_t round_count;
        std::size_t coordinator_id;
        Decoder dec(stringPiece);
        dec >> key_offset >> tid >> round_count >> coordinator_id;

        if (round_count > 1) {
            auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint32_t) +
                                sizeof(round_count);
            auto message_piece_header = MessagePiece::construct_message_piece_header(
                static_cast<uint32_t>(LeFRMessage::READ_VALIDATION_RESPONSE), message_size,
                table_id, partition_id);

            bool success = false;
            lefr::Encoder encoder(responseMessage.data);
            encoder << message_piece_header;
            encoder << success << key_offset << round_count;

            responseMessage.flush();
            return;
        }

        bool success = true;

        if (SiloHelper::remove_lock_bit(latest_tid) != tid) {
            success = false;
        }

        if (SiloHelper::is_locked(latest_tid)) {  // must be locked by others
            success = false;
        }

        // prepare response message header
        auto message_size =
            MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint32_t) + sizeof(round_count);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::READ_VALIDATION_RESPONSE), message_size, table_id,
            partition_id);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        encoder << success << key_offset << round_count;

        responseMessage.flush();
    }

    static void read_validation_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::READ_VALIDATION_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        bool success;
        uint32_t key_offset;
        uint32_t round_count;

        Decoder dec(inputPiece.toStringPiece());
        dec >> success >> key_offset >> round_count;

        if (round_count > 1) {
            DCHECK(txn->pendingResponses > 0);
            if (txn->pendingResponses != 0) {
                txn->pendingResponses--;
            }
            txn->network_size += inputPiece.get_message_length();
            return;
        }

        SiloRWKey &readKey = txn->readSet[key_offset];
        int index = txn->get_index_process_message_read(key_offset);
        if (index != -1) {
            txn->set_process_message_read(index, 1000000);
        }

        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();

        if (!success) {
            txn->abort_read_validation = true;
        }
    }

    static void abort_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(LeFRMessage::ABORT_REQUEST));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size);

        auto stringPiece = inputPiece.toStringPiece();
        const void *key = stringPiece.data();
        std::atomic<uint64_t> &tid = table.search_metadata(key);

        // unlock the key
        SiloHelper::unlock(tid);
    }

    static void write_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(LeFRMessage::WRITE_REQUEST));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();
        auto field_size = table.field_size();

        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size +
                                                      field_size + sizeof(uint32_t) +
                                                      sizeof(std::size_t) + sizeof(std::size_t));

        auto stringPiece = inputPiece.toStringPiece();
        auto stringPiece_ = inputPiece.toStringPiece();
        uint32_t round_count;
        std::size_t coordinator_id;
        std::size_t key_offset;
        stringPiece_.remove_prefix(key_size);
        stringPiece_.remove_prefix(field_size);

        Decoder dec(stringPiece_);
        dec >> round_count;
        dec >> coordinator_id;
        dec >> key_offset;

        if (round_count > 1) {
            auto message_size =
                MessagePiece::get_header_size() + sizeof(key_offset) + sizeof(round_count);
            auto message_piece_header = MessagePiece::construct_message_piece_header(
                static_cast<uint32_t>(LeFRMessage::WRITE_RESPONSE), message_size, table_id,
                partition_id);

            lefr::Encoder encoder(responseMessage.data);
            encoder << message_piece_header << key_offset << round_count;
            responseMessage.flush();
            return;
        }

        const void *key = stringPiece.data();
        stringPiece.remove_prefix(key_size);

        table.deserialize_value(key, stringPiece);

        // prepare response message header
        auto message_size =
            MessagePiece::get_header_size() + sizeof(key_offset) + sizeof(round_count);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::WRITE_RESPONSE), message_size, table_id,
            partition_id);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header << key_offset << round_count;
        responseMessage.flush();
    }

    static void write_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(LeFRMessage::WRITE_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());

        std::size_t key_offset;
        uint32_t round_count;
        Decoder dec(inputPiece.toStringPiece());
        dec >> key_offset >> round_count;

        if (round_count > 1) {
            DCHECK(txn->pendingResponses > 0);
            if (txn->pendingResponses != 0) {
                txn->pendingResponses--;
            }

            txn->network_size += inputPiece.get_message_length();
            return;
        }
        int index = txn->get_index_process_message_write(key_offset);

        if (index != -1) {
            txn->set_process_message_write(index, 1000000);
        }
        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
    }

    static void replication_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::REPLICATION_REQUEST));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();
        auto field_size = table.field_size();

        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + key_size + field_size + sizeof(uint64_t) +
                   sizeof(uint32_t) + sizeof(std::size_t) + sizeof(std::size_t));

        auto stringPiece = inputPiece.toStringPiece();

        const void *key = stringPiece.data();
        stringPiece.remove_prefix(key_size);
        auto valueStringPiece = stringPiece;
        stringPiece.remove_prefix(field_size);

        uint64_t commit_tid;
        uint32_t round_count;
        std::size_t coordinator_id;
        std::size_t key_offset;

        Decoder dec(stringPiece);
        dec >> commit_tid;
        dec >> round_count;
        dec >> coordinator_id;
        dec >> key_offset;

        DCHECK(dec.size() == 0);

        if (round_count > 1) {
            auto message_size =
                MessagePiece::get_header_size() + sizeof(key_offset) + sizeof(round_count);
            auto message_piece_header = MessagePiece::construct_message_piece_header(
                static_cast<uint32_t>(LeFRMessage::REPLICATION_RESPONSE), message_size, table_id,
                partition_id);

            lefr::Encoder encoder(responseMessage.data);
            encoder << message_piece_header << key_offset << round_count;
            responseMessage.flush();
            return;
        }

        std::atomic<uint64_t> &tid = table.search_metadata(key);

        bool success;
        uint64_t last_tid = SiloHelper::lock(tid, success);

        table.deserialize_value(key, valueStringPiece);
        SiloHelper::unlock(tid, commit_tid);

        // prepare response message header
        auto message_size =
            MessagePiece::get_header_size() + sizeof(key_offset) + sizeof(round_count);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::REPLICATION_RESPONSE), message_size, table_id,
            partition_id);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header << key_offset << round_count;
        responseMessage.flush();
    }

    static void replication_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::REPLICATION_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        std::size_t key_offset;
        uint32_t round_count;
        Decoder dec(inputPiece.toStringPiece());
        dec >> key_offset >> round_count;

        if (round_count > 1) {
            DCHECK(txn->pendingResponses > 0);
            if (txn->pendingResponses != 0) {
                txn->pendingResponses--;
            }
            txn->network_size += inputPiece.get_message_length();
            return;
        }

        int index = txn->get_index_process_message_write_2(key_offset);
        if (index != -1) {
            txn->set_process_message_write_2(index, 1000000);
        }
        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        if (round_count == 0) {
            txn->network_size += inputPiece.get_message_length();
            txn->com_message_count++;
        }
    }

    static void release_lock_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::RELEASE_LOCK_REQUEST));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + key_size + sizeof(uint64_t));

        auto stringPiece = inputPiece.toStringPiece();
        const void *key = stringPiece.data();
        stringPiece.remove_prefix(key_size);

        uint64_t commit_tid;
        Decoder dec(stringPiece);
        dec >> commit_tid;
        DCHECK(dec.size() == 0);

        std::atomic<uint64_t> &tid = table.search_metadata(key);
        SiloHelper::unlock(tid);
    }

    static void sync_transaction_info_replication_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::SYNC_TRANSACTION_INFO_REPLICATION_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        uint32_t type;
        std::size_t coordinator_id;
        std::size_t partition_id;
        uint64_t seed;
        uint64_t startTime;

        Decoder dec(stringPiece);
        dec >> type;
        dec >> coordinator_id;
        dec >> partition_id;
        dec >> seed;
        dec >> startTime;

        DCHECK(dec.size() == 0);

        init_replicate_transaction(type, coordinator_id, partition_id, seed, startTime);

        // prepare response message header
        auto message_size = MessagePiece::get_header_size();
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::SYNC_REPLICATION_PER_TRANSACTION_RESPONSE),
            message_size, 0, 0);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;

        responseMessage.flush();
    }

    static void sync_replication_per_transaction_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::SYNC_REPLICATION_PER_TRANSACTION_RESPONSE));

        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
    }

    static void create_sub_txn_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::CREATE_SUB_TXN_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        std::size_t source_coordinator_id;
        std::size_t coordinator_id;
        std::size_t partition_id;
        uint32_t round_count;

        Decoder dec(stringPiece);
        dec >> source_coordinator_id;
        dec >> coordinator_id;
        dec >> partition_id;
        dec >> round_count;

        DCHECK(dec.size() == 0);

        if (round_count > 1) {
            auto message_size =
                MessagePiece::get_header_size() + sizeof(std::size_t) + sizeof(round_count);
            auto message_piece_header = MessagePiece::construct_message_piece_header(
                static_cast<uint32_t>(LeFRMessage::CREATE_SUB_TXN_RESPONSE), message_size, 0, 0);
            lefr::Encoder encoder(responseMessage.data);
            encoder << message_piece_header;
            encoder << coordinator_id << round_count;
            responseMessage.flush();
            return;
        }

        init_sub_transaction(source_coordinator_id, partition_id);

        // prepare response message header
        auto message_size =
            MessagePiece::get_header_size() + sizeof(std::size_t) + sizeof(round_count);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::CREATE_SUB_TXN_RESPONSE), message_size, 0, 0);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        encoder << coordinator_id << round_count;
        responseMessage.flush();
    }

    static void create_sub_txn_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::CREATE_SUB_TXN_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        std::size_t coordinator_id;
        uint32_t round_count;
        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + sizeof(std::size_t) + sizeof(round_count));

        Decoder dec(inputPiece.toStringPiece());
        dec >> coordinator_id >> round_count;

        if (round_count > 1) {
            DCHECK(txn->pendingResponses > 0);
            if (txn->pendingResponses != 0) {
                txn->pendingResponses--;
            }
            txn->network_size += inputPiece.get_message_length();
            return;
        }

        auto node = txn->nodeSet.find(coordinator_id);
        if (node != txn->nodeSet.end()) {
            node->second = true;
        }
        txn->is_create_sub[coordinator_id] = true;
        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
        txn->rw_message_count++;
    }

    static void add_remote_wset_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::ADD_REMOTE_WSET_REQUEST));
        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();
        auto field_size = table.field_size();

        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size +
                                                      field_size + sizeof(uint32_t) +
                                                      sizeof(uint32_t) + sizeof(std::size_t));

        auto stringPiece = inputPiece.toStringPiece();
        subTxn->stringPieces.push_back(stringPiece);
        char *data = new char[stringPiece.length()];
        memcpy(data, stringPiece.data(), stringPiece.length());
        subTxn->stringPieces.back().set(data);

        const void *key = subTxn->stringPieces.back().data();

        stringPiece.remove_prefix(key_size);
        auto valueStringPiece = stringPiece;

        Decoder dec(valueStringPiece);
        void *value = table.deserialize_value(dec, key);
        uint32_t key_offset;
        uint32_t round_count;
        std::size_t coordinator_id;

        dec >> key_offset;
        dec >> round_count;
        dec >> coordinator_id;

        if (round_count > 1) {
            auto message_size =
                MessagePiece::get_header_size() + sizeof(key_offset) + sizeof(round_count);
            auto message_piece_header = MessagePiece::construct_message_piece_header(
                static_cast<uint32_t>(LeFRMessage::ADD_REMOTE_WSET_RESPONSE), message_size,
                table_id, partition_id);
            Encoder encoder(responseMessage.data);
            encoder << message_piece_header;
            encoder << key_offset << round_count;
            responseMessage.flush();

            return;
        }

        SiloRWKey writeKey;

        writeKey.set_table_id(table_id);
        writeKey.set_partition_id(partition_id);

        writeKey.set_key(key);
        writeKey.set_value(value);

        auto &writeSet = subTxn->writeSet;
        writeSet.insert(writeSet.begin(), writeKey);

        if (subTxn->status == TransactionStatus::BEGIN) {
            subTxn->status = TransactionStatus::INPROGRESS;
        }

        // prepare response message header
        auto message_size =
            MessagePiece::get_header_size() + sizeof(key_offset) + sizeof(round_count);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::ADD_REMOTE_WSET_RESPONSE), message_size, table_id,
            partition_id);

        Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        encoder << key_offset << round_count;
        responseMessage.flush();
    }

    static void add_remote_wset_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::ADD_REMOTE_WSET_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());

        auto key_size = table.key_size();
        uint32_t key_offset;
        uint32_t round_count;

        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + sizeof(key_offset) + sizeof(round_count));

        Decoder dec(inputPiece.toStringPiece());
        dec >> key_offset >> round_count;

        if (round_count > 1) {
            DCHECK(txn->pendingResponses > 0);
            if (txn->pendingResponses != 0) {
                txn->pendingResponses--;
            }
            txn->network_size += inputPiece.get_message_length();
            return;
        }

        SiloRWKey &writeKey = txn->writeSet[key_offset];
        writeKey.clear_write_request_bit();
        int index = txn->get_index_process_message_write(key_offset);

        if (index != -1) {
            txn->set_process_message_write(index, 1000000);
        }

        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
        txn->rw_message_count++;
    }

    static void remote_lock_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::REMOTE_LOCK_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        std::size_t coordinator_id;
        uint32_t round_count;
        std::size_t source_coordinator;
        Decoder dec(stringPiece);
        dec >> coordinator_id;
        dec >> round_count;
        dec >> source_coordinator;

        if (round_count > 1) {
            auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint64_t) +
                                sizeof(std::size_t) + sizeof(round_count);
            auto message_piece_header = MessagePiece::construct_message_piece_header(
                static_cast<uint32_t>(LeFRMessage::REMOTE_LOCK_RESPONSE), message_size, 0, 0);
            bool success = false;
            uint64_t maxtid = 0;
            std::size_t coordinator_id = 0;

            lefr::Encoder encoder(responseMessage.data);
            encoder << message_piece_header;
            encoder << success;
            encoder << maxtid;
            encoder << coordinator_id << round_count;
            responseMessage.flush();

            return;
        }

        bool success = subTxn->lock_write_set();
        if (subTxn->status == TransactionStatus::INPROGRESS) {
            subTxn->status = TransactionStatus::COMMITTING;
        }

        // prepare response message header
        auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint64_t) +
                            sizeof(std::size_t) + sizeof(round_count);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::REMOTE_LOCK_RESPONSE), message_size, 0, 0);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        encoder << success;
        encoder << subTxn->max_tid;
        encoder << coordinator_id << round_count;
        responseMessage.flush();
    }

    static void remote_lock_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::REMOTE_LOCK_RESPONSE));
        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + sizeof(bool) +
                                                      sizeof(uint64_t) + sizeof(std::size_t) +
                                                      sizeof(uint32_t));

        bool success;
        uint64_t max_tid;
        std::size_t coordinator_id;
        uint32_t round_count;

        Decoder dec(inputPiece.toStringPiece());
        dec >> success;
        dec >> max_tid;
        dec >> coordinator_id >> round_count;

        if (round_count > 1) {
            DCHECK(txn->pendingResponses > 0);
            if (txn->pendingResponses != 0) {
                txn->pendingResponses--;
            }
            txn->network_size += inputPiece.get_message_length();
            return;
        }

        txn->lock_remote[coordinator_id] = false;
        txn->abort_lock = success;
        txn->max_tid = std::max(txn->max_tid, max_tid);
        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
        txn->pr_message_count++;
    }

    static void remote_read_validation_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::REMOTE_READ_VALIDATION_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        std::size_t coordinator_id;
        uint32_t round_count;
        std::size_t source_coordinator;
        Decoder dec(stringPiece);
        dec >> coordinator_id;
        dec >> round_count;
        dec >> source_coordinator;

        if (round_count > 1) {
            auto message_size = MessagePiece::get_header_size() + sizeof(bool) +
                                sizeof(std::size_t) + sizeof(round_count);
            auto message_piece_header = MessagePiece::construct_message_piece_header(
                static_cast<uint32_t>(LeFRMessage::REMOTE_READ_VALIDATION_RESPONSE), message_size,
                0, 0);

            bool success = false;
            lefr::Encoder encoder(responseMessage.data);
            encoder << message_piece_header;
            encoder << success;
            encoder << coordinator_id << round_count;
            responseMessage.flush();
        }

        bool success = subTxn->validate_read_set();
        if (subTxn->status == TransactionStatus::INPROGRESS) {
            subTxn->status = TransactionStatus::COMMITTING;
        }

        // prepare response message header
        auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(std::size_t) +
                            sizeof(round_count);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::REMOTE_READ_VALIDATION_RESPONSE), message_size, 0,
            0);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        encoder << success;
        encoder << coordinator_id << round_count;
        responseMessage.flush();
    }

    static void remote_read_validation_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::REMOTE_READ_VALIDATION_RESPONSE));
        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + sizeof(bool) +
                                                      sizeof(std::size_t) + sizeof(uint32_t));

        bool success;
        std::size_t coordinator_id;
        uint32_t round_count;
        Decoder dec(inputPiece.toStringPiece());
        dec >> success;
        dec >> coordinator_id >> round_count;

        if (round_count > 1) {
            DCHECK(txn->pendingResponses > 0);
            if (txn->pendingResponses != 0) {
                txn->pendingResponses--;
            }
            txn->network_size += inputPiece.get_message_length();
            return;
        }

        txn->read_remote_validation[coordinator_id] = false;
        txn->abort_read_validation = success;
        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
        txn->pr_message_count++;
    }

    static void remote_write_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::REMOTE_WRITE_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        std::size_t coordinator_id;
        uint32_t round_count;
        std::size_t source_coordinator_id;

        Decoder dec(stringPiece);
        dec >> coordinator_id;
        dec >> round_count;
        dec >> source_coordinator_id;

        if (round_count > 1) {
            auto message_size =
                MessagePiece::get_header_size() + sizeof(coordinator_id) + sizeof(round_count);
            auto message_piece_header = MessagePiece::construct_message_piece_header(
                static_cast<uint32_t>(LeFRMessage::REMOTE_WRITE_RESPONSE), message_size, 0, 0);

            lefr::Encoder encoder(responseMessage.data);
            encoder << message_piece_header << coordinator_id << round_count;
            responseMessage.flush();
            return;
        }
        subTxn->write_local();
        if (subTxn->status == TransactionStatus::COMMITTING) {
            subTxn->status = TransactionStatus::COMMIT;
        }

        // prepare response message header
        auto message_size =
            MessagePiece::get_header_size() + sizeof(coordinator_id) + sizeof(round_count);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::REMOTE_WRITE_RESPONSE), message_size, 0, 0);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header << coordinator_id << round_count;
        responseMessage.flush();
    }

    static void remote_write_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::REMOTE_WRITE_RESPONSE));
        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + sizeof(std::size_t) + sizeof(uint32_t));

        Decoder dec(inputPiece.toStringPiece());
        std::size_t source_coordinator_id;
        uint32_t round_count;
        dec >> source_coordinator_id >> round_count;

        if (round_count > 1) {
            DCHECK(txn->pendingResponses > 0);
            if (txn->pendingResponses != 0) {
                txn->pendingResponses--;
            }
            txn->network_size += inputPiece.get_message_length();
            return;
        }

        txn->remote_write[source_coordinator_id] = false;
        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
        txn->com_message_count++;
    }

    static void remote_release_lock_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::REMOTE_RELEASE_LOCK_REQUEST));
        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + sizeof(std::size_t) + sizeof(uint64_t));

        auto stringPiece = inputPiece.toStringPiece();
        std::size_t coordinator_id;
        uint64_t commit_tid;
        Decoder dec(stringPiece);
        dec >> coordinator_id;
        dec >> commit_tid;

        subTxn->release_lock(commit_tid);
    }

    static void remote_abort_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::REMOTE_ABORT_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        std::size_t coordinator_id;
        Decoder dec(stringPiece);
        dec >> coordinator_id;
        if (subTxn != nullptr) {
            subTxn->abort();
            subTxn->status = TransactionStatus::ABORT;
        }
    }

    static void async_init_txn_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::ASYNC_INIT_TXN_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        uint32_t type;
        std::size_t coordinator_id;
        std::size_t partition_id;
        uint64_t seed;
        uint64_t startTime;

        Decoder dec(stringPiece);
        dec >> type;
        dec >> coordinator_id;
        dec >> partition_id;
        dec >> seed;
        dec >> startTime;

        DCHECK(dec.size() == 0);

        init_replicate_transaction(type, coordinator_id, partition_id, seed, startTime);
    }

    static void async_read_replication_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::ASYNC_READ_REPLICATION_REQUEST));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();
        auto field_size = table.field_size();

        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + key_size + field_size + sizeof(uint64_t));

        auto stringPiece = inputPiece.toStringPiece();
        uint64_t tid;

        replicateTxn->stringPieces.push_back(stringPiece);
        char *data = new char[stringPiece.length()];
        memcpy(data, stringPiece.data(), stringPiece.length());
        replicateTxn->stringPieces.back().set(data);

        const void *key = replicateTxn->stringPieces.back().data();
        stringPiece.remove_prefix(key_size);
        auto valueStringPiece = stringPiece;

        Decoder dec(valueStringPiece);
        void *value = table.deserialize_value(dec, key);
        dec >> tid;

        SiloRWKey readKey;

        readKey.set_table_id(table_id);
        readKey.set_partition_id(partition_id);
        readKey.set_tid(tid);

        readKey.set_key(key);
        readKey.set_value(value);

        replicateTxn->add_to_read_set(readKey);

        if (replicateTxn->status == TransactionStatus::BEGIN) {
            replicateTxn->status = TransactionStatus::INPROGRESS;
        }
    }

    static void async_write_replication_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::ASYNC_WRITE_REPLICATION_REQUEST));
        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();
        auto field_size = table.field_size();

        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + key_size + field_size);

        auto stringPiece = inputPiece.toStringPiece();
        replicateTxn->stringPieces.push_back(stringPiece);
        char *data = new char[stringPiece.length()];
        memcpy(data, stringPiece.data(), stringPiece.length());
        replicateTxn->stringPieces.back().set(data);

        const void *key = replicateTxn->stringPieces.back().data();
        stringPiece.remove_prefix(key_size);
        auto valueStringPiece = stringPiece;

        Decoder dec(valueStringPiece);
        void *value = table.deserialize_value(dec, key);

        SiloRWKey writeKey;

        writeKey.set_table_id(table_id);
        writeKey.set_partition_id(partition_id);

        writeKey.set_key(key);
        writeKey.set_value(value);

        replicateTxn->add_to_write_set(writeKey);

        if (replicateTxn->status == TransactionStatus::BEGIN) {
            replicateTxn->status = TransactionStatus::INPROGRESS;
        }
    }

    static void async_end_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::ASYNC_END_REQUEST));

        if (replicateTxn->status == TransactionStatus::INPROGRESS) {
            replicateTxn->status = TransactionStatus::READY_TO_COMMIT;
        }
    }

    static void async_replication_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::ASYNC_REPLICATION_REQUEST));
        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();
        auto field_size = table.field_size();

        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + key_size + field_size + sizeof(uint64_t) +
                   sizeof(uint32_t) + sizeof(std::size_t) + sizeof(std::size_t));

        auto stringPiece = inputPiece.toStringPiece();

        const void *key = stringPiece.data();
        stringPiece.remove_prefix(key_size);
        auto valueStringPiece = stringPiece;
        stringPiece.remove_prefix(field_size);

        uint64_t commit_tid;
        uint32_t round_count;
        std::size_t coordinator_id;
        std::size_t key_offset;

        Decoder dec(stringPiece);
        dec >> commit_tid;
        dec >> round_count;
        dec >> coordinator_id;
        dec >> key_offset;

        DCHECK(dec.size() == 0);

        std::atomic<uint64_t> &tid = table.search_metadata(key);

        bool success;
        uint64_t last_tid = SiloHelper::lock(tid, success);

        table.deserialize_value(key, valueStringPiece);
        SiloHelper::unlock(tid, commit_tid);
    }

    static void sync_decision_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::SYNC_DECISION_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        std::size_t coordinator_id;
        uint64_t tid;

        Decoder dec(stringPiece);
        dec >> coordinator_id;
        dec >> tid;

        DCHECK(dec.size() == 0);
        if (replicateTxn != nullptr) {
            replicateTxn->cur_tid = tid;
            replicateTxn->status = TransactionStatus::COMMIT;
        }

        // prepare response message header
        auto message_size = MessagePiece::get_header_size();
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(LeFRMessage::SYNC_DECISION_RESPONSE), message_size, 0, 0);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        responseMessage.flush();
    }

    static void sync_decision_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(LeFRMessage::SYNC_DECISION_RESPONSE));

        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        DCHECK(txn->pendingResponses >= 0);
        txn->network_size += inputPiece.get_message_length();
        txn->pr_message_count++;
    }

    static std::vector<std::function<
        void(MessagePiece, Message &, ITable &, Transaction *, Transaction *, Transaction *,
             std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>,
             std::function<void(uint64_t, uint32_t)>, std::function<void(std::size_t, std::size_t)>,
             Partitioner &)>>
    get_message_handlers() {
        std::vector<std::function<void(
            MessagePiece, Message &, ITable &, Transaction *, Transaction *, Transaction *,
            std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>,
            std::function<void(uint64_t, uint32_t)>, std::function<void(std::size_t, std::size_t)>,
            Partitioner &)>>
            v;
        v.resize(static_cast<int>(ControlMessage::NFIELDS));
        v.push_back(init_txn_request_handler);
        v.push_back(init_txn_response_handler);
        v.push_back(redo_request_handler);
        v.push_back(scanreplica_request_handler);
        v.push_back(read_replication_request_handler);
        v.push_back(read_replication_response_handler);
        v.push_back(write_replication_request_handler);
        v.push_back(write_replication_response_handler);
        v.push_back(search_request_handler);
        v.push_back(search_response_handler);
        v.push_back(lock_request_handler);
        v.push_back(lock_response_handler);
        v.push_back(read_validation_request_handler);
        v.push_back(read_validation_response_handler);
        v.push_back(abort_request_handler);
        v.push_back(write_request_handler);
        v.push_back(write_response_handler);
        v.push_back(replication_request_handler);
        v.push_back(replication_response_handler);
        v.push_back(release_lock_request_handler);
        v.push_back(sync_transaction_info_replication_request_handler);
        v.push_back(sync_replication_per_transaction_response_handler);
        v.push_back(create_sub_txn_request_handler);
        v.push_back(create_sub_txn_response_handler);
        v.push_back(add_remote_wset_request_handler);
        v.push_back(add_remote_wset_response_handler);
        v.push_back(remote_lock_request_handler);
        v.push_back(remote_lock_response_handler);
        v.push_back(remote_read_validation_request_handler);
        v.push_back(remote_read_validation_response_handler);
        v.push_back(remote_write_request_handler);
        v.push_back(remote_write_response_handler);
        v.push_back(remote_release_lock_request_handler);
        v.push_back(remote_abort_request_handler);
        v.push_back(async_init_txn_request_handler);
        v.push_back(async_read_replication_request_handler);
        v.push_back(async_write_replication_request_handler);
        v.push_back(async_end_request_handler);
        v.push_back(async_replication_request_handler);
        v.push_back(sync_decision_request_handler);
        v.push_back(sync_decision_response_handler);
        return v;
    }
};

}  // namespace lefr
