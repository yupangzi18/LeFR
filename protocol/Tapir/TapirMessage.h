#pragma once

#include <cstring>
#include <string>

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/LeFR/SiloHelper.h"
#include "protocol/LeFR/SiloRWKey.h"
#include "protocol/Tapir/TapirTransaction.h"

namespace lefr {

enum class TapirMessage {
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
    REMOTE_ABORT_RESPONSE,

    SYNC_DECISION_REQUEST,
    SYNC_DECISION_RESPONSE,
    NFIELDS
};

class TapirMessageFactory {
public:
    static std::size_t new_init_txn_message(Message &message, uint32_t type,
                                            std::size_t coordinator_id, std::size_t partition_id,
                                            uint64_t seed, uint64_t startTime) {
        /*
         * The structure of a init txn request: (type, coordinator id, partition id,
         * seed, start time)
         */

        auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t) +
                            sizeof(std::size_t) + sizeof(std::size_t) + sizeof(uint64_t) +
                            sizeof(uint64_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::INIT_TXN_REQUEST), message_size, 0, 0);
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
            static_cast<uint32_t>(TapirMessage::REDO_REQUEST), message_size, 0, 0);
        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << redo;
        message.flush();
        return message_size;
    }

    static std::size_t new_scanreplica_message(Message &message, uint32_t scanreplica,
                                               uint64_t startTime) {
        /*
         *  The structure of a scanreplica request: (scan replica, start time)
         */

        auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t) + sizeof(uint64_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::SCANREPLICA_REQUEST), message_size, 0, 0);
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
            static_cast<uint32_t>(TapirMessage::READ_REPLICATION_REQUEST), message_size,
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
            static_cast<uint32_t>(TapirMessage::WRITE_REPLICATION_REQUEST), message_size,
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
            static_cast<uint32_t>(TapirMessage::SEARCH_REQUEST), message_size, table.tableID(),
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
         * The structure of a lock request: (primary key, write key offset,
         * round count, coordinator id)
         */

        auto key_size = table.key_size();

        auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset) +
                            sizeof(round_count) + sizeof(coordinator_id);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::LOCK_REQUEST), message_size, table.tableID(),
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
         * The structure of a read validation request: (primary key, read key
         * offset, tid, round count, coordinator id)
         */

        auto key_size = table.key_size();

        auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset) +
                            sizeof(tid) + sizeof(round_count) + sizeof(coordinator_id);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::READ_VALIDATION_REQUEST), message_size,
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
            static_cast<uint32_t>(TapirMessage::ABORT_REQUEST), message_size, table.tableID(),
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
         * The structure of a write request: (primary key, field value, round count,
         * coordinator id, key offset)
         */

        auto key_size = table.key_size();
        auto field_size = table.field_size();

        auto message_size = MessagePiece::get_header_size() + key_size + field_size +
                            sizeof(round_count) + sizeof(std::size_t) + sizeof(key_offset);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::WRITE_REQUEST), message_size, table.tableID(),
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
         * The structure of a replication request: (primary key, field value,
         * commit_tid, round count, coordinator id, key offset)
         */

        auto key_size = table.key_size();
        auto field_size = table.field_size();

        auto message_size = MessagePiece::get_header_size() + key_size + field_size +
                            sizeof(commit_tid) + sizeof(round_count) + sizeof(coordinator_id) +
                            sizeof(key_offset);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::REPLICATION_REQUEST), message_size, table.tableID(),
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
         * The structure of a replication request: (primary key, commit tid)
         */

        auto key_size = table.key_size();

        auto message_size = MessagePiece::get_header_size() + key_size + sizeof(commit_tid);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::RELEASE_LOCK_REQUEST), message_size,
            table.tableID(), table.partitionID());

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
         * The structure of a sync transaction info replication request: (type, coordinator id,
         * partition id, seed)
         */

        auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t) +
                            sizeof(std::size_t) + sizeof(std::size_t) + sizeof(uint64_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::SYNC_TRANSACTION_INFO_REPLICATION_REQUEST),
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
         * The structure of a create sub txn request: (coordinator id, dest node id, partition id,
         * round count)
         */

        auto message_size = MessagePiece::get_header_size() + sizeof(std::size_t) +
                            sizeof(std::size_t) + sizeof(std::size_t) + sizeof(uint32_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::CREATE_SUB_TXN_REQUEST), message_size, 0, 0);
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
            static_cast<uint32_t>(TapirMessage::ADD_REMOTE_WSET_REQUEST), message_size,
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
         * The structure of a remote lock request: (coordinator id, round count, source coordinator)
         */
        auto message_size = MessagePiece::get_header_size() + sizeof(std::size_t) +
                            sizeof(round_count) + sizeof(source_coordinator);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::REMOTE_LOCK_REQUEST), message_size, 0, 0);

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
         * The structure of a remote read validate request: (coordinator id, round count, source
         * coordinator)
         */
        auto message_size = MessagePiece::get_header_size() + sizeof(std::size_t) +
                            sizeof(round_count) + sizeof(source_coordinator);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::REMOTE_READ_VALIDATION_REQUEST), message_size, 0,
            0);

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
                                                std::size_t source_coordinator_id,
                                                uint64_t commit_tid) {
        /*
         * The structure of a remote write request: (coordinator id, round count, source coordinator
         * id, commit tid)
         */
        auto message_size = MessagePiece::get_header_size() + sizeof(std::size_t) +
                            sizeof(round_count) + sizeof(coordinator_id) + sizeof(commit_tid);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::REMOTE_WRITE_REQUEST), message_size, 0, 0);

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << coordinator_id;
        encoder << round_count;
        encoder << source_coordinator_id;
        encoder << commit_tid;
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
            static_cast<uint32_t>(TapirMessage::REMOTE_RELEASE_LOCK_REQUEST), message_size, 0, 0);

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
            static_cast<uint32_t>(TapirMessage::REMOTE_ABORT_REQUEST), message_size, 0, 0);

        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << coordinator_id;
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
            static_cast<uint32_t>(TapirMessage::SYNC_DECISION_REQUEST), message_size, 0, 0);
        Encoder encoder(message.data);
        encoder << message_piece_header;
        encoder << coordinator_id;
        encoder << tid;
        message.flush();
        return message_size;
    }
};

template <class Database>
class TapirMessageHandler {
    using Transaction = TapirTransaction<Database>;

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
               static_cast<uint32_t>(TapirMessage::INIT_TXN_REQUEST));

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
            static_cast<uint32_t>(TapirMessage::INIT_TXN_RESPONSE), message_size, 0, 0);

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
               static_cast<uint32_t>(TapirMessage::INIT_TXN_RESPONSE));

        auto partitionNum = txn->partitioner.get_partition_num();
        for (auto k = 0u; k < partitionNum; k++) {
            if (0 != txn->ft_partitionSet.count(k)) {
                txn->ft_partitionSet[k] += 1;
            }
        }

        txn->pendingResponses--;
        DCHECK(txn->pendingResponses >= 0);
        txn->network_size += inputPiece.get_message_length();
    }

    static void redo_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TapirMessage::REDO_REQUEST));

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
               static_cast<uint32_t>(TapirMessage::SCANREPLICA_REQUEST));

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
               static_cast<uint32_t>(TapirMessage::READ_REPLICATION_REQUEST));

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
            static_cast<uint32_t>(TapirMessage::READ_REPLICATION_RESPONSE), message_size, table_id,
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
               static_cast<uint32_t>(TapirMessage::READ_REPLICATION_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        txn->pendingResponses--;
        txn->network_size += inputPiece.get_message_length();
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
               static_cast<uint32_t>(TapirMessage::WRITE_REPLICATION_REQUEST));

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
            static_cast<uint32_t>(TapirMessage::WRITE_REPLICATION_RESPONSE), message_size, table_id,
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
               static_cast<uint32_t>(TapirMessage::WRITE_REPLICATION_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        txn->pendingResponses--;
        txn->network_size += inputPiece.get_message_length();
    }

    static void search_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(TapirMessage::SEARCH_REQUEST));

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

        // record the related partition which subTxn would process
        if (0 == subTxn->validation_partitionSet.count(partition_id)) {
            subTxn->validation_partitionSet.insert(std::pair<uint32_t, uint32_t>(partition_id, 0));
        }

        readKey.set_key(key);
        readKey.set_value(&std::get<1>(row));
        auto &readSet = subTxn->readSet;

        void *dest = new char[value_size];
        // read to message buffer
        auto tid = SiloHelper::read(row, dest, value_size);

        readKey.set_tid(tid);
        readSet.insert(readSet.begin(), readKey);

        if (subTxn->status == TransactionStatus::BEGIN) {
            subTxn->status = TransactionStatus::INPROGRESS;
        }
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
               static_cast<uint32_t>(TapirMessage::SEARCH_RESPONSE));
        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();
        auto value_size = table.value_size();

        uint64_t tid;
        uint32_t key_offset;

        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + value_size + sizeof(tid) + sizeof(key_offset));

        StringPiece stringPiece = inputPiece.toStringPiece();
        stringPiece.remove_prefix(value_size);
        Decoder dec(stringPiece);
        dec >> tid >> key_offset;

        SiloRWKey &readKey = txn->readSet[key_offset];
        dec = Decoder(inputPiece.toStringPiece());
        dec.read_n_bytes(readKey.get_value(), value_size);
        readKey.set_tid(tid);
        readKey.clear_read_request_bit();
        txn->network_size += inputPiece.get_message_length();
    }

    static void lock_request_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TapirMessage::LOCK_REQUEST));

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

        const void *key = stringPiece.data();
        std::atomic<uint64_t> &tid = table.search_metadata(key);

        bool success;
        uint64_t latest_tid = SiloHelper::lock(tid, success);

        stringPiece.remove_prefix(key_size);
        lefr::Decoder dec(stringPiece);
        dec >> key_offset >> round_count >> coordinator_id;

        DCHECK(dec.size() == 0);

        if (!partitioner.get_coordinator_fault_id(coordinator_id) && round_count > 1) {
            return;
        }
        if (coordinator_id == 0 && round_count > 1) {
            return;
        }

        // prepare response message header
        auto message_size =
            MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint64_t) + sizeof(uint32_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::LOCK_RESPONSE), message_size, table_id,
            partition_id);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        encoder << success << latest_tid << key_offset;
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
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TapirMessage::LOCK_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        bool success;
        uint64_t latest_tid;
        uint32_t key_offset;

        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                      sizeof(success) + sizeof(latest_tid) +
                                                      sizeof(key_offset));

        StringPiece stringPiece = inputPiece.toStringPiece();
        Decoder dec(stringPiece);
        dec >> success >> latest_tid >> key_offset;

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

        txn->pendingResponses--;
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
               static_cast<uint32_t>(TapirMessage::READ_VALIDATION_REQUEST));

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

        if (!partitioner.get_coordinator_fault_id(coordinator_id) && round_count > 1) {
            return;
        }
        if (coordinator_id == 0 && round_count > 1) {
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
        auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint32_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::READ_VALIDATION_RESPONSE), message_size, table_id,
            partition_id);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        encoder << success << key_offset;

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
               static_cast<uint32_t>(TapirMessage::READ_VALIDATION_RESPONSE));
        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        bool success;
        uint32_t key_offset;

        Decoder dec(inputPiece.toStringPiece());
        dec >> success >> key_offset;

        SiloRWKey &readKey = txn->readSet[key_offset];

        txn->pendingResponses--;
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
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TapirMessage::ABORT_REQUEST));

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
        DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TapirMessage::WRITE_REQUEST));

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

        const void *key = stringPiece.data();
        stringPiece.remove_prefix(key_size);

        table.deserialize_value(key, stringPiece);

        // prepare response message header
        auto message_size = MessagePiece::get_header_size() + sizeof(key_offset);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::WRITE_RESPONSE), message_size, table_id,
            partition_id);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header << key_offset;
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
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(TapirMessage::WRITE_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());

        std::size_t key_offset;
        Decoder dec(inputPiece.toStringPiece());
        dec >> key_offset;
        txn->pendingResponses--;
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
               static_cast<uint32_t>(TapirMessage::REPLICATION_REQUEST));

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

        uint64_t last_tid = SiloHelper::lock(tid);
        table.deserialize_value(key, valueStringPiece);
        SiloHelper::unlock(tid, commit_tid);

        // prepare response message header
        auto message_size = MessagePiece::get_header_size() + sizeof(key_offset);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::REPLICATION_RESPONSE), message_size, table_id,
            partition_id);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header << key_offset;
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
               static_cast<uint32_t>(TapirMessage::REPLICATION_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        std::size_t key_offset;
        Decoder dec(inputPiece.toStringPiece());
        dec >> key_offset;
        txn->pendingResponses--;
        txn->network_size += inputPiece.get_message_length();
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
               static_cast<uint32_t>(TapirMessage::RELEASE_LOCK_REQUEST));

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
        SiloHelper::unlock(tid, commit_tid);
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
               static_cast<uint32_t>(TapirMessage::SYNC_TRANSACTION_INFO_REPLICATION_REQUEST));

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
            static_cast<uint32_t>(TapirMessage::SYNC_REPLICATION_PER_TRANSACTION_RESPONSE),
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
               static_cast<uint32_t>(TapirMessage::SYNC_REPLICATION_PER_TRANSACTION_RESPONSE));

        txn->pendingResponses--;
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
               static_cast<uint32_t>(TapirMessage::CREATE_SUB_TXN_REQUEST));

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
            return;
        }

        init_sub_transaction(source_coordinator_id, partition_id);
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
               static_cast<uint32_t>(TapirMessage::CREATE_SUB_TXN_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        std::size_t coordinator_id;
        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + sizeof(std::size_t));

        Decoder dec(inputPiece.toStringPiece());
        dec >> coordinator_id;

        auto node = txn->nodeSet.find(coordinator_id);
        if (node != txn->nodeSet.end()) {
            node->second = true;
        }

        txn->pendingResponses--;
        txn->network_size += inputPiece.get_message_length();
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
               static_cast<uint32_t>(TapirMessage::ADD_REMOTE_WSET_REQUEST));
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

        SiloRWKey writeKey;

        writeKey.set_table_id(table_id);
        writeKey.set_partition_id(partition_id);

        writeKey.set_key(key);
        writeKey.set_value(value);

        // record the partition which subTxn would process
        if (0 == subTxn->lock_partitionSet.count(partition_id)) {
            subTxn->lock_partitionSet.insert(std::pair<uint32_t, uint32_t>(partition_id, 0));
            subTxn->commit_partitionSet.insert(std::pair<uint32_t, uint32_t>(partition_id, 0));
        }
        auto &writeSet = subTxn->writeSet;
        writeSet.insert(writeSet.begin(), writeKey);

        if (subTxn->status == TransactionStatus::BEGIN) {
            subTxn->status = TransactionStatus::INPROGRESS;
        }
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
               static_cast<uint32_t>(TapirMessage::ADD_REMOTE_WSET_RESPONSE));

        auto table_id = inputPiece.get_table_id();
        auto partition_id = inputPiece.get_partition_id();
        DCHECK(table_id == table.tableID());
        DCHECK(partition_id == table.partitionID());
        auto key_size = table.key_size();

        uint32_t key_offset;

        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + sizeof(key_offset));

        Decoder dec(inputPiece.toStringPiece());
        dec >> key_offset;
        SiloRWKey &writeKey = txn->writeSet[key_offset];
        writeKey.clear_write_request_bit();

        txn->pendingResponses--;
        txn->network_size += inputPiece.get_message_length();
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
               static_cast<uint32_t>(TapirMessage::REMOTE_LOCK_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        std::size_t coordinator_id;
        uint32_t round_count;
        std::size_t source_coordinator;
        Decoder dec(stringPiece);
        dec >> coordinator_id;
        dec >> round_count;
        dec >> source_coordinator;

        subTxn->lock_write_set();

        if (subTxn->status == TransactionStatus::INPROGRESS) {
            subTxn->status = TransactionStatus::COMMITTING;
        }

        auto partitionNum = subTxn->partitioner.get_partition_num();

        // prepare response message header
        auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t) * partitionNum +
                            sizeof(uint64_t) + sizeof(std::size_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::REMOTE_LOCK_RESPONSE), message_size, 0, 0);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;

        uint32_t lock_result;

        for (auto k = 0u; k < partitionNum; k++) {
            if (0 != subTxn->lock_partitionSet.count(k)) {
                if (subTxn->lock_partitionSet[k] == 1) {
                    lock_result = 1;
                } else {
                    lock_result = 0;
                }
            } else {
                lock_result = 0;
            }
            encoder << lock_result;
        }

        encoder << subTxn->max_tid;
        encoder << coordinator_id;
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
               static_cast<uint32_t>(TapirMessage::REMOTE_LOCK_RESPONSE));

        auto partitionNum = txn->partitioner.get_partition_num();
        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                      sizeof(uint32_t) * partitionNum +
                                                      sizeof(uint64_t) + sizeof(std::size_t));

        // last txn's unprocessed messages shouldn't be executed
        if (txn->status == TransactionStatus::INPROGRESS) {
            return;
        }

        uint32_t lock_result;
        uint64_t max_tid;
        std::size_t coordinator_id;

        Decoder dec(inputPiece.toStringPiece());
        // collect every partition's result
        for (auto k = 0u; k < partitionNum; k++) {
            dec >> lock_result;
            if (0 != txn->lock_partitionSet.count(k)) {
                if (lock_result == 1) {
                    txn->lock_partitionSet[k] += 1;
                }
            }
        }

        dec >> max_tid;
        dec >> coordinator_id;

        txn->max_tid = std::max(txn->max_tid, max_tid);

        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
        txn->pr_network_size += inputPiece.get_message_length();
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
               static_cast<uint32_t>(TapirMessage::REMOTE_READ_VALIDATION_REQUEST));
        auto stringPiece = inputPiece.toStringPiece();
        std::size_t coordinator_id;
        uint32_t round_count;
        std::size_t source_coordinator;
        Decoder dec(stringPiece);
        dec >> coordinator_id;
        dec >> round_count;
        dec >> source_coordinator;

        subTxn->validate_read_set();
        if (subTxn->status == TransactionStatus::INPROGRESS) {
            subTxn->status = TransactionStatus::COMMITTING;
        }

        auto partitionNum = subTxn->partitioner.get_partition_num();

        // prepare response message header
        auto message_size =
            MessagePiece::get_header_size() + sizeof(uint32_t) * partitionNum + sizeof(std::size_t);
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::REMOTE_READ_VALIDATION_RESPONSE), message_size, 0,
            0);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header;
        for (auto k = 0u; k < partitionNum; k++) {
            if (0 != subTxn->validation_partitionSet.count(k)) {
                if (subTxn->validation_partitionSet[k] == 1) {
                    uint32_t related_failed = 1;
                    encoder << related_failed;
                } else {
                    uint32_t related_succeed = 0;
                    encoder << related_succeed;
                }
            } else {
                uint32_t unrelated = 0;
                encoder << unrelated;
            }
        }
        encoder << coordinator_id;
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
               static_cast<uint32_t>(TapirMessage::REMOTE_READ_VALIDATION_RESPONSE));
        auto partitionNum = txn->partitioner.get_partition_num();
        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                      sizeof(uint32_t) * partitionNum +
                                                      sizeof(std::size_t));

        // last txn's unprocessed messages shouldn't be executed
        if (txn->status == TransactionStatus::INPROGRESS) {
            return;
        }

        uint32_t validation_result;
        std::size_t coordinator_id;
        Decoder dec(inputPiece.toStringPiece());
        // collect every partition's result
        for (auto k = 0u; k < partitionNum; k++) {
            dec >> validation_result;
            if (0 != txn->validation_partitionSet.count(k)) {
                if (validation_result == 1) {
                    txn->validation_partitionSet[k] += 1;
                }
            }
        }
        dec >> coordinator_id;

        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
        txn->pr_network_size += inputPiece.get_message_length();
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
               static_cast<uint32_t>(TapirMessage::REMOTE_WRITE_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        std::size_t coordinator_id;
        uint32_t round_count;
        std::size_t source_coordinator_id;
        uint64_t commit_tid;

        Decoder dec(stringPiece);
        dec >> coordinator_id;
        dec >> round_count;
        dec >> source_coordinator_id;
        dec >> commit_tid;

        subTxn->write_local(commit_tid);

        if (subTxn->status == TransactionStatus::COMMITTING) {
            subTxn->status = TransactionStatus::COMMIT;
        }

        auto partitionNum = subTxn->partitioner.get_partition_num();

        // prepare response message header
        auto message_size = MessagePiece::get_header_size() + sizeof(coordinator_id) +
                            sizeof(uint32_t) * partitionNum;
        auto message_piece_header = MessagePiece::construct_message_piece_header(
            static_cast<uint32_t>(TapirMessage::REMOTE_WRITE_RESPONSE), message_size, 0, 0);

        lefr::Encoder encoder(responseMessage.data);
        encoder << message_piece_header << coordinator_id;
        for (auto k = 0u; k < partitionNum; k++) {
            if (0 != subTxn->commit_partitionSet.count(k)) {
                if (subTxn->commit_partitionSet[k] == 1) {
                    uint32_t related_failed = 1;
                    encoder << related_failed;
                } else {
                    uint32_t related_succeed = 0;
                    encoder << related_succeed;
                }
            } else {
                uint32_t unrelated = 0;
                encoder << unrelated;
            }
        }
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
               static_cast<uint32_t>(TapirMessage::REMOTE_WRITE_RESPONSE));

        auto partitionNum = txn->partitioner.get_partition_num();
        DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() +
                                                      sizeof(std::size_t) +
                                                      sizeof(uint32_t) * partitionNum);

        // last txn's unprocessed messages shouldn't be executed
        if (txn->status == TransactionStatus::INPROGRESS) {
            return;
        }

        uint32_t write_result;
        Decoder dec(inputPiece.toStringPiece());
        std::size_t source_coordinator_id;
        dec >> source_coordinator_id;
        // collect every partition's result
        for (auto k = 0u; k < partitionNum; k++) {
            dec >> write_result;
            if (0 != txn->commit_partitionSet.count(k)) {
                if (write_result == 1) {
                    txn->commit_partitionSet[k] += 1;
                }
            }
        }

        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
        txn->com_network_size += inputPiece.get_message_length();
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
               static_cast<uint32_t>(TapirMessage::REMOTE_RELEASE_LOCK_REQUEST));

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
               static_cast<uint32_t>(TapirMessage::REMOTE_ABORT_REQUEST));

        auto stringPiece = inputPiece.toStringPiece();
        std::size_t coordinator_id;
        Decoder dec(stringPiece);
        dec >> coordinator_id;
        if (subTxn != nullptr) {
            subTxn->abort();
            subTxn->status = TransactionStatus::ABORT;
        }
    }

    static void remote_abort_response_handler(
        MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
        Transaction *replicateTxn, Transaction *subTxn,
        std::function<void(uint32_t, std::size_t, std::size_t, uint64_t, uint64_t)>
            init_replicate_transaction,
        std::function<void(uint64_t, uint32_t)> init_scanreplica_transaction,
        std::function<void(std::size_t, std::size_t)> init_sub_transaction,
        Partitioner &partitioner) {
        DCHECK(inputPiece.get_message_type() ==
               static_cast<uint32_t>(TapirMessage::REMOTE_ABORT_RESPONSE));
        DCHECK(inputPiece.get_message_length() ==
               MessagePiece::get_header_size() + sizeof(std::size_t));

        Decoder dec(inputPiece.toStringPiece());
        std::size_t source_coordinator_id;

        dec >> source_coordinator_id;
        if (txn->abort_pendingResponses != 0) {
            txn->abort_pendingResponses--;
        }
        txn->network_size += inputPiece.get_message_length();
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
               static_cast<uint32_t>(TapirMessage::SYNC_DECISION_REQUEST));

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
            static_cast<uint32_t>(TapirMessage::SYNC_DECISION_RESPONSE), message_size, 0, 0);

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
               static_cast<uint32_t>(TapirMessage::SYNC_DECISION_RESPONSE));

        DCHECK(txn->pendingResponses > 0);
        if (txn->pendingResponses != 0) {
            txn->pendingResponses--;
        }
        DCHECK(txn->pendingResponses >= 0);
        txn->network_size += inputPiece.get_message_length();
        txn->pr_network_size += inputPiece.get_message_length();
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
        v.push_back(remote_abort_response_handler);
        v.push_back(sync_decision_request_handler);
        v.push_back(sync_decision_response_handler);
        return v;
    }
};

}  // namespace lefr
