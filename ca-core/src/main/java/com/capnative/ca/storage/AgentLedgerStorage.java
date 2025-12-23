package com.capnative.ca.storage;

import com.capnative.common.model.AgentLedgerEntry;
import com.capnative.common.model.ProtoConverters;
import com.capnative.common.storage.LmdbEnv;
import com.capnative.common.storage.LmdbSerializer;
import com.capnative.common.storage.proto.AgentLedgerEntryProto;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Storage for per-agent ledgers using LMDB with PROTOBUF serialization.
 * 100% binary format - no JSON.
 */
public class AgentLedgerStorage {
    private static final Logger logger = LoggerFactory.getLogger(AgentLedgerStorage.class);
    private static final String AGENT_LEDGER_DB = "agent_ledger";
    private static final String TX_ID_INDEX_DB = "agent_ledger_tx_id_idx";

    private final LmdbEnv env;
    private final Dbi<ByteBuffer> agentLedgerDb;
    private final Dbi<ByteBuffer> txIdIndexDb;

    public AgentLedgerStorage(LmdbEnv env) {
        this.env = env;
        this.agentLedgerDb = env.openDatabase(AGENT_LEDGER_DB, DbiFlags.MDB_CREATE);
        this.txIdIndexDb = env.openDatabase(TX_ID_INDEX_DB, DbiFlags.MDB_CREATE);
    }

    public void append(AgentLedgerEntry entry) {
        env.executeWrite(txn -> {
            ByteBuffer key = LmdbSerializer.createAgentSeqKey(entry.getAgentId(), entry.getAgentSeq());
            ByteBuffer value = LmdbSerializer.serializeProto(ProtoConverters.toProto(entry));

            agentLedgerDb.put(txn, key, value);

            ByteBuffer txIdKey = LmdbSerializer.serializeString(entry.getTxId());
            txIdIndexDb.put(txn, txIdKey, key);

            logger.debug("Appended entry for agent={}, seq={}, txId={}, decision={}",
                    entry.getAgentId(), entry.getAgentSeq(), entry.getTxId(), entry.getDecision());

            return null;
        });
    }

    public void update(AgentLedgerEntry entry) {
        env.executeWrite(txn -> {
            ByteBuffer key = LmdbSerializer.createAgentSeqKey(entry.getAgentId(), entry.getAgentSeq());
            ByteBuffer value = LmdbSerializer.serializeProto(ProtoConverters.toProto(entry));

            agentLedgerDb.put(txn, key, value);

            logger.debug("Updated entry for agent={}, seq={}, txId={}, decision={}",
                    entry.getAgentId(), entry.getAgentSeq(), entry.getTxId(), entry.getDecision());

            return null;
        });
    }

    public Optional<AgentLedgerEntry> get(String agentId, long agentSeq) {
        return env.executeRead(txn -> {
            ByteBuffer key = LmdbSerializer.createAgentSeqKey(agentId, agentSeq);
            ByteBuffer value = agentLedgerDb.get(txn, key);

            if (value == null) {
                return Optional.empty();
            }

            AgentLedgerEntryProto proto = LmdbSerializer.deserializeProto(value, AgentLedgerEntryProto.parser());
            return Optional.of(ProtoConverters.fromProto(proto));
        });
    }

    public Optional<AgentLedgerEntry> getByTxId(String txId) {
        return env.executeRead(txn -> {
            ByteBuffer txIdKey = LmdbSerializer.serializeString(txId);
            ByteBuffer compositeKey = txIdIndexDb.get(txn, txIdKey);

            if (compositeKey == null) {
                return Optional.empty();
            }

            ByteBuffer value = agentLedgerDb.get(txn, compositeKey);
            if (value == null) {
                return Optional.empty();
            }

            AgentLedgerEntryProto proto = LmdbSerializer.deserializeProto(value, AgentLedgerEntryProto.parser());
            return Optional.of(ProtoConverters.fromProto(proto));
        });
    }

    public List<AgentLedgerEntry> getRange(String agentId, long startSeq, long endSeq) {
        return env.executeRead(txn -> {
            List<AgentLedgerEntry> entries = new ArrayList<>();
            ByteBuffer startKey = LmdbSerializer.createAgentSeqKey(agentId, startSeq);

            try (Cursor<ByteBuffer> cursor = agentLedgerDb.openCursor(txn)) {
                if (!cursor.seek(startKey)) {
                    return entries;
                }

                do {
                    ByteBuffer key = cursor.key();
                    String keyAgentId = LmdbSerializer.extractAgentId(key);

                    if (!keyAgentId.equals(agentId)) {
                        break;
                    }

                    long seq = LmdbSerializer.extractSequence(key);
                    if (seq > endSeq) {
                        break;
                    }

                    ByteBuffer value = cursor.val();
                    AgentLedgerEntryProto proto = LmdbSerializer.deserializeProto(value, AgentLedgerEntryProto.parser());
                    entries.add(ProtoConverters.fromProto(proto));

                } while (cursor.next());
            }

            return entries;
        });
    }

    public long getLatestSeq(String agentId) {
        return env.executeRead(txn -> {
            ByteBuffer prefix = LmdbSerializer.serializeString(agentId);

            try (Cursor<ByteBuffer> cursor = agentLedgerDb.openCursor(txn)) {
                if (!cursor.seek(prefix)) {
                    return -1L;
                }

                long latestSeq = -1L;

                do {
                    ByteBuffer key = cursor.key();
                    String keyAgentId = LmdbSerializer.extractAgentId(key);

                    if (!keyAgentId.equals(agentId)) {
                        if (latestSeq != -1L) {
                            break;
                        }
                        continue;
                    }

                    long seq = LmdbSerializer.extractSequence(key);
                    if (seq > latestSeq) {
                        latestSeq = seq;
                    }

                } while (cursor.next());

                return latestSeq;
            }
        });
    }
}
