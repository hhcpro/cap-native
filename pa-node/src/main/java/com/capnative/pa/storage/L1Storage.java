package com.capnative.pa.storage;

import com.capnative.common.model.L1Entry;
import com.capnative.common.model.ProtoConverters;
import com.capnative.common.storage.LmdbEnv;
import com.capnative.common.storage.LmdbSerializer;
import com.capnative.common.storage.proto.L1EntryProto;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Storage for L1 (pending) log using LMDB with PROTOBUF serialization.
 * 100% binary format - no JSON.
 */
public class L1Storage {
    private static final Logger logger = LoggerFactory.getLogger(L1Storage.class);
    private static final String L1_LOG_DB = "l1_log";
    private static final String TX_ID_INDEX_DB = "l1_tx_id_idx";
    private static final String METADATA_DB = "l1_metadata";
    private static final String LATEST_SEQ_KEY = "latest_seq";

    private final LmdbEnv env;
    private final Dbi<ByteBuffer> l1LogDb;
    private final Dbi<ByteBuffer> txIdIndexDb;
    private final Dbi<ByteBuffer> metadataDb;
    private final AtomicLong latestSeqCache;

    public L1Storage(LmdbEnv env) {
        this.env = env;
        this.l1LogDb = env.openDatabase(L1_LOG_DB, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
        this.txIdIndexDb = env.openDatabase(TX_ID_INDEX_DB, DbiFlags.MDB_CREATE);
        this.metadataDb = env.openDatabase(METADATA_DB, DbiFlags.MDB_CREATE);
        this.latestSeqCache = new AtomicLong(loadLatestSeq());
    }

    public void append(L1Entry entry) {
        env.executeWrite(txn -> {
            ByteBuffer key = LmdbSerializer.serializeLong(entry.getL1Seq());
            ByteBuffer value = LmdbSerializer.serializeProto(ProtoConverters.toProto(entry));

            l1LogDb.put(txn, key, value);

            ByteBuffer txIdKey = LmdbSerializer.serializeString(entry.getTxId());
            txIdIndexDb.put(txn, txIdKey, key);

            ByteBuffer metaKey = LmdbSerializer.serializeString(LATEST_SEQ_KEY);
            ByteBuffer metaValue = LmdbSerializer.serializeLong(entry.getL1Seq());
            metadataDb.put(txn, metaKey, metaValue);

            latestSeqCache.set(entry.getL1Seq());

            env.sync();

            logger.debug("Appended L1 entry: l1Seq={}, txId={}, status={}",
                    entry.getL1Seq(), entry.getTxId(), entry.getStatus());

            return null;
        });
    }

    public void update(L1Entry entry) {
        env.executeWrite(txn -> {
            ByteBuffer key = LmdbSerializer.serializeLong(entry.getL1Seq());
            ByteBuffer value = LmdbSerializer.serializeProto(ProtoConverters.toProto(entry));

            l1LogDb.put(txn, key, value);

            env.sync();

            logger.debug("Updated L1 entry: l1Seq={}, txId={}, status={}",
                    entry.getL1Seq(), entry.getTxId(), entry.getStatus());

            return null;
        });
    }

    public Optional<L1Entry> get(long l1Seq) {
        return env.executeRead(txn -> {
            ByteBuffer key = LmdbSerializer.serializeLong(l1Seq);
            ByteBuffer value = l1LogDb.get(txn, key);

            if (value == null) {
                return Optional.empty();
            }

            L1EntryProto proto = LmdbSerializer.deserializeProto(value, L1EntryProto.parser());
            return Optional.of(ProtoConverters.fromProto(proto));
        });
    }

    public Optional<L1Entry> getByTxId(String txId) {
        return env.executeRead(txn -> {
            ByteBuffer txIdKey = LmdbSerializer.serializeString(txId);
            ByteBuffer seqBuffer = txIdIndexDb.get(txn, txIdKey);

            if (seqBuffer == null) {
                return Optional.empty();
            }

            long l1Seq = LmdbSerializer.deserializeLong(seqBuffer);
            ByteBuffer value = l1LogDb.get(txn, LmdbSerializer.serializeLong(l1Seq));

            if (value == null) {
                return Optional.empty();
            }

            L1EntryProto proto = LmdbSerializer.deserializeProto(value, L1EntryProto.parser());
            return Optional.of(ProtoConverters.fromProto(proto));
        });
    }

    public List<L1Entry> getByStatus(L1Entry.L1Status status) {
        return env.executeRead(txn -> {
            List<L1Entry> entries = new ArrayList<>();

            try (Cursor<ByteBuffer> cursor = l1LogDb.openCursor(txn)) {
                while (cursor.next()) {
                    ByteBuffer value = cursor.val();
                    L1EntryProto proto = LmdbSerializer.deserializeProto(value, L1EntryProto.parser());
                    L1Entry entry = ProtoConverters.fromProto(proto);

                    if (entry.getStatus() == status) {
                        entries.add(entry);
                    }
                }
            }

            return entries;
        });
    }

    public List<L1Entry> getRange(long startSeq, long endSeq) {
        return env.executeRead(txn -> {
            List<L1Entry> entries = new ArrayList<>();
            ByteBuffer startKey = LmdbSerializer.serializeLong(startSeq);

            try (Cursor<ByteBuffer> cursor = l1LogDb.openCursor(txn)) {
                if (!cursor.seek(startKey)) {
                    return entries;
                }

                do {
                    ByteBuffer key = cursor.key();
                    long seq = LmdbSerializer.deserializeLong(key);

                    if (seq > endSeq) {
                        break;
                    }

                    ByteBuffer value = cursor.val();
                    L1EntryProto proto = LmdbSerializer.deserializeProto(value, L1EntryProto.parser());
                    entries.add(ProtoConverters.fromProto(proto));

                } while (cursor.next());
            }

            return entries;
        });
    }

    public long getLatestSeq() {
        return latestSeqCache.get();
    }

    public long getNextSeq() {
        return latestSeqCache.get() + 1;
    }

    private long loadLatestSeq() {
        return env.executeRead(txn -> {
            ByteBuffer metaKey = LmdbSerializer.serializeString(LATEST_SEQ_KEY);
            ByteBuffer metaValue = metadataDb.get(txn, metaKey);

            if (metaValue == null) {
                return -1L;
            }

            return LmdbSerializer.deserializeLong(metaValue);
        });
    }

    public long countByStatus(L1Entry.L1Status status) {
        return getByStatus(status).size();
    }
}
