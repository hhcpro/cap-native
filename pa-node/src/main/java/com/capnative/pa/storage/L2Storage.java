package com.capnative.pa.storage;

import com.capnative.common.model.L2Entry;
import com.capnative.common.model.ProtoConverters;
import com.capnative.common.storage.LmdbEnv;
import com.capnative.common.storage.LmdbSerializer;
import com.capnative.common.storage.proto.L2EntryProto;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Storage for L2 (final) log using LMDB with PROTOBUF serialization.
 * 100% binary format - no JSON.
 */
public class L2Storage {
    private static final Logger logger = LoggerFactory.getLogger(L2Storage.class);
    private static final String L2_LOG_DB = "l2_log";
    private static final String TX_ID_INDEX_DB = "l2_tx_id_idx";
    private static final String METADATA_DB = "l2_metadata";
    private static final String LATEST_OFFSET_KEY = "latest_offset";

    private final LmdbEnv env;
    private final Dbi<ByteBuffer> l2LogDb;
    private final Dbi<ByteBuffer> txIdIndexDb;
    private final Dbi<ByteBuffer> metadataDb;
    private final AtomicLong latestOffsetCache;
    private final Set<String> appliedTxIds;

    public L2Storage(LmdbEnv env) {
        this.env = env;
        this.l2LogDb = env.openDatabase(L2_LOG_DB, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
        this.txIdIndexDb = env.openDatabase(TX_ID_INDEX_DB, DbiFlags.MDB_CREATE);
        this.metadataDb = env.openDatabase(METADATA_DB, DbiFlags.MDB_CREATE);
        this.latestOffsetCache = new AtomicLong(loadLatestOffset());
        this.appliedTxIds = new HashSet<>();
        loadAppliedTxIds();
    }

    public boolean append(L2Entry entry) {
        return env.executeWrite(txn -> {
            if (appliedTxIds.contains(entry.getTxId())) {
                logger.debug("Skipping duplicate L2 entry: caOffset={}, txId={}",
                        entry.getCaOffset(), entry.getTxId());
                return false;
            }

            long expectedOffset = latestOffsetCache.get() + 1;
            if (entry.getCaOffset() != expectedOffset) {
                throw new IllegalArgumentException(
                        String.format("Non-sequential L2 offset: expected=%d, got=%d",
                                expectedOffset, entry.getCaOffset()));
            }

            ByteBuffer key = LmdbSerializer.serializeLong(entry.getCaOffset());
            ByteBuffer value = LmdbSerializer.serializeProto(ProtoConverters.toProto(entry));

            l2LogDb.put(txn, key, value);

            ByteBuffer txIdKey = LmdbSerializer.serializeString(entry.getTxId());
            txIdIndexDb.put(txn, txIdKey, key);

            ByteBuffer metaKey = LmdbSerializer.serializeString(LATEST_OFFSET_KEY);
            ByteBuffer metaValue = LmdbSerializer.serializeLong(entry.getCaOffset());
            metadataDb.put(txn, metaKey, metaValue);

            latestOffsetCache.set(entry.getCaOffset());
            appliedTxIds.add(entry.getTxId());

            env.sync();

            logger.debug("Appended L2 entry: caOffset={}, txId={}",
                    entry.getCaOffset(), entry.getTxId());

            return true;
        });
    }

    public Optional<L2Entry> get(long caOffset) {
        return env.executeRead(txn -> {
            ByteBuffer key = LmdbSerializer.serializeLong(caOffset);
            ByteBuffer value = l2LogDb.get(txn, key);

            if (value == null) {
                return Optional.empty();
            }

            L2EntryProto proto = LmdbSerializer.deserializeProto(value, L2EntryProto.parser());
            return Optional.of(ProtoConverters.fromProto(proto));
        });
    }

    public boolean isApplied(String txId) {
        return appliedTxIds.contains(txId);
    }

    public List<L2Entry> getRange(long startOffset, long endOffset) {
        return env.executeRead(txn -> {
            List<L2Entry> entries = new ArrayList<>();
            ByteBuffer startKey = LmdbSerializer.serializeLong(startOffset);

            try (Cursor<ByteBuffer> cursor = l2LogDb.openCursor(txn)) {
                if (!cursor.seek(startKey)) {
                    return entries;
                }

                do {
                    ByteBuffer key = cursor.key();
                    long offset = LmdbSerializer.deserializeLong(key);

                    if (offset > endOffset) {
                        break;
                    }

                    ByteBuffer value = cursor.val();
                    L2EntryProto proto = LmdbSerializer.deserializeProto(value, L2EntryProto.parser());
                    entries.add(ProtoConverters.fromProto(proto));

                } while (cursor.next());
            }

            return entries;
        });
    }

    public long getLatestOffset() {
        return latestOffsetCache.get();
    }

    public void truncateTo(long lastValidOffset) {
        env.executeWrite(txn -> {
            ByteBuffer startKey = LmdbSerializer.serializeLong(lastValidOffset + 1);

            List<ByteBuffer> keysToDelete = new ArrayList<>();
            List<String> txIdsToRemove = new ArrayList<>();

            try (Cursor<ByteBuffer> cursor = l2LogDb.openCursor(txn)) {
                if (cursor.seek(startKey)) {
                    do {
                        ByteBuffer key = cursor.key();
                        keysToDelete.add(ByteBuffer.allocateDirect(key.remaining()));
                        keysToDelete.get(keysToDelete.size() - 1).put(key);
                        keysToDelete.get(keysToDelete.size() - 1).flip();

                        ByteBuffer value = cursor.val();
                        L2EntryProto proto = LmdbSerializer.deserializeProto(value, L2EntryProto.parser());
                        L2Entry entry = ProtoConverters.fromProto(proto);
                        txIdsToRemove.add(entry.getTxId());

                    } while (cursor.next());
                }
            }

            for (ByteBuffer key : keysToDelete) {
                l2LogDb.delete(txn, key);
            }

            for (String txId : txIdsToRemove) {
                ByteBuffer txIdKey = LmdbSerializer.serializeString(txId);
                txIdIndexDb.delete(txn, txIdKey);
                appliedTxIds.remove(txId);
            }

            ByteBuffer metaKey = LmdbSerializer.serializeString(LATEST_OFFSET_KEY);
            ByteBuffer metaValue = LmdbSerializer.serializeLong(lastValidOffset);
            metadataDb.put(txn, metaKey, metaValue);

            latestOffsetCache.set(lastValidOffset);

            logger.warn("Truncated L2 to offset {}, deleted {} entries",
                    lastValidOffset, keysToDelete.size());

            return null;
        });
    }

    private long loadLatestOffset() {
        return env.executeRead(txn -> {
            ByteBuffer metaKey = LmdbSerializer.serializeString(LATEST_OFFSET_KEY);
            ByteBuffer metaValue = metadataDb.get(txn, metaKey);

            if (metaValue == null) {
                return -1L;
            }

            return LmdbSerializer.deserializeLong(metaValue);
        });
    }

    private void loadAppliedTxIds() {
        env.executeRead(txn -> {
            try (Cursor<ByteBuffer> cursor = txIdIndexDb.openCursor(txn)) {
                while (cursor.next()) {
                    ByteBuffer key = cursor.key();
                    String txId = LmdbSerializer.deserializeString(key);
                    appliedTxIds.add(txId);
                }
            }
            logger.info("Loaded {} applied tx_ids for idempotency", appliedTxIds.size());
            return null;
        });
    }

    public long getCount() {
        return env.executeRead(txn -> l2LogDb.stat(txn).entries);
    }
}
