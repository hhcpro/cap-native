package com.capnative.ca.storage;

import com.capnative.common.model.ConsolidatedLedgerEntry;
import com.capnative.common.model.ProtoConverters;
import com.capnative.common.storage.LmdbEnv;
import com.capnative.common.storage.LmdbSerializer;
import com.capnative.common.storage.proto.ConsolidatedLedgerEntryProto;
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
 * Storage for consolidated global ledger using LMDB with PROTOBUF serialization.
 * 100% binary format - no JSON.
 */
public class ConsolidatedLedgerStorage {
    private static final Logger logger = LoggerFactory.getLogger(ConsolidatedLedgerStorage.class);
    private static final String CONSOLIDATED_LEDGER_DB = "consolidated_ledger";
    private static final String METADATA_DB = "metadata";
    private static final String LATEST_OFFSET_KEY = "latest_offset";

    private final LmdbEnv env;
    private final Dbi<ByteBuffer> consolidatedLedgerDb;
    private final Dbi<ByteBuffer> metadataDb;
    private final AtomicLong latestOffsetCache;

    public ConsolidatedLedgerStorage(LmdbEnv env) {
        this.env = env;
        this.consolidatedLedgerDb = env.openDatabase(CONSOLIDATED_LEDGER_DB, DbiFlags.MDB_CREATE, DbiFlags.MDB_INTEGERKEY);
        this.metadataDb = env.openDatabase(METADATA_DB, DbiFlags.MDB_CREATE);
        this.latestOffsetCache = new AtomicLong(loadLatestOffset());
    }

    public void append(ConsolidatedLedgerEntry entry) {
        env.executeWrite(txn -> {
            long expectedOffset = latestOffsetCache.get() + 1;

            if (entry.getCaOffset() != expectedOffset) {
                throw new IllegalArgumentException(
                        String.format("Non-sequential offset: expected=%d, got=%d",
                                expectedOffset, entry.getCaOffset()));
            }

            ByteBuffer key = LmdbSerializer.serializeLong(entry.getCaOffset());
            ByteBuffer value = LmdbSerializer.serializeProto(ProtoConverters.toProto(entry));

            consolidatedLedgerDb.put(txn, key, value);

            ByteBuffer metaKey = LmdbSerializer.serializeString(LATEST_OFFSET_KEY);
            ByteBuffer metaValue = LmdbSerializer.serializeLong(entry.getCaOffset());
            metadataDb.put(txn, metaKey, metaValue);

            latestOffsetCache.set(entry.getCaOffset());

            logger.debug("Appended entry ca_offset={}, txId={}, agentId={}",
                    entry.getCaOffset(), entry.getTxId(), entry.getAgentId());

            return null;
        });
    }

    public Optional<ConsolidatedLedgerEntry> get(long caOffset) {
        return env.executeRead(txn -> {
            ByteBuffer key = LmdbSerializer.serializeLong(caOffset);
            ByteBuffer value = consolidatedLedgerDb.get(txn, key);

            if (value == null) {
                return Optional.empty();
            }

            ConsolidatedLedgerEntryProto proto = LmdbSerializer.deserializeProto(value, ConsolidatedLedgerEntryProto.parser());
            return Optional.of(ProtoConverters.fromProto(proto));
        });
    }

    public List<ConsolidatedLedgerEntry> getRange(long startOffset, long endOffset) {
        return env.executeRead(txn -> {
            List<ConsolidatedLedgerEntry> entries = new ArrayList<>();
            ByteBuffer startKey = LmdbSerializer.serializeLong(startOffset);

            try (Cursor<ByteBuffer> cursor = consolidatedLedgerDb.openCursor(txn)) {
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
                    ConsolidatedLedgerEntryProto proto = LmdbSerializer.deserializeProto(value, ConsolidatedLedgerEntryProto.parser());
                    entries.add(ProtoConverters.fromProto(proto));

                } while (cursor.next());
            }

            return entries;
        });
    }

    public long getLatestOffset() {
        return latestOffsetCache.get();
    }

    public long getNextOffset() {
        return latestOffsetCache.get() + 1;
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

    public long getCount() {
        return env.executeRead(txn -> consolidatedLedgerDb.stat(txn).entries);
    }
}
