package com.capnative.pa.storage;

import com.capnative.common.storage.LmdbEnv;
import com.capnative.common.storage.LmdbSerializer;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Storage for local projections (read models) built from L2.
 * This is a simple key-value store for account state.
 *
 * In production, you might have multiple projection databases
 * (accounts, balances, transaction history, etc.)
 */
public class ProjectionStorage {
    private static final Logger logger = LoggerFactory.getLogger(ProjectionStorage.class);
    private static final String ACCOUNT_DB = "accounts";
    private static final String METADATA_DB = "projection_metadata";
    private static final String VALID_UP_TO_KEY = "valid_up_to_offset";

    private final LmdbEnv env;
    private final Dbi<ByteBuffer> accountDb;
    private final Dbi<ByteBuffer> metadataDb;
    private final AtomicLong validUpToOffset;

    public ProjectionStorage(LmdbEnv env) {
        this.env = env;
        this.accountDb = env.openDatabase(ACCOUNT_DB, DbiFlags.MDB_CREATE);
        this.metadataDb = env.openDatabase(METADATA_DB, DbiFlags.MDB_CREATE);

        // Load valid_up_to_offset
        this.validUpToOffset = new AtomicLong(loadValidUpToOffset());
    }

    /**
     * Applies ledger diffs to projections.
     * This is where you would update account balances, transaction history, etc.
     *
     * @param caOffset CA offset being applied
     * @param ledgerDiffs Ledger diffs from L2 entry
     */
    public void applyLedgerDiffs(long caOffset, byte[] ledgerDiffs) {
        env.executeWrite(txn -> {
            // In production, parse ledgerDiffs and update appropriate projections
            // For now, this is a placeholder that just tracks the offset

            // Example: if ledgerDiffs contained account updates, you would do:
            // AccountUpdate update = parseLedgerDiffs(ledgerDiffs);
            // ByteBuffer accountKey = LmdbSerializer.serializeString(update.accountId);
            // ByteBuffer accountValue = LmdbSerializer.serializeJson(update.accountState);
            // accountDb.put(txn, accountKey, accountValue);

            // Update valid_up_to_offset
            ByteBuffer metaKey = LmdbSerializer.serializeString(VALID_UP_TO_KEY);
            ByteBuffer metaValue = LmdbSerializer.serializeLong(caOffset);
            metadataDb.put(txn, metaKey, metaValue);

            validUpToOffset.set(caOffset);

            logger.debug("Applied ledger diffs at caOffset={}", caOffset);

            return null;
        });
    }

    /**
     * Gets account state.
     *
     * @param accountId Account ID
     * @return Account state as bytes
     */
    public Optional<byte[]> getAccount(String accountId) {
        return env.executeRead(txn -> {
            ByteBuffer key = LmdbSerializer.serializeString(accountId);
            ByteBuffer value = accountDb.get(txn, key);

            if (value == null) {
                return Optional.empty();
            }

            return Optional.of(LmdbSerializer.deserializeBytes(value));
        });
    }

    /**
     * Updates or creates an account.
     *
     * @param accountId Account ID
     * @param accountData Account state
     */
    public void putAccount(String accountId, byte[] accountData) {
        env.executeWrite(txn -> {
            ByteBuffer key = LmdbSerializer.serializeString(accountId);
            ByteBuffer value = LmdbSerializer.serializeBytes(accountData);

            accountDb.put(txn, key, value);

            logger.debug("Updated account: accountId={}", accountId);

            return null;
        });
    }

    /**
     * Gets the CA offset up to which projections are valid.
     *
     * @return Valid up to offset
     */
    public long getValidUpToOffset() {
        return validUpToOffset.get();
    }

    /**
     * Loads the valid_up_to_offset from the database.
     *
     * @return Valid up to offset, or -1 if not set
     */
    private long loadValidUpToOffset() {
        return env.executeRead(txn -> {
            ByteBuffer metaKey = LmdbSerializer.serializeString(VALID_UP_TO_KEY);
            ByteBuffer metaValue = metadataDb.get(txn, metaKey);

            if (metaValue == null) {
                return -1L;
            }

            return LmdbSerializer.deserializeLong(metaValue);
        });
    }

    /**
     * Resets all projections (for repair scenarios).
     * WARNING: This is a destructive operation.
     */
    public void reset() {
        env.executeWrite(txn -> {
            // Clear all accounts
            accountDb.drop(txn, false);

            // Reset valid_up_to_offset
            ByteBuffer metaKey = LmdbSerializer.serializeString(VALID_UP_TO_KEY);
            ByteBuffer metaValue = LmdbSerializer.serializeLong(-1L);
            metadataDb.put(txn, metaKey, metaValue);

            validUpToOffset.set(-1L);

            logger.warn("Reset all projections");

            return null;
        });
    }
}
