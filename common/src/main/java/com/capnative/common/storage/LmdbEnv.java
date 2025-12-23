package com.capnative.common.storage;

import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Wrapper around LMDB environment providing thread-safe access and lifecycle management.
 */
public class LmdbEnv implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(LmdbEnv.class);

    private final Env<ByteBuffer> env;
    private final File dbPath;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean closed = false;

    /**
     * Creates a new LMDB environment.
     *
     * @param dbPath Path to the database directory
     * @param maxDbSize Maximum database size in bytes
     * @param maxDbs Maximum number of named databases
     */
    public LmdbEnv(File dbPath, long maxDbSize, int maxDbs) {
        this.dbPath = dbPath;

        if (!dbPath.exists()) {
            if (!dbPath.mkdirs()) {
                throw new RuntimeException("Failed to create database directory: " + dbPath);
            }
        }

        this.env = Env.create()
                .setMapSize(maxDbSize)
                .setMaxDbs(maxDbs)
                .setMaxReaders(1024)
                .open(dbPath, EnvFlags.MDB_WRITEMAP, EnvFlags.MDB_NOSYNC);

        logger.info("Opened LMDB environment at {} with max size {} bytes", dbPath, maxDbSize);
    }

    /**
     * Opens or creates a database with the given name.
     *
     * @param name Database name
     * @param flags Optional database flags
     * @return Database handle
     */
    public Dbi<ByteBuffer> openDatabase(String name, DbiFlags... flags) {
        checkNotClosed();
        return env.openDbi(name, flags);
    }

    /**
     * Begins a read transaction.
     *
     * @return Read transaction
     */
    public Txn<ByteBuffer> txnRead() {
        checkNotClosed();
        lock.readLock().lock();
        try {
            return env.txnRead();
        } catch (Exception e) {
            lock.readLock().unlock();
            throw e;
        }
    }

    /**
     * Begins a write transaction.
     *
     * @return Write transaction
     */
    public Txn<ByteBuffer> txnWrite() {
        checkNotClosed();
        lock.writeLock().lock();
        try {
            return env.txnWrite();
        } catch (Exception e) {
            lock.writeLock().unlock();
            throw e;
        }
    }

    /**
     * Commits a read transaction and releases the lock.
     *
     * @param txn Transaction to commit
     */
    public void commitRead(Txn<ByteBuffer> txn) {
        try {
            txn.commit();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Commits a write transaction and releases the lock.
     *
     * @param txn Transaction to commit
     */
    public void commitWrite(Txn<ByteBuffer> txn) {
        try {
            txn.commit();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Aborts a read transaction and releases the lock.
     *
     * @param txn Transaction to abort
     */
    public void abortRead(Txn<ByteBuffer> txn) {
        try {
            txn.abort();
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Aborts a write transaction and releases the lock.
     *
     * @param txn Transaction to abort
     */
    public void abortWrite(Txn<ByteBuffer> txn) {
        try {
            txn.abort();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Executes a read operation within a transaction.
     *
     * @param operation Operation to execute
     * @param <T> Return type
     * @return Operation result
     */
    public <T> T executeRead(ReadOperation<T> operation) {
        Txn<ByteBuffer> txn = txnRead();
        try {
            T result = operation.execute(txn);
            commitRead(txn);
            return result;
        } catch (Exception e) {
            abortRead(txn);
            throw new RuntimeException("Read operation failed", e);
        }
    }

    /**
     * Executes a write operation within a transaction.
     *
     * @param operation Operation to execute
     * @param <T> Return type
     * @return Operation result
     */
    public <T> T executeWrite(WriteOperation<T> operation) {
        Txn<ByteBuffer> txn = txnWrite();
        try {
            T result = operation.execute(txn);
            commitWrite(txn);
            return result;
        } catch (Exception e) {
            abortWrite(txn);
            throw new RuntimeException("Write operation failed", e);
        }
    }

    /**
     * Forces a sync to disk.
     */
    public void sync() {
        checkNotClosed();
        env.sync(true);
    }

    /**
     * Gets statistics about the environment.
     *
     * @return Environment statistics
     */
    public String getStats() {
        return executeRead(txn -> env.stat().toString());
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("LMDB environment is closed");
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }

        lock.writeLock().lock();
        try {
            closed = true;
            env.close();
            logger.info("Closed LMDB environment at {}", dbPath);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @FunctionalInterface
    public interface ReadOperation<T> {
        T execute(Txn<ByteBuffer> txn);
    }

    @FunctionalInterface
    public interface WriteOperation<T> {
        T execute(Txn<ByteBuffer> txn);
    }
}
