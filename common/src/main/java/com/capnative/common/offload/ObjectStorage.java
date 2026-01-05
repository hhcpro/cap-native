package com.capnative.common.offload;

import java.io.IOException;
import java.util.List;

/**
 * Abstract interface for object storage (filesystem, S3, NetApp, etc.)
 * All implementations store binary protobuf data.
 */
public interface ObjectStorage {

    /**
     * Stores a segment.
     *
     * @param key Segment key (e.g., "segments/0-999.pb")
     * @param data Binary protobuf data
     * @throws IOException if storage fails
     */
    void putSegment(String key, byte[] data) throws IOException;

    /**
     * Retrieves a segment.
     *
     * @param key Segment key
     * @return Binary protobuf data
     * @throws IOException if not found or read fails
     */
    byte[] getSegment(String key) throws IOException;

    /**
     * Stores a snapshot.
     *
     * @param key Snapshot key (e.g., "snapshots/10000.pb")
     * @param data Binary protobuf data
     * @throws IOException if storage fails
     */
    void putSnapshot(String key, byte[] data) throws IOException;

    /**
     * Retrieves a snapshot.
     *
     * @param key Snapshot key
     * @return Binary protobuf data
     * @throws IOException if not found or read fails
     */
    byte[] getSnapshot(String key) throws IOException;

    /**
     * Lists all segments.
     *
     * @return List of segment keys
     * @throws IOException if listing fails
     */
    List<String> listSegments() throws IOException;

    /**
     * Lists all snapshots.
     *
     * @return List of snapshot keys
     * @throws IOException if listing fails
     */
    List<String> listSnapshots() throws IOException;

    /**
     * Deletes a segment.
     *
     * @param key Segment key
     * @throws IOException if deletion fails
     */
    void deleteSegment(String key) throws IOException;

    /**
     * Deletes a snapshot.
     *
     * @param key Snapshot key
     * @throws IOException if deletion fails
     */
    void deleteSnapshot(String key) throws IOException;

    /**
     * Checks if storage is healthy.
     *
     * @return true if healthy
     */
    boolean isHealthy();
}
