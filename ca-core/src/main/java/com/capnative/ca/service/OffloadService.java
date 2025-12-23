package com.capnative.ca.service;

import com.capnative.ca.storage.ConsolidatedLedgerStorage;
import com.capnative.common.model.ConsolidatedLedgerEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Offload service for exporting consolidated ledger to object storage.
 * Generates segments and snapshots for PA node bootstrap and repair.
 */
public class OffloadService {
    private static final Logger logger = LoggerFactory.getLogger(OffloadService.class);
    private static final int SEGMENT_SIZE = 1000;  // Entries per segment
    private static final int SNAPSHOT_INTERVAL = 10000;  // Snapshot every N entries

    private final ConsolidatedLedgerStorage consolidatedLedgerStorage;
    private final File storageRoot;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final OffloadMetadata metadata = new OffloadMetadata();

    private long lastExportedOffset = -1;

    public OffloadService(ConsolidatedLedgerStorage consolidatedLedgerStorage, File storageRoot) {
        this.consolidatedLedgerStorage = consolidatedLedgerStorage;
        this.storageRoot = storageRoot;

        // Create storage directories
        new File(storageRoot, "segments").mkdirs();
        new File(storageRoot, "snapshots").mkdirs();
    }

    /**
     * Starts the offload service with periodic exports.
     *
     * @param intervalSeconds Export interval in seconds
     */
    public void start(long intervalSeconds) {
        logger.info("Starting offload service with interval {} seconds", intervalSeconds);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                exportNewSegments();
            } catch (Exception e) {
                logger.error("Error during segment export", e);
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Stops the offload service.
     */
    public void stop() {
        logger.info("Stopping offload service");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Exports new segments since last export.
     */
    private void exportNewSegments() throws IOException {
        long latestOffset = consolidatedLedgerStorage.getLatestOffset();

        if (latestOffset <= lastExportedOffset) {
            logger.debug("No new data to export");
            return;
        }

        long startOffset = lastExportedOffset + 1;
        long endOffset = latestOffset;

        // Export in chunks
        for (long chunkStart = startOffset; chunkStart <= endOffset; chunkStart += SEGMENT_SIZE) {
            long chunkEnd = Math.min(chunkStart + SEGMENT_SIZE - 1, endOffset);

            exportSegment(chunkStart, chunkEnd);

            // Check if we should create a snapshot
            if (chunkEnd % SNAPSHOT_INTERVAL < SEGMENT_SIZE && chunkEnd >= SNAPSHOT_INTERVAL) {
                long snapshotOffset = (chunkEnd / SNAPSHOT_INTERVAL) * SNAPSHOT_INTERVAL;
                exportSnapshot(snapshotOffset);
            }
        }

        lastExportedOffset = endOffset;
        logger.info("Exported segments up to offset {}", endOffset);
    }

    /**
     * Exports a segment to storage.
     *
     * @param startOffset Start offset
     * @param endOffset End offset
     */
    private void exportSegment(long startOffset, long endOffset) throws IOException {
        List<ConsolidatedLedgerEntry> entries = consolidatedLedgerStorage.getRange(startOffset, endOffset);

        if (entries.isEmpty()) {
            return;
        }

        SegmentData segment = new SegmentData();
        segment.startOffset = startOffset;
        segment.endOffset = endOffset;
        segment.schemaVersion = 1;
        segment.entries = entries;

        byte[] data = objectMapper.writeValueAsBytes(segment);
        String checksum = calculateChecksum(data);
        segment.checksum = checksum;

        // Write to file
        File segmentFile = new File(storageRoot, String.format("segments/%d-%d.json", startOffset, endOffset));
        try (FileOutputStream fos = new FileOutputStream(segmentFile)) {
            fos.write(objectMapper.writeValueAsBytes(segment));
        }

        // Update metadata
        metadata.addSegment(startOffset, endOffset, checksum);

        logger.debug("Exported segment [{}, {}] with checksum {}", startOffset, endOffset, checksum);
    }

    /**
     * Exports a snapshot to storage.
     *
     * @param snapshotOffset Snapshot offset
     */
    private void exportSnapshot(long snapshotOffset) throws IOException {
        // In production, this would compute actual state at the given offset
        // For now, create a placeholder snapshot
        SnapshotData snapshot = new SnapshotData();
        snapshot.snapshotOffset = snapshotOffset;
        snapshot.snapshotTimestamp = Instant.now().toEpochMilli();
        snapshot.schemaVersion = 1;
        snapshot.statePayload = createStatePayload(snapshotOffset);

        byte[] data = objectMapper.writeValueAsBytes(snapshot);
        String checksum = calculateChecksum(data);
        snapshot.checksum = checksum;

        // Write to file
        File snapshotFile = new File(storageRoot, String.format("snapshots/%d.json", snapshotOffset));
        try (FileOutputStream fos = new FileOutputStream(snapshotFile)) {
            fos.write(objectMapper.writeValueAsBytes(snapshot));
        }

        // Update metadata
        metadata.setLatestSnapshot(snapshotOffset, checksum);

        logger.info("Exported snapshot at offset {} with checksum {}", snapshotOffset, checksum);
    }

    /**
     * Creates state payload for a snapshot (placeholder implementation).
     *
     * @param offset Snapshot offset
     * @return State payload as byte array
     */
    private byte[] createStatePayload(long offset) throws IOException {
        Map<String, Object> state = new HashMap<>();
        state.put("offset", offset);
        state.put("timestamp", Instant.now().toString());
        state.put("note", "Placeholder state - implement actual state computation");

        return objectMapper.writeValueAsBytes(state);
    }

    /**
     * Calculates SHA-256 checksum of data.
     *
     * @param data Data
     * @return Hex-encoded checksum
     */
    private String calculateChecksum(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            throw new RuntimeException("Error calculating checksum", e);
        }
    }

    /**
     * Gets the current offload metadata.
     *
     * @return Metadata
     */
    public OffloadMetadata getMetadata() {
        return metadata;
    }

    /**
     * Metadata about exported segments and snapshots.
     */
    public static class OffloadMetadata {
        private Long latestSnapshotOffset;
        private String latestSnapshotChecksum;
        private final List<SegmentInfo> segments = new ArrayList<>();

        public synchronized void addSegment(long startOffset, long endOffset, String checksum) {
            segments.add(new SegmentInfo(startOffset, endOffset, checksum));
        }

        public synchronized void setLatestSnapshot(long offset, String checksum) {
            this.latestSnapshotOffset = offset;
            this.latestSnapshotChecksum = checksum;
        }

        public synchronized Long getLatestSnapshotOffset() {
            return latestSnapshotOffset;
        }

        public synchronized String getLatestSnapshotChecksum() {
            return latestSnapshotChecksum;
        }

        public synchronized List<SegmentInfo> getSegments() {
            return new ArrayList<>(segments);
        }

        public synchronized List<SegmentInfo> getSegmentsSince(long offset) {
            return segments.stream()
                    .filter(s -> s.startOffset >= offset)
                    .toList();
        }
    }

    public static class SegmentInfo {
        public final long startOffset;
        public final long endOffset;
        public final String checksum;

        public SegmentInfo(long startOffset, long endOffset, String checksum) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.checksum = checksum;
        }
    }

    // Data classes for serialization
    private static class SegmentData {
        public long startOffset;
        public long endOffset;
        public String checksum;
        public int schemaVersion;
        public List<ConsolidatedLedgerEntry> entries;
    }

    private static class SnapshotData {
        public long snapshotOffset;
        public long snapshotTimestamp;
        public byte[] statePayload;
        public String checksum;
        public int schemaVersion;
    }
}
