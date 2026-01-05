package com.capnative.ca.service;

import com.capnative.ca.storage.ConsolidatedLedgerStorage;
import com.capnative.common.model.ConsolidatedLedgerEntry;
import com.capnative.common.offload.ObjectStorage;
import com.capnative.common.offload.proto.*;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Offload service using 100% BINARY PROTOBUF format.
 * Zero JSON - all segments and snapshots stored as compact binary.
 */
public class OffloadService {
    private static final Logger logger = LoggerFactory.getLogger(OffloadService.class);
    private static final int SEGMENT_SIZE = 1000;
    private static final int SNAPSHOT_INTERVAL = 10000;

    private final ConsolidatedLedgerStorage consolidatedLedgerStorage;
    private final ObjectStorage objectStorage;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final OffloadMetadata metadata = new OffloadMetadata();

    private long lastExportedOffset = -1;

    public OffloadService(ConsolidatedLedgerStorage consolidatedLedgerStorage, ObjectStorage objectStorage) {
        this.consolidatedLedgerStorage = consolidatedLedgerStorage;
        this.objectStorage = objectStorage;
    }

    public void start(long intervalSeconds) {
        logger.info("Starting offload service with interval {} seconds (BINARY PROTOBUF)", intervalSeconds);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                exportNewSegments();
            } catch (Exception e) {
                logger.error("Error during segment export", e);
            }
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

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

    private void exportNewSegments() {
        try {
            long latestOffset = consolidatedLedgerStorage.getLatestOffset();

            if (latestOffset <= lastExportedOffset) {
                logger.debug("No new data to export");
                return;
            }

            long startOffset = lastExportedOffset + 1;
            long endOffset = latestOffset;

            for (long chunkStart = startOffset; chunkStart <= endOffset; chunkStart += SEGMENT_SIZE) {
                long chunkEnd = Math.min(chunkStart + SEGMENT_SIZE - 1, endOffset);

                exportSegment(chunkStart, chunkEnd);

                if (chunkEnd % SNAPSHOT_INTERVAL < SEGMENT_SIZE && chunkEnd >= SNAPSHOT_INTERVAL) {
                    long snapshotOffset = (chunkEnd / SNAPSHOT_INTERVAL) * SNAPSHOT_INTERVAL;
                    exportSnapshot(snapshotOffset);
                }
            }

            lastExportedOffset = endOffset;
            logger.info("Exported segments up to offset {} (binary protobuf)", endOffset);

        } catch (Exception e) {
            logger.error("Error in exportNewSegments", e);
        }
    }

    private void exportSegment(long startOffset, long endOffset) {
        try {
            List<ConsolidatedLedgerEntry> entries = consolidatedLedgerStorage.getRange(startOffset, endOffset);

            if (entries.isEmpty()) {
                return;
            }

            // Convert to protobuf entries
            SegmentProto.Builder segmentBuilder = SegmentProto.newBuilder()
                    .setStartOffset(startOffset)
                    .setEndOffset(endOffset)
                    .setSchemaVersion(1)
                    .setCreatedAt(Instant.now().toEpochMilli());

            for (ConsolidatedLedgerEntry entry : entries) {
                ConsolidatedEntryProto protoEntry = ConsolidatedEntryProto.newBuilder()
                        .setCaOffset(entry.getCaOffset())
                        .setTxId(entry.getTxId())
                        .setAgentId(entry.getAgentId())
                        .setLedgerDiffs(ByteString.copyFrom(entry.getLedgerDiffs()))
                        .setMetadata(ByteString.copyFrom(entry.getMetadata()))
                        .setCommittedAt(entry.getCommittedAt().toEpochMilli())
                        .build();

                segmentBuilder.addEntries(protoEntry);
            }

            // Calculate checksum
            byte[] data = segmentBuilder.build().toByteArray();
            String checksum = calculateChecksum(data);

            // Set checksum and build final segment
            SegmentProto segment = segmentBuilder.setChecksum(checksum).build();

            // Write binary protobuf to storage
            String key = String.format("%d-%d.pb", startOffset, endOffset);
            objectStorage.putSegment(key, segment.toByteArray());

            metadata.addSegment(startOffset, endOffset, checksum);

            logger.debug("Exported binary segment [{}, {}] ({} bytes, checksum: {})",
                    startOffset, endOffset, segment.getSerializedSize(), checksum);

        } catch (Exception e) {
            logger.error("Error exporting segment [{}, {}]", startOffset, endOffset, e);
        }
    }

    private void exportSnapshot(long snapshotOffset) {
        try {
            // Build snapshot state (in production, compute actual state)
            Map<String, Object> state = new HashMap<>();
            state.put("offset", snapshotOffset);
            state.put("timestamp", Instant.now().toString());
            state.put("placeholder", "Implement actual state computation");

            // Convert state to bytes (could use nested protobuf here)
            byte[] stateBytes = state.toString().getBytes();  // TODO: Use proper serialization

            // Calculate Merkle root (placeholder)
            byte[] merkleRoot = calculateChecksum(stateBytes).getBytes();

            // Build snapshot
            SnapshotProto.Builder snapshotBuilder = SnapshotProto.newBuilder()
                    .setSnapshotOffset(snapshotOffset)
                    .setSnapshotTimestamp(Instant.now().toEpochMilli())
                    .setStatePayload(ByteString.copyFrom(stateBytes))
                    .setSchemaVersion(1)
                    .setMerkleRoot(ByteString.copyFrom(merkleRoot))
                    .setStats(SnapshotStats.newBuilder()
                            .setTotalTransactions(snapshotOffset + 1)
                            .setStateSizeBytes(stateBytes.length)
                            .build());

            byte[] data = snapshotBuilder.build().toByteArray();
            String checksum = calculateChecksum(data);

            SnapshotProto snapshot = snapshotBuilder.setChecksum(checksum).build();

            // Write binary protobuf to storage
            String key = String.format("%d.pb", snapshotOffset);
            objectStorage.putSnapshot(key, snapshot.toByteArray());

            metadata.setLatestSnapshot(snapshotOffset, checksum, merkleRoot);

            logger.info("Exported binary snapshot at offset {} ({} bytes, checksum: {})",
                    snapshotOffset, snapshot.getSerializedSize(), checksum);

        } catch (Exception e) {
            logger.error("Error exporting snapshot at offset {}", snapshotOffset, e);
        }
    }

    private String calculateChecksum(byte[] data) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(data);
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            throw new RuntimeException("Error calculating checksum", e);
        }
    }

    public OffloadMetadata getMetadata() {
        return metadata;
    }

    public static class OffloadMetadata {
        private Long latestSnapshotOffset;
        private String latestSnapshotChecksum;
        private byte[] latestSnapshotMerkleRoot;
        private final List<SegmentInfo> segments = new ArrayList<>();

        public synchronized void addSegment(long startOffset, long endOffset, String checksum) {
            segments.add(new SegmentInfo(startOffset, endOffset, checksum, null));
        }

        public synchronized void setLatestSnapshot(long offset, String checksum, byte[] merkleRoot) {
            this.latestSnapshotOffset = offset;
            this.latestSnapshotChecksum = checksum;
            this.latestSnapshotMerkleRoot = merkleRoot;
        }

        public synchronized Long getLatestSnapshotOffset() {
            return latestSnapshotOffset;
        }

        public synchronized String getLatestSnapshotChecksum() {
            return latestSnapshotChecksum;
        }

        public synchronized byte[] getLatestSnapshotMerkleRoot() {
            return latestSnapshotMerkleRoot;
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
        public final byte[] merkleRoot;

        public SegmentInfo(long startOffset, long endOffset, String checksum, byte[] merkleRoot) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.checksum = checksum;
            this.merkleRoot = merkleRoot;
        }
    }
}
