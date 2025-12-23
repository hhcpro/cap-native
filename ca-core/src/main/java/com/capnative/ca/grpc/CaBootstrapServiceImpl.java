package com.capnative.ca.grpc;

import com.capnative.ca.service.OffloadService;
import com.capnative.common.grpc.ca.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

/**
 * gRPC service implementation for CA Bootstrap.
 * Provides access to segments and snapshots for PA node bootstrap and repair.
 */
public class CaBootstrapServiceImpl extends CaBootstrapGrpc.CaBootstrapImplBase {
    private static final Logger logger = LoggerFactory.getLogger(CaBootstrapServiceImpl.class);

    private final OffloadService offloadService;
    private final File storageRoot;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CaBootstrapServiceImpl(OffloadService offloadService, File storageRoot) {
        this.offloadService = offloadService;
        this.storageRoot = storageRoot;
    }

    @Override
    public void getTrustedCheckpoint(
            GetTrustedCheckpointRequest request,
            StreamObserver<TrustedCheckpoint> responseObserver) {

        try {
            OffloadService.OffloadMetadata metadata = offloadService.getMetadata();

            TrustedCheckpoint.Builder checkpointBuilder = TrustedCheckpoint.newBuilder();

            // Add latest snapshot if available
            if (metadata.getLatestSnapshotOffset() != null) {
                checkpointBuilder.setSnapshotOffset(metadata.getLatestSnapshotOffset());
                checkpointBuilder.setSnapshotChecksum(metadata.getLatestSnapshotChecksum());
            }

            // Add all segments
            List<OffloadService.SegmentInfo> segments = metadata.getSegments();
            for (OffloadService.SegmentInfo segment : segments) {
                SegmentRange range = SegmentRange.newBuilder()
                        .setStartOffset(segment.startOffset)
                        .setEndOffset(segment.endOffset)
                        .setChecksum(segment.checksum)
                        .build();
                checkpointBuilder.addSegments(range);
            }

            checkpointBuilder.setCreatedAt(System.currentTimeMillis());

            TrustedCheckpoint checkpoint = checkpointBuilder.build();
            logger.info("Returned trusted checkpoint: snapshotOffset={}, segmentCount={}",
                    checkpoint.getSnapshotOffset(), checkpoint.getSegmentsList().size());

            responseObserver.onNext(checkpoint);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error getting trusted checkpoint", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .withCause(e)
                    .asException());
        }
    }

    @Override
    public void getSegmentData(
            GetSegmentDataRequest request,
            StreamObserver<GetSegmentDataResponse> responseObserver) {

        try {
            File segmentFile = new File(storageRoot,
                    String.format("segments/%d-%d.json", request.getStartOffset(), request.getEndOffset()));

            if (!segmentFile.exists()) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Segment not found")
                        .asException());
                return;
            }

            byte[] data = Files.readAllBytes(segmentFile.toPath());
            SegmentData segmentData = objectMapper.readValue(data, SegmentData.class);

            GetSegmentDataResponse.Builder responseBuilder = GetSegmentDataResponse.newBuilder()
                    .setStartOffset(segmentData.startOffset)
                    .setEndOffset(segmentData.endOffset)
                    .setChecksum(segmentData.checksum)
                    .setSchemaVersion(segmentData.schemaVersion);

            // Convert entries to proto format
            if (segmentData.entries != null) {
                for (Object entryObj : segmentData.entries) {
                    // Parse as generic map since we're reading from JSON
                    @SuppressWarnings("unchecked")
                    var entryMap = (java.util.Map<String, Object>) entryObj;

                    ConsolidatedEntry.Builder entryBuilder = ConsolidatedEntry.newBuilder()
                            .setCaOffset(((Number) entryMap.get("caOffset")).longValue())
                            .setTxId((String) entryMap.get("txId"))
                            .setAgentId((String) entryMap.get("agentId"));

                    // Handle byte arrays
                    if (entryMap.containsKey("ledgerDiffs")) {
                        byte[] ledgerDiffs = objectMapper.convertValue(entryMap.get("ledgerDiffs"), byte[].class);
                        entryBuilder.setLedgerDiffs(ByteString.copyFrom(ledgerDiffs));
                    }

                    if (entryMap.containsKey("metadata")) {
                        byte[] metadata = objectMapper.convertValue(entryMap.get("metadata"), byte[].class);
                        entryBuilder.setMetadata(ByteString.copyFrom(metadata));
                    }

                    responseBuilder.addEntries(entryBuilder.build());
                }
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

            logger.debug("Returned segment data [{}, {}]", request.getStartOffset(), request.getEndOffset());

        } catch (Exception e) {
            logger.error("Error getting segment data", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .withCause(e)
                    .asException());
        }
    }

    @Override
    public void getSnapshotData(
            GetSnapshotDataRequest request,
            StreamObserver<GetSnapshotDataResponse> responseObserver) {

        try {
            File snapshotFile = new File(storageRoot,
                    String.format("snapshots/%d.json", request.getSnapshotOffset()));

            if (!snapshotFile.exists()) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Snapshot not found")
                        .asException());
                return;
            }

            byte[] data = Files.readAllBytes(snapshotFile.toPath());
            SnapshotData snapshotData = objectMapper.readValue(data, SnapshotData.class);

            GetSnapshotDataResponse response = GetSnapshotDataResponse.newBuilder()
                    .setSnapshotOffset(snapshotData.snapshotOffset)
                    .setSnapshotTimestamp(snapshotData.snapshotTimestamp)
                    .setStatePayload(ByteString.copyFrom(snapshotData.statePayload))
                    .setChecksum(snapshotData.checksum)
                    .setSchemaVersion(snapshotData.schemaVersion)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.debug("Returned snapshot data at offset {}", request.getSnapshotOffset());

        } catch (Exception e) {
            logger.error("Error getting snapshot data", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .withCause(e)
                    .asException());
        }
    }

    // Data classes for deserialization
    private static class SegmentData {
        public long startOffset;
        public long endOffset;
        public String checksum;
        public int schemaVersion;
        public List<Object> entries;  // Generic for JSON deserialization
    }

    private static class SnapshotData {
        public long snapshotOffset;
        public long snapshotTimestamp;
        public byte[] statePayload;
        public String checksum;
        public int schemaVersion;
    }
}
