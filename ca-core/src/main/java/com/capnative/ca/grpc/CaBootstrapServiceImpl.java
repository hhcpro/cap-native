package com.capnative.ca.grpc;

import com.capnative.ca.service.OffloadService;
import com.capnative.common.grpc.ca.*;
import com.capnative.common.offload.ObjectStorage;
import com.capnative.common.offload.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * gRPC service for CA Bootstrap using 100% binary protobuf.
 * Zero JSON - all segment and snapshot data is binary.
 */
public class CaBootstrapServiceImpl extends CaBootstrapGrpc.CaBootstrapImplBase {
    private static final Logger logger = LoggerFactory.getLogger(CaBootstrapServiceImpl.class);

    private final OffloadService offloadService;
    private final ObjectStorage objectStorage;

    public CaBootstrapServiceImpl(OffloadService offloadService, ObjectStorage objectStorage) {
        this.offloadService = offloadService;
        this.objectStorage = objectStorage;
    }

    @Override
    public void getTrustedCheckpoint(
            GetTrustedCheckpointRequest request,
            StreamObserver<TrustedCheckpoint> responseObserver) {

        try {
            OffloadService.OffloadMetadata metadata = offloadService.getMetadata();

            TrustedCheckpoint.Builder checkpointBuilder = TrustedCheckpoint.newBuilder();

            if (metadata.getLatestSnapshotOffset() != null) {
                checkpointBuilder.setSnapshotOffset(metadata.getLatestSnapshotOffset());
                checkpointBuilder.setSnapshotChecksum(metadata.getLatestSnapshotChecksum());
            }

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
            logger.info("Returned trusted checkpoint (binary): snapshotOffset={}, segmentCount={}",
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
            String key = String.format("%d-%d.pb", request.getStartOffset(), request.getEndOffset());

            byte[] data = objectStorage.getSegment(key);

            // Parse binary protobuf
            SegmentProto segment = SegmentProto.parseFrom(data);

            GetSegmentDataResponse.Builder responseBuilder = GetSegmentDataResponse.newBuilder()
                    .setStartOffset(segment.getStartOffset())
                    .setEndOffset(segment.getEndOffset())
                    .setChecksum(segment.getChecksum())
                    .setSchemaVersion(segment.getSchemaVersion());

            for (ConsolidatedEntryProto entry : segment.getEntriesList()) {
                com.capnative.common.grpc.ca.ConsolidatedEntry grpcEntry =
                        com.capnative.common.grpc.ca.ConsolidatedEntry.newBuilder()
                        .setCaOffset(entry.getCaOffset())
                        .setTxId(entry.getTxId())
                        .setAgentId(entry.getAgentId())
                        .setLedgerDiffs(entry.getLedgerDiffs())
                        .setMetadata(entry.getMetadata())
                        .setCommittedAt(entry.getCommittedAt())
                        .build();

                responseBuilder.addEntries(grpcEntry);
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

            logger.debug("Returned binary segment data [{}, {}]", request.getStartOffset(), request.getEndOffset());

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
            String key = String.format("%d.pb", request.getSnapshotOffset());

            byte[] data = objectStorage.getSnapshot(key);

            // Parse binary protobuf
            SnapshotProto snapshot = SnapshotProto.parseFrom(data);

            GetSnapshotDataResponse response = GetSnapshotDataResponse.newBuilder()
                    .setSnapshotOffset(snapshot.getSnapshotOffset())
                    .setSnapshotTimestamp(snapshot.getSnapshotTimestamp())
                    .setStatePayload(snapshot.getStatePayload())
                    .setChecksum(snapshot.getChecksum())
                    .setSchemaVersion(snapshot.getSchemaVersion())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

            logger.debug("Returned binary snapshot data at offset {}", request.getSnapshotOffset());

        } catch (Exception e) {
            logger.error("Error getting snapshot data", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .withCause(e)
                    .asException());
        }
    }
}
