package com.capnative.ca.grpc;

import com.capnative.ca.service.BatchTransactionProcessor;
import com.capnative.ca.service.TransactionProcessor;
import com.capnative.ca.storage.ConsolidatedLedgerStorage;
import com.capnative.common.grpc.ca.*;
import com.capnative.common.model.ConsolidatedLedgerEntry;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * gRPC service implementation for CA Core.
 * Supports both legacy TransactionProcessor and high-performance BatchTransactionProcessor.
 */
public class CaCoreServiceImpl extends CaCoreGrpc.CaCoreImplBase {
    private static final Logger logger = LoggerFactory.getLogger(CaCoreServiceImpl.class);

    private final TransactionProcessor transactionProcessor;
    private final BatchTransactionProcessor batchTransactionProcessor;
    private final ConsolidatedLedgerStorage consolidatedLedgerStorage;

    /**
     * Constructor using legacy single-threaded processor.
     */
    public CaCoreServiceImpl(
            TransactionProcessor transactionProcessor,
            ConsolidatedLedgerStorage consolidatedLedgerStorage) {
        this.transactionProcessor = transactionProcessor;
        this.batchTransactionProcessor = null;
        this.consolidatedLedgerStorage = consolidatedLedgerStorage;
    }

    /**
     * Constructor using high-performance batch processor (recommended).
     */
    public CaCoreServiceImpl(
            BatchTransactionProcessor batchTransactionProcessor,
            ConsolidatedLedgerStorage consolidatedLedgerStorage) {
        this.transactionProcessor = null;
        this.batchTransactionProcessor = batchTransactionProcessor;
        this.consolidatedLedgerStorage = consolidatedLedgerStorage;
    }

    @Override
    public void proposeTransaction(
            ProposeRequest request,
            StreamObserver<ProposeResponse> responseObserver) {

        try {
            logger.debug("Received ProposeTransaction: agentId={}, agentSeq={}, txId={}",
                    request.getAgentId(), request.getAgentSeq(), request.getTxId());

            // Validate request
            if (request.getAgentId().isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("agent_id is required")
                        .asException());
                return;
            }

            if (request.getTxId().isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("tx_id is required")
                        .asException());
                return;
            }

            // Process transaction using batch or legacy processor
            TransactionProcessor.TransactionResult result;

            if (batchTransactionProcessor != null) {
                // High-performance batch processing path
                CompletableFuture<TransactionProcessor.TransactionResult> future =
                        batchTransactionProcessor.submitTransaction(
                                request.getAgentId(),
                                request.getAgentSeq(),
                                request.getTxId(),
                                request.getPayload().toByteArray()
                        );

                // Wait for result (with timeout)
                result = future.get(30, TimeUnit.SECONDS);

            } else {
                // Legacy single-threaded path
                result = transactionProcessor.processTransaction(
                        request.getAgentId(),
                        request.getAgentSeq(),
                        request.getTxId(),
                        request.getPayload().toByteArray()
                );
            }

            // Build response
            ProposeResponse.Builder responseBuilder = ProposeResponse.newBuilder()
                    .setTxId(result.getTxId());

            if (result.isCommitted()) {
                responseBuilder
                        .setDecision(ProposeResponse.Decision.COMMITTED)
                        .setCaOffset(result.getCaOffset());

                logger.debug("Transaction committed: txId={}, caOffset={}",
                        result.getTxId(), result.getCaOffset());
            } else {
                responseBuilder
                        .setDecision(ProposeResponse.Decision.REJECTED)
                        .setReason(result.getReason());

                logger.debug("Transaction rejected: txId={}, reason={}",
                        result.getTxId(), result.getReason());
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error processing transaction", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .withCause(e)
                    .asException());
        }
    }

    @Override
    public void getLatestOffset(
            GetLatestOffsetRequest request,
            StreamObserver<GetLatestOffsetResponse> responseObserver) {

        try {
            long latestOffset = consolidatedLedgerStorage.getLatestOffset();

            GetLatestOffsetResponse response = GetLatestOffsetResponse.newBuilder()
                    .setLatestOffset(latestOffset)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error getting latest offset", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .withCause(e)
                    .asException());
        }
    }

    @Override
    public void getTransactionRange(
            GetTransactionRangeRequest request,
            StreamObserver<GetTransactionRangeResponse> responseObserver) {

        try {
            List<ConsolidatedLedgerEntry> entries = consolidatedLedgerStorage.getRange(
                    request.getStartOffset(),
                    request.getEndOffset()
            );

            GetTransactionRangeResponse.Builder responseBuilder = GetTransactionRangeResponse.newBuilder();

            for (ConsolidatedLedgerEntry entry : entries) {
                ConsolidatedEntry protoEntry = ConsolidatedEntry.newBuilder()
                        .setCaOffset(entry.getCaOffset())
                        .setTxId(entry.getTxId())
                        .setAgentId(entry.getAgentId())
                        .setLedgerDiffs(com.google.protobuf.ByteString.copyFrom(entry.getLedgerDiffs()))
                        .setMetadata(com.google.protobuf.ByteString.copyFrom(entry.getMetadata()))
                        .setCommittedAt(entry.getCommittedAt().toEpochMilli())
                        .build();

                responseBuilder.addEntries(protoEntry);
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error getting transaction range", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .withCause(e)
                    .asException());
        }
    }
}
