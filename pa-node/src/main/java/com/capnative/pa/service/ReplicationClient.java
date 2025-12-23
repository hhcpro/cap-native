package com.capnative.pa.service;

import com.capnative.common.grpc.ca.*;
import com.capnative.common.model.L1Entry;
import com.capnative.common.model.L2Entry;
import com.capnative.pa.storage.L1Storage;
import com.capnative.pa.storage.L2Storage;
import com.capnative.pa.storage.ProjectionStorage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Replication client for syncing with CA Core.
 *
 * Two main workflows:
 * 1. Proposal loop: Poll L1 for PENDING_CA, submit to CA, update L1 with results
 * 2. Application loop: Poll L1 for COMMITTED_CA, append to L2, apply to projections
 */
public class ReplicationClient {
    private static final Logger logger = LoggerFactory.getLogger(ReplicationClient.class);
    private static final long PROPOSAL_INTERVAL_MS = 100;  // Poll every 100ms
    private static final long APPLICATION_INTERVAL_MS = 50;  // Poll every 50ms
    private static final int MAX_BATCH_SIZE = 100;

    private final String agentId;
    private final L1Storage l1Storage;
    private final L2Storage l2Storage;
    private final ProjectionStorage projectionStorage;
    private final CaCoreGrpc.CaCoreBlockingStub caCoreStub;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final AtomicLong agentSeqCounter;

    public ReplicationClient(
            String agentId,
            String caHost,
            int caPort,
            L1Storage l1Storage,
            L2Storage l2Storage,
            ProjectionStorage projectionStorage) {
        this.agentId = agentId;
        this.l1Storage = l1Storage;
        this.l2Storage = l2Storage;
        this.projectionStorage = projectionStorage;

        // Create gRPC channel to CA
        ManagedChannel channel = ManagedChannelBuilder.forAddress(caHost, caPort)
                .usePlaintext()
                .build();
        this.caCoreStub = CaCoreGrpc.newBlockingStub(channel);

        // Initialize agent_seq counter
        this.agentSeqCounter = new AtomicLong(0);
    }

    /**
     * Starts the replication loops.
     */
    public void start() {
        logger.info("Starting replication client for agent={}", agentId);

        // Start proposal loop
        scheduler.scheduleAtFixedRate(
                this::runProposalLoop,
                0,
                PROPOSAL_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );

        // Start application loop
        scheduler.scheduleAtFixedRate(
                this::runApplicationLoop,
                0,
                APPLICATION_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Stops the replication loops.
     */
    public void stop() {
        logger.info("Stopping replication client");
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
     * Proposal loop: Submit pending L1 entries to CA.
     */
    private void runProposalLoop() {
        try {
            List<L1Entry> pendingEntries = l1Storage.getByStatus(L1Entry.L1Status.PENDING_CA);

            if (pendingEntries.isEmpty()) {
                return;
            }

            int processed = 0;
            for (L1Entry entry : pendingEntries) {
                if (processed >= MAX_BATCH_SIZE) {
                    break;
                }

                try {
                    // Submit to CA
                    ProposeRequest request = ProposeRequest.newBuilder()
                            .setAgentId(agentId)
                            .setAgentSeq(agentSeqCounter.incrementAndGet())
                            .setTxId(entry.getTxId())
                            .setPayload(com.google.protobuf.ByteString.copyFrom(entry.getPayload()))
                            .build();

                    ProposeResponse response = caCoreStub.proposeTransaction(request);

                    // Update L1 with result
                    if (response.getDecision() == ProposeResponse.Decision.COMMITTED) {
                        L1Entry updatedEntry = entry.withCommitted(response.getCaOffset());
                        l1Storage.update(updatedEntry);

                        logger.debug("Transaction committed: txId={}, caOffset={}",
                                entry.getTxId(), response.getCaOffset());
                    } else {
                        L1Entry updatedEntry = entry.withRejected(response.getReason());
                        l1Storage.update(updatedEntry);

                        logger.debug("Transaction rejected: txId={}, reason={}",
                                entry.getTxId(), response.getReason());
                    }

                    processed++;

                } catch (StatusRuntimeException e) {
                    logger.error("Failed to propose transaction: txId=" + entry.getTxId(), e);
                    // Continue with next entry
                }
            }

            if (processed > 0) {
                logger.debug("Proposed {} transactions to CA", processed);
            }

        } catch (Exception e) {
            logger.error("Error in proposal loop", e);
        }
    }

    /**
     * Application loop: Apply committed L1 entries to L2 and projections.
     */
    private void runApplicationLoop() {
        try {
            List<L1Entry> committedEntries = l1Storage.getByStatus(L1Entry.L1Status.COMMITTED_CA);

            if (committedEntries.isEmpty()) {
                return;
            }

            // Sort by ca_offset to ensure sequential application
            committedEntries.sort((a, b) -> Long.compare(a.getCaOffset(), b.getCaOffset()));

            int applied = 0;
            long currentL2Offset = l2Storage.getLatestOffset();

            for (L1Entry entry : committedEntries) {
                if (applied >= MAX_BATCH_SIZE) {
                    break;
                }

                // Only apply if this is the next sequential offset
                if (entry.getCaOffset() != currentL2Offset + 1) {
                    // Gap detected - stop here
                    if (entry.getCaOffset() > currentL2Offset + 1) {
                        logger.warn("Gap detected in L2: expected={}, got={}",
                                currentL2Offset + 1, entry.getCaOffset());
                    }
                    break;
                }

                try {
                    // Fetch ledger diffs from CA (in production, these would be in the entry)
                    // For now, we'll use the payload as ledger diffs
                    byte[] ledgerDiffs = entry.getPayload();

                    // Create L2 entry
                    L2Entry l2Entry = L2Entry.create(entry.getCaOffset(), entry.getTxId(), ledgerDiffs);

                    // Append to L2 (idempotent)
                    boolean appended = l2Storage.append(l2Entry);

                    if (appended) {
                        // Apply to projections
                        projectionStorage.applyLedgerDiffs(entry.getCaOffset(), ledgerDiffs);

                        currentL2Offset = entry.getCaOffset();
                        applied++;

                        logger.debug("Applied transaction to L2: txId={}, caOffset={}",
                                entry.getTxId(), entry.getCaOffset());
                    }

                } catch (Exception e) {
                    logger.error("Failed to apply transaction: txId=" + entry.getTxId(), e);
                    break;  // Stop on error to maintain consistency
                }
            }

            if (applied > 0) {
                logger.debug("Applied {} transactions to L2", applied);
            }

        } catch (Exception e) {
            logger.error("Error in application loop", e);
        }
    }

    /**
     * Gets the current lag behind CA.
     *
     * @return Lag in offsets
     */
    public long getCaLag() {
        try {
            GetLatestOffsetResponse response = caCoreStub.getLatestOffset(
                    GetLatestOffsetRequest.newBuilder().build()
            );

            long caLatest = response.getLatestOffset();
            long localLatest = l2Storage.getLatestOffset();

            return caLatest - localLatest;

        } catch (StatusRuntimeException e) {
            logger.error("Failed to get CA latest offset", e);
            return -1;
        }
    }

    /**
     * Gets the count of pending transactions.
     *
     * @return Pending count
     */
    public long getPendingCount() {
        return l1Storage.countByStatus(L1Entry.L1Status.PENDING_CA);
    }
}
