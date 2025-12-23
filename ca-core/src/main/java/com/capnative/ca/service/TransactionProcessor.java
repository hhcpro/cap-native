package com.capnative.ca.service;

import com.capnative.ca.storage.AgentLedgerStorage;
import com.capnative.ca.storage.ConsolidatedLedgerStorage;
import com.capnative.common.model.AgentLedgerEntry;
import com.capnative.common.model.ConsolidatedLedgerEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;

/**
 * Core transaction processing logic for CA.
 * Handles validation, ACID transactions, and ledger updates.
 */
public class TransactionProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessor.class);

    private final AgentLedgerStorage agentLedgerStorage;
    private final ConsolidatedLedgerStorage consolidatedLedgerStorage;
    private final BusinessLogicValidator validator;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public TransactionProcessor(
            AgentLedgerStorage agentLedgerStorage,
            ConsolidatedLedgerStorage consolidatedLedgerStorage,
            BusinessLogicValidator validator) {
        this.agentLedgerStorage = agentLedgerStorage;
        this.consolidatedLedgerStorage = consolidatedLedgerStorage;
        this.validator = validator;
    }

    /**
     * Processes a transaction proposal from a PA node.
     * This is the critical path for all transactions.
     *
     * Steps:
     * 1. Append to per-agent ledger as PENDING
     * 2. Run business logic validation
     * 3. On success:
     *    a. Get next ca_offset
     *    b. Append to consolidated ledger
     *    c. Update per-agent ledger to COMMITTED
     * 4. On failure:
     *    a. Update per-agent ledger to REJECTED
     *
     * @param agentId Agent ID
     * @param agentSeq Agent sequence number
     * @param txId Transaction ID
     * @param payload Transaction payload
     * @return Transaction result
     */
    public synchronized TransactionResult processTransaction(
            String agentId,
            long agentSeq,
            String txId,
            byte[] payload) {

        logger.info("Processing transaction: agentId={}, agentSeq={}, txId={}",
                agentId, agentSeq, txId);

        // 1. Append to per-agent ledger as PENDING
        AgentLedgerEntry pendingEntry = AgentLedgerEntry.pending(agentId, agentSeq, txId, payload);
        agentLedgerStorage.append(pendingEntry);

        try {
            // 2. Run business logic validation
            ValidationResult validationResult = validator.validate(agentId, txId, payload);

            if (!validationResult.isValid()) {
                // 3. REJECTED path
                logger.warn("Transaction rejected: txId={}, reason={}",
                        txId, validationResult.getReason());

                AgentLedgerEntry rejectedEntry = pendingEntry.withRejected(validationResult.getReason());
                agentLedgerStorage.update(rejectedEntry);

                return TransactionResult.rejected(txId, validationResult.getReason());
            }

            // 4. COMMITTED path
            long caOffset = consolidatedLedgerStorage.getNextOffset();

            // Create consolidated entry
            ConsolidatedLedgerEntry consolidatedEntry = new ConsolidatedLedgerEntry(
                    caOffset,
                    txId,
                    agentId,
                    validationResult.getLedgerDiffs(),
                    createMetadata(agentId, agentSeq),
                    Instant.now()
            );

            // Append to consolidated ledger (atomic operation)
            consolidatedLedgerStorage.append(consolidatedEntry);

            // Update per-agent ledger
            AgentLedgerEntry committedEntry = pendingEntry.withCommitted(caOffset);
            agentLedgerStorage.update(committedEntry);

            logger.info("Transaction committed: txId={}, caOffset={}", txId, caOffset);

            return TransactionResult.committed(txId, caOffset);

        } catch (Exception e) {
            logger.error("Transaction processing failed: txId=" + txId, e);

            // Mark as rejected on unexpected errors
            AgentLedgerEntry rejectedEntry = pendingEntry.withRejected("Internal error: " + e.getMessage());
            agentLedgerStorage.update(rejectedEntry);

            return TransactionResult.rejected(txId, "Internal error: " + e.getMessage());
        }
    }

    private byte[] createMetadata(String agentId, long agentSeq) {
        try {
            Map<String, Object> metadata = Map.of(
                    "agentId", agentId,
                    "agentSeq", agentSeq,
                    "processedAt", Instant.now().toString()
            );
            return objectMapper.writeValueAsBytes(metadata);
        } catch (Exception e) {
            logger.error("Failed to create metadata", e);
            return new byte[0];
        }
    }

    /**
     * Result of transaction processing.
     */
    public static class TransactionResult {
        private final String txId;
        private final boolean committed;
        private final Long caOffset;
        private final String reason;

        private TransactionResult(String txId, boolean committed, Long caOffset, String reason) {
            this.txId = txId;
            this.committed = committed;
            this.caOffset = caOffset;
            this.reason = reason;
        }

        public static TransactionResult committed(String txId, long caOffset) {
            return new TransactionResult(txId, true, caOffset, null);
        }

        public static TransactionResult rejected(String txId, String reason) {
            return new TransactionResult(txId, false, null, reason);
        }

        public String getTxId() {
            return txId;
        }

        public boolean isCommitted() {
            return committed;
        }

        public Long getCaOffset() {
            return caOffset;
        }

        public String getReason() {
            return reason;
        }
    }

    /**
     * Result of business logic validation.
     */
    public static class ValidationResult {
        private final boolean valid;
        private final String reason;
        private final byte[] ledgerDiffs;

        private ValidationResult(boolean valid, String reason, byte[] ledgerDiffs) {
            this.valid = valid;
            this.reason = reason;
            this.ledgerDiffs = ledgerDiffs;
        }

        public static ValidationResult success(byte[] ledgerDiffs) {
            return new ValidationResult(true, null, ledgerDiffs);
        }

        public static ValidationResult failure(String reason) {
            return new ValidationResult(false, reason, null);
        }

        public boolean isValid() {
            return valid;
        }

        public String getReason() {
            return reason;
        }

        public byte[] getLedgerDiffs() {
            return ledgerDiffs;
        }
    }

    /**
     * Interface for business logic validation.
     * Implement this to define your transaction validation rules.
     */
    public interface BusinessLogicValidator {
        ValidationResult validate(String agentId, String txId, byte[] payload);
    }
}
