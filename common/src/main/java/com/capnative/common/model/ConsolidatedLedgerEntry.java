package com.capnative.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Represents an entry in the consolidated global ledger maintained by CA Core.
 * This is the authoritative, ordered log of all committed transactions.
 */
public class ConsolidatedLedgerEntry {
    private final long caOffset;
    private final String txId;
    private final String agentId;
    private final byte[] ledgerDiffs;  // Account debits/credits, etc.
    private final byte[] metadata;     // Audit info, timestamps, etc.
    private final Instant committedAt;

    @JsonCreator
    public ConsolidatedLedgerEntry(
            @JsonProperty("caOffset") long caOffset,
            @JsonProperty("txId") String txId,
            @JsonProperty("agentId") String agentId,
            @JsonProperty("ledgerDiffs") byte[] ledgerDiffs,
            @JsonProperty("metadata") byte[] metadata,
            @JsonProperty("committedAt") Instant committedAt) {
        this.caOffset = caOffset;
        this.txId = txId;
        this.agentId = agentId;
        this.ledgerDiffs = ledgerDiffs;
        this.metadata = metadata;
        this.committedAt = committedAt;
    }

    public long getCaOffset() {
        return caOffset;
    }

    public String getTxId() {
        return txId;
    }

    public String getAgentId() {
        return agentId;
    }

    public byte[] getLedgerDiffs() {
        return ledgerDiffs;
    }

    public byte[] getMetadata() {
        return metadata;
    }

    public Instant getCommittedAt() {
        return committedAt;
    }
}
