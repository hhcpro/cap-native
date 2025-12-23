package com.capnative.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Represents an entry in the per-agent ledger maintained by CA Core.
 */
public class AgentLedgerEntry {
    private final String agentId;
    private final long agentSeq;
    private final String txId;
    private final byte[] payload;
    private final Decision decision;
    private final String reason;
    private final Instant createdAt;
    private final Instant decidedAt;
    private final Long caOffset;  // Only set when committed

    @JsonCreator
    public AgentLedgerEntry(
            @JsonProperty("agentId") String agentId,
            @JsonProperty("agentSeq") long agentSeq,
            @JsonProperty("txId") String txId,
            @JsonProperty("payload") byte[] payload,
            @JsonProperty("decision") Decision decision,
            @JsonProperty("reason") String reason,
            @JsonProperty("createdAt") Instant createdAt,
            @JsonProperty("decidedAt") Instant decidedAt,
            @JsonProperty("caOffset") Long caOffset) {
        this.agentId = agentId;
        this.agentSeq = agentSeq;
        this.txId = txId;
        this.payload = payload;
        this.decision = decision;
        this.reason = reason;
        this.createdAt = createdAt;
        this.decidedAt = decidedAt;
        this.caOffset = caOffset;
    }

    public static AgentLedgerEntry pending(String agentId, long agentSeq, String txId, byte[] payload) {
        return new AgentLedgerEntry(
                agentId,
                agentSeq,
                txId,
                payload,
                Decision.PENDING_DECISION,
                null,
                Instant.now(),
                null,
                null
        );
    }

    public AgentLedgerEntry withCommitted(long caOffset) {
        return new AgentLedgerEntry(
                this.agentId,
                this.agentSeq,
                this.txId,
                this.payload,
                Decision.COMMITTED,
                null,
                this.createdAt,
                Instant.now(),
                caOffset
        );
    }

    public AgentLedgerEntry withRejected(String reason) {
        return new AgentLedgerEntry(
                this.agentId,
                this.agentSeq,
                this.txId,
                this.payload,
                Decision.REJECTED,
                reason,
                this.createdAt,
                Instant.now(),
                null
        );
    }

    public String getAgentId() {
        return agentId;
    }

    public long getAgentSeq() {
        return agentSeq;
    }

    public String getTxId() {
        return txId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public Decision getDecision() {
        return decision;
    }

    public String getReason() {
        return reason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getDecidedAt() {
        return decidedAt;
    }

    public Long getCaOffset() {
        return caOffset;
    }

    public enum Decision {
        PENDING_DECISION,
        COMMITTED,
        REJECTED
    }
}
