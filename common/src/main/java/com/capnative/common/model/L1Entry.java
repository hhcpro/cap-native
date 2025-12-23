package com.capnative.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Represents an entry in the L1 (pending) log of a PA node.
 * L1 entries track transactions from submission through CA approval.
 */
public class L1Entry {
    private final long l1Seq;
    private final String txId;
    private final byte[] payload;
    private final L1Status status;
    private final Long caOffset;
    private final String reason;
    private final Instant createdAt;
    private final Instant updatedAt;

    @JsonCreator
    public L1Entry(
            @JsonProperty("l1Seq") long l1Seq,
            @JsonProperty("txId") String txId,
            @JsonProperty("payload") byte[] payload,
            @JsonProperty("status") L1Status status,
            @JsonProperty("caOffset") Long caOffset,
            @JsonProperty("reason") String reason,
            @JsonProperty("createdAt") Instant createdAt,
            @JsonProperty("updatedAt") Instant updatedAt) {
        this.l1Seq = l1Seq;
        this.txId = txId;
        this.payload = payload;
        this.status = status;
        this.caOffset = caOffset;
        this.reason = reason;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public static L1Entry pending(long l1Seq, String txId, byte[] payload) {
        Instant now = Instant.now();
        return new L1Entry(l1Seq, txId, payload, L1Status.PENDING_CA, null, null, now, now);
    }

    public L1Entry withCommitted(long caOffset) {
        return new L1Entry(
                this.l1Seq,
                this.txId,
                this.payload,
                L1Status.COMMITTED_CA,
                caOffset,
                null,
                this.createdAt,
                Instant.now()
        );
    }

    public L1Entry withRejected(String reason) {
        return new L1Entry(
                this.l1Seq,
                this.txId,
                this.payload,
                L1Status.REJECTED_CA,
                null,
                reason,
                this.createdAt,
                Instant.now()
        );
    }

    public long getL1Seq() {
        return l1Seq;
    }

    public String getTxId() {
        return txId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public L1Status getStatus() {
        return status;
    }

    public Long getCaOffset() {
        return caOffset;
    }

    public String getReason() {
        return reason;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public enum L1Status {
        PENDING_CA,      // Awaiting CA approval
        COMMITTED_CA,    // Approved by CA
        REJECTED_CA      // Rejected by CA
    }
}
