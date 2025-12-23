package com.capnative.common.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;

/**
 * Represents an entry in the L2 (final) log of a PA node.
 * L2 entries are CA-approved transactions applied to local projections.
 */
public class L2Entry {
    private final long caOffset;
    private final String txId;
    private final byte[] ledgerDiffs;
    private final Instant appliedAt;

    @JsonCreator
    public L2Entry(
            @JsonProperty("caOffset") long caOffset,
            @JsonProperty("txId") String txId,
            @JsonProperty("ledgerDiffs") byte[] ledgerDiffs,
            @JsonProperty("appliedAt") Instant appliedAt) {
        this.caOffset = caOffset;
        this.txId = txId;
        this.ledgerDiffs = ledgerDiffs;
        this.appliedAt = appliedAt;
    }

    public static L2Entry create(long caOffset, String txId, byte[] ledgerDiffs) {
        return new L2Entry(caOffset, txId, ledgerDiffs, Instant.now());
    }

    public long getCaOffset() {
        return caOffset;
    }

    public String getTxId() {
        return txId;
    }

    public byte[] getLedgerDiffs() {
        return ledgerDiffs;
    }

    public Instant getAppliedAt() {
        return appliedAt;
    }
}
