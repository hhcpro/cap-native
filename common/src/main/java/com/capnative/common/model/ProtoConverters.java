package com.capnative.common.model;

import com.capnative.common.storage.proto.*;
import com.google.protobuf.ByteString;

import java.time.Instant;

/**
 * Converts between Java domain models and protobuf messages.
 * All LMDB storage uses protobuf - these converters are for business logic only.
 */
public class ProtoConverters {

    // ========== AgentLedgerEntry ==========

    public static AgentLedgerEntryProto toProto(AgentLedgerEntry entry) {
        AgentLedgerEntryProto.Builder builder = AgentLedgerEntryProto.newBuilder()
                .setAgentId(entry.getAgentId())
                .setAgentSeq(entry.getAgentSeq())
                .setTxId(entry.getTxId())
                .setPayload(ByteString.copyFrom(entry.getPayload()))
                .setDecision(toProtoDecision(entry.getDecision()))
                .setCreatedAt(entry.getCreatedAt().toEpochMilli());

        if (entry.getReason() != null) {
            builder.setReason(entry.getReason());
        }
        if (entry.getDecidedAt() != null) {
            builder.setDecidedAt(entry.getDecidedAt().toEpochMilli());
        }
        if (entry.getCaOffset() != null) {
            builder.setCaOffset(entry.getCaOffset());
        }

        return builder.build();
    }

    public static AgentLedgerEntry fromProto(AgentLedgerEntryProto proto) {
        return new AgentLedgerEntry(
                proto.getAgentId(),
                proto.getAgentSeq(),
                proto.getTxId(),
                proto.getPayload().toByteArray(),
                fromProtoDecision(proto.getDecision()),
                proto.hasReason() ? proto.getReason() : null,
                Instant.ofEpochMilli(proto.getCreatedAt()),
                proto.hasDecidedAt() ? Instant.ofEpochMilli(proto.getDecidedAt()) : null,
                proto.hasCaOffset() ? proto.getCaOffset() : null
        );
    }

    private static AgentLedgerEntryProto.Decision toProtoDecision(AgentLedgerEntry.Decision decision) {
        switch (decision) {
            case PENDING_DECISION: return AgentLedgerEntryProto.Decision.PENDING_DECISION;
            case COMMITTED: return AgentLedgerEntryProto.Decision.COMMITTED;
            case REJECTED: return AgentLedgerEntryProto.Decision.REJECTED;
            default: throw new IllegalArgumentException("Unknown decision: " + decision);
        }
    }

    private static AgentLedgerEntry.Decision fromProtoDecision(AgentLedgerEntryProto.Decision decision) {
        switch (decision) {
            case PENDING_DECISION: return AgentLedgerEntry.Decision.PENDING_DECISION;
            case COMMITTED: return AgentLedgerEntry.Decision.COMMITTED;
            case REJECTED: return AgentLedgerEntry.Decision.REJECTED;
            default: throw new IllegalArgumentException("Unknown decision: " + decision);
        }
    }

    // ========== ConsolidatedLedgerEntry ==========

    public static ConsolidatedLedgerEntryProto toProto(ConsolidatedLedgerEntry entry) {
        return ConsolidatedLedgerEntryProto.newBuilder()
                .setCaOffset(entry.getCaOffset())
                .setTxId(entry.getTxId())
                .setAgentId(entry.getAgentId())
                .setLedgerDiffs(ByteString.copyFrom(entry.getLedgerDiffs()))
                .setMetadata(ByteString.copyFrom(entry.getMetadata()))
                .setCommittedAt(entry.getCommittedAt().toEpochMilli())
                .build();
    }

    public static ConsolidatedLedgerEntry fromProto(ConsolidatedLedgerEntryProto proto) {
        return new ConsolidatedLedgerEntry(
                proto.getCaOffset(),
                proto.getTxId(),
                proto.getAgentId(),
                proto.getLedgerDiffs().toByteArray(),
                proto.getMetadata().toByteArray(),
                Instant.ofEpochMilli(proto.getCommittedAt())
        );
    }

    // ========== L1Entry ==========

    public static L1EntryProto toProto(L1Entry entry) {
        L1EntryProto.Builder builder = L1EntryProto.newBuilder()
                .setL1Seq(entry.getL1Seq())
                .setTxId(entry.getTxId())
                .setPayload(ByteString.copyFrom(entry.getPayload()))
                .setStatus(toProtoL1Status(entry.getStatus()))
                .setCreatedAt(entry.getCreatedAt().toEpochMilli())
                .setUpdatedAt(entry.getUpdatedAt().toEpochMilli());

        if (entry.getCaOffset() != null) {
            builder.setCaOffset(entry.getCaOffset());
        }
        if (entry.getReason() != null) {
            builder.setReason(entry.getReason());
        }

        return builder.build();
    }

    public static L1Entry fromProto(L1EntryProto proto) {
        return new L1Entry(
                proto.getL1Seq(),
                proto.getTxId(),
                proto.getPayload().toByteArray(),
                fromProtoL1Status(proto.getStatus()),
                proto.hasCaOffset() ? proto.getCaOffset() : null,
                proto.hasReason() ? proto.getReason() : null,
                Instant.ofEpochMilli(proto.getCreatedAt()),
                Instant.ofEpochMilli(proto.getUpdatedAt())
        );
    }

    private static L1EntryProto.L1Status toProtoL1Status(L1Entry.L1Status status) {
        switch (status) {
            case PENDING_CA: return L1EntryProto.L1Status.PENDING_CA;
            case COMMITTED_CA: return L1EntryProto.L1Status.COMMITTED_CA;
            case REJECTED_CA: return L1EntryProto.L1Status.REJECTED_CA;
            default: throw new IllegalArgumentException("Unknown status: " + status);
        }
    }

    private static L1Entry.L1Status fromProtoL1Status(L1EntryProto.L1Status status) {
        switch (status) {
            case PENDING_CA: return L1Entry.L1Status.PENDING_CA;
            case COMMITTED_CA: return L1Entry.L1Status.COMMITTED_CA;
            case REJECTED_CA: return L1Entry.L1Status.REJECTED_CA;
            default: throw new IllegalArgumentException("Unknown status: " + status);
        }
    }

    // ========== L2Entry ==========

    public static L2EntryProto toProto(L2Entry entry) {
        return L2EntryProto.newBuilder()
                .setCaOffset(entry.getCaOffset())
                .setTxId(entry.getTxId())
                .setLedgerDiffs(ByteString.copyFrom(entry.getLedgerDiffs()))
                .setAppliedAt(entry.getAppliedAt().toEpochMilli())
                .build();
    }

    public static L2Entry fromProto(L2EntryProto proto) {
        return new L2Entry(
                proto.getCaOffset(),
                proto.getTxId(),
                proto.getLedgerDiffs().toByteArray(),
                Instant.ofEpochMilli(proto.getAppliedAt())
        );
    }
}
