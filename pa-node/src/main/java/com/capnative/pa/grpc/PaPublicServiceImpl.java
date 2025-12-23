package com.capnative.pa.grpc;

import com.capnative.common.grpc.pa.*;
import com.capnative.common.model.L1Entry;
import com.capnative.pa.service.HealthController;
import com.capnative.pa.storage.L1Storage;
import com.capnative.pa.storage.ProjectionStorage;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;

/**
 * gRPC service implementation for PA Public API.
 * Provides client-facing APIs for transaction submission and queries.
 */
public class PaPublicServiceImpl extends PaPublicGrpc.PaPublicImplBase {
    private static final Logger logger = LoggerFactory.getLogger(PaPublicServiceImpl.class);

    private final L1Storage l1Storage;
    private final ProjectionStorage projectionStorage;
    private final HealthController healthController;

    public PaPublicServiceImpl(
            L1Storage l1Storage,
            ProjectionStorage projectionStorage,
            HealthController healthController) {
        this.l1Storage = l1Storage;
        this.projectionStorage = projectionStorage;
        this.healthController = healthController;
    }

    @Override
    public void submitTransaction(
            ClientTx request,
            StreamObserver<ClientTxReceipt> responseObserver) {

        try {
            logger.info("Received SubmitTransaction: accountId={}, operation={}",
                    request.getAccountId(), request.getOperation());

            // Validate request
            if (request.getAccountId().isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("account_id is required")
                        .asException());
                return;
            }

            if (request.getOperation().isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("operation is required")
                        .asException());
                return;
            }

            // Generate tx_id if not provided
            String txId = request.getTxId().isEmpty() ?
                    UUID.randomUUID().toString() :
                    request.getTxId();

            // Get next L1 sequence
            long l1Seq = l1Storage.getNextSeq();

            // Create L1 entry
            L1Entry entry = L1Entry.pending(l1Seq, txId, request.getPayload().toByteArray());

            // Append to L1 (durably)
            l1Storage.append(entry);

            // Return receipt
            ClientTxReceipt receipt = ClientTxReceipt.newBuilder()
                    .setTxId(txId)
                    .setStatus(ClientTxReceipt.ReceiptStatus.ACCEPTED)
                    .setL1Seq(l1Seq)
                    .setAcceptedAt(System.currentTimeMillis())
                    .build();

            logger.info("Transaction accepted: txId={}, l1Seq={}", txId, l1Seq);

            responseObserver.onNext(receipt);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error submitting transaction", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .withCause(e)
                    .asException());
        }
    }

    @Override
    public void getAccountView(
            AccountRequest request,
            StreamObserver<AccountView> responseObserver) {

        try {
            if (request.getAccountId().isEmpty()) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("account_id is required")
                        .asException());
                return;
            }

            // Retrieve account from projections
            Optional<byte[]> accountData = projectionStorage.getAccount(request.getAccountId());

            if (accountData.isEmpty()) {
                responseObserver.onError(Status.NOT_FOUND
                        .withDescription("Account not found")
                        .asException());
                return;
            }

            // Build response
            AccountView view = AccountView.newBuilder()
                    .setAccountId(request.getAccountId())
                    .setAccountData(ByteString.copyFrom(accountData.get()))
                    .setValidUpToCaOffset(projectionStorage.getValidUpToOffset())
                    .setAsOfTimestamp(System.currentTimeMillis())
                    .build();

            logger.debug("Returned account view: accountId={}, validUpTo={}",
                    request.getAccountId(), view.getValidUpToCaOffset());

            responseObserver.onNext(view);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error getting account view", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .withCause(e)
                    .asException());
        }
    }

    @Override
    public void getHealth(
            HealthRequest request,
            StreamObserver<HealthResponse> responseObserver) {

        try {
            HealthResponse.HealthState protoState = convertHealthState(healthController.getState());

            HealthResponse response = HealthResponse.newBuilder()
                    .setState(protoState)
                    .setLagCa(healthController.getLag())
                    .setLocalCaOffset(healthController.getLocalCaOffset())
                    .setPendingL1Count(healthController.getPendingL1Count())
                    .setDetails(healthController.getStatusDetails())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (Exception e) {
            logger.error("Error getting health", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error: " + e.getMessage())
                    .withCause(e)
                    .asException());
        }
    }

    private HealthResponse.HealthState convertHealthState(com.capnative.common.model.HealthState state) {
        switch (state) {
            case HEALTHY:
                return HealthResponse.HealthState.HEALTHY;
            case DEGRADED:
                return HealthResponse.HealthState.DEGRADED;
            case REPAIR:
                return HealthResponse.HealthState.REPAIR;
            case UNHEALTHY:
                return HealthResponse.HealthState.UNHEALTHY;
            default:
                return HealthResponse.HealthState.UNHEALTHY;
        }
    }
}
