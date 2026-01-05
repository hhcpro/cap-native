package com.capnative.ca;

import com.capnative.ca.grpc.CaBootstrapServiceImpl;
import com.capnative.ca.grpc.CaCoreServiceImpl;
import com.capnative.ca.service.BatchTransactionProcessor;
import com.capnative.ca.service.OffloadService;
import com.capnative.ca.service.SimpleValidator;
import com.capnative.ca.storage.AgentLedgerStorage;
import com.capnative.ca.storage.ConsolidatedLedgerStorage;
import com.capnative.common.offload.FilesystemObjectStorage;
import com.capnative.common.offload.ObjectStorage;
import com.capnative.common.storage.LmdbEnv;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Main entry point for CA Core service.
 * 100% binary protobuf - zero JSON.
 * High-performance batch transaction processing.
 */
public class CaCoreMain {
    private static final Logger logger = LoggerFactory.getLogger(CaCoreMain.class);

    private static final int CA_CORE_PORT = 50051;
    private static final int CA_BOOTSTRAP_PORT = 50052;
    private static final long MAX_DB_SIZE = 10L * 1024 * 1024 * 1024; // 10GB
    private static final int MAX_DBS = 10;
    private static final long OFFLOAD_INTERVAL_SECONDS = 60;

    // Batch processor configuration
    private static final int BATCH_SIZE = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "100"));
    private static final int VALIDATION_THREADS = Integer.parseInt(
            System.getenv().getOrDefault("VALIDATION_THREADS", String.valueOf(Runtime.getRuntime().availableProcessors())));
    private static final int QUEUE_SIZE = Integer.parseInt(System.getenv().getOrDefault("QUEUE_SIZE", "10000"));

    private final Server caCoreServer;
    private final Server caBootstrapServer;
    private final LmdbEnv lmdbEnv;
    private final OffloadService offloadService;
    private final BatchTransactionProcessor batchTransactionProcessor;

    public CaCoreMain(String dbPath, String storagePath, String caNodeId) {
        File dbDir = new File(dbPath);
        this.lmdbEnv = new LmdbEnv(dbDir, MAX_DB_SIZE, MAX_DBS);

        AgentLedgerStorage agentLedgerStorage = new AgentLedgerStorage(lmdbEnv);
        ConsolidatedLedgerStorage consolidatedLedgerStorage = new ConsolidatedLedgerStorage(lmdbEnv);

        SimpleValidator validator = new SimpleValidator();

        // High-performance batch transaction processor
        this.batchTransactionProcessor = new BatchTransactionProcessor(
                agentLedgerStorage,
                consolidatedLedgerStorage,
                validator,
                BATCH_SIZE,
                VALIDATION_THREADS,
                QUEUE_SIZE,
                caNodeId
        );

        // Use filesystem storage (can swap for S3ObjectStorage in production)
        ObjectStorage objectStorage = new FilesystemObjectStorage(storagePath);
        this.offloadService = new OffloadService(consolidatedLedgerStorage, objectStorage);

        CaCoreServiceImpl caCoreService = new CaCoreServiceImpl(batchTransactionProcessor, consolidatedLedgerStorage);
        CaBootstrapServiceImpl caBootstrapService = new CaBootstrapServiceImpl(offloadService, objectStorage);

        this.caCoreServer = ServerBuilder.forPort(CA_CORE_PORT)
                .addService(caCoreService)
                .addService(ProtoReflectionService.newInstance())
                .build();

        this.caBootstrapServer = ServerBuilder.forPort(CA_BOOTSTRAP_PORT)
                .addService(caBootstrapService)
                .addService(ProtoReflectionService.newInstance())
                .build();

        logger.info("CA Core initialized (BINARY PROTOBUF + BATCH PROCESSING): dbPath={}, storagePath={}, batchSize={}, validationThreads={}",
                dbPath, storagePath, BATCH_SIZE, VALIDATION_THREADS);
    }

    public void start() throws Exception {
        // Start batch transaction processor
        batchTransactionProcessor.start();
        logger.info("Batch transaction processor started");

        caCoreServer.start();
        logger.info("CA Core gRPC server started on port {}", CA_CORE_PORT);

        caBootstrapServer.start();
        logger.info("CA Bootstrap gRPC server started on port {}", CA_BOOTSTRAP_PORT);

        offloadService.start(OFFLOAD_INTERVAL_SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            try {
                CaCoreMain.this.stop();
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));

        logger.info("CA Core is running (100% binary protobuf storage + batch processing)");
    }

    public void stop() throws Exception {
        logger.info("Stopping CA Core");

        batchTransactionProcessor.stop();
        offloadService.stop();

        if (caCoreServer != null) {
            caCoreServer.shutdown();
            caCoreServer.awaitTermination(10, TimeUnit.SECONDS);
        }

        if (caBootstrapServer != null) {
            caBootstrapServer.shutdown();
            caBootstrapServer.awaitTermination(10, TimeUnit.SECONDS);
        }

        if (lmdbEnv != null) {
            lmdbEnv.close();
        }

        logger.info("CA Core stopped");
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (caCoreServer != null) {
            caCoreServer.awaitTermination();
        }
    }

    public static void main(String[] args) throws Exception {
        String dbPath = System.getenv().getOrDefault("CA_DB_PATH", "/tmp/ca-core/db");
        String storagePath = System.getenv().getOrDefault("CA_STORAGE_PATH", "/tmp/ca-core/storage");
        String caNodeId = System.getenv().getOrDefault("CA_NODE_ID", "ca-core-1");

        logger.info("Starting CA Core: DB_PATH={}, STORAGE_PATH={}, CA_NODE_ID={}", dbPath, storagePath, caNodeId);

        CaCoreMain caCoreMain = new CaCoreMain(dbPath, storagePath, caNodeId);
        caCoreMain.start();
        caCoreMain.blockUntilShutdown();
    }
}
