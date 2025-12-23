package com.capnative.ca;

import com.capnative.ca.grpc.CaBootstrapServiceImpl;
import com.capnative.ca.grpc.CaCoreServiceImpl;
import com.capnative.ca.service.OffloadService;
import com.capnative.ca.service.SimpleValidator;
import com.capnative.ca.service.TransactionProcessor;
import com.capnative.ca.storage.AgentLedgerStorage;
import com.capnative.ca.storage.ConsolidatedLedgerStorage;
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
 */
public class CaCoreMain {
    private static final Logger logger = LoggerFactory.getLogger(CaCoreMain.class);

    private static final int CA_CORE_PORT = 50051;
    private static final int CA_BOOTSTRAP_PORT = 50052;
    private static final long MAX_DB_SIZE = 10L * 1024 * 1024 * 1024; // 10GB
    private static final int MAX_DBS = 10;
    private static final long OFFLOAD_INTERVAL_SECONDS = 60; // Export every minute

    private final Server caCoreServer;
    private final Server caBootstrapServer;
    private final LmdbEnv lmdbEnv;
    private final OffloadService offloadService;

    public CaCoreMain(String dbPath, String storagePath) {
        // Initialize LMDB environment
        File dbDir = new File(dbPath);
        this.lmdbEnv = new LmdbEnv(dbDir, MAX_DB_SIZE, MAX_DBS);

        // Initialize storage layers
        AgentLedgerStorage agentLedgerStorage = new AgentLedgerStorage(lmdbEnv);
        ConsolidatedLedgerStorage consolidatedLedgerStorage = new ConsolidatedLedgerStorage(lmdbEnv);

        // Initialize transaction processor
        SimpleValidator validator = new SimpleValidator();
        TransactionProcessor transactionProcessor = new TransactionProcessor(
                agentLedgerStorage,
                consolidatedLedgerStorage,
                validator
        );

        // Initialize offload service
        File storageDir = new File(storagePath);
        this.offloadService = new OffloadService(consolidatedLedgerStorage, storageDir);

        // Create gRPC services
        CaCoreServiceImpl caCoreService = new CaCoreServiceImpl(transactionProcessor, consolidatedLedgerStorage);
        CaBootstrapServiceImpl caBootstrapService = new CaBootstrapServiceImpl(offloadService, storageDir);

        // Build servers
        this.caCoreServer = ServerBuilder.forPort(CA_CORE_PORT)
                .addService(caCoreService)
                .addService(ProtoReflectionService.newInstance())
                .build();

        this.caBootstrapServer = ServerBuilder.forPort(CA_BOOTSTRAP_PORT)
                .addService(caBootstrapService)
                .addService(ProtoReflectionService.newInstance())
                .build();

        logger.info("CA Core initialized with dbPath={}, storagePath={}", dbPath, storagePath);
    }

    public void start() throws Exception {
        // Start gRPC servers
        caCoreServer.start();
        logger.info("CA Core gRPC server started on port {}", CA_CORE_PORT);

        caBootstrapServer.start();
        logger.info("CA Bootstrap gRPC server started on port {}", CA_BOOTSTRAP_PORT);

        // Start offload service
        offloadService.start(OFFLOAD_INTERVAL_SECONDS);

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            try {
                CaCoreMain.this.stop();
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));

        logger.info("CA Core is running");
    }

    public void stop() throws Exception {
        logger.info("Stopping CA Core");

        // Stop offload service
        offloadService.stop();

        // Stop gRPC servers
        if (caCoreServer != null) {
            caCoreServer.shutdown();
            caCoreServer.awaitTermination(10, TimeUnit.SECONDS);
        }

        if (caBootstrapServer != null) {
            caBootstrapServer.shutdown();
            caBootstrapServer.awaitTermination(10, TimeUnit.SECONDS);
        }

        // Close LMDB environment
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

        logger.info("Starting CA Core with DB_PATH={}, STORAGE_PATH={}", dbPath, storagePath);

        CaCoreMain caCoreMain = new CaCoreMain(dbPath, storagePath);
        caCoreMain.start();
        caCoreMain.blockUntilShutdown();
    }
}
