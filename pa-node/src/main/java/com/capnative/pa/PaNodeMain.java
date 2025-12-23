package com.capnative.pa;

import com.capnative.common.storage.LmdbEnv;
import com.capnative.pa.grpc.PaPublicServiceImpl;
import com.capnative.pa.service.HealthController;
import com.capnative.pa.service.ReplicationClient;
import com.capnative.pa.storage.L1Storage;
import com.capnative.pa.storage.L2Storage;
import com.capnative.pa.storage.ProjectionStorage;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Main entry point for PA Node service.
 */
public class PaNodeMain {
    private static final Logger logger = LoggerFactory.getLogger(PaNodeMain.class);

    private static final int PA_PUBLIC_PORT = 50053;
    private static final long MAX_DB_SIZE = 10L * 1024 * 1024 * 1024; // 10GB
    private static final int MAX_DBS = 20;

    private final Server paPublicServer;
    private final LmdbEnv lmdbEnv;
    private final ReplicationClient replicationClient;
    private final HealthController healthController;

    public PaNodeMain(String agentId, String dbPath, String caHost, int caPort) {
        // Initialize LMDB environment
        File dbDir = new File(dbPath);
        this.lmdbEnv = new LmdbEnv(dbDir, MAX_DB_SIZE, MAX_DBS);

        // Initialize storage layers
        L1Storage l1Storage = new L1Storage(lmdbEnv);
        L2Storage l2Storage = new L2Storage(lmdbEnv);
        ProjectionStorage projectionStorage = new ProjectionStorage(lmdbEnv);

        // Initialize replication client
        this.replicationClient = new ReplicationClient(
                agentId,
                caHost,
                caPort,
                l1Storage,
                l2Storage,
                projectionStorage
        );

        // Initialize health controller
        this.healthController = new HealthController(replicationClient, l2Storage);

        // Create gRPC service
        PaPublicServiceImpl paPublicService = new PaPublicServiceImpl(
                l1Storage,
                projectionStorage,
                healthController
        );

        // Build server
        this.paPublicServer = ServerBuilder.forPort(PA_PUBLIC_PORT)
                .addService(paPublicService)
                .addService(ProtoReflectionService.newInstance())
                .build();

        logger.info("PA Node initialized: agentId={}, dbPath={}, ca={}:{}",
                agentId, dbPath, caHost, caPort);
    }

    public void start() throws Exception {
        // Start gRPC server
        paPublicServer.start();
        logger.info("PA Public gRPC server started on port {}", PA_PUBLIC_PORT);

        // Start replication client
        replicationClient.start();

        // Start health controller
        healthController.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received");
            try {
                PaNodeMain.this.stop();
            } catch (Exception e) {
                logger.error("Error during shutdown", e);
            }
        }));

        logger.info("PA Node is running");
    }

    public void stop() throws Exception {
        logger.info("Stopping PA Node");

        // Stop health controller
        if (healthController != null) {
            healthController.stop();
        }

        // Stop replication client
        if (replicationClient != null) {
            replicationClient.stop();
        }

        // Stop gRPC server
        if (paPublicServer != null) {
            paPublicServer.shutdown();
            paPublicServer.awaitTermination(10, TimeUnit.SECONDS);
        }

        // Close LMDB environment
        if (lmdbEnv != null) {
            lmdbEnv.close();
        }

        logger.info("PA Node stopped");
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (paPublicServer != null) {
            paPublicServer.awaitTermination();
        }
    }

    public static void main(String[] args) throws Exception {
        String agentId = System.getenv().getOrDefault("AGENT_ID", "pa-node-1");
        String dbPath = System.getenv().getOrDefault("PA_DB_PATH", "/tmp/pa-node/db");
        String caHost = System.getenv().getOrDefault("CA_HOST", "localhost");
        int caPort = Integer.parseInt(System.getenv().getOrDefault("CA_PORT", "50051"));

        logger.info("Starting PA Node: agentId={}, DB_PATH={}, CA={}:{}",
                agentId, dbPath, caHost, caPort);

        PaNodeMain paNodeMain = new PaNodeMain(agentId, dbPath, caHost, caPort);
        paNodeMain.start();
        paNodeMain.blockUntilShutdown();
    }
}
