package com.capnative.pa.service;

import com.capnative.common.model.HealthState;
import com.capnative.pa.storage.L2Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Health state machine for PA nodes.
 *
 * States:
 * - HEALTHY: Normal operation, low lag, no gaps
 * - DEGRADED: Elevated lag but no data issues
 * - REPAIR: Detected gaps/corruption, actively repairing
 * - UNHEALTHY: Failed repair or exceeded thresholds
 *
 * Thresholds:
 * - HEALTHY <-> DEGRADED: lag > LAG_THRESHOLD
 * - Any -> REPAIR: gap detected
 * - REPAIR -> UNHEALTHY: repair time > MAX_REPAIR_TIME or gap > MAX_GAP_SIZE
 */
public class HealthController {
    private static final Logger logger = LoggerFactory.getLogger(HealthController.class);

    // Thresholds
    private static final long LAG_THRESHOLD = 1000;  // Offsets
    private static final long MAX_REPAIR_TIME_MS = 300_000;  // 5 minutes
    private static final long MAX_GAP_SIZE = 10_000;  // Offsets
    private static final long HEALTH_CHECK_INTERVAL_MS = 1000;  // 1 second

    private final ReplicationClient replicationClient;
    private final L2Storage l2Storage;
    private final AtomicReference<HealthState> currentState;
    private final AtomicLong repairStartTime;
    private final AtomicLong repairAttempts;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private volatile boolean gapDetected = false;
    private volatile String statusDetails = "Initializing";

    public HealthController(ReplicationClient replicationClient, L2Storage l2Storage) {
        this.replicationClient = replicationClient;
        this.l2Storage = l2Storage;
        this.currentState = new AtomicReference<>(HealthState.HEALTHY);
        this.repairStartTime = new AtomicLong(-1);
        this.repairAttempts = new AtomicLong(0);
    }

    /**
     * Starts the health monitoring loop.
     */
    public void start() {
        logger.info("Starting health controller");

        scheduler.scheduleAtFixedRate(
                this::runHealthCheck,
                0,
                HEALTH_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Stops the health monitoring.
     */
    public void stop() {
        logger.info("Stopping health controller");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Main health check loop.
     */
    private void runHealthCheck() {
        try {
            long lag = replicationClient.getCaLag();
            long pendingCount = replicationClient.getPendingCount();

            HealthState oldState = currentState.get();
            HealthState newState = determineState(lag, pendingCount);

            if (newState != oldState) {
                transitionState(oldState, newState, lag, pendingCount);
            }

            updateStatusDetails(newState, lag, pendingCount);

        } catch (Exception e) {
            logger.error("Error in health check", e);
        }
    }

    /**
     * Determines the appropriate health state based on metrics.
     */
    private HealthState determineState(long lag, long pendingCount) {
        HealthState current = currentState.get();

        // Check for gaps (highest priority)
        if (gapDetected) {
            return HealthState.REPAIR;
        }

        // REPAIR state transitions
        if (current == HealthState.REPAIR) {
            long repairDuration = System.currentTimeMillis() - repairStartTime.get();

            // Check if repair failed
            if (repairDuration > MAX_REPAIR_TIME_MS) {
                logger.error("Repair timed out after {}ms", repairDuration);
                return HealthState.UNHEALTHY;
            }

            if (lag > MAX_GAP_SIZE) {
                logger.error("Gap size {} exceeds maximum {}", lag, MAX_GAP_SIZE);
                return HealthState.UNHEALTHY;
            }

            // Check if repair succeeded
            if (!gapDetected && lag < LAG_THRESHOLD) {
                logger.info("Repair completed successfully after {}ms", repairDuration);
                repairStartTime.set(-1);
                return HealthState.HEALTHY;
            }

            // Still repairing
            return HealthState.REPAIR;
        }

        // UNHEALTHY is terminal unless manually recovered
        if (current == HealthState.UNHEALTHY) {
            return HealthState.UNHEALTHY;
        }

        // Normal HEALTHY <-> DEGRADED transitions
        if (lag > LAG_THRESHOLD) {
            return HealthState.DEGRADED;
        } else {
            return HealthState.HEALTHY;
        }
    }

    /**
     * Handles state transitions.
     */
    private void transitionState(HealthState oldState, HealthState newState, long lag, long pendingCount) {
        logger.warn("Health state transition: {} -> {} (lag={}, pending={})",
                oldState, newState, lag, pendingCount);

        currentState.set(newState);

        // Handle entry actions
        switch (newState) {
            case REPAIR:
                repairStartTime.set(System.currentTimeMillis());
                repairAttempts.incrementAndGet();
                logger.warn("Entering REPAIR state (attempt #{})", repairAttempts.get());
                // In production, trigger repair process here
                break;

            case UNHEALTHY:
                logger.error("Entering UNHEALTHY state - node is evicted");
                // In production, notify monitoring systems, stop serving requests, etc.
                break;

            case HEALTHY:
                if (oldState == HealthState.REPAIR) {
                    logger.info("Recovery complete, returning to HEALTHY state");
                }
                break;

            case DEGRADED:
                logger.warn("Entering DEGRADED state - elevated lag detected");
                break;
        }
    }

    /**
     * Updates status details for health endpoint.
     */
    private void updateStatusDetails(HealthState state, long lag, long pendingCount) {
        switch (state) {
            case HEALTHY:
                statusDetails = String.format("Operating normally (lag=%d, pending=%d)", lag, pendingCount);
                break;

            case DEGRADED:
                statusDetails = String.format("Elevated lag detected (lag=%d, pending=%d)", lag, pendingCount);
                break;

            case REPAIR:
                long repairDuration = System.currentTimeMillis() - repairStartTime.get();
                statusDetails = String.format("Repairing (duration=%dms, attempt=%d, lag=%d)",
                        repairDuration, repairAttempts.get(), lag);
                break;

            case UNHEALTHY:
                statusDetails = "Node evicted - manual recovery required";
                break;
        }
    }

    /**
     * Signals that a gap has been detected.
     */
    public void reportGap() {
        if (!gapDetected) {
            logger.error("Gap detected in L2 - triggering repair");
            gapDetected = true;
        }
    }

    /**
     * Signals that the gap has been resolved.
     */
    public void clearGap() {
        if (gapDetected) {
            logger.info("Gap resolved");
            gapDetected = false;
        }
    }

    /**
     * Gets the current health state.
     */
    public HealthState getState() {
        return currentState.get();
    }

    /**
     * Gets the current lag.
     */
    public long getLag() {
        return replicationClient.getCaLag();
    }

    /**
     * Gets the local CA offset.
     */
    public long getLocalCaOffset() {
        return l2Storage.getLatestOffset();
    }

    /**
     * Gets the pending L1 count.
     */
    public long getPendingL1Count() {
        return replicationClient.getPendingCount();
    }

    /**
     * Gets status details.
     */
    public String getStatusDetails() {
        return statusDetails;
    }

    /**
     * Manually triggers recovery from UNHEALTHY state.
     */
    public void triggerRecovery() {
        logger.warn("Manual recovery triggered");
        gapDetected = false;
        repairStartTime.set(-1);
        repairAttempts.set(0);
        currentState.set(HealthState.HEALTHY);
        statusDetails = "Manually recovered";
    }
}
