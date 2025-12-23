package com.capnative.common.model;

/**
 * Health states for PA nodes.
 */
public enum HealthState {
    /**
     * Node is operating normally, low CA lag, no gaps.
     */
    HEALTHY,

    /**
     * Node has elevated CA lag but no data gaps or corruption.
     */
    DEGRADED,

    /**
     * Node detected gaps or corruption and is actively repairing.
     */
    REPAIR,

    /**
     * Node failed repair or exceeded thresholds and is evicted.
     */
    UNHEALTHY
}
