package com.smartlogix.simulator;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Optional alert-shaped event for dashboard and notification-channel testing
 * before the real alert engine is available.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertSimulationEvent {

    private String alertId;
    private String alertType;
    private String severity;
    private String shipmentId;
    private String vehicleId;
    private String warehouseId;
    private String message;
    private long createdAt;
}
