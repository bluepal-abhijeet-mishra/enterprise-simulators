package com.smartlogix.simulator;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * High-frequency vehicle telemetry event for live map, route, idle, and
 * cold-chain monitoring.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VehicleTelemetryEvent {

    private String vehicleId;
    private String shipmentId;
    private String driverId;
    private String eventType;
    private double lat;
    private double lng;
    private double speedKmph;
    private double temperatureCelsius;
    private boolean idle;
    private boolean atWarehouse;
    private String nearestWarehouseId;
    private long timestamp;
}
