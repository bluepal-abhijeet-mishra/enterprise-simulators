package com.smartlogix.simulator;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Warehouse activity event used for barcode scans, bay operations, and
 * congestion testing.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class WarehouseEvent {

    private String warehouseId;
    private String shipmentId;
    private String vehicleId;
    private String eventType;
    private String bayId;
    private int queueDepth;
    private int currentLoad;
    private int capacity;
    private long timestamp;
}
