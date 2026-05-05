package com.smartlogix.simulator;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Canonical Data Model representing a single event in the lifecycle
 * of a shipment. This is serialized to JSON and sent to Kafka.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShipmentEvent {

    /**
     * Unique identifier for the shipment.
     * Used as the Kafka Partition Key to guarantee event ordering per shipment.
     */
    private String shipmentId;

    /**
     * The current state or anomaly.
     * Examples: shipment_created, vehicle_departed, warehouse_arrived,
     * delivery_completed, route_deviation, temperature_change.
     */
    private String eventType;

    /**
     * Unique identifier for the vehicle carrying the shipment.
     */
    private String vehicleId;

    /**
     * Current latitude of the shipment/vehicle.
     */
    private double currentLat;

    /**
     * Current longitude of the shipment/vehicle.
     */
    private double currentLng;

    /**
     * Epoch milliseconds representing when this event occurred.
     */
    private long timestamp;
}
