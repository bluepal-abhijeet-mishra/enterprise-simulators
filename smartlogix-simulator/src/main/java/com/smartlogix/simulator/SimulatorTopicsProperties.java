package com.smartlogix.simulator;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "app.simulator.topics")
public class SimulatorTopicsProperties {

    private String shipmentEvents = "shipment-events";

    private String vehicleTelemetry = "vehicle-telemetry";

    private String warehouseEvents = "warehouse-events";

    private String alerts = "alerts";

    private String analyticsMetrics = "analytics-metrics";
}
