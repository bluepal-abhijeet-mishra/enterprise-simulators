package com.smartlogix.simulator;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SimulatorStartupReporter implements ApplicationRunner {

    private final SimulatorProperties properties;
    private final SimulatorTopicsProperties topics;

    public SimulatorStartupReporter(SimulatorProperties properties, SimulatorTopicsProperties topics) {
        this.properties = properties;
        this.topics = topics;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("SmartLogix simulator ready: mode=producer-only tickRateMs={} maxActiveShipments={} newShipmentProbability={}",
                properties.getTickRateMs(), properties.getMaxActiveShipments(), properties.getNewShipmentProbability());
        log.info("Kafka event contract topics: shipmentEvents={} vehicleTelemetry={} warehouseEvents={} alerts={} analyticsMetrics={}",
                topics.getShipmentEvents(), topics.getVehicleTelemetry(), topics.getWarehouseEvents(),
                topics.getAlerts(), topics.getAnalyticsMetrics());
    }
}
