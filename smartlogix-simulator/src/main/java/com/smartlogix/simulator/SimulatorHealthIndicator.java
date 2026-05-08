package com.smartlogix.simulator;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class SimulatorHealthIndicator implements HealthIndicator {

    private final SimulatorMetrics metrics;
    private final SimulatorProperties properties;

    public SimulatorHealthIndicator(SimulatorMetrics metrics, SimulatorProperties properties) {
        this.metrics = metrics;
        this.properties = properties;
    }

    @Override
    public Health health() {
        return Health.up()
                .withDetail("mode", "producer-only")
                .withDetail("activeShipments", metrics.getActiveShipments())
                .withDetail("maxActiveShipments", properties.getMaxActiveShipments())
                .withDetail("tickRateMs", properties.getTickRateMs())
                .withDetail("alertSimulationEnabled", properties.isAlertSimulationEnabled())
                .withDetail("analyticsSimulationEnabled", properties.isAnalyticsSimulationEnabled())
                .build();
    }
}
