package com.smartlogix.simulator;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties(prefix = "simulator.logistics")
public class SimulatorProperties {

    @Min(100)
    private long tickRateMs = 5000;

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    private double newShipmentProbability = 0.20;

    @Min(1)
    private int maxActiveShipments = 250;

    private boolean alertSimulationEnabled = true;

    private boolean analyticsSimulationEnabled = true;

    @Valid
    private AnomalyRates anomalyRates = new AnomalyRates();

    @Valid
    private DelaysMs delaysMs = new DelaysMs();

    @Valid
    private ColdChain coldChain = new ColdChain();

    @Valid
    private Warehouse warehouse = new Warehouse();

    @AssertTrue(message = "sum of anomaly rates must be <= 1.0")
    public boolean isAnomalyRateTotalValid() {
        return anomalyRates.temperature + anomalyRates.routeDeviation + anomalyRates.unauthorizedStop <= 1.0;
    }

    @Data
    public static class AnomalyRates {
        @DecimalMin("0.0")
        @DecimalMax("1.0")
        private double temperature = 0.05;

        @DecimalMin("0.0")
        @DecimalMax("1.0")
        private double routeDeviation = 0.02;

        @DecimalMin("0.0")
        @DecimalMax("1.0")
        private double unauthorizedStop = 0.01;
    }

    @Data
    public static class DelaysMs {
        @Min(1000)
        private long loading = 15000;

        @Min(1000)
        private long driving = 30000;

        @Min(1000)
        private long unloading = 20000;
    }

    @Data
    public static class ColdChain {
        private double minTemperatureCelsius = 2.0;

        private double maxTemperatureCelsius = 8.0;

        @AssertTrue(message = "max temperature must be greater than min temperature")
        public boolean isRangeValid() {
            return maxTemperatureCelsius > minTemperatureCelsius;
        }
    }

    @Data
    public static class Warehouse {
        @Min(1)
        private int congestionQueueThreshold = 25;

        @DecimalMin("0.0")
        @DecimalMax("1.0")
        private double barcodeScanProbability = 0.30;

        @DecimalMin("0.0")
        @DecimalMax("1.0")
        private double bayAssignmentProbability = 0.65;
    }
}
