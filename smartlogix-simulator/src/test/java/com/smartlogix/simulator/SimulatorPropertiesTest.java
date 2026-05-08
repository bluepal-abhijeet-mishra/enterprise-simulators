package com.smartlogix.simulator;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SimulatorPropertiesTest {

    private final Validator validator = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    void defaultConfigurationIsValid() {
        SimulatorProperties properties = new SimulatorProperties();

        Set<ConstraintViolation<SimulatorProperties>> violations = validator.validate(properties);

        assertThat(violations).isEmpty();
    }

    @Test
    void rejectsInvalidAnomalyRateTotal() {
        SimulatorProperties properties = new SimulatorProperties();
        properties.getAnomalyRates().setTemperature(0.50);
        properties.getAnomalyRates().setRouteDeviation(0.40);
        properties.getAnomalyRates().setUnauthorizedStop(0.20);

        Set<ConstraintViolation<SimulatorProperties>> violations = validator.validate(properties);

        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .contains("sum of anomaly rates must be <= 1.0");
    }

    @Test
    void rejectsInvalidColdChainRange() {
        SimulatorProperties properties = new SimulatorProperties();
        properties.getColdChain().setMinTemperatureCelsius(8.0);
        properties.getColdChain().setMaxTemperatureCelsius(2.0);

        Set<ConstraintViolation<SimulatorProperties>> violations = validator.validate(properties);

        assertThat(violations)
                .extracting(ConstraintViolation::getMessage)
                .contains("max temperature must be greater than min temperature");
    }
}
