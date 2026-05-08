package com.smartlogix.simulator;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class SimulatorMetrics {

    private final MeterRegistry meterRegistry;
    private final Map<String, Counter> sentCounters = new ConcurrentHashMap<>();
    private final Map<String, Counter> failedCounters = new ConcurrentHashMap<>();
    private final AtomicInteger activeShipments = new AtomicInteger();

    public SimulatorMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        Gauge.builder("smartlogix.simulator.active_shipments", activeShipments, AtomicInteger::get)
                .description("Current number of active simulated shipments")
                .register(meterRegistry);
    }

    public void setActiveShipments(int value) {
        activeShipments.set(value);
    }

    public int getActiveShipments() {
        return activeShipments.get();
    }

    public void recordSent(String topic) {
        sentCounters.computeIfAbsent(topic, this::sentCounter).increment();
    }

    public void recordFailed(String topic) {
        failedCounters.computeIfAbsent(topic, this::failedCounter).increment();
    }

    private Counter sentCounter(String topic) {
        return Counter.builder("smartlogix.simulator.events.sent")
                .description("Kafka events successfully sent by the simulator")
                .tag("topic", topic)
                .register(meterRegistry);
    }

    private Counter failedCounter(String topic) {
        return Counter.builder("smartlogix.simulator.events.failed")
                .description("Kafka events the simulator failed to send")
                .tag("topic", topic)
                .register(meterRegistry);
    }
}
