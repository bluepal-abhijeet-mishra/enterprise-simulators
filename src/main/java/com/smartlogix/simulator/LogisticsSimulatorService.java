package com.smartlogix.simulator;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Core service for the SmartLogix Logistics Simulator.
 * Simulates a realistic state machine for shipments, including GPS tracking and
 * occasional anomalies, publishing events to Kafka.
 */
@Service
@Slf4j
public class LogisticsSimulatorService {

    private final KafkaTemplate<String, ShipmentEvent> kafkaTemplate;
    private final String topicName;

    @Value("${simulator.logistics.anomaly-rates.temperature:0.05}")
    private double tempAnomalyRate;

    @Value("${simulator.logistics.anomaly-rates.route-deviation:0.02}")
    private double routeAnomalyRate;

    @Value("${simulator.logistics.delays-ms.loading:15000}")
    private long loadingDelayMs;

    @Value("${simulator.logistics.delays-ms.driving:30000}")
    private long drivingDelayMs;

    @Value("${simulator.logistics.delays-ms.unloading:20000}")
    private long unloadingDelayMs;

    // Track active shipments. Thread-safe map for scheduler access.
    // Key: Shipment ID, Value: ShipmentContext (Event + state entered timestamp)
    private final Map<String, ShipmentContext> activeShipments = new ConcurrentHashMap<>();

    // Valid lifecycle states
    private static final String STATE_CREATED = "shipment_created";
    private static final String STATE_DEPARTED = "vehicle_departed";
    private static final String STATE_ARRIVED = "warehouse_arrived";
    private static final String STATE_COMPLETED = "delivery_completed";

    // Warehouse specific event
    private static final String EVENT_BARCODE_SCANNED = "barcode_scanned";

    // Anomalies
    private static final String ANOMALY_ROUTE_DEVIATION = "route_deviation";
    private static final String ANOMALY_TEMP_CHANGE = "temperature_change";

    public LogisticsSimulatorService(
            KafkaTemplate<String, ShipmentEvent> kafkaTemplate,
            @Value("${app.simulator.topic:shipment-events}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    /**
     * Executes every 5 seconds to run the simulation loop.
     */
    @Scheduled(fixedRate = 5000)
    public void simulateTick() {
        log.debug("Simulation tick started. Active shipments: {}", activeShipments.size());

        tryAddNewShipment();
        processActiveShipments();

        log.debug("Simulation tick finished.");
    }

    /**
     * 20% chance to instantiate a new shipment and add it to the active pool.
     */
    private void tryAddNewShipment() {
        if (ThreadLocalRandom.current().nextDouble() < 0.20) {
            String shipmentId = "SHP-" + UUID.randomUUID().toString().substring(0, 8);
            String vehicleId = "TRK-" + ThreadLocalRandom.current().nextInt(1000, 9999);

            // Starting coordinates (e.g., somewhere in US)
            double startLat = 37.7749 + (ThreadLocalRandom.current().nextDouble() * 5 - 2.5); // Around SF
            double startLng = -122.4194 + (ThreadLocalRandom.current().nextDouble() * 5 - 2.5);

            long now = System.currentTimeMillis();
            ShipmentEvent newShipment = ShipmentEvent.builder()
                    .shipmentId(shipmentId)
                    .eventType(STATE_CREATED)
                    .vehicleId(vehicleId)
                    .currentLat(startLat)
                    .currentLng(startLng)
                    .timestamp(now)
                    .build();

            activeShipments.put(shipmentId, new ShipmentContext(newShipment, now));
            publishEvent(newShipment);
            log.info("Started new shipment: {}", shipmentId);
        }
    }

    /**
     * Iterate over active shipments and process state transitions, GPS drift, and anomalies.
     */
    private void processActiveShipments() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, ShipmentContext>> iterator = activeShipments.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, ShipmentContext> entry = iterator.next();
            ShipmentContext context = entry.getValue();
            ShipmentEvent currentEvent = context.getEvent();

            // Clone the event to modify it for the next state
            ShipmentEvent nextEvent = cloneEvent(currentEvent);
            nextEvent.setTimestamp(now);

            boolean stateChanged = false;
            boolean eventEmitted = false;
            long timeInState = now - context.getStateEnteredTimestamp();

            switch (currentEvent.getEventType()) {
                case STATE_CREATED:
                    if (timeInState >= loadingDelayMs) {
                        nextEvent.setEventType(STATE_DEPARTED);
                        stateChanged = true;
                    }
                    break;

                case STATE_DEPARTED:
                    driftGps(nextEvent);

                    double anomalyRand = ThreadLocalRandom.current().nextDouble();
                    if (anomalyRand < tempAnomalyRate) {
                        injectAnomaly(nextEvent, ANOMALY_TEMP_CHANGE);
                        eventEmitted = true;
                    } else if (anomalyRand < tempAnomalyRate + routeAnomalyRate) {
                        injectAnomaly(nextEvent, ANOMALY_ROUTE_DEVIATION);
                        eventEmitted = true;
                    }

                    if (!eventEmitted && timeInState >= drivingDelayMs) {
                        nextEvent.setEventType(STATE_ARRIVED);
                        stateChanged = true;
                    }
                    break;

                case STATE_ARRIVED:
                    // 10% probability to emit a barcode scan event while in the warehouse
                    if (ThreadLocalRandom.current().nextDouble() < 0.10) {
                        ShipmentEvent scanEvent = cloneEvent(nextEvent);
                        scanEvent.setEventType(EVENT_BARCODE_SCANNED);
                        publishEvent(scanEvent);
                        log.info("Barcode scanned for shipment: {}", scanEvent.getShipmentId());
                    }

                    if (timeInState >= unloadingDelayMs) {
                        nextEvent.setEventType(STATE_COMPLETED);
                        stateChanged = true;
                    }
                    break;

                case STATE_COMPLETED:
                    // Should be cleaned up, but just in case
                    iterator.remove();
                    continue;
            }

            if (stateChanged) {
                if (STATE_COMPLETED.equals(nextEvent.getEventType())) {
                    iterator.remove();
                    log.info("Shipment completed: {}", nextEvent.getShipmentId());
                } else {
                    context.setEvent(nextEvent);
                    context.setStateEnteredTimestamp(now);
                }
                publishEvent(nextEvent);
            } else if (!eventEmitted && STATE_DEPARTED.equals(currentEvent.getEventType())) {
                 context.setEvent(nextEvent);
                 publishEvent(nextEvent);
            }
        }
    }

    /**
     * Simulates slight movement along a route.
     */
    private void driftGps(ShipmentEvent event) {
        // Drift by up to 0.01 degrees (~1km) per tick
        double latDrift = (ThreadLocalRandom.current().nextDouble() * 0.02) - 0.01;
        double lngDrift = (ThreadLocalRandom.current().nextDouble() * 0.02) - 0.01;

        event.setCurrentLat(event.getCurrentLat() + latDrift);
        event.setCurrentLng(event.getCurrentLng() + lngDrift);
    }

    /**
     * Injects an anomaly event.
     */
    private void injectAnomaly(ShipmentEvent baseEvent, String anomalyType) {
        ShipmentEvent anomalyEvent = cloneEvent(baseEvent);
        anomalyEvent.setTimestamp(System.currentTimeMillis());
        anomalyEvent.setEventType(anomalyType);

        if (ANOMALY_ROUTE_DEVIATION.equals(anomalyType)) {
            // Sudden spike in GPS
            anomalyEvent.setCurrentLat(anomalyEvent.getCurrentLat() + 0.5); // Big jump
            anomalyEvent.setCurrentLng(anomalyEvent.getCurrentLng() + 0.5);
        }

        log.warn("Anomaly injected: {} for shipment {}", anomalyType, anomalyEvent.getShipmentId());
        publishEvent(anomalyEvent);
    }

    /**
     * Helper to clone an event.
     */
    private ShipmentEvent cloneEvent(ShipmentEvent source) {
        return ShipmentEvent.builder()
                .shipmentId(source.getShipmentId())
                .eventType(source.getEventType())
                .vehicleId(source.getVehicleId())
                .currentLat(source.getCurrentLat())
                .currentLng(source.getCurrentLng())
                .timestamp(source.getTimestamp())
                .build();
    }

    /**
     * Publishes the event to Kafka.
     * Uses shipmentId as the partition key.
     */
    private void publishEvent(ShipmentEvent event) {
        kafkaTemplate.send(topicName, event.getShipmentId(), event)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.debug("Sent message=[{}] with offset=[{}]", event.getEventType(), result.getRecordMetadata().offset());
                } else {
                    log.error("Unable to send message=[{}] due to : {}", event.getEventType(), ex.getMessage());
                }
            });
    }

    @Data
    @AllArgsConstructor
    private static class ShipmentContext {
        private ShipmentEvent event;
        private long stateEnteredTimestamp;
    }
}
