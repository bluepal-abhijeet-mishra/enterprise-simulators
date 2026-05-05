package com.smartlogix.simulator;

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

    // Track active shipments. Thread-safe map for scheduler access.
    // Key: Shipment ID, Value: Current ShipmentEvent state
    private final Map<String, ShipmentEvent> activeShipments = new ConcurrentHashMap<>();

    // Valid lifecycle states
    private static final String STATE_CREATED = "shipment_created";
    private static final String STATE_DEPARTED = "vehicle_departed";
    private static final String STATE_ARRIVED = "warehouse_arrived";
    private static final String STATE_COMPLETED = "delivery_completed";

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

            ShipmentEvent newShipment = ShipmentEvent.builder()
                    .shipmentId(shipmentId)
                    .eventType(STATE_CREATED)
                    .vehicleId(vehicleId)
                    .currentLat(startLat)
                    .currentLng(startLng)
                    .timestamp(System.currentTimeMillis())
                    .build();

            activeShipments.put(shipmentId, newShipment);
            publishEvent(newShipment);
            log.info("Started new shipment: {}", shipmentId);
        }
    }

    /**
     * Iterate over active shipments and process state transitions, GPS drift, and anomalies.
     */
    private void processActiveShipments() {
        Iterator<Map.Entry<String, ShipmentEvent>> iterator = activeShipments.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, ShipmentEvent> entry = iterator.next();
            ShipmentEvent currentEvent = entry.getValue();

            // Clone the event to modify it for the next state
            ShipmentEvent nextEvent = cloneEvent(currentEvent);
            nextEvent.setTimestamp(System.currentTimeMillis());

            boolean stateChanged = false;
            boolean eventEmitted = false;

            switch (currentEvent.getEventType()) {
                case STATE_CREATED:
                    // 30% chance to depart on each tick
                    if (ThreadLocalRandom.current().nextDouble() < 0.30) {
                        nextEvent.setEventType(STATE_DEPARTED);
                        stateChanged = true;
                    }
                    break;

                case STATE_DEPARTED:
                    // Drift GPS
                    driftGps(nextEvent);

                    // 5% chance to inject an anomaly mid-journey
                    if (ThreadLocalRandom.current().nextDouble() < 0.05) {
                        injectAnomaly(nextEvent);
                        eventEmitted = true; // Anomaly was emitted
                        // Revert GPS if anomaly changed it wildly, or keep it.
                        // Let's reset the event type back to departed for the map storage
                        // The anomaly event was sent, but the shipment is STILL departed.
                    }

                    // 10% chance to arrive at warehouse (if no anomaly was processed to keep logic simple)
                    if (!eventEmitted && ThreadLocalRandom.current().nextDouble() < 0.10) {
                        nextEvent.setEventType(STATE_ARRIVED);
                        stateChanged = true;
                    }
                    break;

                case STATE_ARRIVED:
                    // 40% chance to complete delivery
                    if (ThreadLocalRandom.current().nextDouble() < 0.40) {
                        nextEvent.setEventType(STATE_COMPLETED);
                        stateChanged = true;
                    }
                    break;

                case STATE_COMPLETED:
                    // Should be cleaned up, but just in case
                    iterator.remove();
                    continue;
            }

            // Update map and publish if state changed
            if (stateChanged) {
                // If we reached completed state, remove from map
                if (STATE_COMPLETED.equals(nextEvent.getEventType())) {
                    iterator.remove();
                    log.info("Shipment completed: {}", nextEvent.getShipmentId());
                } else {
                    activeShipments.put(nextEvent.getShipmentId(), nextEvent);
                }
                publishEvent(nextEvent);
            } else if (!eventEmitted && STATE_DEPARTED.equals(currentEvent.getEventType())) {
                 // Always emit GPS updates when departed, even if state didn't change
                 activeShipments.put(nextEvent.getShipmentId(), nextEvent);
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
    private void injectAnomaly(ShipmentEvent baseEvent) {
        ShipmentEvent anomalyEvent = cloneEvent(baseEvent);
        anomalyEvent.setTimestamp(System.currentTimeMillis());

        if (ThreadLocalRandom.current().nextBoolean()) {
            anomalyEvent.setEventType(ANOMALY_ROUTE_DEVIATION);
            // Sudden spike in GPS
            anomalyEvent.setCurrentLat(anomalyEvent.getCurrentLat() + 0.5); // Big jump
            anomalyEvent.setCurrentLng(anomalyEvent.getCurrentLng() + 0.5);
            log.warn("Anomaly injected: {} for shipment {}", ANOMALY_ROUTE_DEVIATION, anomalyEvent.getShipmentId());
        } else {
            anomalyEvent.setEventType(ANOMALY_TEMP_CHANGE);
            log.warn("Anomaly injected: {} for shipment {}", ANOMALY_TEMP_CHANGE, anomalyEvent.getShipmentId());
        }

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
}
