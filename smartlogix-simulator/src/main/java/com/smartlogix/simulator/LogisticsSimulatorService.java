package com.smartlogix.simulator;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import jakarta.annotation.PreDestroy;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * SmartLogix simulator for shipment lifecycle, vehicle telemetry, warehouse
 * operations, and controlled anomaly scenarios.
 */
@Service
@Slf4j
public class LogisticsSimulatorService {

    private static final String STATE_CREATED = "shipment_created";
    private static final String STATE_DEPARTED = "vehicle_departed";
    private static final String STATE_ARRIVED = "warehouse_arrived";
    private static final String STATE_COMPLETED = "delivery_completed";

    private static final String EVENT_POSITION_UPDATED = "vehicle_position_updated";
    private static final String EVENT_IDLE_STARTED = "vehicle_idle_started";
    private static final String EVENT_IDLE_ENDED = "vehicle_idle_ended";
    private static final String EVENT_TEMP_READING = "temperature_reading";
    private static final String EVENT_BARCODE_SCANNED = "barcode_scanned";
    private static final String EVENT_QUEUE_DEPTH_CHANGED = "queue_depth_changed";
    private static final String EVENT_BAY_ASSIGNED = "bay_assigned";
    private static final String EVENT_LOADING_COMPLETED = "loading_completed";
    private static final String EVENT_UNLOADING_COMPLETED = "unloading_completed";

    private static final String ANOMALY_ROUTE_DEVIATION = "route_deviation";
    private static final String ANOMALY_TEMP_CHANGE = "temperature_change";
    private static final String ANOMALY_UNAUTHORIZED_STOP = "unauthorized_stop";

    private static final WarehouseProfile[] WAREHOUSES = {
            new WarehouseProfile("WH-SEA", 47.6062, -122.3321, 120),
            new WarehouseProfile("WH-SFO", 37.7749, -122.4194, 160),
            new WarehouseProfile("WH-LAX", 34.0522, -118.2437, 180),
            new WarehouseProfile("WH-DEN", 39.7392, -104.9903, 140),
            new WarehouseProfile("WH-CHI", 41.8781, -87.6298, 200)
    };

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final SimulatorProperties properties;
    private final SimulatorTopicsProperties topics;
    private final SimulatorMetrics metrics;

    private final Map<String, ShipmentContext> activeShipments = new ConcurrentHashMap<>();
    private final Map<String, WarehouseRuntime> warehouseRuntime = new ConcurrentHashMap<>();

    public LogisticsSimulatorService(
            KafkaTemplate<String, Object> kafkaTemplate,
            SimulatorProperties properties,
            SimulatorTopicsProperties topics,
            SimulatorMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.topics = topics;
        this.metrics = metrics;

        for (WarehouseProfile warehouse : WAREHOUSES) {
            warehouseRuntime.put(warehouse.id(), new WarehouseRuntime(warehouse.capacity() / 3, 0));
        }
        metrics.setActiveShipments(activeShipments.size());
    }

    /**
     * Executes the simulator loop at a configurable cadence.
     */
    @Scheduled(fixedRateString = "${simulator.logistics.tick-rate-ms:5000}")
    public void simulateTick() {
        long now = System.currentTimeMillis();
        tryAddNewShipment(now);
        processActiveShipments(now);
        emitWarehouseMetrics(now);
        metrics.setActiveShipments(activeShipments.size());
    }

    @PreDestroy
    public void shutdown() {
        log.info("Stopping SmartLogix simulator with {} active shipments; flushing Kafka producer", activeShipments.size());
        kafkaTemplate.flush();
    }

    @KafkaListener(topics = "${app.simulator.topics.simulator-commands:simulator-commands}")
    public void injectDynamicShipment(ShipmentSimulationCommand command) {
        if (command == null || isBlank(command.getShipmentId())) {
            log.warn("Ignoring simulator command because shipmentId is missing");
            return;
        }

        WarehouseProfile origin;
        WarehouseProfile destination;
        try {
            origin = warehouseById(command.getOriginWarehouseId());
            destination = warehouseById(command.getDestinationWarehouseId());
        } catch (IllegalArgumentException ex) {
            log.warn("Ignoring simulator command for shipment {} because {}", command.getShipmentId(), ex.getMessage());
            return;
        }

        if (origin.id().equals(destination.id())) {
            log.warn("Ignoring simulator command for shipment {} because origin and destination are both {}",
                    command.getShipmentId(), origin.id());
            return;
        }

        long now = System.currentTimeMillis();
        String vehicleId = generateVehicleId();
        String driverId = generateDriverId();

        ShipmentEvent created = ShipmentEvent.builder()
                .shipmentId(command.getShipmentId())
                .eventType(STATE_CREATED)
                .vehicleId(vehicleId)
                .driverId(driverId)
                .originWarehouseId(origin.id())
                .destinationWarehouseId(destination.id())
                .currentLat(origin.lat())
                .currentLng(origin.lng())
                .plannedEta(plannedEta(now))
                .temperatureCelsius(normalStartingTemperature())
                .delayRiskScore(0.05)
                .timestamp(now)
                .build();

        ShipmentContext context = new ShipmentContext(created, now, origin, destination, 0.0, false, false, 0);
        ShipmentContext previous = activeShipments.put(command.getShipmentId(), context);
        if (previous == null) {
            incrementWarehouseLoad(origin.id(), 1);
        }
        metrics.setActiveShipments(activeShipments.size());

        publish(topics.getShipmentEvents(), command.getShipmentId(), created);
        publishWarehouseEvent(origin.id(), command.getShipmentId(), vehicleId, EVENT_BARCODE_SCANNED, now);

        log.info("Dynamic shipment {} injected from {} to {} with vehicleId={} driverId={}",
                command.getShipmentId(), origin.id(), destination.id(), vehicleId, driverId);
    }

    private void tryAddNewShipment(long now) {
        if (activeShipments.size() >= properties.getMaxActiveShipments()) {
            log.debug("Skipping new shipment because active shipment limit {} is reached",
                    properties.getMaxActiveShipments());
            return;
        }

        if (ThreadLocalRandom.current().nextDouble() >= properties.getNewShipmentProbability()) {
            return;
        }

        WarehouseProfile origin = randomWarehouse();
        WarehouseProfile destination = randomWarehouseExcept(origin.id());
        String shipmentId = "SHP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String vehicleId = generateVehicleId();
        String driverId = generateDriverId();

        ShipmentEvent created = ShipmentEvent.builder()
                .shipmentId(shipmentId)
                .eventType(STATE_CREATED)
                .vehicleId(vehicleId)
                .driverId(driverId)
                .originWarehouseId(origin.id())
                .destinationWarehouseId(destination.id())
                .currentLat(origin.lat())
                .currentLng(origin.lng())
                .plannedEta(plannedEta(now))
                .temperatureCelsius(normalStartingTemperature())
                .delayRiskScore(0.05)
                .timestamp(now)
                .build();

        ShipmentContext context = new ShipmentContext(created, now, origin, destination, 0.0, false, false, 0);
        activeShipments.put(shipmentId, context);
        incrementWarehouseLoad(origin.id(), 1);
        metrics.setActiveShipments(activeShipments.size());

        publish(topics.getShipmentEvents(), shipmentId, created);
        publishWarehouseEvent(origin.id(), shipmentId, vehicleId, EVENT_BARCODE_SCANNED, now);
        log.info("Started shipment {} from {} to {}", shipmentId, origin.id(), destination.id());
    }

    private void processActiveShipments(long now) {
        Iterator<Map.Entry<String, ShipmentContext>> iterator = activeShipments.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, ShipmentContext> entry = iterator.next();
            ShipmentContext context = entry.getValue();
            ShipmentEvent current = context.getEvent();
            ShipmentEvent next = cloneEvent(current);
            next.setTimestamp(now);

            long timeInState = now - context.getStateEnteredTimestamp();

            switch (current.getEventType()) {
                case STATE_CREATED:
                    emitLoadingTelemetry(context, now);
                    if (timeInState >= properties.getDelaysMs().getLoading()) {
                        transition(context, next, STATE_DEPARTED, now);
                        publishWarehouseEvent(current.getOriginWarehouseId(), current.getShipmentId(),
                                current.getVehicleId(), EVENT_LOADING_COMPLETED, now);
                    }
                    break;

                case STATE_DEPARTED:
                    advanceInTransit(context, next, now, timeInState);
                    break;

                case STATE_ARRIVED:
                    emitWarehouseDwellEvents(context, now);
                    if (timeInState >= properties.getDelaysMs().getUnloading()) {
                        transition(context, next, STATE_COMPLETED, now);
                        publishWarehouseEvent(current.getDestinationWarehouseId(), current.getShipmentId(),
                                current.getVehicleId(), EVENT_UNLOADING_COMPLETED, now);
                        incrementWarehouseLoad(current.getDestinationWarehouseId(), -1);
                        iterator.remove();
                        log.info("Completed shipment {}", current.getShipmentId());
                    }
                    break;

                case STATE_COMPLETED:
                    iterator.remove();
                    break;

                default:
                    log.warn("Unknown state {} for shipment {}", current.getEventType(), current.getShipmentId());
                    iterator.remove();
                    break;
            }
        }
    }

    private void emitLoadingTelemetry(ShipmentContext context, long now) {
        ShipmentEvent event = context.getEvent();
        publishVehicleTelemetry(context, EVENT_IDLE_STARTED, 0.0, true, true, event.getOriginWarehouseId(), now);
    }

    private void advanceInTransit(ShipmentContext context, ShipmentEvent next, long now, long timeInState) {
        context.setRouteProgress(Math.min(1.0, context.getRouteProgress() + ThreadLocalRandom.current().nextDouble(0.12, 0.24)));
        applyRoutePosition(context, next);
        next.setTemperatureCelsius(normalTemperature(next.getTemperatureCelsius()));
        next.setDelayRiskScore(calculateDelayRisk(next, now));
        context.setEvent(next);

        publishVehicleTelemetry(context, EVENT_POSITION_UPDATED, ThreadLocalRandom.current().nextDouble(45.0, 88.0),
                false, false, null, now);
        publishVehicleTelemetry(context, EVENT_TEMP_READING, ThreadLocalRandom.current().nextDouble(45.0, 88.0),
                false, false, null, now);

        maybeInjectAnomaly(context, next, now);

        if (timeInState >= properties.getDelaysMs().getDriving() || context.getRouteProgress() >= 1.0) {
            next.setCurrentLat(context.getDestination().lat());
            next.setCurrentLng(context.getDestination().lng());
            transition(context, next, STATE_ARRIVED, now);
            incrementWarehouseLoad(context.getDestination().id(), 1);
            publishWarehouseEvent(context.getDestination().id(), next.getShipmentId(), next.getVehicleId(),
                    EVENT_BARCODE_SCANNED, now);
            publishWarehouseEvent(context.getDestination().id(), next.getShipmentId(), next.getVehicleId(),
                    EVENT_BAY_ASSIGNED, now);
        } else {
            publish(topics.getShipmentEvents(), next.getShipmentId(), next);
        }
    }

    private void emitWarehouseDwellEvents(ShipmentContext context, long now) {
        ShipmentEvent event = context.getEvent();
        if (ThreadLocalRandom.current().nextDouble() < properties.getWarehouse().getBarcodeScanProbability()) {
            publishWarehouseEvent(event.getDestinationWarehouseId(), event.getShipmentId(), event.getVehicleId(),
                    EVENT_BARCODE_SCANNED, now);
        }
        publishVehicleTelemetry(context, EVENT_IDLE_STARTED, 0.0, true, true, event.getDestinationWarehouseId(), now);
    }

    private void maybeInjectAnomaly(ShipmentContext context, ShipmentEvent baseEvent, long now) {
        double anomalyRand = ThreadLocalRandom.current().nextDouble();
        SimulatorProperties.AnomalyRates anomalyRates = properties.getAnomalyRates();

        if (anomalyRand < anomalyRates.getTemperature()) {
            ShipmentEvent anomaly = cloneEvent(baseEvent);
            anomaly.setEventType(ANOMALY_TEMP_CHANGE);
            anomaly.setTemperatureCelsius(properties.getColdChain().getMaxTemperatureCelsius()
                    + ThreadLocalRandom.current().nextDouble(3.0, 9.0));
            anomaly.setAnomalyReason("Cold-chain reading outside configured temperature band");
            publish(topics.getShipmentEvents(), anomaly.getShipmentId(), anomaly);
            publishAlert("COLD_CHAIN_VIOLATION", "CRITICAL", anomaly, null, anomaly.getAnomalyReason(), now);
            return;
        }

        if (anomalyRand < anomalyRates.getTemperature() + anomalyRates.getRouteDeviation()) {
            ShipmentEvent anomaly = cloneEvent(baseEvent);
            anomaly.setEventType(ANOMALY_ROUTE_DEVIATION);
            anomaly.setCurrentLat(anomaly.getCurrentLat() + ThreadLocalRandom.current().nextDouble(0.20, 0.55));
            anomaly.setCurrentLng(anomaly.getCurrentLng() + ThreadLocalRandom.current().nextDouble(0.20, 0.55));
            anomaly.setAnomalyReason("GPS coordinates deviated more than 500m from planned route");
            context.setRouteDeviationCount(context.getRouteDeviationCount() + 1);
            publish(topics.getShipmentEvents(), anomaly.getShipmentId(), anomaly);
            publishAlert("ROUTE_DEVIATION", "HIGH", anomaly, null, anomaly.getAnomalyReason(), now);

            if (context.getRouteDeviationCount() > 3) {
                publishAlert("REPEATED_ROUTE_DEVIATION", "HIGH", anomaly, null,
                        "Shipment crossed repeated route-deviation threshold", now);
            }
            return;
        }

        if (anomalyRand < anomalyRates.getTemperature() + anomalyRates.getRouteDeviation()
                + anomalyRates.getUnauthorizedStop()) {
            ShipmentEvent anomaly = cloneEvent(baseEvent);
            anomaly.setEventType(ANOMALY_UNAUTHORIZED_STOP);
            anomaly.setAnomalyReason("Vehicle idle outside warehouse or depot");
            context.setUnauthorizedStopActive(true);
            publish(topics.getShipmentEvents(), anomaly.getShipmentId(), anomaly);
            publishVehicleTelemetry(context, EVENT_IDLE_STARTED, 0.0, true, false, null, now);
            publishAlert("UNAUTHORIZED_STOP", "HIGH", anomaly, null, anomaly.getAnomalyReason(), now);
        } else if (context.isUnauthorizedStopActive()) {
            context.setUnauthorizedStopActive(false);
            publishVehicleTelemetry(context, EVENT_IDLE_ENDED, ThreadLocalRandom.current().nextDouble(35.0, 65.0),
                    false, false, null, now);
        }
    }

    private void emitWarehouseMetrics(long now) {
        for (Map.Entry<String, WarehouseRuntime> entry : warehouseRuntime.entrySet()) {
            WarehouseRuntime runtime = entry.getValue();
            int queueDelta = ThreadLocalRandom.current().nextInt(-2, 4);
            runtime.setQueueDepth(Math.max(0, runtime.getQueueDepth() + queueDelta));

            publishWarehouseEvent(entry.getKey(), null, null, EVENT_QUEUE_DEPTH_CHANGED, now);

            if (properties.isAnalyticsSimulationEnabled()) {
                Map<String, String> tags = new HashMap<>();
                tags.put("warehouse_id", entry.getKey());
                publish(topics.getAnalyticsMetrics(), "warehouse_load", AnalyticsMetricEvent.builder()
                        .metricType("warehouse_load")
                        .entityId(entry.getKey())
                        .value(runtime.getCurrentLoad())
                        .unit("shipments")
                        .tags(tags)
                        .timestamp(now)
                        .build());
            }

            if (runtime.getQueueDepth() > properties.getWarehouse().getCongestionQueueThreshold()) {
                publishAlert("WAREHOUSE_CONGESTION", "MEDIUM", null, entry.getKey(),
                        "Warehouse queue depth exceeded simulator threshold", now);
            }
        }
    }

    private void transition(ShipmentContext context, ShipmentEvent event, String eventType, long now) {
        event.setEventType(eventType);
        event.setTimestamp(now);
        event.setAnomalyReason(null);
        context.setEvent(event);
        context.setStateEnteredTimestamp(now);
        publish(topics.getShipmentEvents(), event.getShipmentId(), event);
    }

    private void publishVehicleTelemetry(ShipmentContext context, String eventType, double speed, boolean idle,
                                         boolean atWarehouse, String nearestWarehouseId, long now) {
        ShipmentEvent event = context.getEvent();
        publish(topics.getVehicleTelemetry(), event.getVehicleId(), VehicleTelemetryEvent.builder()
                .vehicleId(event.getVehicleId())
                .shipmentId(event.getShipmentId())
                .driverId(event.getDriverId())
                .eventType(eventType)
                .lat(event.getCurrentLat())
                .lng(event.getCurrentLng())
                .speedKmph(speed)
                .temperatureCelsius(event.getTemperatureCelsius())
                .idle(idle)
                .atWarehouse(atWarehouse)
                .nearestWarehouseId(nearestWarehouseId)
                .timestamp(now)
                .build());
    }

    private void publishWarehouseEvent(String warehouseId, String shipmentId, String vehicleId, String eventType, long now) {
        WarehouseRuntime runtime = warehouseRuntime.get(warehouseId);
        if (runtime == null) {
            return;
        }

        WarehouseProfile profile = warehouseById(warehouseId);
        String bayId = ThreadLocalRandom.current().nextDouble() < properties.getWarehouse().getBayAssignmentProbability()
                ? "BAY-" + ThreadLocalRandom.current().nextInt(1, 24)
                : null;

        publish(topics.getWarehouseEvents(), warehouseId, WarehouseEvent.builder()
                .warehouseId(warehouseId)
                .shipmentId(shipmentId)
                .vehicleId(vehicleId)
                .eventType(eventType)
                .bayId(bayId)
                .queueDepth(runtime.getQueueDepth())
                .currentLoad(runtime.getCurrentLoad())
                .capacity(profile.capacity())
                .timestamp(now)
                .build());
    }

    private void publishAlert(String type, String severity, ShipmentEvent shipmentEvent, String warehouseId,
                              String message, long now) {
        if (!properties.isAlertSimulationEnabled()) {
            return;
        }

        publish(topics.getAlerts(), type, AlertSimulationEvent.builder()
                .alertId("ALT-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase())
                .alertType(type)
                .severity(severity)
                .shipmentId(shipmentEvent != null ? shipmentEvent.getShipmentId() : null)
                .vehicleId(shipmentEvent != null ? shipmentEvent.getVehicleId() : null)
                .warehouseId(warehouseId)
                .message(message)
                .createdAt(now)
                .build());
    }

    private void publish(String topic, String key, Object event) {
        kafkaTemplate.send(topic, key, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        metrics.recordSent(topic);
                        log.debug("Sent topic={} key={} offset={}", topic, key, result.getRecordMetadata().offset());
                    } else {
                        metrics.recordFailed(topic);
                        log.error("Unable to send topic={} key={} because {}", topic, key, ex.getMessage());
                    }
                });
    }

    private void applyRoutePosition(ShipmentContext context, ShipmentEvent event) {
        double progress = context.getRouteProgress();
        double lat = interpolate(context.getOrigin().lat(), context.getDestination().lat(), progress);
        double lng = interpolate(context.getOrigin().lng(), context.getDestination().lng(), progress);
        event.setCurrentLat(lat + ThreadLocalRandom.current().nextDouble(-0.01, 0.01));
        event.setCurrentLng(lng + ThreadLocalRandom.current().nextDouble(-0.01, 0.01));
    }

    private double calculateDelayRisk(ShipmentEvent event, long now) {
        long remainingMs = event.getPlannedEta() - now;
        double etaPressure = remainingMs < 30 * 60 * 1000L ? 0.70 : 0.15;
        double randomNoise = ThreadLocalRandom.current().nextDouble(0.0, 0.20);
        return Math.min(1.0, etaPressure + randomNoise);
    }

    private double normalTemperature(double current) {
        double next = current + ThreadLocalRandom.current().nextDouble(-0.35, 0.35);
        return Math.max(properties.getColdChain().getMinTemperatureCelsius(),
                Math.min(properties.getColdChain().getMaxTemperatureCelsius(), next));
    }

    private double normalStartingTemperature() {
        return ThreadLocalRandom.current().nextDouble(
                properties.getColdChain().getMinTemperatureCelsius(),
                properties.getColdChain().getMaxTemperatureCelsius());
    }

    private long plannedEta(long now) {
        return now + properties.getDelaysMs().getLoading()
                + properties.getDelaysMs().getDriving()
                + properties.getDelaysMs().getUnloading()
                + ThreadLocalRandom.current().nextLong(60000, 180000);
    }

    private String generateVehicleId() {
        return "TRK-" + ThreadLocalRandom.current().nextInt(1000, 9999);
    }

    private String generateDriverId() {
        return "DRV-" + ThreadLocalRandom.current().nextInt(10000, 99999);
    }

    private void incrementWarehouseLoad(String warehouseId, int delta) {
        WarehouseRuntime runtime = warehouseRuntime.get(warehouseId);
        if (runtime != null) {
            WarehouseProfile profile = warehouseById(warehouseId);
            runtime.setCurrentLoad(Math.max(0, Math.min(profile.capacity(), runtime.getCurrentLoad() + delta)));
        }
    }

    private ShipmentEvent cloneEvent(ShipmentEvent source) {
        return ShipmentEvent.builder()
                .shipmentId(source.getShipmentId())
                .eventType(source.getEventType())
                .vehicleId(source.getVehicleId())
                .driverId(source.getDriverId())
                .originWarehouseId(source.getOriginWarehouseId())
                .destinationWarehouseId(source.getDestinationWarehouseId())
                .currentLat(source.getCurrentLat())
                .currentLng(source.getCurrentLng())
                .plannedEta(source.getPlannedEta())
                .temperatureCelsius(source.getTemperatureCelsius())
                .delayRiskScore(source.getDelayRiskScore())
                .anomalyReason(source.getAnomalyReason())
                .timestamp(source.getTimestamp())
                .build();
    }

    private WarehouseProfile randomWarehouse() {
        return WAREHOUSES[ThreadLocalRandom.current().nextInt(WAREHOUSES.length)];
    }

    private WarehouseProfile randomWarehouseExcept(String warehouseId) {
        WarehouseProfile warehouse;
        do {
            warehouse = randomWarehouse();
        } while (warehouse.id().equals(warehouseId));
        return warehouse;
    }

    private WarehouseProfile warehouseById(String warehouseId) {
        for (WarehouseProfile warehouse : WAREHOUSES) {
            if (warehouse.id().equals(warehouseId)) {
                return warehouse;
            }
        }
        throw new IllegalArgumentException("Unknown warehouse: " + warehouseId);
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private double interpolate(double start, double end, double progress) {
        return start + ((end - start) * progress);
    }

    private record WarehouseProfile(String id, double lat, double lng, int capacity) {
    }

    @Data
    @AllArgsConstructor
    private static class ShipmentContext {
        private ShipmentEvent event;
        private long stateEnteredTimestamp;
        private WarehouseProfile origin;
        private WarehouseProfile destination;
        private double routeProgress;
        private boolean unauthorizedStopActive;
        private boolean fraudScenarioActive;
        private int routeDeviationCount;
    }

    @Data
    @AllArgsConstructor
    private static class WarehouseRuntime {
        private int currentLoad;
        private int queueDepth;
    }
}
