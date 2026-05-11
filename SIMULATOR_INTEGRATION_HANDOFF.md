# SmartLogix Simulator Integration Handoff

Document version: v1.2  
Service: `smartlogix-simulator`  
Owner: Simulator team  
Purpose: PRD Phase 1 Kafka event generator and stateful mock shipment simulator  
Runtime: Java 17, Spring Boot 3.2.x, Apache Kafka

## Executive Summary

The SmartLogix simulator is a Spring Boot Kafka service that generates realistic logistics events for the SmartLogix real-time monitoring platform.

It is designed to help backend, data engineering, alerting, fraud detection, QA, and dashboard teams build and test their services before real IoT, warehouse, driver, and shipment integrations are available.

The simulator publishes JSON messages to the PRD-defined Kafka topics. It also consumes dynamic shipment simulation commands from `simulator-commands`, so downstream teams can inject a real user-created `shipmentId` into the simulator state machine and receive realistic lifecycle events for that exact parcel.

This service is intentionally not a backend API service. It does not expose business REST APIs, does not write to PostgreSQL, and does not implement RBAC or fraud logic. Its responsibility is reliable, realistic event generation plus command-driven shipment injection for integration testing.

## PRD Alignment

The simulator satisfies the PRD Phase 1 requirement:

> Build Kafka producer for simulated shipment events.

It supports the following PRD areas:

| PRD Area | Simulator Support |
| --- | --- |
| Real-Time Shipment Tracking | Publishes lifecycle events to `shipment-events` |
| Vehicle Telemetry | Publishes GPS, speed, idle, and temperature telemetry to `vehicle-telemetry` |
| Warehouse Activity | Publishes barcode, bay, queue, load, loading, and unloading events to `warehouse-events` |
| Alert Engine Integration | Optionally publishes alert-shaped events to `alerts` for early integration |
| Analytics/ETL Integration | Publishes warehouse metric events to `analytics-metrics` |
| Fraud Detection Integration | Emits route deviation and unauthorized stop events that fraud services can consume |
| Dashboard Integration | Provides live data streams for maps, alerts, warehouse load, and SLA panels |
| User-Driven Parcel Flow Testing | Consumes `simulator-commands` and simulates the requested `shipmentId` |

The simulator does not replace the real Alert Engine, Fraud Detection module, ETL jobs, REST APIs, PostgreSQL persistence, ClickHouse/Elasticsearch analytics store, or React dashboard. Those are downstream responsibilities.

## System Architecture

```text
smartlogix-simulator
        |
        | Kafka producer
        v
Apache Kafka
        ^
        | Kafka command producer
Logistics Backend
        |
        | Kafka consumers / Kafka Streams / ETL jobs
        v
Backend APIs / Alert Engine / Fraud Detection / Data Pipeline / Dashboard
```

Downstream projects should not call the simulator through REST. They connect to the same Kafka broker. Backend services consume simulator events, and the logistics backend may publish shipment simulation commands to `simulator-commands`.

## Module Location

The simulator Maven project is located at:

```text
enterprise-simulators/smartlogix-simulator
```

Important files:

| File | Purpose |
| --- | --- |
| `src/main/java/com/smartlogix/simulator/LogisticsSimulatorService.java` | Main event generation loop |
| `src/main/java/com/smartlogix/simulator/ShipmentSimulationCommand.java` | Command DTO consumed from `simulator-commands` |
| `src/main/java/com/smartlogix/simulator/KafkaTopicConfig.java` | PRD topic creation |
| `src/main/java/com/smartlogix/simulator/SimulatorProperties.java` | Typed and validated simulator configuration |
| `src/main/java/com/smartlogix/simulator/SimulatorTopicsProperties.java` | Topic name binding |
| `src/main/java/com/smartlogix/simulator/SimulatorMetrics.java` | Custom metrics |
| `src/main/resources/application.yml` | Default runtime configuration |
| `src/main/resources/application-local.yml` | Local development profile |
| `src/main/resources/application-docker.yml` | Docker profile |
| `src/main/resources/application-load-test.yml` | Higher-volume load-test profile |
| `docker-compose.yml` | Kafka + simulator local stack |

## How The Simulator Works

The simulator runs a scheduled logistics tick.

Default configuration:

```yaml
simulator:
  logistics:
    tick-rate-ms: 5000
    new-shipment-probability: 0.20
    max-active-shipments: 250
```

By default, every 5 seconds the simulator:

1. Decides whether to create a new shipment.
2. Advances active shipments through their lifecycle.
3. Emits vehicle GPS, idle, speed, and temperature telemetry.
4. Emits warehouse queue/load/barcode/bay events.
5. Completes shipments after configured loading, driving, and unloading durations.
6. Randomly injects anomalies such as cold-chain violations, route deviations, and unauthorized stops.
7. Optionally emits simulated alert events.
8. Optionally emits analytics metric events.
9. Updates health and metrics for operational visibility.

Dynamic shipment command flow:

1. Logistics backend creates a parcel from a user action.
2. Logistics backend publishes a JSON command to Kafka topic `simulator-commands`.
3. Simulator consumes the command using consumer group `logistics-simulator-group`.
4. Simulator validates the origin and destination warehouse IDs.
5. Simulator creates a `shipment_created` event using the exact inbound `shipmentId`.
6. Simulator inserts that shipment into the same in-memory state machine used by synthetic shipments.
7. The scheduled tick advances that parcel through `vehicle_departed`, `warehouse_arrived`, and `delivery_completed`.
8. Downstream consumers receive events for that real user-created `shipmentId` on `shipment-events`.

Shipment lifecycle:

```text
shipment_created
        |
        v
vehicle_departed
        |
        v
warehouse_arrived
        |
        v
delivery_completed
```

Anomaly events can be emitted during transit:

```text
temperature_change
route_deviation
unauthorized_stop
```

## Kafka Connection Model

All teams must connect to the same Kafka broker.

Same machine:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
```

Different machines:

```yaml
spring:
  kafka:
    bootstrap-servers: <KAFKA_HOST_OR_IP>:9092
```

For remote Kafka usage, Kafka must advertise a host/IP reachable by both the simulator and downstream services. Do not use `localhost` as the advertised listener when services run on different machines.

## Kafka Topics

The simulator creates the five PRD event topics and one command topic on startup.

| Topic | Partitions | Retention | Partition Key | Main Consumers |
| --- | ---: | --- | --- | --- |
| `shipment-events` | 12 | 7 days | `shipmentId` | Shipment service, ETL, alert engine, fraud detection |
| `vehicle-telemetry` | 12 | 3 days | `vehicleId` | Live map, vehicle service, alert engine, fraud detection |
| `warehouse-events` | 6 | 7 days | `warehouseId` | Warehouse service, ETL, dashboard analytics |
| `alerts` | 6 | 30 days | `alertType` | Alert API, dashboard, notification service |
| `analytics-metrics` | 4 | 90 days | `metricType` | ETL, analytics store loaders, dashboard metrics |
| `simulator-commands` | 6 | 7 days | `shipmentId` | Simulator command listener |

Partitioning follows the PRD intent:

- `shipmentId` preserves per-shipment lifecycle ordering.
- `vehicleId` groups telemetry by vehicle.
- `warehouseId` groups operational activity by warehouse.
- `alertType` groups alert streams by type.
- `metricType` groups analytics data by metric family.
- `shipmentId` on `simulator-commands` keeps command ordering stable per user-created parcel.

## Event Contract

All events are JSON serialized by Spring Kafka. Type headers are disabled, so consumers should deserialize based on the topic and JSON field names.

### `shipment-events`

Purpose: shipment lifecycle, ETA risk, and anomaly event stream.

Event types:

- `shipment_created`
- `vehicle_departed`
- `warehouse_arrived`
- `delivery_completed`
- `temperature_change`
- `route_deviation`
- `unauthorized_stop`

Partition key:

```text
shipmentId
```

Sample payload:

```json
{
  "shipmentId": "SHP-27DF2C15",
  "eventType": "shipment_created",
  "vehicleId": "TRK-4821",
  "driverId": "DRV-18472",
  "originWarehouseId": "WH-SEA",
  "destinationWarehouseId": "WH-DEN",
  "currentLat": 47.6062,
  "currentLng": -122.3321,
  "plannedEta": 1778146500000,
  "temperatureCelsius": 5.4,
  "delayRiskScore": 0.05,
  "anomalyReason": null,
  "timestamp": 1778146106378
}
```

Consumer recommendations:

- Persist every event to PostgreSQL `shipment_events`.
- Update current shipment status in `shipments`.
- Use `eventType` to drive status transitions.
- Use `temperature_change`, `route_deviation`, and `unauthorized_stop` for alert/fraud rules.
- Treat `timestamp` and `plannedEta` as epoch milliseconds.

### `simulator-commands`

Purpose: command topic used by the logistics backend to inject a user-created parcel into the simulator state machine.

Producer:

```text
Logistics backend / parcel service
```

Consumer:

```text
smartlogix-simulator
```

Simulator consumer group:

```text
logistics-simulator-group
```

Partition key:

```text
shipmentId
```

Sample command payload:

```json
{
  "shipmentId": "PARCEL-100045",
  "originWarehouseId": "WH-SEA",
  "destinationWarehouseId": "WH-LAX"
}
```

Required command fields:

| Field | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `shipmentId` | string | No | Must be the exact parcel/shipment ID created by the logistics backend |
| `originWarehouseId` | string | No | Must match a simulator warehouse ID |
| `destinationWarehouseId` | string | No | Must match a simulator warehouse ID and be different from origin |

Supported warehouse IDs:

```text
WH-SEA
WH-SFO
WH-LAX
WH-DEN
WH-CHI
```

Simulator behavior after consuming the command:

- Generates a realistic `vehicleId`, for example `TRK-4821`.
- Generates a realistic `driverId`, for example `DRV-18472`.
- Builds a `shipment_created` event using the exact command `shipmentId`.
- Inserts the shipment into the active in-memory state machine.
- Publishes the initial `shipment_created` event to `shipment-events`.
- Continues publishing lifecycle, vehicle telemetry, warehouse events, and possible anomaly events during later ticks.

Invalid commands:

- Missing `shipmentId` is ignored and logged.
- Unknown warehouse IDs are ignored and logged.
- Commands where origin and destination are the same are ignored and logged.
- The simulator does not write rejection records to a dead-letter topic yet.

Example Spring Kafka producer from the logistics backend:

```java
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ShipmentSimulationCommandProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public ShipmentSimulationCommandProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void requestSimulation(String shipmentId, String originWarehouseId, String destinationWarehouseId) {
        ShipmentSimulationCommand command = new ShipmentSimulationCommand();
        command.setShipmentId(shipmentId);
        command.setOriginWarehouseId(originWarehouseId);
        command.setDestinationWarehouseId(destinationWarehouseId);

        kafkaTemplate.send("simulator-commands", shipmentId, command);
    }
}
```

Producer-side recommendation:

- Publish the command only after the parcel has been persisted in the logistics backend.
- Use the same `shipmentId` as the parcel record and Kafka key.
- Make command production idempotent from the backend perspective; repeated commands for the same `shipmentId` may restart that active simulation.
- Continue consuming `shipment-events` to update the parcel status shown to users.

### `vehicle-telemetry`

Purpose: live vehicle location, movement, idle state, and cold-chain telemetry.

Event types:

- `vehicle_position_updated`
- `temperature_reading`
- `vehicle_idle_started`
- `vehicle_idle_ended`

Partition key:

```text
vehicleId
```

Sample payload:

```json
{
  "vehicleId": "TRK-4821",
  "shipmentId": "SHP-27DF2C15",
  "driverId": "DRV-18472",
  "eventType": "vehicle_position_updated",
  "lat": 43.8123,
  "lng": -113.9282,
  "speedKmph": 72.4,
  "temperatureCelsius": 5.9,
  "idle": false,
  "atWarehouse": false,
  "nearestWarehouseId": null,
  "timestamp": 1778146136378
}
```

Consumer recommendations:

- Update current vehicle location in `vehicles`.
- Feed live shipment map/dashboard data.
- Track idle duration outside warehouses.
- Use `temperatureCelsius` for cold-chain monitoring.
- Use `atWarehouse` and `nearestWarehouseId` for idle/stop rules.

### `warehouse-events`

Purpose: warehouse scanning, bay assignment, loading/unloading, queue depth, and load activity.

Event types:

- `barcode_scanned`
- `bay_assigned`
- `loading_completed`
- `unloading_completed`
- `queue_depth_changed`

Partition key:

```text
warehouseId
```

Sample payload:

```json
{
  "warehouseId": "WH-SEA",
  "shipmentId": "SHP-27DF2C15",
  "vehicleId": "TRK-4821",
  "eventType": "barcode_scanned",
  "bayId": "BAY-14",
  "queueDepth": 3,
  "currentLoad": 41,
  "capacity": 120,
  "timestamp": 1778146106378
}
```

Consumer recommendations:

- Update warehouse `current_load` and queue depth.
- Persist barcode and bay events if needed by warehouse operations.
- Use queue depth for congestion alerting.
- Feed warehouse load heatmap and throughput dashboards.

### `alerts`

Purpose: simulated alert-shaped events for early integration before the real Alert Engine is completed.

Supported alert types:

- `COLD_CHAIN_VIOLATION`
- `ROUTE_DEVIATION`
- `REPEATED_ROUTE_DEVIATION`
- `UNAUTHORIZED_STOP`
- `WAREHOUSE_CONGESTION`

Partition key:

```text
alertType
```

Sample payload:

```json
{
  "alertId": "ALT-A1B2C3D4",
  "alertType": "COLD_CHAIN_VIOLATION",
  "severity": "CRITICAL",
  "shipmentId": "SHP-27DF2C15",
  "vehicleId": "TRK-4821",
  "warehouseId": null,
  "message": "Cold-chain reading outside configured temperature band",
  "createdAt": 1778146136378
}
```

Consumer recommendations:

- Use this stream to build dashboard alert panels before the real Alert Engine is finished.
- Persist to the `alerts` table only if the team wants simulator-generated alerts in test data.
- In production architecture, the real Alert Engine should become the owner of alert creation.

Disable simulated alerts:

```yaml
simulator:
  logistics:
    alert-simulation-enabled: false
```

### `analytics-metrics`

Purpose: synthetic metrics for early dashboard and ETL integration.

Current metric type:

```text
warehouse_load
```

Partition key:

```text
metricType
```

Sample payload:

```json
{
  "metricType": "warehouse_load",
  "entityId": "WH-SEA",
  "value": 41.0,
  "unit": "shipments",
  "tags": {
    "warehouse_id": "WH-SEA"
  },
  "timestamp": 1778146136378
}
```

Consumer recommendations:

- Load to ClickHouse or Elasticsearch for dashboard testing.
- Use for warehouse load charts until the real ETL pipeline produces aggregated metrics.

Disable analytics simulation:

```yaml
simulator:
  logistics:
    analytics-simulation-enabled: false
```

## DTO Field Reference

Consumers may create equivalent DTOs in their own package. Field names must match the JSON names below.

### ShipmentEvent

| Field | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `shipmentId` | string | No | Kafka key for `shipment-events` |
| `eventType` | string | No | Lifecycle or anomaly type |
| `vehicleId` | string | No | Assigned vehicle |
| `driverId` | string | No | Assigned driver |
| `originWarehouseId` | string | No | Origin warehouse |
| `destinationWarehouseId` | string | No | Destination warehouse |
| `currentLat` | number | No | Current latitude |
| `currentLng` | number | No | Current longitude |
| `plannedEta` | number | No | Epoch milliseconds |
| `temperatureCelsius` | number | No | Cold-chain reading |
| `delayRiskScore` | number | No | 0.0 to 1.0 |
| `anomalyReason` | string | Yes | Present for anomaly events |
| `timestamp` | number | No | Epoch milliseconds |

### VehicleTelemetryEvent

| Field | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `vehicleId` | string | No | Kafka key for `vehicle-telemetry` |
| `shipmentId` | string | No | Active shipment |
| `driverId` | string | No | Active driver |
| `eventType` | string | No | Telemetry event type |
| `lat` | number | No | Current latitude |
| `lng` | number | No | Current longitude |
| `speedKmph` | number | No | Speed in km/h |
| `temperatureCelsius` | number | No | Cold-chain reading |
| `idle` | boolean | No | Whether vehicle is idle |
| `atWarehouse` | boolean | No | Whether stop is at warehouse |
| `nearestWarehouseId` | string | Yes | Present when near warehouse |
| `timestamp` | number | No | Epoch milliseconds |

### WarehouseEvent

| Field | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `warehouseId` | string | No | Kafka key for `warehouse-events` |
| `shipmentId` | string | Yes | Null for warehouse-only queue events |
| `vehicleId` | string | Yes | Null for warehouse-only queue events |
| `eventType` | string | No | Warehouse event type |
| `bayId` | string | Yes | Randomly assigned for some events |
| `queueDepth` | number | No | Current queue depth |
| `currentLoad` | number | No | Current simulated load |
| `capacity` | number | No | Warehouse capacity |
| `timestamp` | number | No | Epoch milliseconds |

### AlertSimulationEvent

| Field | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `alertId` | string | No | Simulated alert ID |
| `alertType` | string | No | Kafka key for `alerts` |
| `severity` | string | No | `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |
| `shipmentId` | string | Yes | Present for shipment/vehicle alerts |
| `vehicleId` | string | Yes | Present for vehicle alerts |
| `warehouseId` | string | Yes | Present for warehouse alerts |
| `message` | string | No | Human-readable alert text |
| `createdAt` | number | No | Epoch milliseconds |

### AnalyticsMetricEvent

| Field | Type | Nullable | Notes |
| --- | --- | --- | --- |
| `metricType` | string | No | Kafka key for `analytics-metrics` |
| `entityId` | string | No | Warehouse, vehicle, route, or future entity ID |
| `value` | number | No | Metric value |
| `unit` | string | No | Metric unit |
| `tags` | object | No | Additional dimensions |
| `timestamp` | number | No | Epoch milliseconds |

## Downstream Team Responsibilities

| Team/Service | Topics To Consume | Expected Implementation |
| --- | --- | --- |
| Shipment backend | `shipment-events` | Persist events, maintain shipment status, expose shipment APIs |
| Logistics backend / Parcel service | Produce to `simulator-commands`; consume `shipment-events` | Inject user-created parcels into simulator and update parcel status from lifecycle events |
| Vehicle backend | `vehicle-telemetry` | Maintain vehicle location/status, feed live map APIs |
| Warehouse backend | `warehouse-events` | Maintain warehouse load, queue, scan, and bay state |
| Alert Engine | `shipment-events`, `vehicle-telemetry`, `warehouse-events` | Evaluate PRD alert rules and produce real alerts |
| Fraud Detection | `shipment-events`, `vehicle-telemetry` | Detect repeated route deviation, unauthorized stops, suspicious timelines |
| ETL/Data Pipeline | All event topics | Transform and load PostgreSQL/ClickHouse/Elasticsearch tables |
| Dashboard backend | All relevant topics or DB projections | Serve `/analytics/*` endpoints |
| QA | All topics | Validate integration, event ordering, replay, and load scenarios |

## Example Spring Boot Consumer

Maven dependency:

```xml
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>
```

Consumer configuration:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: smartlogix-backend
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
```

Example listener:

```java
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class ShipmentEventConsumer {

    @KafkaListener(topics = "shipment-events", groupId = "smartlogix-backend")
    public void consumeShipmentEvent(ShipmentEvent event) {
        // Persist to shipment_events
        // Update current shipment status
        // Evaluate alert/fraud rules or forward to processing layer
        // Update dashboard read model
    }
}
```

For production-style consumers, downstream teams should add error handlers, dead-letter topics, idempotent database writes, validation, and monitoring for consumer lag.

## Running The Simulator

### Option 1: Local Kafka

Requirements:

- JDK 17
- Maven
- Kafka running on `localhost:9092`

Commands:

```bash
cd enterprise-simulators/smartlogix-simulator
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

Expected startup log:

```text
SmartLogix simulator ready
Kafka event contract topics: shipmentEvents=shipment-events ...
Started LogisticsSimulatorApplication
```

### Option 2: Docker Compose

Build the jar:

```bash
cd enterprise-simulators/smartlogix-simulator
mvn clean package
```

Start Kafka and simulator:

```bash
docker compose up --build
```

Kafka is exposed to host-machine backend projects at:

```text
localhost:9092
```

The simulator container connects to Kafka internally at:

```text
kafka:29092
```

### Option 3: Load-Test Profile

Use this only when downstream consumers are ready for higher event volume.

```bash
cd enterprise-simulators/smartlogix-simulator
mvn spring-boot:run -Dspring-boot.run.profiles=load-test
```

Load-test mode increases shipment creation probability, lowers tick interval, shortens lifecycle delays, and raises the active shipment cap.

## Runtime Configuration

Main configuration is in:

```text
smartlogix-simulator/src/main/resources/application.yml
```

Key settings:

```yaml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: logistics-simulator-group
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
      properties:
        spring.json.trusted.packages: "*"
        spring.json.value.default.type: com.smartlogix.simulator.ShipmentSimulationCommand

app:
  simulator:
    topics:
      shipment-events: shipment-events
      vehicle-telemetry: vehicle-telemetry
      warehouse-events: warehouse-events
      alerts: alerts
      analytics-metrics: analytics-metrics
      simulator-commands: simulator-commands

simulator:
  logistics:
    tick-rate-ms: 5000
    new-shipment-probability: 0.20
    max-active-shipments: 250
    alert-simulation-enabled: true
    analytics-simulation-enabled: true
    anomaly-rates:
      temperature: 0.05
      route-deviation: 0.02
      unauthorized-stop: 0.01
    delays-ms:
      loading: 15000
      driving: 30000
      unloading: 20000
    cold-chain:
      min-temperature-celsius: 2.0
      max-temperature-celsius: 8.0
    warehouse:
      congestion-queue-threshold: 25
      barcode-scan-probability: 0.30
      bay-assignment-probability: 0.65
```

Configuration is validated at startup. Invalid values, such as anomaly rates above `1.0` or an invalid cold-chain temperature range, will fail fast.

## Health And Observability

The simulator exposes Spring Boot Actuator endpoints.

Health:

```text
GET http://localhost:8080/actuator/health
```

Metrics:

```text
GET http://localhost:8080/actuator/metrics
GET http://localhost:8080/actuator/prometheus
```

Important custom metrics:

```text
smartlogix.simulator.active_shipments
smartlogix.simulator.events.sent
smartlogix.simulator.events.failed
```

These metrics help teams verify:

- Simulator is running.
- Active shipment count is within expected range.
- Events are successfully sent to Kafka.
- Kafka send failures are visible.
- Prometheus/Grafana integration can scrape simulator metrics.

## Verification Commands

List Kafka topics:

```bash
kafka-topics --bootstrap-server localhost:9092 --list
```

Expected topics:

```text
shipment-events
vehicle-telemetry
warehouse-events
alerts
analytics-metrics
simulator-commands
```

Send a dynamic shipment command:

```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic simulator-commands --property parse.key=true --property key.separator=:
```

Then enter one line:

```text
PARCEL-100045:{"shipmentId":"PARCEL-100045","originWarehouseId":"WH-SEA","destinationWarehouseId":"WH-LAX"}
```

Consume shipment events:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic shipment-events --from-beginning
```

Consume vehicle telemetry:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic vehicle-telemetry --from-beginning
```

Consume warehouse events:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic warehouse-events --from-beginning
```

Check simulator health:

```bash
curl http://localhost:8080/actuator/health
```

## Integration Checklist

Before downstream teams start development:

- Kafka is running and reachable.
- Simulator starts successfully.
- The five PRD event topics and `simulator-commands` exist.
- Console consumer can read from `shipment-events`.
- Logistics backend can publish a valid command to `simulator-commands`.
- Backend project uses the same Kafka bootstrap server.
- Backend consumer group has a stable `group-id`.
- DTO field names match the simulator JSON contract.
- Command payload uses valid simulator warehouse IDs.
- Consumers treat event timestamps as epoch milliseconds.
- Consumers use idempotent writes where possible.
- Alert/fraud teams understand simulated alerts are optional test data, not the final production alert engine.

## Ownership Boundary

Simulator team owns:

- Kafka event generation.
- Kafka listener for `simulator-commands`.
- Topic creation for the five PRD event topics and the simulator command topic.
- Simulator configuration and runtime profiles.
- JSON event contract documentation.
- Health and metrics for the simulator service.
- Support for local, Docker, and load-test execution.

Downstream teams own:

- Publishing valid dynamic shipment commands after user-created parcels are persisted.
- Kafka consumers.
- Kafka Streams topologies.
- PostgreSQL writes.
- ClickHouse/Elasticsearch loaders.
- Redis cache.
- Alert Engine rule evaluation.
- Fraud Detection rule evaluation.
- REST analytics APIs.
- JWT/RBAC enforcement.
- React dashboard and Grafana integration.
- Notification delivery channels.

## PRD Caveats

- Java 17 is the PRD target. The simulator Maven build targets Java 17.
- This simulator generates realistic synthetic data, not real IoT/driver/warehouse integrations.
- Dynamic command processing is intended for integration testing user-driven flows. It is not a production carrier orchestration API.
- Default profile is demo/integration oriented, not a 50K events/min benchmark profile.
- Load testing requires environment tuning, downstream consumer readiness, and possibly multiple simulator instances.
- Simulated alert events are for early integration. Final alert ownership belongs to the Alert Engine.

## Recommended Handoff Statement

Use this summary when handing the simulator to another team:

> The SmartLogix simulator is a PRD-aligned Kafka simulator for logistics integration testing. It creates the required Kafka topics, publishes shipment lifecycle, vehicle telemetry, warehouse activity, simulated alert, and analytics metric events, and consumes `simulator-commands` so the logistics backend can inject user-created parcels by `shipmentId`. Downstream backend, data, alerting, fraud, and dashboard services should consume these Kafka topics to implement persistence, ETL, rule processing, APIs, and UI features. The simulator includes local, Docker, and load-test profiles plus health and Prometheus metrics for integration visibility.
