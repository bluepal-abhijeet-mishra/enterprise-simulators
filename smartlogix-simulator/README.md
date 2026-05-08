# SmartLogix Simulator

Kafka event generator for the SmartLogix real-time logistics platform.

This service is intentionally simulator-only. It does not consume Kafka events, write to PostgreSQL, expose dashboard APIs, or implement alert/fraud engines. Its responsibility is to produce realistic logistics data so backend, data, dashboard, alerting, and QA teams can integrate against stable streams.

## Production-Style Capabilities

- PRD-aligned Kafka topic creation and JSON event production
- Validated simulator configuration using typed Spring Boot properties
- Runtime profiles for `local`, `docker`, and `load-test`
- Actuator health and metrics endpoints
- Prometheus-compatible metric export
- Per-topic Kafka send success/failure counters
- Active shipment gauge
- Configurable max active shipments to prevent runaway load
- Graceful shutdown with Kafka producer flush
- Docker Compose setup with Kafka and simulator services
- Unit tests for simulator configuration validation

## Generated Kafka Topics

| Topic | Partitions | Retention | Key |
| --- | ---: | --- | --- |
| `shipment-events` | 12 | 7 days | `shipmentId` |
| `vehicle-telemetry` | 12 | 3 days | `vehicleId` |
| `warehouse-events` | 6 | 7 days | `warehouseId` |
| `alerts` | 6 | 30 days | `alertType` |
| `analytics-metrics` | 4 | 90 days | `metricType` |

## Event Families

### Shipment events

Lifecycle and anomaly events for shipment processing:

- `shipment_created`
- `vehicle_departed`
- `warehouse_arrived`
- `delivery_completed`
- `temperature_change`
- `route_deviation`
- `unauthorized_stop`

### Vehicle telemetry

High-frequency vehicle updates for live map, route, idle, and cold-chain scenarios:

- `vehicle_position_updated`
- `temperature_reading`
- `vehicle_idle_started`
- `vehicle_idle_ended`

### Warehouse events

Warehouse operations and congestion signals:

- `barcode_scanned`
- `bay_assigned`
- `loading_completed`
- `unloading_completed`
- `queue_depth_changed`

### Optional simulated alerts

The simulator can publish alert-shaped events before the real alert engine is ready:

- `COLD_CHAIN_VIOLATION`
- `ROUTE_DEVIATION`
- `REPEATED_ROUTE_DEVIATION`
- `UNAUTHORIZED_STOP`
- `WAREHOUSE_CONGESTION`

## Configuration

Main settings are in `src/main/resources/application.yml`.

```yaml
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
```

Use lower tick rates and higher probabilities for demos or load-style testing. Keep anomaly rates low when downstream teams need mostly normal operational data.

## Runtime Profiles

| Profile | Purpose |
| --- | --- |
| `local` | Local development against Kafka on `localhost:9092` |
| `docker` | Docker Compose mode, simulator connects to Kafka at `kafka:29092` |
| `load-test` | Higher event volume for downstream consumer/load testing |

## Running Locally

Requirements:

- JDK 17
- Maven
- Kafka running on `localhost:9092`

Start the simulator:

```bash
mvn spring-boot:run
```

The app creates the required Kafka topics on startup through Spring Kafka admin configuration.

Run with an explicit local profile:

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

## Running With Docker Compose

Build the jar first:

```bash
mvn clean package
```

Start Kafka and the simulator:

```bash
docker compose up --build
```

Kafka is exposed to local backend projects at:

```text
localhost:9092
```

The simulator uses the internal Docker listener:

```text
kafka:29092
```

## Health And Metrics

When the simulator is running, use:

```text
GET http://localhost:8080/actuator/health
GET http://localhost:8080/actuator/metrics
GET http://localhost:8080/actuator/prometheus
```

Important custom metrics:

```text
smartlogix.simulator.active_shipments
smartlogix.simulator.events.sent
smartlogix.simulator.events.failed
```

These make the simulator observable during demos, integration testing, and load-test runs.

## Load-Test Mode

Start the simulator with higher event volume:

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=load-test
```

The load-test profile increases shipment creation rate and shortens shipment lifecycle delays. Downstream services should use this mode only when they are ready to absorb higher event volume.

## Tests

Run:

```bash
mvn test
```
