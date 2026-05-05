package com.smartlogix.simulator;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Configuration class to ensure the required Kafka topic is created.
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${app.simulator.topics.shipment-events:shipment-events}")
    private String shipmentEventsTopic;

    @Value("${app.simulator.topics.vehicle-telemetry:vehicle-telemetry}")
    private String vehicleTelemetryTopic;

    @Value("${app.simulator.topics.warehouse-events:warehouse-events}")
    private String warehouseEventsTopic;

    @Value("${app.simulator.topics.alerts:alerts}")
    private String alertsTopic;

    @Value("${app.simulator.topics.analytics-metrics:analytics-metrics}")
    private String analyticsMetricsTopic;

    @Bean
    public NewTopic shipmentEventsTopic() {
        return TopicBuilder.name(shipmentEventsTopic)
                .partitions(12)
                .replicas(1)
                .config("retention.ms", String.valueOf(days(7)))
                .build();
    }

    @Bean
    public NewTopic vehicleTelemetryTopic() {
        return TopicBuilder.name(vehicleTelemetryTopic)
                .partitions(12)
                .replicas(1)
                .config("retention.ms", String.valueOf(days(3)))
                .build();
    }

    @Bean
    public NewTopic warehouseEventsTopic() {
        return TopicBuilder.name(warehouseEventsTopic)
                .partitions(6)
                .replicas(1)
                .config("retention.ms", String.valueOf(days(7)))
                .build();
    }

    @Bean
    public NewTopic alertsTopic() {
        return TopicBuilder.name(alertsTopic)
                .partitions(6)
                .replicas(1)
                .config("retention.ms", String.valueOf(days(30)))
                .build();
    }

    @Bean
    public NewTopic analyticsMetricsTopic() {
        return TopicBuilder.name(analyticsMetricsTopic)
                .partitions(4)
                .replicas(1)
                .config("retention.ms", String.valueOf(days(90)))
                .build();
    }

    private long days(int value) {
        return value * 24L * 60L * 60L * 1000L;
    }
}
