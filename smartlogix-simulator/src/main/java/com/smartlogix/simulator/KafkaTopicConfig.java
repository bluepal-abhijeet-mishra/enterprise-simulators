package com.smartlogix.simulator;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Configuration class to ensure the required Kafka topic is created.
 */
@Configuration
public class KafkaTopicConfig {

    private final SimulatorTopicsProperties topics;

    public KafkaTopicConfig(SimulatorTopicsProperties topics) {
        this.topics = topics;
    }

    @Bean
    public NewTopic shipmentEventsTopic() {
        return TopicBuilder.name(topics.getShipmentEvents())
                .partitions(12)
                .replicas(1)
                .config("retention.ms", String.valueOf(days(7)))
                .build();
    }

    @Bean
    public NewTopic vehicleTelemetryTopic() {
        return TopicBuilder.name(topics.getVehicleTelemetry())
                .partitions(12)
                .replicas(1)
                .config("retention.ms", String.valueOf(days(3)))
                .build();
    }

    @Bean
    public NewTopic warehouseEventsTopic() {
        return TopicBuilder.name(topics.getWarehouseEvents())
                .partitions(6)
                .replicas(1)
                .config("retention.ms", String.valueOf(days(7)))
                .build();
    }

    @Bean
    public NewTopic alertsTopic() {
        return TopicBuilder.name(topics.getAlerts())
                .partitions(6)
                .replicas(1)
                .config("retention.ms", String.valueOf(days(30)))
                .build();
    }

    @Bean
    public NewTopic analyticsMetricsTopic() {
        return TopicBuilder.name(topics.getAnalyticsMetrics())
                .partitions(4)
                .replicas(1)
                .config("retention.ms", String.valueOf(days(90)))
                .build();
    }

    private long days(int value) {
        return value * 24L * 60L * 60L * 1000L;
    }
}
