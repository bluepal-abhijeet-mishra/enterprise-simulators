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

    @Value("${app.simulator.topic:shipment-events}")
    private String topicName;

    @Bean
    public NewTopic shipmentEventsTopic() {
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
