package com.smartlogix.simulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({SimulatorProperties.class, SimulatorTopicsProperties.class})
public class LogisticsSimulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(LogisticsSimulatorApplication.class, args);
    }
}
