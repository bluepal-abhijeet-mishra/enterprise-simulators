package com.smartlogix.simulator;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Synthetic analytics metric event for early dashboard and ETL integration.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnalyticsMetricEvent {

    private String metricType;
    private String entityId;
    private double value;
    private String unit;
    private Map<String, String> tags;
    private long timestamp;
}
