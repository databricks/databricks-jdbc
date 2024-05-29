package com.databricks.jdbc.telemetry;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import java.io.IOException;
import java.util.HashMap;

public class Metrics {
  private static final HashMap<String, Gauge> gaugeMetrics = new HashMap<>();
  private static final HashMap<String, Counter> counterMetrics = new HashMap<>();

  private Metrics() throws IOException {
    // Private constructor to prevent instantiation
  }

  public static void SetGaugeMetric(String name, double value) {
    if (!gaugeMetrics.containsKey(name)) {
      Gauge gauge = Gauge.builder().name(name).help(name + "_total").register();
      gaugeMetrics.put(name, gauge);
    }
    gaugeMetrics.get(name).set(value);
  }

  public static double getGaugeMetric(String name) {
    return gaugeMetrics.get(name).get();
  }

  public static void IncCounterMetric(String name, double value) {
    if (!counterMetrics.containsKey(name)) {
      Counter counter = Counter.builder().name(name).help(name + "_total").register();
      counterMetrics.put(name, counter);
    }
    counterMetrics.get(name).inc(value);
  }

  public static double getCounterMetric(String name) {
    return counterMetrics.get(name).get();
  }
}
