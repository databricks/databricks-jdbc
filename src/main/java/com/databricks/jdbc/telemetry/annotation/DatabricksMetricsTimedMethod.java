package com.databricks.jdbc.telemetry.annotation;

import com.databricks.jdbc.commons.CommandLatencyMetrics;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface DatabricksMetricsTimedMethod {
  String methodName();

  CommandLatencyMetrics metricName() default CommandLatencyMetrics.DEFAULT;
}
