package com.databricks.jdbc.annotation;

import com.databricks.jdbc.commons.MetricsList;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface DatabricksMetricsTimedMethod {
  String methodName();

  MetricsList metricName() default MetricsList.DEFAULT;
}
