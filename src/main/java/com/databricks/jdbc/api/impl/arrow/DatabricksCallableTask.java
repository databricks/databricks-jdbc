package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.telemetry.latency.DatabricksMetricsTimed;
import java.util.concurrent.Callable;

public interface DatabricksCallableTask extends Callable<Void> {
  @Override
  @DatabricksMetricsTimed
  Void call() throws Exception;
}
