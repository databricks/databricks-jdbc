package com.databricks.jdbc.telemetry.latency;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportLatencyLog;

import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.model.telemetry.SqlExecutionEvent;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class DatabricksMetricsTimedProcessor {

  @SuppressWarnings("unchecked")
  public static <T> T createProxy(T obj) {
    Class<?> clazz = obj.getClass();
    Class<?>[] interfaces = clazz.getInterfaces();

    if (interfaces.length == 0) {
      throw new IllegalArgumentException("The target class does not implement any interfaces.");
    }

    return (T)
        Proxy.newProxyInstance(
            clazz.getClassLoader(), interfaces, new TimedInvocationHandler<>(obj));
  }

  private static class TimedInvocationHandler<T> implements InvocationHandler {
    private final T target;

    public TimedInvocationHandler(T target) {
      this.target = target;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      // Check if the method is annotated with @DatabricksMetricsTimed
      if (method.isAnnotationPresent(DatabricksMetricsTimed.class)) {
        long startTime = System.currentTimeMillis();
        try {
          // Invoke the actual method
          Object result = method.invoke(target, args);
          // Calculate and print execution time
          long executionTime = System.currentTimeMillis() - startTime;
          System.out.println(
              "HIIII Method " + method.getName() + " executed in " + executionTime + " ms");
          SqlExecutionEvent executionEvent =
              new SqlExecutionEvent()
                  .setDriverStatementType(DatabricksThreadContextHolder.getStatementType())
                  .setChunkId(DatabricksThreadContextHolder.getChunkId());
          exportLatencyLog(
              DatabricksThreadContextHolder.getConnectionContext(),
              executionTime,
              executionEvent,
              DatabricksThreadContextHolder.getStatementId());
          return result;
        } catch (Throwable throwable) {
          // Handle exceptions from the target method
          throw throwable.getCause() != null ? throwable.getCause() : throwable;
        }
      }

      // Default behavior for methods without @DatabricksMetricsTimed
      return method.invoke(target, args);
    }
  }
}
