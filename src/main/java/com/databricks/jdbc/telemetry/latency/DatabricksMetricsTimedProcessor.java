package com.databricks.jdbc.telemetry.latency;

import static com.databricks.jdbc.telemetry.TelemetryHelper.exportLatencyLog;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class DatabricksMetricsTimedProcessor {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksMetricsTimedProcessor.class);

  @SuppressWarnings("unchecked")
  public static <T> T createProxy(T obj) {
    if (obj == null) {
      return null;
    }

    Class<?> clazz = obj.getClass();
    if (clazz == null) {
      LOGGER.trace("Cannot create proxy for null object, skipping latency processing.");
      return obj;
    }

    Class<?>[] interfaces = clazz.getInterfaces();
    if (interfaces == null || interfaces.length == 0) {
      LOGGER.trace(
          "Proxy creation skipped — target class {} does not implement any interfaces, skipping latency processing.",
          obj.getClass().getName());
      return obj;
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
      if (proxy == null || method == null) {
        return method != null ? method.invoke(target, args) : null;
      }

      // Check if the method is annotated with @DatabricksMetricsTimed
      if (method.isAnnotationPresent(DatabricksMetricsTimed.class)) {
        String methodName = method.getName() != null ? method.getName() : "unknown";
        long startTime = System.nanoTime();
        try {
          // Invoke the actual method
          Object result = method.invoke(target, args);
          // Calculate execution time in nanoseconds
          long executionTimeNanos = System.nanoTime() - startTime;
          // Convert to milliseconds for consistency with existing logging
          long executionTimeMillis = executionTimeNanos / 1_000_000;
          // Log method name, arguments and execution time
          String argsStr =
              (args != null)
                  ? String.join(
                      ", ",
                      java.util.Arrays.stream(args)
                          .map(arg -> arg != null ? arg.toString() : "null")
                          .toArray(String[]::new))
                  : "none";
          LOGGER.debug(
              "Method [{}] with args [{}] execution time: {}ms",
              methodName,
              argsStr,
              executionTimeMillis);
          try {
            exportLatencyLog(executionTimeMillis);
          } catch (Exception e) {
            LOGGER.trace(
                "Failed to export latency metrics for method {}: {}", methodName, e.getMessage());
          }
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
