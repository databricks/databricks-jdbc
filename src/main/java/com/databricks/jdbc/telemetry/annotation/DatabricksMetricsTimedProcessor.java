package com.databricks.jdbc.telemetry.annotation;

import com.databricks.jdbc.telemetry.DatabricksMetrics;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class DatabricksMetricsTimedProcessor {

  // This method is used to create a proxy object for the given object.
  // The proxy object will intercept the method calls and record the time taken to execute the
  // method.
  @SuppressWarnings("unchecked")
  public static <T> T createProxy(T obj, DatabricksMetrics metricsExporter) {
    Class<?> clazz = obj.getClass();
    Class<?>[] interfaces = clazz.getInterfaces();

    for (Class<?> iface : interfaces) {
      DatabricksMetricsTimedClass databricksMetricsTimedClass =
          iface.getAnnotation(DatabricksMetricsTimedClass.class);
      if (databricksMetricsTimedClass != null) {
        return (T)
            Proxy.newProxyInstance(
                clazz.getClassLoader(),
                clazz.getInterfaces(),
                new TimedInvocationHandler<>(obj, metricsExporter));
      }
    }

    throw new IllegalArgumentException(
        "Class " + clazz.getName() + " does not implement an interface annotated with @TimedClass");
  }

  private static class TimedInvocationHandler<T> implements InvocationHandler {
    private final T target;
    private final DatabricksMetrics metricsExporter;

    public TimedInvocationHandler(T target, DatabricksMetrics metricsExporter) {
      this.target = target;
      this.metricsExporter = metricsExporter;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Class<?> declaringClass = method.getDeclaringClass();
      DatabricksMetricsTimedClass databricksMetricsTimedClass =
          declaringClass.getAnnotation(DatabricksMetricsTimedClass.class);
      if (databricksMetricsTimedClass != null) {
        for (DatabricksMetricsTimedMethod databricksMetricsTimedMethod :
            databricksMetricsTimedClass.methods()) {
          if (method.getName().equals(databricksMetricsTimedMethod.methodName())) {
            String metricName = databricksMetricsTimedMethod.metricName().name();

            // Record execution time
            long startTime = System.currentTimeMillis();
            Object result = method.invoke(target, args);
            long endTime = System.currentTimeMillis();

            // Record the metric
            metricsExporter.record(metricName, endTime - startTime);

            return result;
          }
        }
      }
      return method.invoke(target, args);
    }
  }
}
