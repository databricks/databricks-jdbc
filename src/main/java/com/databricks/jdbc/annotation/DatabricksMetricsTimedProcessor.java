package com.databricks.jdbc.annotation;

import com.databricks.jdbc.core.DatabricksSession;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

public class DatabricksMetricsTimedProcessor {

  // This method is used to create a proxy object for the given object.
  // The proxy object will intercept the method calls and record the time taken to execute the
  // method.
  @SuppressWarnings("unchecked")
  public static <T> T createProxy(T obj) {
    Class<?> clazz = obj.getClass();
    DatabricksMetricsTimedClass databricksMetricsTimedClass =
        clazz.getAnnotation(DatabricksMetricsTimedClass.class);

    if (databricksMetricsTimedClass == null) {
      throw new IllegalArgumentException(
          "Class " + clazz.getName() + " is not annotated with @TimedClass");
    }
    Map<String, DatabricksMetricsTimedMethod> methodsToTime = new HashMap<>();
    for (DatabricksMetricsTimedMethod timedMethod : databricksMetricsTimedClass.methods()) {
      methodsToTime.put(timedMethod.methodName(), timedMethod);
    }
    return (T)
        Proxy.newProxyInstance(
            clazz.getClassLoader(),
            clazz.getInterfaces(),
            new TimedInvocationHandler<>(obj, methodsToTime));
  }

  private static class TimedInvocationHandler<T> implements InvocationHandler {
    private final T target;
    private final Map<String, DatabricksMetricsTimedMethod> methodsToTime;

    public TimedInvocationHandler(
        T target, Map<String, DatabricksMetricsTimedMethod> methodsToTime) {
      this.target = target;
      this.methodsToTime = methodsToTime;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      // Check if the method is present in the hashmap
      if (methodsToTime.containsKey(method.getName())) {
        // Get the annotation and the metric name
        DatabricksMetricsTimedMethod databricksMetricsTimedMethod =
            methodsToTime.get(method.getName());
        String metricName = databricksMetricsTimedMethod.metricName().name();

        // Record execution time
        long startTime = System.currentTimeMillis();
        Object result = method.invoke(target, args);
        long endTime = System.currentTimeMillis();

        // Get the connection context
        IDatabricksConnectionContext connectionContext = null;

        boolean isMetricMetadataSea = metricName.endsWith("METADATA_SEA");
        boolean isMetricThrift = metricName.endsWith("THRIFT");
        boolean isMetricSdk = metricName.endsWith("SDK");

        // Get the connection context based on the metric type
        if (isMetricMetadataSea && args != null && args[0].getClass() == DatabricksSession.class) {
          connectionContext = ((IDatabricksSession) args[0]).getConnectionContext();
        } else if (isMetricThrift || isMetricSdk) {
          connectionContext =
              (IDatabricksConnectionContext)
                  target.getClass().getMethod("getConnectionContext").invoke(target);
        }
        // Record the metric
        assert connectionContext != null;
        connectionContext.getMetricsExporter().record(metricName, endTime - startTime);
        return result;
      } else {
        return method.invoke(target, args);
      }
    }
  }
}
