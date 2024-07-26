package com.databricks.jdbc.telemetry.annotation;

import com.databricks.jdbc.core.DatabricksSession;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class DatabricksMetricsTimedProcessor {

  // This method is used to create a proxy object for the given object.
  // The proxy object will intercept the method calls and record the time taken to execute the
  // method.
  @SuppressWarnings("unchecked")
  public static <T> T createProxy(T obj) {
    Class<?> clazz = obj.getClass();
    Class<?>[] interfaces = clazz.getInterfaces();

    for (Class<?> iface : interfaces) {
      DatabricksMetricsTimedClass databricksMetricsTimedClass =
          iface.getAnnotation(DatabricksMetricsTimedClass.class);
      if (databricksMetricsTimedClass != null) {
        return (T)
            Proxy.newProxyInstance(
                clazz.getClassLoader(), clazz.getInterfaces(), new TimedInvocationHandler<>(obj));
      }
    }

    throw new IllegalArgumentException(
        "Class " + clazz.getName() + " does not implement an interface annotated with @TimedClass");
  }

  private static class TimedInvocationHandler<T> implements InvocationHandler {
    private final T target;

    public TimedInvocationHandler(T target) {
      this.target = target;
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

            // Get the connection context
            IDatabricksConnectionContext connectionContext = null;

            boolean isMetricMetadata = metricName.startsWith("LIST");

            // Get the connection context based on the metric type
            if (isMetricMetadata && args != null && args[0].getClass() == DatabricksSession.class) {
              connectionContext = ((IDatabricksSession) args[0]).getConnectionContext();
            } else {
              connectionContext =
                  (IDatabricksConnectionContext)
                      target.getClass().getMethod("getConnectionContext").invoke(target);
            }

            // Record the metric
            if (connectionContext != null) {
              connectionContext.getMetricsExporter().record(metricName, endTime - startTime);
            }
            return result;
          }
        }
      }
      return method.invoke(target, args);
    }
  }
}
