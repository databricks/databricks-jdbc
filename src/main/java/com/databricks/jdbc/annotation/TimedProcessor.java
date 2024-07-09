package com.databricks.jdbc.annotation;


import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.*;

public class TimedProcessor{

  @SuppressWarnings("unchecked")
  public static <T> T createProxy(T obj) {
    Class<?> clazz = obj.getClass();
    TimedClass timedClass = clazz.getAnnotation(TimedClass.class);

    if (timedClass == null) {
      throw new IllegalArgumentException(
          "Class " + clazz.getName() + " is not annotated with @TimedClass");
    }

    Map<String, TimedMethod> methodsToTime = new HashMap<>();
    for (TimedMethod timedMethod : timedClass.methods()) {
      methodsToTime.put(timedMethod.name(), timedMethod);
    }

    return (T)
        Proxy.newProxyInstance(
            clazz.getClassLoader(),
            clazz.getInterfaces(),
            new TimedInvocationHandler<>(obj, methodsToTime));
  }

  private static class TimedInvocationHandler<T> implements InvocationHandler {
    private final T target;
    private final Map<String, TimedMethod> methodsToTime;

    public TimedInvocationHandler(T target, Map<String, TimedMethod> methodsToTime) {
      this.target = target;
      this.methodsToTime = methodsToTime;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (methodsToTime.containsKey(method.getName())) {
        TimedMethod timedMethod = methodsToTime.get(method.getName());
        String parameterValue = timedMethod.parameters();
        long startTime = System.currentTimeMillis();
        Object result = method.invoke(target, args);
        long endTime = System.currentTimeMillis();
        System.out.println("Inside invoke method of TimedProcessor");
        System.out.println(
            "Execution time of " + method.getName() + ": " + (endTime - startTime) + " ms");

        Method getConnectionContext = target.getClass().getMethod("getConnectionContext");
        IDatabricksConnectionContext connectionContext =
            (IDatabricksConnectionContext) getConnectionContext.invoke(target);
        connectionContext.getMetricsExporter().record(parameterValue, endTime - startTime);
        System.out.println("Hello bhuvan: ");
        //connectionContext.getMetricsExporter().record(parameterValue, endTime - startTime);
        return result;
      } else {
        return method.invoke(target, args);
      }
    }
  }
}
