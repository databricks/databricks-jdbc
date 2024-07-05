package com.databricks.jdbc.annotation;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class TimingUtility {

  @SuppressWarnings("unchecked")
  public static <T> T createTimedInstance(T target, Class<T> interfaceType) {
    return (T) Proxy.newProxyInstance(
            interfaceType.getClassLoader(),
            new Class<?>[]{interfaceType},
            new TimingInvocationHandler(target)
    );
  }

  private static class TimingInvocationHandler implements InvocationHandler {
    private final Object target;

    public TimingInvocationHandler(Object target) {
      this.target = target;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Method targetMethod = target.getClass().getMethod(method.getName(), method.getParameterTypes());
      if (targetMethod.isAnnotationPresent(Timed.class)) {
        long startTime = System.nanoTime();
        Object result = method.invoke(target, args);
        long endTime = System.nanoTime();
        System.out.println("Execution time of " + method.getName() + ": " + (endTime - startTime) + " nanoseconds");
        return result;
      }
      return method.invoke(target, args);
    }
  }
}


