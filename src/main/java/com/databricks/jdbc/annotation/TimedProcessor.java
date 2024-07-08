package com.databricks.jdbc.annotation;

import java.lang.reflect.Method;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class TimedProcessor {

    @SuppressWarnings("unchecked")
    public static <T> T createProxy(T obj) {
        Class<?> clazz = obj.getClass();
        return (T) Proxy.newProxyInstance(
                clazz.getClassLoader(),
                clazz.getInterfaces(),
                new TimedInvocationHandler(obj)
        );
    }

    private static class TimedInvocationHandler implements InvocationHandler {
        private final Object target;

        public TimedInvocationHandler(Object target) {
            this.target = target;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            System.out.println("invoke method: " + method.getName());
            if (method.isAnnotationPresent(Timed.class)) {
                long startTime = System.currentTimeMillis();
                Object result = method.invoke(target, args);
                long endTime = System.currentTimeMillis();
                System.out.println("Execution time of " + method.getName() + ": " + (endTime - startTime) + " ms");
                return result;
            } else {
                System.out.println("Method " + method.getName() + " is not annotated with @Timed");
                return method.invoke(target, args);
            }
        }
    }
}


