package com.jayant;

import com.jayant.testparams.TestParams;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.provider.Arguments;

public class ReflectionUtils {
  private static final Set<Object[]> NO_PARAMS = new HashSet<>();

  static {
    NO_PARAMS.add(new Object[] {});
  }

  public static Stream<Arguments> provideMethodsForClass(Class<?> clazz, TestParams testParams) {
    Set<Map.Entry<String, Integer>> acceptedKnownDiffs = testParams.getAcceptedKnownDiffs();
    Map<Map.Entry<String, Integer>, Set<Object[]>> functionToArgsMap =
        testParams.getFunctionToArgsMap();
    Set<Arguments> argumentsStream = new HashSet<>();
    try {
      Method[] methods = clazz.getMethods();
      for (Method method : methods) {
        String methodName = method.getName();
        int parameterCount = method.getParameterCount();
        Map.Entry<String, Integer> methodWithArgs = Map.entry(methodName, parameterCount);
        if (acceptedKnownDiffs.contains(methodWithArgs)) {
          continue;
        }
        Set<Object[]> paramSet = functionToArgsMap.getOrDefault(methodWithArgs, NO_PARAMS);
        for (Object[] params : paramSet) {
          if (parameterCount != params.length) {
            // This will ensure that we do not skip any methods in the class
            throw new RuntimeException("Please provide parameters for method: " + method);
          }
          Arguments arguments = Arguments.of(methodName, params);
          argumentsStream.add(arguments);
        }
      }
    } catch (SecurityException e) {
      e.printStackTrace();
    }

    return argumentsStream.stream();
  }

  public static Object executeMethod(Object object, String methodName, Object[] args)
      throws SQLException {
    Object result;
    try {
      // Get the method by its name and parameter types
      Method method;
      if (args == null || args.length == 0) {
        method = object.getClass().getMethod(methodName);
      } else {
        // Create an array of parameter types to match the method signature
        Class<?>[] paramTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
          paramTypes[i] = String.class; // Assuming all arguments are Strings
        }

        method = object.getClass().getMethod(methodName, paramTypes);
      }

      // Invoke the method dynamically with the arguments
      try {
        result = method.invoke(object, args);
      } catch (InvocationTargetException e) {
        if (e.getCause() instanceof UnsupportedOperationException) {
          result = "UnsupportedOperationException";
        } else {
          throw e;
        }
      }
    } catch (NoSuchMethodException e) {
      // This is generally thrown due to the difference in JDBC spec versions
      result = "NoSuchMethodException";
    } catch (Exception e) {
      throw new SQLException("Error executing method: " + methodName, e);
    }
    return result;
  }
}
