package com.jayant;

import com.jayant.testparams.TestParams;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
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
    Set<Method> methodsWhereArgsAreNotProvided = new HashSet<>();
    Set<Map.Entry<String, Integer>> argumentsAdded = new HashSet<>();
    try {
      Method[] methods = clazz.getMethods();
      for (Method method : methods) {
        String methodName = method.getName();
        int parameterCount = method.getParameterCount();
        Map.Entry<String, Integer> methodWithArgs = Map.entry(methodName, parameterCount);
        if (argumentsAdded.contains(methodWithArgs)) {
          continue;
        }
        if (acceptedKnownDiffs.contains(methodWithArgs)) {
          continue;
        }
        Set<Object[]> paramSet = functionToArgsMap.getOrDefault(methodWithArgs, NO_PARAMS);
        for (Object[] params : paramSet) {
          if (parameterCount != params.length) {
            // This will ensure that we do not skip any methods in the class
            methodsWhereArgsAreNotProvided.add(method);
          }
          Arguments arguments = Arguments.of(methodName, params);
          argumentsStream.add(arguments);
        }
        argumentsAdded.add(methodWithArgs);
      }
    } catch (SecurityException e) {
      e.printStackTrace();
    }

    if (!methodsWhereArgsAreNotProvided.isEmpty()) {
      throw new RuntimeException(
          "Method parameters were not provided for:\n"
              + methodsWhereArgsAreNotProvided.stream()
                  .map(Method::toString)
                  .collect(Collectors.joining("\n")));
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
          paramTypes[i] = args[i] == null ? String.class : args[i].getClass();
        }

        method = object.getClass().getMethod(methodName, paramTypes);
      }

      // Invoke the method dynamically with the arguments
      try {
        result = method.invoke(object, args);
      } catch (InvocationTargetException e) {
        result = e.getCause();
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
