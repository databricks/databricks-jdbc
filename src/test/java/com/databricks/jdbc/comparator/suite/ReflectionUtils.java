package com.databricks.jdbc.comparator.suite;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;

/**
 * Utility for dynamically invoking methods by name and arguments. Used by {@link
 * DatabaseMetaDataProvider} to call arbitrary DatabaseMetaData methods without hardcoding each one.
 */
public class ReflectionUtils {

  /**
   * Invokes the named method on the target object with the given arguments.
   *
   * @return the method's return value, or the caught exception (as an object) if the method threw
   */
  public static Object executeMethod(Object target, String methodName, Object[] args)
      throws SQLException {
    try {
      Method method = findMethod(target.getClass(), methodName, args);
      if (method == null) {
        throw new NoSuchMethodException(
            "No matching method: " + methodName + " with " + args.length + " args");
      }
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException e) {
        // Method threw — return the cause as the "result" for comparison
        return e.getCause();
      }
    } catch (Exception e) {
      throw new SQLException("Error executing method: " + methodName, e);
    }
  }

  private static Method findMethod(Class<?> clazz, String methodName, Object[] args) {
    for (Method m : clazz.getMethods()) {
      if (!m.getName().equals(methodName)) continue;
      Class<?>[] paramTypes = m.getParameterTypes();
      if (paramTypes.length != args.length) continue;

      boolean match = true;
      for (int i = 0; i < paramTypes.length; i++) {
        if (args[i] == null) continue; // null matches any reference type
        if (paramTypes[i].isPrimitive()) {
          if (!isPrimitiveCompatible(paramTypes[i], args[i].getClass())) {
            match = false;
            break;
          }
        } else if (!paramTypes[i].isAssignableFrom(args[i].getClass())) {
          match = false;
          break;
        }
      }
      if (match) return m;
    }
    return null;
  }

  private static boolean isPrimitiveCompatible(Class<?> primitiveType, Class<?> wrapperType) {
    if (primitiveType == int.class) return wrapperType == Integer.class;
    if (primitiveType == boolean.class) return wrapperType == Boolean.class;
    if (primitiveType == long.class) return wrapperType == Long.class;
    if (primitiveType == short.class) return wrapperType == Short.class;
    if (primitiveType == byte.class) return wrapperType == Byte.class;
    if (primitiveType == char.class) return wrapperType == Character.class;
    if (primitiveType == float.class) return wrapperType == Float.class;
    if (primitiveType == double.class) return wrapperType == Double.class;
    return false;
  }
}
