package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Objects;

/**
 * A utility class that provides access to the memory address of direct ByteBuffers without
 * requiring reflective access to the protected Buffer.address field.
 *
 * <p>This approach uses the sun.misc.Unsafe API to directly access the memory address.
 */
public class UnsafeDirectBufferUtility {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(UnsafeDirectBufferUtility.class);

  private static final Object UNSAFE;
  private static final Method UNSAFE_GET_LONG;
  private static final Method UNSAFE_GET_OBJECT;
  private static final long ADDRESS_OFFSET;
  private static final boolean INITIALIZED;

  static {
    Object unsafe = null;
    Method getInt = null;
    Method getObject = null;
    long addressOffset = 0;
    boolean initialized = false;

    try {
      // Access the sun.misc.Unsafe class
      Class<?> unsafeClass =
          AccessController.doPrivileged(
              (PrivilegedAction<Class<?>>)
                  () -> {
                    try {
                      return Class.forName("sun.misc.Unsafe");
                    } catch (ClassNotFoundException e) {
                      return null;
                    }
                  });

      if (unsafeClass != null) {
        // Get the Unsafe instance
        Field theUnsafeField =
            AccessController.doPrivileged(
                (PrivilegedAction<Field>)
                    () -> {
                      try {
                        Field field = unsafeClass.getDeclaredField("theUnsafe");
                        field.setAccessible(true);
                        return field;
                      } catch (NoSuchFieldException e) {
                        return null;
                      }
                    });

        if (theUnsafeField != null) {
          unsafe = theUnsafeField.get(null);

          // Get the getLong method from Unsafe
          getInt =
              AccessController.doPrivileged(
                  (PrivilegedAction<Method>)
                      () -> {
                        try {
                          return unsafeClass.getMethod("getLong", Object.class, long.class);
                        } catch (NoSuchMethodException e) {
                          return null;
                        }
                      });

          // Get the getObject method from Unsafe
          getObject =
              AccessController.doPrivileged(
                  (PrivilegedAction<Method>)
                      () -> {
                        try {
                          return unsafeClass.getMethod("objectFieldOffset", Field.class);
                        } catch (NoSuchMethodException e) {
                          return null;
                        }
                      });

          // Get the address field offset
          Field addressField =
              AccessController.doPrivileged(
                  (PrivilegedAction<Field>)
                      () -> {
                        try {
                          Field field = Buffer.class.getDeclaredField("address");
                          field.setAccessible(true);
                          return field;
                        } catch (NoSuchFieldException e) {
                          return null;
                        }
                      });

          if (addressField != null && getObject != null) {
            addressOffset = (long) getObject.invoke(unsafe, addressField);
            initialized = true;
          }
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to initialize UnsafeDirectBufferUtility: {}", e.getMessage());
    }

    UNSAFE = unsafe;
    UNSAFE_GET_LONG = getInt;
    UNSAFE_GET_OBJECT = getObject;
    ADDRESS_OFFSET = addressOffset;
    INITIALIZED = initialized;
  }

  /**
   * Checks if this utility is properly initialized and can access direct buffer addresses.
   *
   * @return true if initialized and can access addresses, false otherwise
   */
  public static boolean isInitialized() {
    return INITIALIZED;
  }

  /**
   * Gets the memory address of a direct ByteBuffer without requiring reflective access to the
   * protected Buffer.address field.
   *
   * @param buffer The ByteBuffer to get the address of (must be direct)
   * @return The memory address as a long
   * @throws IllegalArgumentException If the buffer is not direct or if the address cannot be
   *     obtained
   */
  public static long getDirectBufferAddress(ByteBuffer buffer) {
    Objects.requireNonNull(buffer, "Buffer cannot be null");

    if (!buffer.isDirect()) {
      throw new IllegalArgumentException("Not a direct buffer");
    }

    if (!INITIALIZED) {
      throw new IllegalStateException("UnsafeDirectBufferUtility is not initialized");
    }

    try {
      return (long) UNSAFE_GET_LONG.invoke(UNSAFE, buffer, ADDRESS_OFFSET);
    } catch (Exception e) {
      LOGGER.warn("Failed to get direct buffer address: {}", e.getMessage());
      throw new IllegalStateException("Could not access direct buffer address", e);
    }
  }
}
