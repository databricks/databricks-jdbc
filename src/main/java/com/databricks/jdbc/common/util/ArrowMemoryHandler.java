package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.nio.ByteBuffer;

/**
 * A utility class that provides a consistent interface for handling Arrow memory operations without
 * requiring the --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag.
 *
 * <p>This class is intended to be used by both our code and the shaded Arrow code through class
 * rewriting or reflection, providing a safe way to access direct buffer addresses.
 */
public class ArrowMemoryHandler {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowMemoryHandler.class);

  // Static initializer to ensure we're set up early
  static {
    initialize();
  }

  /** Initializes all the memory utilities needed for Arrow to work without JVM flags. */
  public static void initialize() {
    // Initialize our utilities
    ArrowMemoryInitializer.initialize();

    // Fix the internal Arrow memory utilities
    InternalArrowMemoryUtilFixer.apply();
  }

  /**
   * Gets the memory address of a direct ByteBuffer without requiring reflective access to protected
   * fields.
   *
   * @param buffer The buffer to get the address from (must be a direct byte buffer)
   * @return The memory address as a long
   * @throws IllegalArgumentException If the buffer is not a direct byte buffer or the address
   *     cannot be obtained
   */
  public static long getDirectBufferAddress(ByteBuffer buffer) {
    if (!buffer.isDirect()) {
      throw new IllegalArgumentException("Buffer must be direct");
    }

    try {
      return InternalArrowMemoryUtilFixer.getDirectBufferAddress(buffer);
    } catch (Exception e) {
      // Fall back to our other utilities
      try {
        return UnsafeAccessUtil.getBufferAddress(buffer);
      } catch (Exception e2) {
        if (UnsafeDirectBufferUtility.isInitialized()) {
          return UnsafeDirectBufferUtility.getDirectBufferAddress(buffer);
        }
        throw new IllegalArgumentException(
            "Could not access direct buffer address using any method", e2);
      }
    }
  }

  /**
   * Checks if direct buffer address access is available through any of our methods.
   *
   * @return true if we can access direct buffer addresses, false otherwise
   */
  public static boolean canAccessDirectBufferAddress() {
    // Create a small direct buffer for testing
    ByteBuffer buffer = null;
    try {
      buffer = ByteBuffer.allocateDirect(8);
      getDirectBufferAddress(buffer);
      return true;
    } catch (Exception e) {
      LOGGER.debug("Cannot access direct buffer address: {}", e.getMessage());
      return false;
    } finally {
      if (buffer != null) {
        try {
          // Try to clean up the direct buffer
          // This might fail if the cleaner isn't accessible either
          cleanDirectBuffer(buffer);
        } catch (Exception e) {
          // Ignore cleanup failures - this is just a test
        }
      }
    }
  }

  /**
   * Attempts to clean up a direct ByteBuffer to release native memory.
   *
   * @param buffer The direct ByteBuffer to clean
   */
  public static void cleanDirectBuffer(ByteBuffer buffer) {
    if (!buffer.isDirect()) {
      return;
    }

    try {
      // Try different methods to clean the buffer
      // JDK 9+ method first
      try {
        // Use cleaner() method if available
        Object cleaner = buffer.getClass().getMethod("cleaner").invoke(buffer);
        if (cleaner != null) {
          cleaner.getClass().getMethod("clean").invoke(cleaner);
          return;
        }
      } catch (Exception e) {
        // Fall through to next approach
      }

      // Try sun.misc.Cleaner for older JDKs
      try {
        Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
        java.lang.reflect.Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
        theUnsafeField.setAccessible(true);
        Object theUnsafe = theUnsafeField.get(null);
        java.lang.reflect.Method invokeCleanerMethod =
            unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
        invokeCleanerMethod.invoke(theUnsafe, buffer);
      } catch (Exception e) {
        // Ignore - we've done our best
      }
    } catch (Exception e) {
      LOGGER.debug("Failed to clean direct buffer: {}", e.getMessage());
    }
  }
}
