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
    try {
      // Initialize our core memory utilities first
      initializeAllMemoryUtilities();

      // Initialize class redefiner for dynamic patching
      ArrowClassRedefiner.initialize();

      // Initialize the internal fixer
      InternalArrowMemoryUtilFixer.apply();

      LOGGER.debug("Arrow memory handler successfully initialized");
    } catch (Exception e) {
      LOGGER.warn("Error initializing Arrow memory handler: {}", e.getMessage());
    }
  }

  /** Initialize all memory utilities. */
  private static void initializeAllMemoryUtilities() {
    // Initialize all of our utilities
    ArrowMemoryInitializer.initialize();

    // Explicitly initialize the MemoryUtilAccess class
    try {
      boolean canAccess = MemoryUtilAccess.canAccessDirectBuffer();
      LOGGER.debug("MemoryUtilAccess initialized, direct buffer access available: {}", canAccess);
    } catch (Throwable t) {
      LOGGER.debug("Error initializing MemoryUtilAccess: {}", t.getMessage());
    }
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
      // Use our comprehensive MemoryUtilAccess class
      return MemoryUtilAccess.getDirectBufferAddress(buffer);
    } catch (Exception e) {
      // Fall back to other methods if that fails
      try {
        return InternalArrowMemoryUtilFixer.getDirectBufferAddress(buffer);
      } catch (Exception e2) {
        try {
          return UnsafeAccessUtil.getBufferAddress(buffer);
        } catch (Exception e3) {
          if (UnsafeDirectBufferUtility.isInitialized()) {
            return UnsafeDirectBufferUtility.getDirectBufferAddress(buffer);
          }
          throw new IllegalArgumentException(
              "Could not access direct buffer address using any method", e3);
        }
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
