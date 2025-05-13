package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.InputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

/**
 * A proxy class for safely creating and working with Arrow readers. This avoids the need for the
 * JVM flag --add-opens=java.base/java.nio=ALL-UNNAMED
 */
public class ArrowReaderProxy {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowReaderProxy.class);
  private static boolean directAccessAvailable = false;

  static {
    initializeEarly();
  }

  /** Initialize memory utilities early, before any Arrow classes are loaded. */
  private static void initializeEarly() {
    try {
      // Initialize our most aggressive solution first
      ArrowMemoryHook.initialize();

      // Then initialize our other utilities
      ArrowMemoryHandler.initialize();

      // Test if direct access is available
      directAccessAvailable = testDirectAccess();

      LOGGER.debug("ArrowReaderProxy initialized, direct access: {}", directAccessAvailable);
    } catch (Exception e) {
      LOGGER.error("Failed to initialize ArrowReaderProxy", e);
    }
  }

  /** Test if we can directly access Buffer.address. */
  private static boolean testDirectAccess() {
    try {
      return UnsafeAccessUtil.hasDirectAddressAccess()
          || UnsafeDirectBufferUtility.isInitialized()
          || MemoryUtilAccess.canAccessDirectBuffer();
    } catch (Exception e) {
      LOGGER.debug("Direct access test failed: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Creates a safe ArrowStreamReader that works with or without JVM flags.
   *
   * @param inputStream The input stream to read Arrow data from
   * @param allocator The allocator to use
   * @return A new ArrowStreamReader
   */
  public static ArrowStreamReader createReader(InputStream inputStream, BufferAllocator allocator) {
    try {
      // First initiate our memory hook to try and fix Arrow's internal MemoryUtil
      ArrowMemoryHook.initialize();

      return new ArrowStreamReader(inputStream, allocator);
    } catch (Exception e) {
      if (isMemoryAccessException(e)) {
        LOGGER.warn(
            "Memory access exception when creating ArrowStreamReader: {}. "
                + "Consider using '--add-opens=java.base/java.nio=ALL-UNNAMED' JVM flag.",
            e.getMessage());
      }

      // Rethrow as RuntimeException to maintain compatible method signature
      throw new RuntimeException("Failed to create ArrowStreamReader", e);
    }
  }

  /** Check if an exception is related to memory access. */
  private static boolean isMemoryAccessException(Throwable t) {
    Throwable cause = t;
    while (cause != null) {
      if (cause.getClass().getName().equals("java.lang.reflect.InaccessibleObjectException")
          || cause.getMessage() != null && cause.getMessage().contains("Buffer.address")) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  /**
   * Check if direct Buffer access is available.
   *
   * @return true if direct access is available, false otherwise
   */
  public static boolean isDirectAccessAvailable() {
    return directAccessAvailable;
  }
}
