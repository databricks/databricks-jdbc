package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;

/**
 * Utility class to initialize Arrow memory access in a way that doesn't require the
 * --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag.
 *
 * <p>This class should be statically initialized at the earliest possible point before any Arrow
 * classes are loaded.
 */
public class ArrowMemoryInitializer {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ArrowMemoryInitializer.class);

  // Flag to track whether we've initialized our utilities
  private static volatile boolean initialized = false;

  /**
   * Initializes our utilities to handle Arrow's memory access without requiring JVM flags. This
   * method should be called as early as possible, before any Arrow classes are loaded.
   */
  public static void initialize() {
    if (initialized) {
      return;
    }

    synchronized (ArrowMemoryInitializer.class) {
      if (initialized) {
        return;
      }

      try {
        LOGGER.debug("Initializing safe Arrow memory access utilities");

        // Initialize our access utilities first
        UnsafeAccessUtil.hasDirectAddressAccess();
        UnsafeDirectBufferUtility.isInitialized();
        ArrowReaderProxy.isDirectAccessAvailable();

        // Initialize the internal fixer to handle shaded Arrow classes
        InternalArrowMemoryUtilFixer.apply();

        // Mark as initialized
        initialized = true;

        LOGGER.debug("Successfully initialized Arrow memory access utilities");
      } catch (Exception e) {
        LOGGER.error("Failed to initialize Arrow memory access utilities: {}", e.getMessage());
      }
    }
  }

  /**
   * Checks if our utilities have been initialized.
   *
   * @return true if initialized, false otherwise
   */
  public static boolean isInitialized() {
    return initialized;
  }
}
