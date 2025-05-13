package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;

/**
 * Bootstrap hook for Arrow memory initialization.
 *
 * <p>This class is statically initialized very early to ensure that memory utilities are properly
 * set up before Arrow classes that need them are loaded.
 */
public class ArrowBootstrapHook {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowBootstrapHook.class);

  // Trigger early initialization of all our utilities
  static {
    try {
      LOGGER.debug("Initializing Arrow bootstrap hook");

      // Initialize our memory handler first
      ArrowMemoryHandler.initialize();

      // Now initialize the redefiner that will attempt to patch loaded Arrow classes
      ArrowClassRedefiner.initialize();

      LOGGER.debug("Arrow bootstrap hook initialization completed");
    } catch (Throwable t) {
      LOGGER.error("Failed to initialize Arrow bootstrap hook", t);
    }
  }

  /** Forces loading of this class. */
  public static void initialize() {
    // No-op - just forces class initialization
  }
}
