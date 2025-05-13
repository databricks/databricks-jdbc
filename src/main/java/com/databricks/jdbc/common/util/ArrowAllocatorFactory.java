package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * Factory class for creating Arrow allocators that work without requiring the
 * --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag.
 */
public class ArrowAllocatorFactory {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowAllocatorFactory.class);

  /**
   * Creates a RootAllocator that works with or without the JVM flag by initializing our custom
   * utilities before Arrow tries to access Buffer.address.
   *
   * @param limit Memory limit for the allocator
   * @return A RootAllocator instance
   */
  public static BufferAllocator createAllocator(long limit) {
    try {
      // Make sure our utilities are initialized first, starting with the most aggressive option
      ArrowMemoryHook.initialize();
      ArrowMemoryHandler.initialize();

      // Now create the allocator which should use our workarounds if needed
      return new RootAllocator(limit);
    } catch (Exception e) {
      LOGGER.warn("Error initializing utilities before allocator creation: {}", e.getMessage());

      // Still try to create the allocator with our custom memory hook
      try {
        // Try again with our most aggressive solution
        ArrowMemoryHook.initialize();
        return new RootAllocator(limit);
      } catch (Exception e2) {
        LOGGER.warn("Second attempt to create allocator failed: {}", e2.getMessage());

        // Last resort - just create it directly
        return new RootAllocator(limit);
      }
    }
  }
}
