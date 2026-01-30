package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DatabricksBufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * Creates {@link BufferAllocator} instances.
 *
 * <p>First tries to create a {@link RootAllocator} which uses off-heap memory and is faster. If
 * that fails (usually due to JVM reflection restrictions), falls back to {@link
 * DatabricksBufferAllocator} which uses heap memory.
 */
public class ArrowBufferAllocator {
  /** Should the RootAllocator be used. */
  private static final boolean canUseRootAllocator;

  /** Logger instance. */
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowBufferAllocator.class);

  /* Check if the RootAllocator can be used. */
  static {
    canUseRootAllocator = canUseRootAllocator();
  }

  /**
   * @return true iff the RootAllocator can be used.
   */
  static boolean canUseRootAllocator() {
    RootAllocator rootAllocator = null;
    ArrowBuf buffer = null;
    boolean canWriteWithRootAllocator = false;

    try {
      rootAllocator = new RootAllocator();
      buffer = rootAllocator.buffer(64);
      buffer.writeByte(0);
      canWriteWithRootAllocator = true;
    } catch (Throwable t) {
      String message = t.getMessage();
      if (message == null) {
        message = t.getCause() != null ? t.getCause().getMessage() : "";
      }
      LOGGER.info(
          "Failed to create RootAllocator, will use DatabricksBufferAllocator as fallback: "
              + message);
    }

    if (rootAllocator != null) {
      try {
        if (buffer != null) {
          buffer.close();
        }
        rootAllocator.close();
      } catch (Throwable t) {
        LOGGER.warn("RootAllocator could not be closed: " + t.getMessage());
      }
    }

    return canWriteWithRootAllocator;
  }

  /**
   * @return an instance of the {@code BufferAllocator}.
   */
  public static BufferAllocator getBufferAllocator() {
    if (canUseRootAllocator) {
      return new RootAllocator();
    } else {
      return new DatabricksBufferAllocator();
    }
  }

  /**
   * @return true iff the patched Databricks allocator is being used.
   */
  public static boolean isUsingPatchedAllocator() {
    return !canUseRootAllocator;
  }
}
