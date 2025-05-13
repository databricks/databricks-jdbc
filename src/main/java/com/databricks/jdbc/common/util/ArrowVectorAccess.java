package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.nio.ByteBuffer;

/**
 * Utility class for working with Arrow vectors and buffers without requiring the
 * --add-opens=java.base/java.nio=ALL-UNNAMED JVM flag.
 *
 * <p>This class provides safe methods for accessing Arrow vectors and their underlying memory.
 */
public class ArrowVectorAccess {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ArrowVectorAccess.class);

  /**
   * Safely access a direct buffer with a consumer function, ensuring proper setup of memory
   * utilities.
   *
   * @param buffer The direct buffer to access
   * @param accessor Consumer function that accesses the buffer
   * @param <T> The buffer type
   * @param <E> Exception type that might be thrown
   * @throws E if the accessor throws an exception
   */
  public static <T extends ByteBuffer, E extends Exception> void safelyAccessBuffer(
      T buffer, BufferAccessor<T, E> accessor) throws E {
    try {
      // Ensure memory utilities are initialized
      ArrowMemoryHook.initialize();

      // Now safely access the buffer
      accessor.access(buffer);
    } catch (RuntimeException e) {
      if (isMemoryAccessException(e)) {
        LOGGER.warn("Memory access exception during buffer access: {}", e.getMessage());
        throw new RuntimeException(
            "Unable to access direct buffer without JVM flag --add-opens=java.base/java.nio=ALL-UNNAMED",
            e);
      }
      throw e;
    }
  }

  /** Check if an exception is related to memory access issues. */
  private static boolean isMemoryAccessException(Throwable t) {
    Throwable cause = t;
    while (cause != null) {
      if (cause instanceof IllegalAccessException) {
        return true;
      }
      if (cause.getClass().getName().contains("InaccessibleObjectException")) {
        return true;
      }
      if (cause.getMessage() != null && cause.getMessage().contains("Buffer.address")) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  /**
   * Functional interface for accessing a buffer.
   *
   * @param <T> The buffer type
   * @param <E> Exception type that might be thrown
   */
  @FunctionalInterface
  public interface BufferAccessor<T, E extends Exception> {
    /**
     * Access the buffer.
     *
     * @param buffer The buffer to access
     * @throws E if an error occurs
     */
    void access(T buffer) throws E;
  }

  /**
   * Get the memory address of a direct buffer, using our safe utilities.
   *
   * @param buffer The direct buffer
   * @return The memory address
   */
  public static long getDirectBufferAddress(ByteBuffer buffer) {
    return MemoryUtilAccess.getDirectBufferAddress(buffer);
  }
}
